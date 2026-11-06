//! Sarvam Bulbul TTS over one WebSocket per call, reused across turns
//! (the connection is billed on open, so it is opened lazily by the agent).
//!
//! Protocol facts (verified live in the Go server):
//! * `send_completion_event=true` is REQUIRED in the URL, else Sarvam accepts
//!   everything and silently emits zero audio.
//! * TTS needs a ~20s `{"type":"ping"}` keepalive (STT must never get one).
//! * `request_id` on audio is a session id, not per request. Audio and `final`
//!   events arrive strictly in send order, so a FIFO of outstanding requests
//!   attributes each chunk to a generation.
//! * Whitespace-only text is rejected with an error INSTEAD of a `final`;
//!   the FIFO must pop on error too or that generation's Done never fires.
//! * There is no "stop" message: a barge-in closes the whole connection.
//!
//! Events (audio, done, closed) travel on ONE channel, so a generation's audio
//! is always delivered before its Done.

use super::{WsStream, urlenc, ws_connect};
use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as Ws;
use tokio_util::sync::CancellationToken;

const BASE_URL: &str = "wss://api.sarvam.ai/text-to-speech/ws";
const KEEPALIVE: Duration = Duration::from_secs(20);
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct TtsConfig {
    pub ws_url: String,
    pub api_key: String,
    pub model: String,
    pub voice: String,
    pub language: String,
}

#[derive(Debug)]
pub enum TtsEvent {
    /// Decoded 16kHz s16le PCM, not frame aligned. `req`/`text` name the
    /// Speak call (one sentence) it belongs to, to know how far playback got.
    Audio { pcm: Vec<u8>, generation: u64, req: u64, text: String },
    /// Every Speak(generation) call's audio has fully arrived.
    Done(u64),
    /// The connection is gone (provider closed it, or we did).
    Closed,
}

#[derive(Clone)]
struct Pending {
    generation: u64,
    req: u64,
    text: String,
    /// A recorded clip (`play`): never sent to Sarvam, its audio goes out as soon
    /// as every request before it has finished.
    pcm: Option<Arc<Vec<u8>>>,
}

#[derive(Default)]
struct Book {
    pending: Vec<Pending>, // FIFO, one per outstanding Speak, in send order
    ended: HashSet<u64>,   // EndGeneration called
    last: Option<Pending>, // most recent request, for audio arriving between requests
    next_req: u64,
    closed: bool,
}

impl Book {
    fn has_pending(&self, generation: u64) -> bool {
        self.pending.iter().any(|p| p.generation == generation)
    }
    /// Pops the oldest request (final or error). Returns its text and the events
    /// that follow: Done if that finished its generation, then any recorded clips
    /// queued behind it, which play now.
    fn on_final(&mut self) -> (String, Vec<TtsEvent>) {
        if self.pending.is_empty() {
            return (String::new(), vec![]);
        }
        let p = self.pending.remove(0);
        let mut evs = vec![];
        if self.ended.contains(&p.generation) && !self.has_pending(p.generation) {
            evs.push(TtsEvent::Done(p.generation));
        }
        evs.extend(self.drain_clips());
        (p.text, evs)
    }

    /// Emits every recorded clip at the head of `pending`: once the requests
    /// before one have finished, its audio is next in line.
    fn drain_clips(&mut self) -> Vec<TtsEvent> {
        let mut evs = vec![];
        while self.pending.first().is_some_and(|p| p.pcm.is_some()) {
            let p = self.pending.remove(0);
            evs.push(TtsEvent::Audio {
                pcm: p.pcm.as_deref().cloned().unwrap_or_default(),
                generation: p.generation,
                req: p.req,
                text: p.text,
            });
            if self.ended.contains(&p.generation) && !self.has_pending(p.generation) {
                evs.push(TtsEvent::Done(p.generation));
            }
        }
        evs
    }
    fn attribute(&self) -> Option<Pending> {
        self.pending.first().or(self.last.as_ref()).cloned()
    }
}

pub struct TtsClient {
    book: Arc<Mutex<Book>>,
    out: mpsc::UnboundedSender<String>, // messages for the writer task
    events: mpsc::UnboundedSender<TtsEvent>,
    cancel: CancellationToken,
}

impl TtsClient {
    pub async fn connect(call_id: &str, cfg: TtsConfig, parent: &CancellationToken) -> Result<(Self, mpsc::UnboundedReceiver<TtsEvent>)> {
        let base = if cfg.ws_url.is_empty() { BASE_URL } else { &cfg.ws_url };
        let url = format!("{base}?model={}&send_completion_event=true", urlenc(&cfg.model));
        let (ws, rid) = ws_connect(&url, "api-subscription-key", &cfg.api_key, DIAL_TIMEOUT).await.context("tts: connect")?;
        tracing::info!(call_id, event = "tts.connected", request_id = rid.as_deref().unwrap_or(""), "tts connected");

        let cancel = parent.child_token();
        let book = Arc::new(Mutex::new(Book { next_req: 0, ..Default::default() }));
        let (out_tx, out_rx) = mpsc::unbounded_channel();
        let (ev_tx, ev_rx) = mpsc::unbounded_channel();

        // Config goes first; everything else follows on the same ordered queue.
        let mut speech = serde_json::json!({
            "target_language_code": cfg.language,
            "speaker": cfg.voice,
            "speech_sample_rate": "16000",
            "enable_preprocessing": false,
            "output_audio_codec": "linear16",
            "pace": 1.0,
            "model": cfg.model,
        });
        // Bulbul v4 always preprocesses and can't switch it off.
        if cfg.model.starts_with("bulbul:v4") {
            speech.as_object_mut().unwrap().remove("enable_preprocessing");
        }
        let _ = out_tx.send(serde_json::json!({"type": "config", "data": speech}).to_string());

        tokio::spawn(run(call_id.to_string(), ws, out_rx, book.clone(), ev_tx.clone(), cancel.clone()));
        Ok((Self { book, out: out_tx, events: ev_tx, cancel }, ev_rx))
    }

    /// Queues `text` for synthesis, tagged `generation`. Whitespace-only text is a no-op.
    pub fn speak(&self, text: &str, generation: u64) -> Result<()> {
        if text.trim().is_empty() {
            return Ok(());
        }
        {
            let mut b = self.book.lock().unwrap();
            if b.closed {
                anyhow::bail!("tts: client closed");
            }
            b.next_req += 1;
            let p = Pending { generation, req: b.next_req, text: text.to_string(), pcm: None };
            b.last = Some(p.clone());
            b.pending.push(p);
        }
        let send = |v: serde_json::Value| self.out.send(v.to_string()).map_err(|_| anyhow::anyhow!("tts: connection closed"));
        send(serde_json::json!({"type": "text", "data": {"text": text}}))?;
        send(serde_json::json!({"type": "flush"}))
    }

    /// Queues a recorded clip (16kHz s16le PCM) in `speak`'s place: delivered as
    /// audio tagged `generation` and `text`, in order after every earlier
    /// speak/play call's audio, and counted toward the generation's Done.
    /// Nothing is sent to Sarvam.
    pub fn play(&self, pcm: Arc<Vec<u8>>, text: &str, generation: u64) -> Result<()> {
        if pcm.is_empty() {
            return Ok(());
        }
        let evs = {
            let mut b = self.book.lock().unwrap();
            if b.closed {
                anyhow::bail!("tts: client closed");
            }
            b.next_req += 1;
            let p = Pending { generation, req: b.next_req, text: text.to_string(), pcm: Some(pcm) };
            b.pending.push(p);
            b.drain_clips()
        };
        for e in evs {
            let _ = self.events.send(e);
        }
        Ok(())
    }

    /// No more `speak(generation)` calls are coming. Done fires once their audio has
    /// all arrived -- immediately if none was ever pending.
    pub fn end_generation(&self, generation: u64) {
        let mut b = self.book.lock().unwrap();
        b.ended.insert(generation);
        if !b.has_pending(generation) && !b.closed {
            let _ = self.events.send(TtsEvent::Done(generation));
        }
    }

    /// Barge-in cut #3: drops the connection and any in-flight audio.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

impl Drop for TtsClient {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[derive(Deserialize)]
struct Inbound {
    #[serde(default, rename = "type")]
    kind: String,
    #[serde(default)]
    data: InboundData,
}
#[derive(Deserialize, Default)]
struct InboundData {
    #[serde(default)]
    audio: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    event_type: String,
}

async fn run(
    call_id: String,
    ws: WsStream,
    mut out: mpsc::UnboundedReceiver<String>,
    book: Arc<Mutex<Book>>,
    events: mpsc::UnboundedSender<TtsEvent>,
    cancel: CancellationToken,
) {
    let (mut sink, mut rd) = ws.split();
    let mut ping = tokio::time::interval(KEEPALIVE);
    ping.tick().await; // first tick is immediate

    let self_closed = loop {
        tokio::select! {
            _ = cancel.cancelled() => break true,
            _ = ping.tick() => {
                if sink.send(Ws::text(r#"{"type":"ping"}"#)).await.is_err() { break false; }
            }
            m = out.recv() => {
                let Some(m) = m else { break true };
                if sink.send(Ws::text(m)).await.is_err() { break false; }
            }
            msg = rd.next() => match msg {
                Some(Ok(Ws::Text(t))) => handle(&call_id, t.as_str(), &book, &events),
                Some(Ok(Ws::Close(_))) | None => break false,
                Some(Ok(_)) => {}
                Some(Err(e)) => { tracing::error!(call_id = %call_id, error = %e, "tts websocket error"); break false; }
            }
        }
    };
    if !self_closed {
        // A silent exit made "Sarvam stopped responding" and "we closed it"
        // indistinguishable in production: log loudly only if not ours.
        tracing::error!(call_id = %call_id, "tts websocket closed by provider");
    }
    book.lock().unwrap().closed = true;
    let _ = events.send(TtsEvent::Closed);
    let _ = sink.close().await;
}

fn handle(call_id: &str, text: &str, book: &Mutex<Book>, events: &mpsc::UnboundedSender<TtsEvent>) {
    let Ok(m) = serde_json::from_str::<Inbound>(text) else { return };
    match (m.kind.as_str(), m.data.event_type.as_str()) {
        ("error", _) => {
            // Sarvam sends error INSTEAD of final: popping resolves the slot
            // and names exactly the sentence that was rejected.
            let (rejected, evs) = book.lock().unwrap().on_final();
            tracing::warn!(call_id, message = %m.data.message, rejected_text = %truncate(&rejected, 80), "tts provider error");
            for e in evs {
                let _ = events.send(e);
            }
        }
        ("event", "final") => {
            let evs = book.lock().unwrap().on_final().1;
            for e in evs {
                let _ = events.send(e);
            }
        }
        ("audio", _) if !m.data.audio.is_empty() => {
            let Ok(pcm) = STANDARD.decode(m.data.audio) else { return };
            let p = book.lock().unwrap().attribute();
            let (generation, req, text) = p.map(|p| (p.generation, p.req, p.text)).unwrap_or_default();
            let _ = events.send(TtsEvent::Audio { pcm, generation, req, text });
        }
        _ => {}
    }
}

fn truncate(s: &str, max: usize) -> String {
    let mut it = s.chars();
    let head: String = it.by_ref().take(max).collect();
    if it.next().is_some() { head + "…" } else { head }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed(book: &Mutex<Book>, tx: &mpsc::UnboundedSender<TtsEvent>, json: &str) {
        handle("t", json, book, tx);
    }
    fn speak(book: &Mutex<Book>, generation: u64, text: &str) {
        let mut b = book.lock().unwrap();
        b.next_req += 1;
        let p = Pending { generation, req: b.next_req, text: text.into(), pcm: None };
        b.last = Some(p.clone());
        b.pending.push(p);
    }

    #[test]
    fn audio_is_attributed_fifo_and_done_fires_after_the_last_final() {
        let book = Mutex::new(Book::default());
        let (tx, mut rx) = mpsc::unbounded_channel();
        speak(&book, 7, "one.");
        speak(&book, 7, "two.");
        book.lock().unwrap().ended.insert(7);

        let audio = format!(r#"{{"type":"audio","data":{{"audio":"{}"}}}}"#, STANDARD.encode([1u8, 2, 3, 4]));
        feed(&book, &tx, &audio);
        feed(&book, &tx, r#"{"type":"event","data":{"event_type":"final"}}"#);
        feed(&book, &tx, &audio);
        // an error (rejected whitespace) resolves the slot exactly like a final
        feed(&book, &tx, r#"{"type":"error","data":{"message":"400: 'text' cannot be empty"}}"#);

        let mut got = vec![];
        while let Ok(ev) = rx.try_recv() {
            got.push(ev);
        }
        assert_eq!(got.len(), 3);
        assert!(matches!(&got[0], TtsEvent::Audio { generation: 7, req: 1, text, .. } if text == "one."));
        assert!(matches!(&got[1], TtsEvent::Audio { generation: 7, req: 2, text, .. } if text == "two."));
        assert!(matches!(got[2], TtsEvent::Done(7)));
    }

    #[test]
    fn a_recorded_clip_plays_after_the_requests_before_it_and_counts_toward_done() {
        let book = Mutex::new(Book::default());
        let (tx, mut rx) = mpsc::unbounded_channel();
        speak(&book, 4, "intro.");
        {
            // play(): a clip queued behind "intro." waits its turn.
            let mut b = book.lock().unwrap();
            b.next_req += 1;
            let p = Pending { generation: 4, req: b.next_req, text: "● clip1 hello ".into(), pcm: Some(Arc::new(vec![9, 9])) };
            b.pending.push(p);
            b.ended.insert(4);
            assert!(b.drain_clips().is_empty(), "the clip must wait for the request before it");
        }
        feed(&book, &tx, r#"{"type":"event","data":{"event_type":"final"}}"#);
        let evs: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        assert!(
            matches!(&evs[0], TtsEvent::Audio { pcm, generation: 4, req: 2, text } if pcm == &[9, 9] && text == "● clip1 hello "),
            "{evs:?}"
        );
        assert!(matches!(evs[1], TtsEvent::Done(4)), "Done follows the clip, the last thing of the generation: {evs:?}");
        assert_eq!(evs.len(), 2);
    }

    #[test]
    fn done_waits_for_end_generation() {
        let book = Mutex::new(Book::default());
        let (tx, mut rx) = mpsc::unbounded_channel();
        speak(&book, 1, "x");
        feed(&book, &tx, r#"{"type":"event","data":{"event_type":"final"}}"#);
        assert!(rx.try_recv().is_err()); // LLM may still produce more text for generation 1
        assert_eq!(truncate("abcdef", 3), "abc…");
    }
}

//! Sarvam saaras realtime STT over WebSocket (protocol verified live in Go).
//!
//! * Audio goes as JSON: `{"audio":{"data":<b64 pcm>,"sample_rate":16000,"encoding":"audio/wav"}}`.
//! * NO keepalive: Sarvam closes the socket on any message without an "audio"
//!   field, a ping included. An idle socket survives fine.
//! * Failures arrive in `data.message` (sometimes `data.error`); log both.
//! * `vad_signals=true`: START_SPEECH/END_SPEECH come out on the SAME channel
//!   as transcripts, in order (START -> END -> transcript); a second channel
//!   would let a select reorder them. They drive the caller-silence timer
//!   only -- barge-in stays local (Earshot).
//! * Reconnect is lazy and one-shot with backoff, never a retry loop: a failed
//!   send drops that frame and the next frame triggers one redial.

use super::{WsStream, urlenc, ws_connect};
use crate::media::Frame;
use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt, stream::SplitStream};
use serde::Deserialize;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as Ws;
use tokio_util::sync::CancellationToken;

const BASE_URL: &str = "wss://api.sarvam.ai/speech-to-text/ws";
const SAMPLE_RATE: u32 = 16000;
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
const BACKOFF_MIN: Duration = Duration::from_millis(200);
const BACKOFF_MAX: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct SttConfig {
    pub ws_url: String, // "" = Sarvam production
    pub api_key: String,
    pub model: String,
    pub language: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SttEvent {
    SpeechStarted,
    SpeechEnded,
    Final(String),
}

enum Cmd {
    Audio(Frame),
}

#[derive(Clone)]
pub struct SttClient {
    tx: mpsc::Sender<Cmd>,
}

impl SttClient {
    /// Connects (failing if Sarvam is unreachable) and starts streaming results.
    /// The connection closes when `cancel` fires.
    pub async fn connect(call_id: &str, cfg: SttConfig, cancel: CancellationToken) -> Result<(Self, mpsc::UnboundedReceiver<SttEvent>)> {
        let url = build_url(&cfg);
        let (ws, rid) = ws_connect(&url, "Api-Subscription-Key", &cfg.api_key, DIAL_TIMEOUT).await.context("stt: connect")?;
        tracing::info!(call_id, event = "stt.connected", request_id = rid.as_deref().unwrap_or(""), "stt connected");

        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let (ev_tx, ev_rx) = mpsc::unbounded_channel();
        tokio::spawn(run(call_id.to_string(), url, cfg.api_key, ws, cmd_rx, ev_tx, cancel));
        Ok((Self { tx: cmd_tx }, ev_rx))
    }

    /// Never blocks: a full queue (provider slow or down) drops the frame.
    pub fn feed(&self, frame: &Frame) -> bool {
        self.tx.try_send(Cmd::Audio(*frame)).is_ok()
    }
}

fn build_url(cfg: &SttConfig) -> String {
    let base = if cfg.ws_url.is_empty() { BASE_URL } else { &cfg.ws_url };
    let lang = if cfg.language.is_empty() { "unknown" } else { &cfg.language };
    format!("{base}?model={}&language-code={}&mode=transcribe&sample_rate={SAMPLE_RATE}&vad_signals=true", urlenc(&cfg.model), urlenc(lang))
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
    transcript: String,
    #[serde(default)]
    signal_type: String,
    #[serde(default)]
    error: String,
    #[serde(default)]
    message: String,
}

pub(crate) fn parse(text: &str, call_id: &str) -> Option<SttEvent> {
    let m: Inbound = serde_json::from_str(text).ok()?;
    if !m.data.error.is_empty() {
        tracing::warn!(call_id, error = %m.data.error, "stt provider error");
    }
    if !m.data.message.is_empty() {
        tracing::warn!(call_id, message = %m.data.message, "stt provider error");
    }
    match (m.kind.as_str(), m.data.signal_type.as_str()) {
        ("events", "START_SPEECH") => Some(SttEvent::SpeechStarted),
        ("events", "END_SPEECH") => Some(SttEvent::SpeechEnded),
        ("data", _) if !m.data.transcript.is_empty() => Some(SttEvent::Final(m.data.transcript)),
        _ => None,
    }
}

type Sink = futures_util::stream::SplitSink<WsStream, Ws>;

async fn next_msg(rd: &mut Option<SplitStream<WsStream>>) -> Option<Result<Ws, tokio_tungstenite::tungstenite::Error>> {
    match rd {
        Some(r) => r.next().await,
        None => std::future::pending().await,
    }
}

async fn run(
    call_id: String,
    url: String,
    key: String,
    ws: WsStream,
    mut cmds: mpsc::Receiver<Cmd>,
    events: mpsc::UnboundedSender<SttEvent>,
    cancel: CancellationToken,
) {
    let (sink, rd) = ws.split();
    let (mut sink, mut rd): (Option<Sink>, Option<SplitStream<WsStream>>) = (Some(sink), Some(rd));
    let (mut failures, mut next_dial) = (0u32, Instant::now());

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            msg = next_msg(&mut rd) => match msg {
                Some(Ok(Ws::Text(t))) => {
                    if let Some(ev) = parse(t.as_str(), &call_id) && events.send(ev).is_err() { break; }
                }
                Some(Ok(_)) => {}
                // Closed or errored: forget the connection; the next frame redials.
                _ => { sink = None; rd = None; }
            },
            cmd = cmds.recv() => {
                let Some(cmd) = cmd else { break };
                if sink.is_none() {
                    // One redial per backoff window; frames meanwhile are dropped.
                    if Instant::now() < next_dial { continue; }
                    match ws_connect(&url, "Api-Subscription-Key", &key, DIAL_TIMEOUT).await {
                        Ok((ws, _)) => {
                            let (s, r) = ws.split();
                            (sink, rd, failures) = (Some(s), Some(r), 0);
                            tracing::info!(call_id = %call_id, "stt reconnected");
                        }
                        Err(e) => {
                            failures += 1;
                            let wait = (BACKOFF_MIN * 2u32.saturating_pow(failures - 1)).min(BACKOFF_MAX);
                            next_dial = Instant::now() + wait;
                            tracing::warn!(call_id = %call_id, error = %e, attempt = failures, next_in_ms = wait.as_millis() as u64, "stt reconnect failed");
                            continue;
                        }
                    }
                }
                let Cmd::Audio(f) = cmd;
                let payload = serde_json::json!({"audio": {
                    "data": STANDARD.encode(f), "sample_rate": SAMPLE_RATE, "encoding": "audio/wav"}});
                if let Some(s) = sink.as_mut() && s.send(Ws::text(payload.to_string())).await.is_err() {
                    (sink, rd) = (None, None); // drop this frame; the next one redials
                }
            }
        }
    }
    if let Some(mut s) = sink {
        let _ = s.close().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_signals_transcripts_and_ignores_the_rest() {
        let p = |s| parse(s, "t");
        assert_eq!(p(r#"{"type":"events","data":{"signal_type":"START_SPEECH"}}"#), Some(SttEvent::SpeechStarted));
        assert_eq!(p(r#"{"type":"events","data":{"signal_type":"END_SPEECH"}}"#), Some(SttEvent::SpeechEnded));
        assert_eq!(p(r#"{"type":"data","data":{"transcript":"नमस्ते"}}"#), Some(SttEvent::Final("नमस्ते".into())));
        assert_eq!(p(r#"{"type":"data","data":{"transcript":""}}"#), None);
        assert_eq!(p(r#"{"type":"error","data":{"message":"boom"}}"#), None);
        assert_eq!(p("garbage"), None);
    }

    #[test]
    fn url_carries_model_language_and_vad_signals() {
        let u = build_url(&SttConfig { ws_url: String::new(), api_key: "k".into(), model: "saaras:v4".into(), language: "hi-IN".into() });
        assert_eq!(
            u,
            "wss://api.sarvam.ai/speech-to-text/ws?model=saaras%3Av4&language-code=hi-IN&mode=transcribe&sample_rate=16000&vad_signals=true"
        );
    }
}

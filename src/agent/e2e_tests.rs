//! End-to-end tests of the agent task against mock Sarvam STT/TTS WebSocket
//! servers and a mock OpenAI-compatible LLM: no network, real protocol.

use super::{
    handler::{AgentConfig, AgentIo, run},
    vad::VoteDetector,
    workflow::{Node, Workflow},
};
use crate::{
    ai::{llm::LlmClient, stt::{SttClient, SttConfig}, tts::TtsConfig},
    media::{FRAME_SIZE, Frame, SILENCE, call::{MediaIo, MediaStats}},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use std::{
    sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering::SeqCst}},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};
use tokio_tungstenite::tungstenite::Message as Ws;
use tokio_util::sync::CancellationToken;

const TONE: i16 = 1000;

fn tone_b64(frames: usize) -> String {
    let mut pcm = Vec::with_capacity(frames * FRAME_SIZE);
    for _ in 0..frames * FRAME_SIZE / 2 {
        pcm.extend_from_slice(&TONE.to_le_bytes());
    }
    STANDARD.encode(pcm)
}

#[derive(Default)]
struct TtsLog {
    texts: Mutex<Vec<String>>,
    connections: AtomicUsize,
    closed: AtomicUsize,
}

/// Mock Sarvam TTS: every `text`+`flush` is answered with `frames` frames of
/// tone, then a `final` event.
async fn mock_tts(frames: usize) -> (String, Arc<TtsLog>) {
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/ws", lis.local_addr().unwrap());
    let log = Arc::new(TtsLog::default());
    let l = log.clone();
    tokio::spawn(async move {
        loop {
            let (s, _) = lis.accept().await.unwrap();
            let l = l.clone();
            tokio::spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(s).await.unwrap();
                l.connections.fetch_add(1, SeqCst);
                let mut pending = String::new();
                while let Some(Ok(m)) = ws.next().await {
                    let Ws::Text(t) = m else { continue };
                    let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
                    match v["type"].as_str() {
                        Some("text") => pending = v["data"]["text"].as_str().unwrap().to_string(),
                        Some("flush") => {
                            l.texts.lock().unwrap().push(std::mem::take(&mut pending));
                            let audio = format!(r#"{{"type":"audio","data":{{"audio":"{}"}}}}"#, tone_b64(frames));
                            let _ = ws.send(Ws::text(audio)).await;
                            let _ = ws.send(Ws::text(r#"{"type":"event","data":{"event_type":"final"}}"#)).await;
                        }
                        _ => {}
                    }
                }
                l.closed.fetch_add(1, SeqCst);
            });
        }
    });
    (url, log)
}

/// Mock Sarvam STT: counts audio messages, sends whatever the test pushes.
async fn mock_stt() -> (String, mpsc::UnboundedSender<String>, Arc<AtomicUsize>) {
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/ws", lis.local_addr().unwrap());
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let audio = Arc::new(AtomicUsize::new(0));
    let a = audio.clone();
    tokio::spawn(async move {
        let (s, _) = lis.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(s).await.unwrap();
        loop {
            tokio::select! {
                m = ws.next() => match m {
                    Some(Ok(Ws::Text(t))) => {
                        if t.as_str().contains("\"audio\"") { a.fetch_add(1, SeqCst); }
                    }
                    Some(Ok(_)) => {}
                    _ => break,
                },
                Some(out) = rx.recv() => { let _ = ws.send(Ws::text(out)).await; }
            }
        }
    });
    (url, tx, audio)
}

/// Mock LLM: answers each request with `reply(request_body)` as SSE.
async fn mock_llm(reply: impl Fn(&serde_json::Value) -> String + Send + Sync + 'static) -> (String, Arc<Mutex<Vec<serde_json::Value>>>) {
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", lis.local_addr().unwrap());
    let reqs = Arc::new(Mutex::new(vec![]));
    let r = reqs.clone();
    let reply = Arc::new(reply);
    tokio::spawn(async move {
        loop {
            let (mut s, _) = lis.accept().await.unwrap();
            let (r, reply) = (r.clone(), reply.clone());
            tokio::spawn(async move {
                let mut buf = vec![];
                let body = loop {
                    let mut chunk = [0u8; 8192];
                    let n = s.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 { return; }
                    buf.extend_from_slice(&chunk[..n]);
                    let text = String::from_utf8_lossy(&buf).to_string();
                    if let Some((head, body)) = text.split_once("\r\n\r\n") {
                        let len = head.to_lowercase().split("content-length:").nth(1).and_then(|v| v.split("\r\n").next()?.trim().parse::<usize>().ok()).unwrap_or(0);
                        if body.len() >= len { break serde_json::from_str::<serde_json::Value>(&body[..len]).unwrap(); }
                    }
                };
                let sse = reply(&body);
                r.lock().unwrap().push(body);
                let head = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", sse.len());
                let _ = s.write_all(head.as_bytes()).await;
                let _ = s.write_all(sse.as_bytes()).await;
            });
        }
    });
    (url, reqs)
}

fn sse(text: &str) -> String {
    format!("data: {}\n\ndata: [DONE]\n\n", serde_json::json!({"choices":[{"delta":{"content":text}}]}))
}

struct Rig {
    inbound: mpsc::Sender<Frame>,
    outbound: mpsc::Receiver<Frame>,
    stt_tx: mpsc::UnboundedSender<String>,
    stt_audio: Arc<AtomicUsize>,
    tts: Arc<TtsLog>,
    llm_reqs: Arc<Mutex<Vec<serde_json::Value>>>,
    cancel: CancellationToken,
    hangups: Arc<AtomicUsize>,
    done: tokio::task::JoinHandle<super::handler::Summary>,
}

async fn rig(tts_frames: usize, allow_interrupt: bool, llm_text: &'static str) -> Rig {
    let (tts_url, tts) = mock_tts(tts_frames).await;
    let (stt_url, stt_tx, stt_audio) = mock_stt().await;
    let (llm_url, llm_reqs) = mock_llm(move |_| sse(llm_text)).await;
    let cancel = CancellationToken::new();

    let (stt, stt_events) = SttClient::connect(
        "t",
        SttConfig { ws_url: stt_url, api_key: "k".into(), model: "m".into(), language: "hi-IN".into() },
        cancel.clone(),
    )
    .await
    .unwrap();

    let (in_tx, in_rx) = mpsc::channel(5);
    let (out_tx, out_rx) = mpsc::channel(5);
    let media = MediaIo { inbound: in_rx, outbound: out_tx, closed: CancellationToken::new(), stats: Arc::new(MediaStats::default()) };
    let hangups = Arc::new(AtomicUsize::new(0));
    let h = hangups.clone();

    let wf = Workflow {
        start: 0,
        nodes: vec![Node { name: "start".into(), prompt: "You are an agent.".into(), allow_interrupt, ..Default::default() }],
    };
    let cfg = AgentConfig {
        call_id: "t".into(),
        workflow: Arc::new(wf),
        llm: LlmClient::new(&llm_url, "k", "m"),
        tts: TtsConfig { ws_url: tts_url, api_key: "k".into(), model: "bulbul:v3".into(), voice: "v".into(), language: "hi-IN".into() },
        // Fake hop verdict: loud samples are speech; tone (1000) is below.
        vad: Box::new(VoteDetector::new(Box::new(|h| h.iter().all(|s| s.abs() > 5000)))),
        barge_in_enabled: true,
        barge_in_min_speech: Duration::ZERO,
        barge_in_guard: Duration::ZERO,
        post_cut_silence: Duration::from_millis(100),
        ack_filter: true,
        ack_end_margin: Duration::from_millis(300),
        idle_timeout: Duration::ZERO,
        max_duration: Duration::ZERO,
        llm_max_failed_turns: 3,
        llm_fallback_text: String::new(),
        hangup: Arc::new(move || { h.fetch_add(1, SeqCst); }),
    };
    let done = tokio::spawn(run(cfg, AgentIo { media, stt, stt_events }, cancel.clone()));
    Rig { inbound: in_tx, outbound: out_rx, stt_tx, stt_audio, tts, llm_reqs, cancel, hangups, done }
}

fn is_tone(f: &Frame) -> bool {
    f[..2] == TONE.to_le_bytes()
}

/// Drains outbound until `n` tone frames were seen (or `max` elapsed).
async fn collect_tone(r: &mut Rig, n: usize, max: Duration) -> usize {
    let mut seen = 0;
    let _ = tokio::time::timeout(max, async {
        while let Some(f) = r.outbound.recv().await {
            if is_tone(&f) {
                seen += 1;
                if seen >= n { break; }
            }
        }
    })
    .await;
    seen
}

fn loud() -> Frame {
    let mut f = SILENCE;
    for c in f.chunks_exact_mut(2) {
        c.copy_from_slice(&9000i16.to_le_bytes());
    }
    f
}

#[tokio::test]
async fn llm_opening_is_spoken_in_full_then_agent_listens() {
    let mut r = rig(10, true, "Hello caller. How are you?").await;
    // Two sentences -> two TTS requests of 10 frames each.
    let seen = collect_tone(&mut r, 20, Duration::from_secs(5)).await;
    assert_eq!(seen, 20, "both sentences must play in full");
    assert_eq!(r.tts.connections.load(SeqCst), 1, "one TTS connection reused");
    assert_eq!(*r.tts.texts.lock().unwrap(), ["Hello caller.", " How are you?"]);
    assert!(r.stt_audio.load(SeqCst) > 0, "caller audio is fed to the STT continuously");

    // After playout the reply is in history: a caller turn's request carries it.
    tokio::time::sleep(Duration::from_millis(100)).await;
    r.stt_tx.send(r#"{"type":"data","data":{"transcript":"mujhe madad chahiye"}}"#.into()).unwrap();
    let _ = collect_tone(&mut r, 20, Duration::from_secs(5)).await;
    let reqs = r.llm_reqs.lock().unwrap().clone();
    assert_eq!(reqs.len(), 2);
    let msgs = reqs[1]["messages"].as_array().unwrap();
    let roles: Vec<_> = msgs.iter().map(|m| m["role"].as_str().unwrap()).collect();
    assert_eq!(roles, ["system", "assistant", "user"]);
    assert_eq!(msgs[1]["content"], "Hello caller. How are you?");
    assert_eq!(r.hangups.load(SeqCst), 0);

    r.cancel.cancel();
    let s = r.done.await.unwrap();
    assert_eq!(s.turns, 1);
}

#[tokio::test]
async fn confirmed_barge_in_pauses_then_cuts_all_five() {
    let mut r = rig(100, true, "One long sentence here.").await; // 2s of audio
    assert!(collect_tone(&mut r, 5, Duration::from_secs(5)).await >= 5, "reply is playing");

    // Caller talks over the agent: detected -> paused, nothing is cut yet.
    for _ in 0..8 {
        r.inbound.send(loud()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Drain what the writer already had queued, then expect silence only.
    tokio::time::sleep(Duration::from_millis(150)).await;
    while r.outbound.try_recv().is_ok() {}
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut during_pause = 0;
    while let Ok(f) = r.outbound.try_recv() {
        if is_tone(&f) { during_pause += 1; }
    }
    assert_eq!(during_pause, 0, "paused: no reply audio plays");
    assert_eq!(r.tts.closed.load(SeqCst), 0, "a pause is not a cut: TTS connection stays open");

    // Past the post-cut silence window, a real transcript confirms: all cuts.
    tokio::time::sleep(Duration::from_millis(150)).await;
    r.stt_tx.send(r#"{"type":"data","data":{"transcript":"ruko mujhe kuch aur poochna hai"}}"#.into()).unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert_eq!(r.tts.closed.load(SeqCst), 1, "cut #3: the TTS connection was closed");
    // Cut #2/#4: a new LLM turn was started for the caller's words.
    let reqs = r.llm_reqs.lock().unwrap().clone();
    assert_eq!(reqs.len(), 2);
    let msgs = reqs[1]["messages"].as_array().unwrap();
    assert_eq!(msgs.last().unwrap()["content"], "ruko mujhe kuch aur poochna hai");
    // The interrupted reply hadn't been heard past its first sentence: the
    // history keeps that sentence (it started playing) and the new turn
    // opened a fresh TTS connection.
    assert!(msgs.iter().any(|m| m["role"] == "assistant" && m["content"] == "One long sentence here."));
    assert_eq!(r.tts.connections.load(SeqCst), 2, "a fresh connection for the next turn");

    r.cancel.cancel();
    let s = r.done.await.unwrap();
    assert_eq!(s.interruptions, 1);
}

#[tokio::test]
async fn backchannel_over_a_paused_reply_resumes_it() {
    let mut r = rig(60, true, "Please hold on a moment.").await; // 1.2s
    assert!(collect_tone(&mut r, 3, Duration::from_secs(5)).await >= 3);
    for _ in 0..8 {
        r.inbound.send(loud()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(250)).await; // past post-cut silence
    r.stt_tx.send(r#"{"type":"data","data":{"transcript":"जी"}}"#.into()).unwrap();
    // The reply carries on (replayed from the sentence start): tone returns.
    assert!(collect_tone(&mut r, 30, Duration::from_secs(5)).await >= 30, "reply resumed after an acknowledgement");
    assert_eq!(r.tts.closed.load(SeqCst), 0);
    assert_eq!(r.llm_reqs.lock().unwrap().len(), 1, "no new turn: it was only an acknowledgement");
    r.cancel.cancel();
    let s = r.done.await.unwrap();
    assert_eq!((s.interruptions, s.false_interruptions), (0, 1));
}

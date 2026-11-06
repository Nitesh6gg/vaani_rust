//! Mock Sarvam STT/TTS WebSocket servers and a mock OpenAI-compatible LLM:
//! the real wire protocol, no network.

use crate::media::FRAME_SIZE;
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering::SeqCst},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};
use tokio_tungstenite::tungstenite::Message as Ws;

pub(crate) const TONE: i16 = 1000;

pub(crate) fn tone_b64(frames: usize) -> String {
    let mut pcm = Vec::with_capacity(frames * FRAME_SIZE);
    for _ in 0..frames * FRAME_SIZE / 2 {
        pcm.extend_from_slice(&TONE.to_le_bytes());
    }
    STANDARD.encode(pcm)
}

#[derive(Default)]
pub(crate) struct TtsLog {
    pub texts: Mutex<Vec<String>>,
    pub connections: AtomicUsize,
    pub closed: AtomicUsize,
}

/// Mock Sarvam TTS: every `text`+`flush` is answered with `frames` frames of
/// tone, then a `final` event.
pub(crate) async fn mock_tts(frames: usize) -> (String, Arc<TtsLog>) {
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
pub(crate) async fn mock_stt() -> (String, mpsc::UnboundedSender<String>, Arc<AtomicUsize>) {
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
pub(crate) async fn mock_llm(
    reply: impl Fn(&serde_json::Value) -> String + Send + Sync + 'static,
) -> (String, Arc<Mutex<Vec<serde_json::Value>>>) {
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
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    let text = String::from_utf8_lossy(&buf).to_string();
                    if let Some((head, body)) = text.split_once("\r\n\r\n") {
                        let len = head
                            .to_lowercase()
                            .split("content-length:")
                            .nth(1)
                            .and_then(|v| v.split("\r\n").next()?.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        if body.len() >= len {
                            break serde_json::from_str::<serde_json::Value>(&body[..len]).unwrap();
                        }
                    }
                };
                let sse = reply(&body);
                r.lock().unwrap().push(body);
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    sse.len()
                );
                let _ = s.write_all(head.as_bytes()).await;
                let _ = s.write_all(sse.as_bytes()).await;
            });
        }
    });
    (url, reqs)
}

pub(crate) fn sse(text: &str) -> String {
    format!("data: {}\n\ndata: [DONE]\n\n", serde_json::json!({"choices":[{"delta":{"content":text}}]}))
}

/// Mock Sarvam STT accepting any number of connections; only counts audio messages.
pub(crate) async fn mock_stt_multi() -> (String, Arc<AtomicUsize>) {
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/ws", lis.local_addr().unwrap());
    let audio = Arc::new(AtomicUsize::new(0));
    let a = audio.clone();
    tokio::spawn(async move {
        loop {
            let (s, _) = lis.accept().await.unwrap();
            let a = a.clone();
            tokio::spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(s).await.unwrap();
                while let Some(Ok(m)) = ws.next().await {
                    if matches!(&m, Ws::Text(t) if t.as_str().contains("\"audio\"")) {
                        a.fetch_add(1, SeqCst);
                    }
                }
            });
        }
    });
    (url, audio)
}

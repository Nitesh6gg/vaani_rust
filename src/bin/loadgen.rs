//! Load generator for the Vaani engine: a fake Asterisk (ARI REST + events WebSocket + AudioSocket
//! callers) and mock Sarvam STT/TTS and LLM servers in one process, so one engine can be pushed to
//! hundreds or thousands of concurrent calls without a SIP trunk, Asterisk or provider cost.
//!
//! It simulates INBOUND calls only: each caller enters Stasis, the engine answers, asks for an
//! externalMedia channel and bridges; the generator then connects the AudioSocket, streams 20 ms frames of
//! silence, and reads the agent's audio back. The mock STT "hears" the caller say something every
//! `--turn-secs`, which drives an LLM request and a TTS reply, as on a real call.
//!
//! See `docs/LOADTEST.md`. Run `loadgen --help` for the options.

use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    time::MissedTickBehavior,
};
use tokio_tungstenite::tungstenite::Message as Ws;
use tokio_util::sync::CancellationToken;

const FRAME: usize = 640;
const KIND_UUID: u8 = 0x01;
const KIND_SLIN16: u8 = 0x12;
/// A gap this long between two frames of agent audio is audible: the engine's writer should send one every 20 ms.
const GAP: Duration = Duration::from_millis(60);
const SCHEMA: &str = "vaani_loadgen";

struct Opts {
    host: String,
    port: u16,
    concurrent: usize,
    ramp: f64,
    call_secs: u64,
    duration: u64,
    turn_secs: u64,
    llm_ms: u64,
    burst: u64,
    minio_ms: u64,
    metrics: String,
    db: String,
}

const HELP: &str = "loadgen: fake Asterisk + mock Sarvam/LLM to load-test one Vaani engine (inbound calls)

  --concurrent N   calls kept up at once (default 100)
  --ramp R         calls started per second while ramping up (default 20)
  --call-secs S    length of each call, +-20% (default 30)
  --duration D     seconds to hold the load before letting calls finish (default 120)
  --turn-secs T    the caller 'speaks' every T seconds, +-30% (default 8)
  --llm-ms M       the mock LLM's time to first token (default 200)
  --minio-ms M     the mock MinIO takes M ms to answer each upload (default 50). Raise it (e.g. 4000) to see
                   whether a slow MinIO starves the engine's Postgres pool
  --metrics URL    the engine's /metrics (e.g. http://127.0.0.1:9091/metrics): its Postgres pool is
                   reported every 5 s and its peak in the summary
  --burst N        send the caller's audio N frames at a time, every N x 20 ms, as an Asterisk that
                   delivers in clumps does (default 1: one frame every 20 ms)
  --host H         address the engine reaches this generator at (default 127.0.0.1)
  --port P         first of four ports: ARI, STT, TTS, LLM (default 18088)
  --db URL         a Postgres to create the `vaani_loadgen` schema in (a minimal Dograh: one workflow).
                   Without it, point the engine at a Dograh database yourself.

It prints the environment to start the engine with, waits for the engine to connect, then runs.";

fn opts() -> Opts {
    let mut o = Opts {
        host: "127.0.0.1".into(),
        port: 18088,
        concurrent: 100,
        ramp: 20.0,
        call_secs: 30,
        duration: 120,
        turn_secs: 8,
        llm_ms: 200,
        burst: 1,
        minio_ms: 50,
        metrics: String::new(),
        db: String::new(),
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let val = || args.get(i + 1).cloned().unwrap_or_else(|| die(&format!("{} needs a value", args[i])));
        let num = |v: String| v.parse::<f64>().unwrap_or_else(|_| die(&format!("{}: {v:?} is not a number", args[i])));
        match args[i].as_str() {
            "--help" | "-h" => {
                println!("{HELP}");
                std::process::exit(0);
            }
            "--host" => o.host = val(),
            "--db" => o.db = val(),
            "--metrics" => o.metrics = val(),
            "--minio-ms" => o.minio_ms = num(val()) as u64,
            "--port" => o.port = num(val()) as u16,
            "--concurrent" => o.concurrent = num(val()) as usize,
            "--ramp" => o.ramp = num(val()).max(0.1),
            "--call-secs" => o.call_secs = (num(val()) as u64).max(1),
            "--duration" => o.duration = num(val()) as u64,
            "--turn-secs" => o.turn_secs = (num(val()) as u64).max(1),
            "--llm-ms" => o.llm_ms = num(val()) as u64,
            "--burst" => o.burst = (num(val()) as u64).clamp(1, 50),
            other => die(&format!("unknown option {other} (try --help)")),
        }
        i += 2;
    }
    o
}

fn die(msg: &str) -> ! {
    eprintln!("loadgen: {msg}");
    std::process::exit(2);
}

#[derive(Default)]
struct Stats {
    started: AtomicU64,
    connected: AtomicU64,
    active: AtomicU64,
    finished: AtomicU64,
    setup_failed: AtomicU64,
    engine_hangups: AtomicU64,
    media_lost: AtomicU64,
    frames_out: AtomicU64,
    frames_in: AtomicU64,
    gaps: AtomicU64,
    stt_conns: AtomicU64,
    stt_audio: AtomicU64,
    tts_requests: AtomicU64,
    llm_requests: AtomicU64,
    ari_requests: AtomicU64,
    setup_ms: Mutex<Vec<u64>>,
    minio_puts: AtomicU64,
    minio_bytes: AtomicU64,
    minio_now: AtomicU64,
    minio_peak: AtomicU64,
    pool_in_use_peak: AtomicU64,
    pool_waiting_peak: AtomicU64,
    pool_timeouts: AtomicU64,
}

/// Where the engine asked the externalMedia channel to connect, and the UUID to announce.
struct Media {
    host: String,
    uuid: String,
}

#[derive(Default)]
struct Asterisk {
    /// The engine's events WebSocket, once connected.
    events: Mutex<Option<mpsc::UnboundedSender<String>>>,
    /// externalMedia channel id -> where to connect.
    external: Mutex<HashMap<String, Media>>,
    /// Caller channel id -> its call task, waiting for the bridge.
    waiting: Mutex<HashMap<String, oneshot::Sender<Media>>>,
    /// Caller channel id -> cancelled when the engine hangs the channel up.
    live: Mutex<HashMap<String, CancellationToken>>,
    stats: Stats,
}

impl Asterisk {
    fn emit(&self, event: serde_json::Value) {
        if let Some(tx) = self.events.lock().unwrap().as_ref() {
            let _ = tx.send(event.to_string());
        }
    }
}

#[tokio::main]
async fn main() {
    let o = Arc::new(opts());
    let ast = Arc::new(Asterisk::default());

    let bind = |p: u16| async move { TcpListener::bind(("0.0.0.0", p)).await.unwrap_or_else(|e| die(&format!("bind port {p}: {e}"))) };
    let (ari, stt, tts, llm, minio) =
        (bind(o.port).await, bind(o.port + 1).await, bind(o.port + 2).await, bind(o.port + 3).await, bind(o.port + 4).await);
    tokio::spawn(serve_ari(ari, ast.clone()));
    tokio::spawn(serve_stt(stt, ast.clone(), o.turn_secs));
    tokio::spawn(serve_tts(tts, ast.clone()));
    tokio::spawn(serve_llm(llm, ast.clone(), o.llm_ms));
    tokio::spawn(serve_minio(minio, ast.clone(), o.minio_ms));

    let llm_url = format!("http://{}:{}", o.host, o.port + 3);
    let db_url = if o.db.is_empty() {
        "<your Dograh database; its workflow's LLM base_url must be the LLM mock below>".to_string()
    } else {
        setup_db(&o.db, &llm_url).await.unwrap_or_else(|e| die(&format!("database setup: {e}")))
    };
    println!(
        "Start the engine (release build) with:\n\n  ARI_URL=http://{h}:{p}/ari ARI_USER=load ARI_PASS=load ARI_APP=loadgen \\\n  SARVAM_STT_WS_URL=ws://{h}:{p1}/ws SARVAM_TTS_WS_URL=ws://{h}:{p2}/ws \\\n  DOGRAH_DB_URL='{db_url}' DOGRAH_WORKFLOW_ID=42 \\\n  MEDIA_IP=<the engine's address, as this generator reaches it> MEDIA_PORT_COUNT={ports} \\\n  CAMPAIGNS_ENABLED=0 ORPHAN_SWEEP_SECONDS=0 OTEL_EXPORTER_OTLP_ENDPOINT= \\\n  MINIO_ENDPOINT={h}:{p4} MINIO_ACCESS_KEY=load MINIO_SECRET_KEY=loadloadload MINIO_BUCKET=voice-audio \\\n  ./target/release/vaani\n\n(mock LLM: {llm_url}; mock MinIO answers uploads in {minio_ms} ms)\nWaiting for the engine's events WebSocket...",
        h = o.host,
        p = o.port,
        p1 = o.port + 1,
        p2 = o.port + 2,
        p4 = o.port + 4,
        minio_ms = o.minio_ms,
        ports = (o.concurrent + 64).max(256),
    );
    while ast.events.lock().unwrap().is_none() {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    println!("Engine connected. {} concurrent calls of ~{} s, ramp {}/s, held for {} s.\n", o.concurrent, o.call_secs, o.ramp, o.duration);

    let begun = Instant::now();
    let deadline = begun + Duration::from_secs(o.duration);
    let reporter = tokio::spawn(report(ast.clone(), begun, o.metrics.clone()));
    let mut slots = vec![];
    for slot in 0..o.concurrent {
        let (ast, o) = (ast.clone(), o.clone());
        slots.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs_f64(slot as f64 / o.ramp)).await;
            let mut n = 0;
            while Instant::now() < deadline {
                call(&ast, &format!("load-{slot}-{n}"), jitter(o.call_secs * 1000, 0.2), o.burst).await;
                n += 1;
            }
        }));
    }
    for s in slots {
        let _ = s.await;
    }
    reporter.abort();
    tokio::time::sleep(Duration::from_secs(2)).await; // the engine's last teardown requests
    summary(&ast.stats, begun.elapsed());
}

/// `ms` +- `spread` (a fraction), as a duration.
fn jitter(ms: u64, spread: f64) -> Duration {
    let d = (ms as f64 * spread) as u64;
    Duration::from_millis(ms - d + rand::random_range(0..=2 * d))
}

// ------------------------------------------------------------------ calls ---

/// One inbound call, from entering Stasis to the caller hanging up after `length`.
async fn call(ast: &Arc<Asterisk>, id: &str, length: Duration, burst: u64) {
    let st = &ast.stats;
    let (tx, rx) = oneshot::channel();
    let hung = CancellationToken::new();
    ast.waiting.lock().unwrap().insert(id.to_string(), tx);
    ast.live.lock().unwrap().insert(id.to_string(), hung.clone());
    st.started.fetch_add(1, Relaxed);
    let entered = Instant::now();
    ast.emit(serde_json::json!({"type": "StasisStart", "args": [], "channel": {
        "id": id, "name": format!("PJSIP/load-{id}"), "caller": {"name": "", "number": "9000000000"},
        "dialplan": {"context": "from-trunk", "exten": "1000", "priority": 1}}}));

    let media = tokio::select! {
        m = tokio::time::timeout(Duration::from_secs(20), rx) => m.ok().and_then(Result::ok),
        _ = hung.cancelled() => None,
    };
    match media {
        None => {
            st.setup_failed.fetch_add(1, Relaxed); // refused (CPU guard), or never bridged
        }
        Some(m) => match TcpStream::connect(&m.host).await {
            Err(_) => {
                st.setup_failed.fetch_add(1, Relaxed);
            }
            Ok(sock) => {
                st.setup_ms.lock().unwrap().push(entered.elapsed().as_millis() as u64);
                st.connected.fetch_add(1, Relaxed);
                st.active.fetch_add(1, Relaxed);
                stream(sock, &m.uuid, length, burst, &hung, st).await;
                st.active.fetch_sub(1, Relaxed);
            }
        },
    }
    ast.waiting.lock().unwrap().remove(id);
    ast.live.lock().unwrap().remove(id);
    st.finished.fetch_add(1, Relaxed);
    // The caller hung up (or the engine did): the channel leaves Stasis and is destroyed.
    ast.emit(serde_json::json!({"type": "StasisEnd", "channel": {"id": id}}));
    ast.emit(serde_json::json!({"type": "ChannelDestroyed", "cause": 16, "cause_txt": "Normal Clearing", "channel": {"id": id}}));
}

/// The call's audio: a frame of silence out every 20 ms, the agent's frames in.
async fn stream(sock: TcpStream, uuid: &str, length: Duration, burst: u64, hung: &CancellationToken, st: &Stats) {
    let _ = sock.set_nodelay(true);
    let (mut rd, mut wr) = sock.into_split();
    let id: Vec<u8> =
        uuid.replace('-', "").as_bytes().chunks(2).filter_map(|h| u8::from_str_radix(std::str::from_utf8(h).ok()?, 16).ok()).collect();
    let mut hello = vec![KIND_UUID, 0, id.len() as u8];
    hello.extend_from_slice(&id);
    if wr.write_all(&hello).await.is_err() {
        st.media_lost.fetch_add(1, Relaxed);
        return;
    }
    let mut frame = [0u8; 3 + FRAME];
    frame[0] = KIND_SLIN16;
    frame[1..3].copy_from_slice(&(FRAME as u16).to_be_bytes());

    let closed = CancellationToken::new();
    let reader = {
        let closed = closed.clone();
        async move {
            let (mut head, mut body, mut last) = ([0u8; 3], vec![0u8; 65536], None::<Instant>);
            loop {
                if rd.read_exact(&mut head).await.is_err() {
                    break;
                }
                let len = u16::from_be_bytes([head[1], head[2]]) as usize;
                if rd.read_exact(&mut body[..len]).await.is_err() || head[0] == 0 {
                    break;
                }
                if head[0] == KIND_SLIN16 {
                    st.frames_in.fetch_add(1, Relaxed);
                    let now = Instant::now();
                    if last.is_some_and(|l| now - l > GAP) {
                        st.gaps.fetch_add(1, Relaxed);
                    }
                    last = Some(now);
                }
            }
            closed.cancel();
        }
    };
    let writer = async {
        let mut tick = tokio::time::interval(Duration::from_millis(20 * burst));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let end = tokio::time::sleep(length);
        tokio::pin!(end);
        loop {
            tokio::select! {
                _ = &mut end => break,
                _ = hung.cancelled() => break,
                _ = closed.cancelled() => { st.media_lost.fetch_add(1, Relaxed); break }
                _ = tick.tick() => {
                    let mut failed = false;
                    for _ in 0..burst {
                        failed |= wr.write_all(&frame).await.is_err();
                    }
                    if failed { st.media_lost.fetch_add(1, Relaxed); break }
                    st.frames_out.fetch_add(burst, Relaxed);
                }
            }
        }
        let _ = wr.shutdown().await;
    };
    tokio::select! { _ = reader => {}, _ = writer => {} }
}

// -------------------------------------------------------------------- ARI ---

async fn serve_ari(lis: TcpListener, ast: Arc<Asterisk>) {
    loop {
        let Ok((sock, _)) = lis.accept().await else { continue };
        let ast = ast.clone();
        tokio::spawn(async move {
            let mut peek = [0u8; 16];
            let n = sock.peek(&mut peek).await.unwrap_or(0);
            if peek[..n].starts_with(b"GET /ari/events") { events(sock, ast).await } else { rest(sock, ast).await }
        });
    }
}

/// The events WebSocket: everything `emit` sends goes to the engine.
async fn events(sock: TcpStream, ast: Arc<Asterisk>) {
    let Ok(mut ws) = tokio_tungstenite::accept_async(sock).await else { return };
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    *ast.events.lock().unwrap() = Some(tx);
    loop {
        tokio::select! {
            Some(ev) = rx.recv() => if ws.send(Ws::text(ev)).await.is_err() { break },
            m = ws.next() => if !matches!(m, Some(Ok(_))) { break },
        }
    }
    eprintln!("loadgen: the engine's events WebSocket closed");
}

/// The REST side, keep-alive: one request after another on the connection.
async fn rest(mut sock: TcpStream, ast: Arc<Asterisk>) {
    let mut buf = Vec::<u8>::new();
    loop {
        let (head_end, len) = loop {
            if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buf[..end]).to_lowercase();
                let len =
                    head.split("content-length:").nth(1).and_then(|v| v.split("\r\n").next()?.trim().parse::<usize>().ok()).unwrap_or(0);
                if buf.len() >= end + 4 + len {
                    break (end, len);
                }
            }
            let mut chunk = [0u8; 8192];
            match sock.read(&mut chunk).await {
                Ok(n) if n > 0 => buf.extend_from_slice(&chunk[..n]),
                _ => return,
            }
        };
        let line = String::from_utf8_lossy(&buf[..head_end]).lines().next().unwrap_or_default().to_string();
        let body = buf[head_end + 4..head_end + 4 + len].to_vec();
        buf.drain(..head_end + 4 + len);
        let mut it = line.split_whitespace();
        let reply = handle(&ast, it.next().unwrap_or_default(), it.next().unwrap_or_default(), &body);
        let resp = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{reply}", reply.len());
        if sock.write_all(resp.as_bytes()).await.is_err() {
            return;
        }
    }
}

fn handle(ast: &Arc<Asterisk>, method: &str, target: &str, body: &[u8]) -> &'static str {
    ast.stats.ari_requests.fetch_add(1, Relaxed);
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let path = path.trim_start_matches("/ari");
    match (method, path) {
        ("POST", "/channels/externalMedia") => {
            let v: serde_json::Value = serde_json::from_slice(body).unwrap_or_default();
            let text = |k: &str| v[k].as_str().unwrap_or_default().to_string();
            let id = text("channelId");
            ast.external.lock().unwrap().insert(id.clone(), Media { host: text("external_host"), uuid: text("data") });
            // The new channel enters the Stasis app.
            ast.emit(serde_json::json!({"type": "StasisStart", "args": [], "channel": {"id": id, "name": format!("AudioSocket/{}-{}", text("external_host"), text("data"))}}));
            "{}"
        }
        ("POST", p) if p.starts_with("/bridges/") && p.ends_with("/addChannel") => {
            // Both legs are in the bridge: Asterisk now connects the AudioSocket.
            let channels = query.split('&').find_map(|kv| kv.strip_prefix("channel=")).unwrap_or_default().replace("%2C", ",");
            let ids: Vec<&str> = channels.split(',').collect();
            let media = ids.iter().find_map(|id| ast.external.lock().unwrap().remove(*id));
            let waiter = ids.iter().find_map(|id| ast.waiting.lock().unwrap().remove(*id));
            if let (Some(m), Some(w)) = (media, waiter) {
                let _ = w.send(m);
            }
            "{}"
        }
        ("DELETE", p) if p.starts_with("/channels/") => {
            let id = p.trim_start_matches("/channels/");
            ast.external.lock().unwrap().remove(id);
            if let Some(t) = ast.live.lock().unwrap().get(id)
                && !t.is_cancelled()
            {
                t.cancel(); // the engine hung the caller up
                ast.stats.engine_hangups.fetch_add(1, Relaxed);
            }
            "{}"
        }
        ("GET", "/channels") => "[]",
        _ => "{}", // answer, bridge create/delete, removeChannel
    }
}

// -------------------------------------------------------------- providers ---

/// Mock Sarvam STT: counts the audio it is sent and "hears" the caller every `turn_secs`.
async fn serve_stt(lis: TcpListener, ast: Arc<Asterisk>, turn_secs: u64) {
    loop {
        let Ok((sock, _)) = lis.accept().await else { continue };
        let ast = ast.clone();
        tokio::spawn(async move {
            let Ok(mut ws) = tokio_tungstenite::accept_async(sock).await else { return };
            ast.stats.stt_conns.fetch_add(1, Relaxed);
            let mut next = Box::pin(tokio::time::sleep(jitter(turn_secs * 1000, 0.3)));
            loop {
                tokio::select! {
                    m = ws.next() => match m {
                        Some(Ok(Ws::Text(t))) => if t.as_str().contains("\"audio\"") { ast.stats.stt_audio.fetch_add(1, Relaxed); },
                        Some(Ok(_)) => {}
                        _ => break,
                    },
                    _ = &mut next => {
                        for msg in [
                            r#"{"type":"events","data":{"signal_type":"START_SPEECH"}}"#,
                            r#"{"type":"events","data":{"signal_type":"END_SPEECH"}}"#,
                            r#"{"type":"data","data":{"transcript":"haan ji, mujhe is baare mein aur jaankari chahiye"}}"#,
                        ] {
                            if ws.send(Ws::text(msg)).await.is_err() { return }
                        }
                        next = Box::pin(tokio::time::sleep(jitter(turn_secs * 1000, 0.3)));
                    }
                }
            }
        });
    }
}

/// Mock Sarvam TTS: every `text` + `flush` is answered with 2 s of tone, then a `final` event.
async fn serve_tts(lis: TcpListener, ast: Arc<Asterisk>) {
    let mut pcm = Vec::with_capacity(100 * FRAME);
    for _ in 0..100 * FRAME / 2 {
        pcm.extend_from_slice(&1000i16.to_le_bytes());
    }
    let audio = Arc::new(format!(r#"{{"type":"audio","data":{{"audio":"{}"}}}}"#, STANDARD.encode(pcm)));
    loop {
        let Ok((sock, _)) = lis.accept().await else { continue };
        let (ast, audio) = (ast.clone(), audio.clone());
        tokio::spawn(async move {
            let Ok(mut ws) = tokio_tungstenite::accept_async(sock).await else { return };
            while let Some(Ok(m)) = ws.next().await {
                let Ws::Text(t) = m else { continue };
                if t.as_str().contains("\"flush\"") {
                    ast.stats.tts_requests.fetch_add(1, Relaxed);
                    let _ = ws.send(Ws::text(audio.as_str())).await;
                    let _ = ws.send(Ws::text(r#"{"type":"event","data":{"event_type":"final"}}"#)).await;
                }
            }
        });
    }
}

/// Mock OpenAI-compatible LLM: one sentence as SSE after `first_token_ms`, with a usage chunk.
async fn serve_llm(lis: TcpListener, ast: Arc<Asterisk>, first_token_ms: u64) {
    let sse = format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        serde_json::json!({"id": "chatcmpl-load", "choices": [{"delta": {"content": "Ji bilkul, main aapki madad karti hoon. Aap bataiye aapko kya jaanna hai?"}}]}),
        serde_json::json!({"choices": [], "usage": {"prompt_tokens": 400, "completion_tokens": 24, "total_tokens": 424}})
    );
    let resp = Arc::new(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\n\r\n{sse}", sse.len()));
    loop {
        let Ok((mut sock, _)) = lis.accept().await else { continue };
        let (ast, resp) = (ast.clone(), resp.clone());
        tokio::spawn(async move {
            let mut buf = Vec::<u8>::new();
            loop {
                // One request: its head, then its body.
                let total = loop {
                    if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buf[..end]).to_lowercase();
                        let len = head
                            .split("content-length:")
                            .nth(1)
                            .and_then(|v| v.split("\r\n").next()?.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        if buf.len() >= end + 4 + len {
                            break end + 4 + len;
                        }
                    }
                    let mut chunk = [0u8; 16384];
                    match sock.read(&mut chunk).await {
                        Ok(n) if n > 0 => buf.extend_from_slice(&chunk[..n]),
                        _ => return,
                    }
                };
                buf.drain(..total);
                ast.stats.llm_requests.fetch_add(1, Relaxed);
                tokio::time::sleep(Duration::from_millis(first_token_ms)).await;
                if sock.write_all(resp.as_bytes()).await.is_err() {
                    return;
                }
            }
        });
    }
}

/// Mock MinIO: takes every upload (counting its bytes), answers 200 after `delay`; keep-alive.
async fn serve_minio(lis: TcpListener, ast: Arc<Asterisk>, delay_ms: u64) {
    loop {
        let Ok((mut sock, _)) = lis.accept().await else { continue };
        let ast = ast.clone();
        tokio::spawn(async move {
            let mut buf = Vec::<u8>::new();
            loop {
                let (head_end, len) = loop {
                    if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buf[..end]).to_lowercase();
                        break (
                            end + 4,
                            head.split("content-length:")
                                .nth(1)
                                .and_then(|v| v.split("\r\n").next()?.trim().parse::<usize>().ok())
                                .unwrap_or(0),
                        );
                    }
                    let mut chunk = [0u8; 65536];
                    match sock.read(&mut chunk).await {
                        Ok(n) if n > 0 => buf.extend_from_slice(&chunk[..n]),
                        _ => return,
                    }
                };
                // The body is counted, not kept.
                let mut have = buf.len() - head_end;
                buf.clear();
                while have < len {
                    let mut chunk = [0u8; 65536];
                    match sock.read(&mut chunk).await {
                        Ok(n) if n > 0 => have += n,
                        _ => return,
                    }
                }
                buf.clear();
                let st = &ast.stats;
                st.minio_puts.fetch_add(1, Relaxed);
                st.minio_bytes.fetch_add(len as u64, Relaxed);
                let now = st.minio_now.fetch_add(1, Relaxed) + 1;
                st.minio_peak.fetch_max(now, Relaxed);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                st.minio_now.fetch_sub(1, Relaxed);
                if sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n").await.is_err() {
                    return;
                }
            }
        });
    }
}

/// The engine's Postgres pool, from its /metrics: (in use, waiting, max, timeouts so far).
async fn pool_reading(url: &str) -> Option<(u64, u64, u64, u64)> {
    let text = reqwest::Client::new().get(url).timeout(Duration::from_secs(2)).send().await.ok()?.text().await.ok()?;
    let v = |name: &str| text.lines().find_map(|l| l.strip_prefix(name)?.trim().parse::<f64>().ok()).map(|x| x as u64);
    Some((v("vaani_db_pool_in_use ")?, v("vaani_db_pool_waiting ")?, v("vaani_db_pool_max ")?, v("vaani_db_pool_timeouts_total ")?))
}

// --------------------------------------------------------------- database ---

/// A minimal Dograh in schema `vaani_loadgen`: one workflow (id 42) whose LLM is the mock. Returns the
/// engine's `DOGRAH_DB_URL`.
async fn setup_db(url: &str, llm_url: &str) -> Result<String, tokio_postgres::Error> {
    let (db, conn) = tokio_postgres::connect(url, tokio_postgres::NoTls).await?;
    tokio::spawn(conn);
    db.batch_execute(&format!(
        r#"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE; CREATE SCHEMA {SCHEMA}; SET search_path = {SCHEMA};
        CREATE TABLE workflows (id serial PRIMARY KEY, user_id int, organization_id int, released_definition_id int, template_context_variables jsonb, call_disposition_codes json);
        CREATE TABLE workflow_runs (id bigserial PRIMARY KEY, name text, workflow_id int, definition_id bigint, mode text, call_type text, state text,
            is_completed bool, usage_info json, cost_info json, initial_context json, gathered_context json, logs json, annotations json,
            created_at timestamptz, recording_url text, transcript_url text, storage_backend text, campaign_id int, queued_run_id int);
        CREATE TABLE users (id serial PRIMARY KEY, selected_organization_id bigint);
        CREATE TABLE organizations (id bigint PRIMARY KEY, price_per_second_usd float8);
        CREATE TABLE organization_usage_cycles (id serial PRIMARY KEY, organization_id bigint, period_start timestamptz, period_end timestamptz,
            used_dograh_tokens float8 DEFAULT 0, total_duration_seconds bigint DEFAULT 0, used_amount_usd float8);
        CREATE TABLE organization_configurations (id serial PRIMARY KEY, organization_id bigint, key text, value jsonb);
        CREATE TABLE workflow_definitions (id serial PRIMARY KEY, workflow_id int, workflow_json jsonb, template_context_variables jsonb, workflow_configurations jsonb, is_current bool);
        CREATE TABLE user_configurations (id serial PRIMARY KEY, user_id int, configuration jsonb);
        CREATE TABLE tools (tool_uuid uuid, organization_id int, name text, description text, category text, status text, definition jsonb);
        CREATE TABLE external_credentials (credential_uuid varchar, organization_id int, credential_type text, credential_data jsonb, is_active bool);
        INSERT INTO users (id, selected_organization_id) VALUES (7, 3);
        INSERT INTO organizations (id, price_per_second_usd) VALUES (3, 0.001);
        INSERT INTO organization_usage_cycles (organization_id, period_start, period_end, used_amount_usd)
          VALUES (3, now() - interval '1 day', now() + interval '30 days', 0);
        INSERT INTO user_configurations (user_id, configuration) VALUES (7, '{{
          "llm": {{"provider":"speaches","base_url":"{llm_url}","api_key":"k","model":"load-llm"}},
          "stt": {{"provider":"sarvam","api_key":"k"}}, "tts": {{"provider":"sarvam","api_key":"k"}}}}');
        INSERT INTO workflows (id, user_id, organization_id) VALUES (42, 7, 3);
        INSERT INTO workflow_definitions (workflow_id, workflow_json, workflow_configurations, is_current)
          VALUES (42, '{{"nodes":[{{"id":"1","type":"startCall","data":{{"name":"Start","prompt":"You are a helpful agent. Greet the caller and answer their questions.","is_start":true,"allow_interrupt":true}}}}]}}',
                  '{{"max_call_duration": 3600, "max_user_idle_timeout": 60}}', true);"#
    ))
    .await?;
    Ok(format!("{url}{}options=-csearch_path%3D{SCHEMA}", if url.contains('?') { "&" } else { "?" }))
}

// ---------------------------------------------------------------- reports ---

async fn report(ast: Arc<Asterisk>, begun: Instant, metrics: String) {
    let st = &ast.stats;
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    tick.tick().await;
    let (mut last_in, mut last_out, mut last_gaps) = (0, 0, 0);
    loop {
        tick.tick().await;
        let (fin, fout, gaps) = (st.frames_in.load(Relaxed), st.frames_out.load(Relaxed), st.gaps.load(Relaxed));
        // The engine should send a frame for every frame a caller sends: one every 20 ms, both ways.
        let ratio = (fin - last_in) as f64 / (fout - last_out).max(1) as f64;
        println!(
            "{:>4}s  active {:>5}  started {:>6}  setup-failed {:>4}  engine-hangups {:>4}  media-lost {:>4}  agent audio {:>4.2} (1.00 = on time)  gaps(>60ms) +{:<5} llm {:>6}  tts {:>6}",
            begun.elapsed().as_secs(),
            st.active.load(Relaxed),
            st.started.load(Relaxed),
            st.setup_failed.load(Relaxed),
            st.engine_hangups.load(Relaxed),
            st.media_lost.load(Relaxed),
            ratio,
            gaps - last_gaps,
            st.llm_requests.load(Relaxed),
            st.tts_requests.load(Relaxed),
        );
        (last_in, last_out, last_gaps) = (fin, fout, gaps);
        if !metrics.is_empty() {
            let uploads = format!("uploads {} (in flight {})", st.minio_puts.load(Relaxed), st.minio_now.load(Relaxed));
            match pool_reading(&metrics).await {
                Some((in_use, waiting, max, timeouts)) => {
                    st.pool_in_use_peak.fetch_max(in_use, Relaxed);
                    st.pool_waiting_peak.fetch_max(waiting, Relaxed);
                    st.pool_timeouts.store(timeouts, Relaxed);
                    println!("       postgres pool {in_use}/{max} in use, {waiting} waiting, {timeouts} timeouts   {uploads}");
                }
                None => println!("       (the engine's metrics could not be read)   {uploads}"),
            }
        }
    }
}

fn summary(st: &Stats, ran: Duration) {
    let mut setup = st.setup_ms.lock().unwrap().clone();
    setup.sort_unstable();
    let pct = |p: f64| setup.get(((setup.len() as f64 * p).ceil() as usize).saturating_sub(1)).copied().unwrap_or(0);
    let c = |a: &AtomicU64| a.load(Relaxed);
    let talk_s = c(&st.frames_out) as f64 / 50.0;
    println!(
        "\n--- summary ({} s) ---\ncalls started {}  connected {}  setup failed {}  hung up by the engine {}  media lost {}\n\
         talk time {:.0} s   call setup (Stasis -> audio) p50 {} ms  p95 {} ms  max {} ms\n\
         agent audio: {} frames in, {:.2} per frame sent (1.00 = a frame every 20 ms), {} gaps over 60 ms ({:.2} per call-minute)\n\
         providers: {} STT connections ({} audio messages), {} LLM requests ({:.1}/call-minute), {} TTS requests; {} ARI requests",
        ran.as_secs(),
        c(&st.started),
        c(&st.connected),
        c(&st.setup_failed),
        c(&st.engine_hangups),
        c(&st.media_lost),
        talk_s,
        pct(0.5),
        pct(0.95),
        setup.last().copied().unwrap_or(0),
        c(&st.frames_in),
        c(&st.frames_in) as f64 / c(&st.frames_out).max(1) as f64,
        c(&st.gaps),
        c(&st.gaps) as f64 / (talk_s / 60.0).max(1e-9),
        c(&st.stt_conns),
        c(&st.stt_audio),
        c(&st.llm_requests),
        c(&st.llm_requests) as f64 / (talk_s / 60.0).max(1e-9),
        c(&st.tts_requests),
        c(&st.ari_requests),
    );
    if c(&st.minio_puts) > 0 {
        println!(
            "uploads: {} ({:.1} MB), at most {} in flight; engine Postgres pool peak {} in use, {} waiting, {} timeouts (needs --metrics)",
            c(&st.minio_puts),
            c(&st.minio_bytes) as f64 / 1e6,
            c(&st.minio_peak),
            c(&st.pool_in_use_peak),
            c(&st.pool_waiting_peak),
            c(&st.pool_timeouts)
        );
    }
    let bad = c(&st.setup_failed) + c(&st.media_lost);
    println!(
        "{}",
        if bad == 0 {
            "RESULT: every call was set up and kept its audio."
        } else {
            "RESULT: some calls failed (see setup failed / media lost)."
        }
    );
}

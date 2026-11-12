//! End-to-end tests of the agent task against mock Sarvam STT/TTS WebSocket
//! servers and a mock OpenAI-compatible LLM: no network, real protocol.

use super::{
    handler::{AgentConfig, AgentIo, run},
    vad::VoteDetector,
    workflow::{Node, Workflow},
};
use crate::{
    ai::{
        llm::LlmClient,
        stt::{SttClient, SttConfig},
        tts::TtsConfig,
    },
    media::{
        FRAME_SIZE, Frame, SILENCE,
        call::{MediaIo, MediaStats},
    },
    testkit::*,
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

struct Rig {
    inbound: mpsc::Sender<Frame>,
    outbound: mpsc::Receiver<Frame>,
    stt_tx: mpsc::UnboundedSender<String>,
    stt_audio: Arc<AtomicUsize>,
    stt_flushes: Arc<AtomicUsize>,
    tts: Arc<TtsLog>,
    llm_reqs: Arc<Mutex<Vec<serde_json::Value>>>,
    cancel: CancellationToken,
    hangups: Arc<AtomicUsize>,
    done: tokio::task::JoinHandle<(super::calllog::Summary, super::extract::Finisher)>,
}

async fn rig(tts_frames: usize, allow_interrupt: bool, llm_text: &'static str) -> Rig {
    let wf = Workflow {
        start: 0,
        nodes: vec![Node { name: "start".into(), prompt: "You are an agent.".into(), allow_interrupt, ..Default::default() }],
    };
    rig_wf(wf, tts_frames, move |_| sse(llm_text)).await
}

async fn rig_wf(wf: Workflow, tts_frames: usize, llm_reply: impl Fn(&serde_json::Value) -> String + Send + Sync + 'static) -> Rig {
    rig_tuned(wf, tts_frames, llm_reply, |_| {}).await
}

async fn rig_tuned(
    wf: Workflow,
    tts_frames: usize,
    llm_reply: impl Fn(&serde_json::Value) -> String + Send + Sync + 'static,
    tune: impl FnOnce(&mut AgentConfig),
) -> Rig {
    let (tts_url, tts) = mock_tts(tts_frames).await;
    let (stt_url, stt_tx, stt_audio, stt_flushes) = mock_stt().await;
    let (llm_url, llm_reqs) = mock_llm(llm_reply).await;
    let cancel = CancellationToken::new();

    let (stt, stt_events) = SttClient::connect(
        "t",
        SttConfig { ws_url: stt_url, api_key: "k".into(), model: "m".into(), language: "hi-IN".into(), flush_signal: false },
        cancel.clone(),
    )
    .await
    .unwrap();

    let (in_tx, in_rx) = mpsc::channel(crate::media::call::INBOUND_QUEUE);
    let (out_tx, out_rx) = mpsc::channel(5);
    let media = MediaIo { inbound: in_rx, outbound: out_tx, closed: CancellationToken::new(), stats: Arc::new(MediaStats::default()) };
    let hangups = Arc::new(AtomicUsize::new(0));
    let h = hangups.clone();

    let mut cfg = AgentConfig {
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
        stt_flush_after: Duration::ZERO,
        stt_gate: None,
        ack_filter: true,
        ack_end_margin: Duration::from_millis(300),
        idle_timeout: Duration::ZERO,
        max_duration: Duration::ZERO,
        llm_max_failed_turns: 3,
        llm_fallback_text: String::new(),
        hangup: Arc::new(move || {
            h.fetch_add(1, SeqCst);
        }),
        transfer: None,
        hold_audio: tone_pcm(HOLD_TONE, 10),
        beep_audio: tone_pcm(BEEP_TONE, 3),
    };
    tune(&mut cfg);
    let done = tokio::spawn(run(cfg, AgentIo { media, stt, stt_events }, cancel.clone()));
    Rig { inbound: in_tx, outbound: out_rx, stt_tx, stt_audio, stt_flushes, tts, llm_reqs, cancel, hangups, done }
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
                if seen >= n {
                    break;
                }
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
    let (mut s, fin) = r.done.await.unwrap();
    fin.finish(&mut s).await;
    assert_eq!(s.user_turns, 1);
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
        if is_tone(&f) {
            during_pause += 1;
        }
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
    let (mut s, fin) = r.done.await.unwrap();
    fin.finish(&mut s).await;
    assert_eq!(s.barge_ins, 1);
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
    let (mut s, fin) = r.done.await.unwrap();
    fin.finish(&mut s).await;
    assert_eq!((s.barge_ins, s.false_interruptions), (0, 1));
}

/// Rough capacity probe, not a unit test: `BENCH_CALLS=500 cargo test --release bench_concurrent_agents -- --ignored --nocapture`.
/// N agents run against shared mock providers (so the figures include the mocks'
/// own work in this process -- an upper bound). Prints RSS and CPU time per call
/// and the worst 20ms tick lateness seen by one probe agent.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn bench_concurrent_agents() {
    fn rss_kb() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| s.lines().find(|l| l.starts_with("VmRSS:"))?.split_whitespace().nth(1)?.parse().ok())
            .unwrap_or(0)
    }
    fn cpu_secs() -> f64 {
        let s = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
        let f: Vec<&str> = s.rsplit(") ").next().unwrap_or("").split_whitespace().collect();
        (f.get(11).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0) + f.get(12).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0))
            / 100.0
    }

    let n: usize = std::env::var("BENCH_CALLS").ok().and_then(|v| v.parse().ok()).unwrap_or(200);
    let secs: u64 = std::env::var("BENCH_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(10);
    let (tts_url, _tts) = mock_tts(10).await;
    let (stt_url, stt_audio) = mock_stt_multi().await;
    let (llm_url, _reqs) = mock_llm(|_| sse("Namaste. Main aapki kya madad kar sakti hoon?")).await;
    let cancel = CancellationToken::new();

    let (rss0, cpu0) = (rss_kb(), cpu_secs());
    for i in 0..n {
        let (stt, stt_events) = SttClient::connect(
            "b",
            SttConfig { ws_url: stt_url.clone(), api_key: "k".into(), model: "m".into(), language: "hi-IN".into(), flush_signal: false },
            cancel.clone(),
        )
        .await
        .unwrap();
        let (in_tx, in_rx) = mpsc::channel(5);
        let (out_tx, mut out_rx) = mpsc::channel(5);
        let media = MediaIo { inbound: in_rx, outbound: out_tx, closed: CancellationToken::new(), stats: Arc::new(MediaStats::default()) };
        // Stand-in for the writer task: drain agent frames, as the AudioSocket writer would.
        tokio::spawn(async move { while out_rx.recv().await.is_some() {} });
        let wf =
            Workflow { start: 0, nodes: vec![Node { name: "s".into(), prompt: "p".into(), allow_interrupt: true, ..Default::default() }] };
        let cfg = AgentConfig {
            call_id: format!("b{i}"),
            workflow: Arc::new(wf),
            llm: LlmClient::new(&llm_url, "k", "m"),
            tts: TtsConfig {
                ws_url: tts_url.clone(),
                api_key: "k".into(),
                model: "bulbul:v3".into(),
                voice: "v".into(),
                language: "hi-IN".into(),
            },
            vad: Box::new(VoteDetector::earshot(0.5)),
            barge_in_enabled: true,
            barge_in_min_speech: Duration::from_millis(200),
            barge_in_guard: Duration::from_millis(300),
            post_cut_silence: Duration::from_millis(1200),
            stt_flush_after: Duration::ZERO,
            stt_gate: None,
            ack_filter: true,
            ack_end_margin: Duration::from_millis(300),
            idle_timeout: Duration::ZERO,
            max_duration: Duration::ZERO,
            llm_max_failed_turns: 3,
            llm_fallback_text: String::new(),
            hangup: Arc::new(|| {}),
            transfer: None,
            hold_audio: tone_pcm(HOLD_TONE, 10),
            beep_audio: tone_pcm(BEEP_TONE, 3),
        };
        let c = cancel.clone();
        tokio::spawn(async move {
            let h = tokio::spawn(run(cfg, AgentIo { media, stt, stt_events }, c));
            // Caller side: a steady stream of (silent) frames at 20ms.
            let mut t = crate::media::call::new_ticker();
            while !h.is_finished() {
                t.tick().await;
                let _ = in_tx.try_send(SILENCE);
            }
        });
    }

    // Probe: how late does a 20ms tick fire while all of that runs?
    let mut t = crate::media::call::new_ticker();
    let (mut worst, mut last) = (Duration::ZERO, tokio::time::Instant::now());
    let end = tokio::time::Instant::now() + Duration::from_secs(secs);
    t.tick().await;
    while tokio::time::Instant::now() < end {
        t.tick().await;
        let now = tokio::time::Instant::now();
        worst = worst.max((now - last).saturating_sub(Duration::from_millis(20)));
        last = now;
    }
    let (rss1, cpu1) = (rss_kb(), cpu_secs());
    println!(
        "calls={n} over {secs}s: RSS {:.1} MB ({:.0} KB/call), CPU {:.2}s ({:.1}% of one core, {:.3}% per call), worst tick lateness {:?}, stt msgs {}",
        (rss1 - rss0) as f64 / 1024.0,
        (rss1 - rss0) as f64 / n as f64,
        cpu1 - cpu0,
        (cpu1 - cpu0) / secs as f64 * 100.0,
        (cpu1 - cpu0) / secs as f64 * 100.0 / n as f64,
        worst,
        stt_audio.load(SeqCst),
    );
    cancel.cancel();
}

// ------------------------------------------------------ recorded clips ---

use super::workflow::{Clip, Edge};
use crate::ai::llm::FunctionDef;

const CLIP_TONE: i16 = 2000;

fn clip(id: &str, frames: usize, transcript: &str) -> Arc<Clip> {
    let pcm: Vec<u8> = (0..frames * FRAME_SIZE / 2).flat_map(|_| CLIP_TONE.to_le_bytes()).collect();
    Arc::new(Clip { id: id.into(), pcm: Arc::new(pcm), transcript: transcript.into() })
}

fn is_clip(f: &Frame) -> bool {
    f[..2] == CLIP_TONE.to_le_bytes()
}

/// Drains outbound until `n` frames of the given kind were seen; returns the
/// order the two kinds of audio were heard in ('c' clip, 't' tts tone).
async fn heard(r: &mut Rig, clip_frames: usize, tts_frames: usize, max: Duration) -> String {
    let (mut order, mut c, mut t) = (String::new(), 0, 0);
    let _ = tokio::time::timeout(max, async {
        while let Some(f) = r.outbound.recv().await {
            let kind = if is_clip(&f) {
                'c'
            } else if is_tone(&f) {
                't'
            } else {
                continue;
            };
            if !order.ends_with(kind) {
                order.push(kind);
            }
            (c, t) = (c + usize::from(kind == 'c'), t + usize::from(kind == 't'));
            if c >= clip_frames && t >= tts_frames {
                break;
            }
        }
    })
    .await;
    order
}

fn start_node() -> Node {
    Node { name: "start".into(), prompt: "You are an agent.".into(), allow_interrupt: true, ..Default::default() }
}

#[tokio::test]
async fn a_recorded_greeting_plays_without_tts_and_is_remembered_as_its_transcript() {
    let wf = Workflow { start: 0, nodes: vec![Node { greeting_clip: Some(clip("hello", 6, "Namaste!")), ..start_node() }] };
    let mut r = rig_wf(wf, 10, |_| sse("ok.")).await;
    let order = heard(&mut r, 6, 0, Duration::from_secs(5)).await;
    assert_eq!(order, "c", "only the recording plays");
    assert!(r.tts.texts.lock().unwrap().is_empty(), "nothing was synthesized");
    assert!(r.llm_reqs.lock().unwrap().is_empty(), "the LLM isn't asked to open the call");

    // The greeting is in history as the agent's own turn: the caller's turn sees it.
    tokio::time::sleep(Duration::from_millis(100)).await;
    r.stt_tx.send(r#"{"type":"data","data":{"transcript":"hello"}}"#.into()).unwrap();
    let _ = heard(&mut r, 0, 10, Duration::from_secs(5)).await;
    let msgs = r.llm_reqs.lock().unwrap()[0]["messages"].clone();
    assert_eq!(msgs[1]["content"], "Namaste!");
    r.cancel.cancel();
    let (mut s, fin) = r.done.await.unwrap();
    fin.finish(&mut s).await;
    assert_eq!(s.tts_chars, 3, "only the LLM turn's \"ok.\" is billed; the recorded greeting costs no TTS characters");
}

#[tokio::test]
async fn the_llm_can_answer_with_a_recording_or_with_speech() {
    let mut clips = std::collections::HashMap::new();
    clips.insert("offer_01".to_string(), clip("offer_01", 6, "Our offer is great."));
    let wf = Workflow { start: 0, nodes: vec![Node { clips: Some(Arc::new(clips)), ..start_node() }] };
    // First reply: a recording, then (a second marker is dropped). Second: speech.
    let mut r = rig_wf(wf, 10, |req| {
        let asked_before = req["messages"].as_array().unwrap().iter().any(|m| m["role"] == "user" && m["content"] != "You are an agent.");
        if asked_before { sse("▸ Sure, here you go.") } else { sse("● offer_01 Our offer is great.") }
    })
    .await;
    assert_eq!(heard(&mut r, 6, 0, Duration::from_secs(5)).await, "c");
    assert!(r.tts.texts.lock().unwrap().is_empty(), "the clip's transcript is not synthesized");

    tokio::time::sleep(Duration::from_millis(100)).await;
    r.stt_tx.send(r#"{"type":"data","data":{"transcript":"tell me more"}}"#.into()).unwrap();
    assert_eq!(heard(&mut r, 0, 10, Duration::from_secs(5)).await, "t");
    assert_eq!(*r.tts.texts.lock().unwrap(), ["Sure, here you go."], "the marker is stripped before TTS");

    // The LLM's history keeps "● id transcript" (the format it must follow); the log has only what was said.
    let reqs = r.llm_reqs.lock().unwrap().clone();
    let assistant = reqs[1]["messages"].as_array().unwrap().iter().find(|m| m["role"] == "assistant").unwrap();
    assert_eq!(assistant["content"], "● offer_01 Our offer is great.");
    r.cancel.cancel();
    let (mut s, fin) = r.done.await.unwrap();
    fin.finish(&mut s).await;
    let said: Vec<_> =
        s.events.iter().filter(|e| e.kind == "rtf-bot-text").map(|e| e.payload["text"].as_str().unwrap().to_string()).collect();
    assert_eq!(said, ["Our offer is great.", "Sure, here you go."]);
}

#[tokio::test]
async fn a_missing_recording_is_spoken_instead_of_silence() {
    let wf = Workflow { start: 0, nodes: vec![Node { clips: Some(Arc::new(Default::default())), ..start_node() }] };
    let mut r = rig_wf(wf, 10, |_| sse("● gone_01 Please hold on.")).await;
    assert_eq!(heard(&mut r, 0, 10, Duration::from_secs(5)).await, "t");
    assert_eq!(*r.tts.texts.lock().unwrap(), ["Please hold on."]);
    r.cancel.cancel();
}

#[tokio::test]
async fn a_transition_clip_plays_before_the_next_nodes_reply() {
    let edge = Edge {
        def: FunctionDef { name: "go_next".into(), description: "when ready".into(), parameters: None },
        speech: String::new(),
        speech_clip: Some(clip("bridge", 6, "Let us begin.")),
        to: 1,
    };
    let wf = Workflow {
        start: 0,
        nodes: vec![
            Node { edges: vec![edge], ..start_node() },
            Node { name: "main".into(), prompt: "Ask the questions.".into(), allow_interrupt: true, ..Default::default() },
        ],
    };
    let mut r = rig_wf(wf, 10, |req| {
        // The second round runs at the next node: its own system prompt.
        if req["messages"][0]["content"] == "Ask the questions." {
            sse("First question?")
        } else {
            format!("data: {}\n\ndata: [DONE]\n\n", serde_json::json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"go_next","arguments":""}}]}}]}))
        }
    })
    .await;
    assert_eq!(heard(&mut r, 6, 10, Duration::from_secs(6)).await, "ct", "the clip first, then the LLM's reply");
    assert_eq!(*r.tts.texts.lock().unwrap(), ["First question?"]);
    r.cancel.cancel();
    let (mut s, fin) = r.done.await.unwrap();
    fin.finish(&mut s).await;
    assert_eq!(s.nodes_visited, ["start", "main"]);
}

// ---------------------------------------------------------- extraction ---

use super::workflow::{Extraction, ExtractionVar};

fn extraction(var: &str) -> Option<Arc<Extraction>> {
    Some(Arc::new(Extraction {
        prompt: String::new(),
        vars: vec![ExtractionVar { name: var.into(), kind: "string".into(), prompt: Some(format!("the {var}")) }],
    }))
}

#[tokio::test]
async fn variables_are_extracted_when_leaving_a_node_and_when_the_call_ends() {
    let edge = Edge {
        def: FunctionDef { name: "go_next".into(), description: "when ready".into(), parameters: None },
        speech: String::new(),
        speech_clip: None,
        to: 1,
    };
    let wf = Workflow {
        start: 0,
        nodes: vec![
            Node { edges: vec![edge], extraction: extraction("party"), ..start_node() },
            Node {
                name: "main".into(),
                prompt: "Ask the questions.".into(),
                allow_interrupt: true,
                extraction: extraction("vote"),
                ..Default::default()
            },
        ],
    };
    let mut r = rig_wf(wf, 10, |req| {
        let first = req["messages"][0]["content"].as_str().unwrap_or("");
        let user = req["messages"][1]["content"].as_str().unwrap_or("");
        if first.contains("extracting structured data") {
            // Leaving node 1 extracts the party; the call's end extracts the vote.
            let body = if user.contains("- party") {
                r#"{"party": "BJP", "zeta": 1}"#
            } else {
                "```json\n{\"vote\": \"yes\", \"party\": \"NOTA\"}\n```"
            };
            return sse(body);
        }
        if first == "Ask the questions." {
            return sse("First question?");
        }
        format!(
            "data: {}\n\ndata: [DONE]\n\n",
            serde_json::json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"go_next","arguments":""}}]}}]})
        )
    })
    .await;
    assert_eq!(heard(&mut r, 0, 10, Duration::from_secs(6)).await, "t");
    r.cancel.cancel();
    let (mut s, fin) = r.done.await.unwrap();
    assert!(s.extracted.is_none(), "nothing is extracted until the finisher runs (after teardown)");
    fin.finish(&mut s).await;

    let ex = s.extracted.expect("variables were extracted");
    // Later extractions win a conflict (the end-of-call one overwrote "party"); keys keep arrival order.
    assert_eq!(serde_json::Value::Object(ex), serde_json::json!({"party": "NOTA", "zeta": 1, "vote": "yes"}));
    assert_eq!(s.extracted_keys, ["party", "zeta", "vote"]);
}

// ------------------------------------------------- STT gate and flush ---

use super::sttgate::GateConfig;

async fn feed_frames(r: &Rig, f: Frame, n: usize) {
    for _ in 0..n {
        r.inbound.send(f).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn the_gate_sends_the_stt_only_speech_with_its_preroll() {
    let wf = Workflow { start: 0, nodes: vec![Node { greeting: "Hi.".into(), ..start_node() }] };
    let mut r = rig_tuned(
        wf,
        2,
        |_| sse("ok."),
        |c| {
            c.stt_gate = Some(GateConfig { preroll: Duration::from_millis(200), hangover: Duration::from_millis(200), rms_floor: 0.0 });
        },
    )
    .await;
    // Let the greeting play out, then 1s of quiet: the gate is closed, the STT is sent nothing.
    let _ = heard(&mut r, 0, 2, Duration::from_secs(3)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before = r.stt_audio.load(SeqCst);
    feed_frames(&r, SILENCE, 50).await;
    assert!(r.stt_audio.load(SeqCst) - before <= 2, "quiet frames are not sent ({} were)", r.stt_audio.load(SeqCst) - before);

    // Speech opens it: the 200ms pre-roll (10 frames) goes first, then the live frames.
    let before = r.stt_audio.load(SeqCst);
    feed_frames(&r, loud(), 10).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let sent = r.stt_audio.load(SeqCst) - before;
    assert!((18..=26).contains(&sent), "expected ~10 pre-roll + ~10 live frames, got {sent}");

    r.cancel.cancel();
    let (s, _) = r.done.await.unwrap();
    assert!(s.stt_gated);
    assert!(s.stt_seconds < 2.0, "billed audio is far below the call's length: {}s", s.stt_seconds);
}

#[tokio::test]
async fn the_stt_is_asked_to_finalize_once_the_vad_hears_quiet_after_speech() {
    let wf = Workflow { start: 0, nodes: vec![Node { greeting: "Hi.".into(), ..start_node() }] };
    let mut r = rig_tuned(wf, 2, |_| sse("ok."), |c| c.stt_flush_after = Duration::from_millis(200)).await;
    let _ = heard(&mut r, 0, 2, Duration::from_secs(3)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The STT reports speech starting; the (fake) VAD hears it; then the caller goes quiet.
    r.stt_tx.send(r#"{"type":"events","data":{"signal_type":"START_SPEECH"}}"#.into()).unwrap();
    feed_frames(&r, loud(), 10).await;
    assert_eq!(r.stt_flushes.load(SeqCst), 0, "no flush while they are still talking");
    feed_frames(&r, SILENCE, 20).await; // 400ms of quiet > 200ms
    assert_eq!(r.stt_flushes.load(SeqCst), 1, "one flush, sent once the quiet was long enough");
    feed_frames(&r, SILENCE, 20).await;
    assert_eq!(r.stt_flushes.load(SeqCst), 1, "at most once per utterance");
    r.cancel.cancel();
}

// ------------------------------------------------------------ transfer ---

use super::{
    handler::{ConnectFn, TransferFn},
    workflow::{Tool, ToolKind},
};

const HOLD_TONE: i16 = 3000;
const BEEP_TONE: i16 = 4000;

fn tone_pcm(v: i16, frames: usize) -> Arc<Vec<u8>> {
    Arc::new((0..frames * FRAME_SIZE / 2).flat_map(|_| v.to_le_bytes()).collect())
}

/// A workflow whose start node has a transfer tool (with a spoken message).
fn transfer_workflow() -> Workflow {
    let tool = Tool {
        def: FunctionDef { name: "transfer_to_support".into(), description: "hand over".into(), parameters: None },
        kind: ToolKind::Transfer { destination: "PJSIP/100@pbx".into(), timeout: Duration::from_secs(5) },
        message: "Please hold.".into(),
        message_clip: None,
    };
    Workflow { start: 0, nodes: vec![Node { tools: vec![tool], ..start_node() }] }
}

fn transfer_call_sse() -> String {
    format!(
        "data: {}\n\ndata: [DONE]\n\n",
        serde_json::json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"t1","function":{"name":"transfer_to_support","arguments":""}}]}}]})
    )
}

/// Collects the audio kinds heard until `max` elapses: 'm' TTS message, 'h' hold loop, 'b' beep.
async fn heard_kinds(r: &mut Rig, max: Duration) -> String {
    let mut order = String::new();
    let _ = tokio::time::timeout(max, async {
        while let Some(f) = r.outbound.recv().await {
            let first = [f[0], f[1]];
            let kind = match first {
                x if x == TONE.to_le_bytes() => 'm',
                x if x == HOLD_TONE.to_le_bytes() => 'h',
                x if x == BEEP_TONE.to_le_bytes() => 'b',
                _ => continue,
            };
            if !order.ends_with(kind) {
                order.push(kind);
            }
        }
    })
    .await;
    order
}

#[tokio::test]
async fn a_transfer_rings_with_hold_audio_beeps_and_hands_the_call_over() {
    let connected = Arc::new(AtomicUsize::new(0));
    type Dialed = Arc<Mutex<Vec<(String, Duration)>>>;
    let (dialed, cancel_slot): (Dialed, Arc<Mutex<Option<CancellationToken>>>) = Default::default();
    let transfer: TransferFn = {
        let (connected, dialed, cancel_slot) = (connected.clone(), dialed.clone(), cancel_slot.clone());
        Arc::new(move |dest, timeout| {
            let (connected, dialed, cancel_slot) = (connected.clone(), dialed.clone(), cancel_slot.clone());
            Box::pin(async move {
                dialed.lock().unwrap().push((dest, timeout));
                tokio::time::sleep(Duration::from_millis(400)).await; // ringing
                let connect: ConnectFn = Box::new(move || {
                    Box::pin(async move {
                        connected.fetch_add(1, SeqCst);
                        // Like the manager: the media token is cancelled, which ends the agent.
                        cancel_slot.lock().unwrap().take().unwrap().cancel();
                        Ok(())
                    })
                });
                Ok(connect)
            })
        })
    };
    let mut r = rig_tuned(transfer_workflow(), 6, |_| transfer_call_sse(), move |c| c.transfer = Some(transfer)).await;
    *cancel_slot.lock().unwrap() = Some(r.cancel.clone());

    let order = heard_kinds(&mut r, Duration::from_secs(3)).await;
    assert_eq!(order, "mhb", "the message first, then the hold loop while ringing, then the beep");
    assert_eq!(*dialed.lock().unwrap(), [("PJSIP/100@pbx".to_string(), Duration::from_secs(5))]);
    assert_eq!(connected.load(SeqCst), 1);
    assert_eq!(r.llm_reqs.lock().unwrap().len(), 1, "after a transfer the LLM isn't asked again");
    assert_eq!(r.hangups.load(SeqCst), 0, "the agent doesn't hang up: the caller is with the destination");

    let (s, _) = r.done.await.unwrap();
    assert_eq!(s.end_reason, Some("transfer_call"));
}

#[tokio::test]
async fn a_failed_transfer_tells_the_llm_and_the_call_carries_on() {
    let transfer: TransferFn =
        Arc::new(|_, _| Box::pin(async { Err(anyhow::anyhow!("transfer destination did not answer (Busy, cause 17)")) }));
    let mut r = rig_tuned(
        transfer_workflow(),
        6,
        |req| {
            let failed = req["messages"]
                .as_array()
                .unwrap()
                .iter()
                .any(|m| m["role"] == "tool" && m["content"].as_str().is_some_and(|c| c.contains("Busy")));
            if failed { sse("Sorry, nobody is available.") } else { transfer_call_sse() }
        },
        |c| c.transfer = Some(transfer),
    )
    .await;
    let order = heard_kinds(&mut r, Duration::from_secs(3)).await;
    assert!(order.starts_with('m') && order.ends_with('m'), "message, then the apology: {order}");
    assert!(!order.ends_with('h'), "no hold loop remains");
    assert_eq!(*r.tts.texts.lock().unwrap(), ["Please hold. ", "Sorry, nobody is available."]);
    r.cancel.cancel();
    let (s, _) = r.done.await.unwrap();
    assert_eq!(s.end_reason, None, "a failed transfer doesn't end the call");
}

/// Caller audio that arrives in a clump (the call starting up, Asterisk catching up after a stall) all
/// reaches the STT: the agent takes every frame waiting on a tick, not one. Taking one per tick could
/// never catch up, and what overflowed the queue was the caller's first words.
#[tokio::test]
async fn a_clump_of_caller_audio_all_reaches_the_stt() {
    let r = rig(10, false, "ok").await;
    tokio::time::sleep(Duration::from_millis(100)).await; // the agent is ticking
    let before = r.stt_audio.load(SeqCst);
    for _ in 0..30 {
        r.inbound.try_send(crate::media::SILENCE).expect("the queue holds a second of audio");
    }
    // 30 frames are 600 ms of audio; at one frame per tick only ~5 would be through after 100 ms.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let got = r.stt_audio.load(SeqCst) - before;
    assert!(got >= 30, "all 30 frames of the clump were heard at once, got {got}");
    assert!(got <= 40, "and nothing was sent twice, got {got}");
}

//! The per-call agent: caller audio -> STT -> LLM -> sentence chunks -> TTS ->
//! 20ms playout, with barge-in.
//!
//! One task owns ALL mutable call state and multiplexes three things in a
//! `select!`: the 20ms release tick (caller frame in, agent frame out), the
//! STT's events, and events posted by the helper tasks (LLM turn, TTS
//! forwarder, timers). No locks and no cross-thread flags: the Go version's
//! atomics-plus-signals between the frame goroutine and its event goroutine
//! become plain method calls.
//!
//! States: Listening -> (STT final) -> Thinking -> (first TTS audio) ->
//! Speaking -> (queue drained + TTS delivery done) -> Listening.
//!
//! INVARIANT #7 (barge-in is 5 cuts): the cuts run only once an interruption
//! is CONFIRMED. A detected one first *pauses* playback (no frames popped, STT
//! still fed, LLM/TTS keep queueing); a transcript confirms it and runs all
//! five cuts (stop feeding the outbound queue, STT context, LLM cancel, TTS
//! connection close, queue flush) while no transcript and 2s of quiet resumes
//! the reply from the start of the interrupted sentence. A pause is not a cut.
//! The STT is fed the caller's audio continuously in every state; transcripts
//! are filtered, not audio.

use super::{
    backchannel::backchannel,
    calllog::{self, CallLog, Summary},
    extract::{self, Finisher},
    recorder::MixRecorder,
    recordings::{ClipRouter, RECORDING_MARKER, strip_recording_refs},
    sttgate::{GateConfig, SttGate},
    vad::BargeInDetector,
    workflow::{Clip, Edge, Node, Tool, ToolKind, Workflow, plain_history, request_messages},
};
use crate::{
    ai::{
        chunker::{SentenceChunker, is_digit, is_letter, is_mark},
        llm::{LlmClient, LlmError, Message, ToolCall, Usage},
        stt::{SttClient, SttEvent},
        tts::{TtsClient, TtsConfig, TtsEvent},
    },
    media::{
        FRAME_SIZE, Frame, SILENCE,
        call::{MediaIo, new_ticker},
    },
    metrics,
};
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio_util::sync::CancellationToken;

/// ~30s of queued playout; synthesis outruns real time so a whole reply can
/// arrive long before it has played.
const OUTBOUND_BUFFER_FRAMES: usize = 1500;
/// 10s of ticks Speaking with an empty queue and no TTS delivery-done: dead.
const SPEAKING_DRAIN_TIMEOUT_TICKS: u32 = 500;
/// A paused reply resumes when no transcript confirmed the interruption and the
/// caller has been quiet this long; the cap holds even if the STT's VAD never
/// reports the caller stopping.
const FALSE_INTERRUPTION: Duration = Duration::from_secs(2);
const FALSE_INTERRUPTION_MAX_PAUSE: Duration = Duration::from_secs(10);
const MAX_TOOL_ROUNDS: usize = 5;
const TOOL_TIMEOUT: Duration = Duration::from_secs(10);
/// An STT speech start older than this belongs to an earlier utterance.
const MAX_UTTERANCE_AGE: Duration = Duration::from_secs(60);

/// Dograh's caller-silence instructions, added as user messages exactly as it does.
const IDLE_FIRST: &str =
    "The user has been quiet. Politely and briefly ask if they're still there in the language that the user has been speaking so far.";
const IDLE_FINAL: &str = "The user has been quiet. We will be disconnecting the call now. Wish them a good day in the language that the user has been speaking so far.";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum State {
    Listening,
    Transcribing,
    Thinking,
    Speaking,
}

/// Hands the caller over to the transfer destination (called once it answered).
pub type ConnectFn = Box<dyn FnOnce() -> futures_util::future::BoxFuture<'static, anyhow::Result<()>> + Send>;
/// Dials `destination` and waits (up to `timeout`) for it to answer; on success returns the
/// function that completes the handover.
pub type TransferFn = Arc<dyn Fn(String, Duration) -> futures_util::future::BoxFuture<'static, anyhow::Result<ConnectFn>> + Send + Sync>;

/// How long hold music waits for the transfer's spoken message to start arriving from TTS before
/// starting anyway -- so the ring doesn't start and then get interrupted by the message.
const HOLD_MESSAGE_WAIT_TICKS: u32 = 150;

pub struct AgentConfig {
    pub call_id: String,
    pub workflow: Arc<Workflow>,
    pub llm: LlmClient,
    pub tts: TtsConfig,
    pub vad: Box<dyn BargeInDetector>,
    pub barge_in_enabled: bool,
    pub barge_in_min_speech: Duration,
    pub barge_in_guard: Duration,
    pub post_cut_silence: Duration,
    /// 0 = off: see `Config::stt_flush_after`.
    pub stt_flush_after: Duration,
    /// `Some`: send the STT only the caller's speech (see `sttgate`).
    pub stt_gate: Option<GateConfig>,
    pub ack_filter: bool,
    pub ack_end_margin: Duration,
    /// 0 disables caller-silence handling.
    pub idle_timeout: Duration,
    /// 0 = no cap.
    pub max_duration: Duration,
    pub llm_max_failed_turns: u32,
    pub llm_fallback_text: String,
    /// Hangs the caller up (fire-and-forget: it's an ARI REST call).
    pub hangup: Arc<dyn Fn() + Send + Sync>,
    /// `None`: the switch can't transfer, and a transfer tool reports so to the LLM.
    pub transfer: Option<TransferFn>,
    /// Loops to the caller while a transfer rings; the beep plays when it answers.
    pub hold_audio: Arc<Vec<u8>>,
    pub beep_audio: Arc<Vec<u8>>,
}

pub struct AgentIo {
    pub media: MediaIo,
    pub stt: SttClient,
    pub stt_events: UnboundedReceiver<SttEvent>,
}

// ---------------------------------------------------------------- events ---

enum Event {
    /// TTS connection `conn` was opened by a turn task.
    TtsOpened {
        conn: u64,
        client: Arc<TtsClient>,
    },
    TtsOpenFailed {
        generation: u64,
    },
    Tts {
        conn: u64,
        ev: TtsEvent,
    },
    NodeEntered {
        generation: u64,
        idx: usize,
    },
    /// The call is ending (end_call tool, or an end node was reached): no more interruptions.
    Closing {
        generation: u64,
        reason: &'static str,
    },
    /// The conversation is leaving node `node`, which has an extraction: run it in the background.
    StartExtraction {
        node: usize,
        history: Vec<Message>,
    },
    /// The LLM called a tool or took an edge (`result` set when it finished).
    Function {
        name: String,
        call_id: String,
        result: Option<String>,
    },
    /// A transfer is ringing or has answered: what the caller hears meanwhile.
    Hold(HoldCmd),
    LlmUsage(Usage),
    TtsChars(u64),
    LlmDone(LlmDone),
    Resume(u64),
    Idle(u64),
    MaxDuration,
    /// end_call ran in a turn that an interruption then cancelled: the call must still end.
    EndCallTool,
}

enum HoldCmd {
    /// Loop the hold audio (after the tool's spoken message, if `wait_for_message`).
    Ring {
        wait_for_message: bool,
    },
    /// Play the beep once; `done` fires when it finished.
    Beep {
        done: tokio::sync::oneshot::Sender<()>,
    },
    Off,
}

/// Canned audio (hold loop or beep) fed to the caller frame by frame during a transfer.
struct Hold {
    pcm: Arc<Vec<u8>>,
    looped: bool,
    wait_for_message: bool,
    pos: usize,
    waited: u32,
    saw_message: bool,
    done: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Hold {
    /// The next 20ms frame, or `None` once a one-shot player has finished (or there's nothing to play).
    fn next(&mut self) -> Option<Frame> {
        if self.pos + FRAME_SIZE > self.pcm.len() {
            if !self.looped || self.pcm.len() < FRAME_SIZE {
                if let Some(d) = self.done.take() {
                    let _ = d.send(());
                }
                return None;
            }
            self.pos = 0;
        }
        let mut f = [0u8; FRAME_SIZE];
        f.copy_from_slice(&self.pcm[self.pos..self.pos + FRAME_SIZE]);
        self.pos += FRAME_SIZE;
        Some(f)
    }
}

struct LlmDone {
    generation: u64,
    failed: bool,
    tool_msgs: Vec<Message>,
    end_call: bool,
    ttft: Option<Duration>,
}

struct OutFrame {
    pcm: Frame,
    generation: u64,
    req: u64,
}

struct SpokenChunk {
    req: u64,
    text: String,
}

struct Held {
    text: String,
    generation: u64,
    at: Instant,
}

struct TtsHandle {
    client: Arc<TtsClient>,
    conn: u64,
}

// ----------------------------------------------------------------- agent ---

struct Agent {
    cfg: AgentConfig,
    id: String,
    ev_tx: UnboundedSender<Event>,
    stt: SttClient,
    cancel: CancellationToken,
    log: CallLog,
    recorder: MixRecorder,
    stt_frames: u64,
    gate: Option<SttGate>,
    flushed_utt: Option<Instant>,
    llm_usage_key: String,
    turn_speech_end: Option<Instant>,
    /// Extractions running in the background, in the order they started.
    extracting: Vec<extract::Pending>,
    hold: Option<Hold>,

    state: State,
    node: usize,
    history: Vec<Message>,
    generation: u64,
    turn: u32,
    turn_cancel: Option<CancellationToken>,
    llm_failures: Arc<AtomicU32>,
    tool_running: Arc<AtomicBool>,
    transferred: Arc<AtomicBool>,

    tts: Option<TtsHandle>,
    next_conn: u64,
    tts_buf: Vec<u8>,
    out_q: VecDeque<OutFrame>,
    tts_delivery_done: bool,
    empty_ticks: u32,
    drop_warned_gen: u64,

    // reply bookkeeping (what the caller actually heard)
    turn_chunks: Vec<SpokenChunk>,
    turn_tool_msgs: Vec<Message>,
    hangup_after_turn: bool,
    playing_req: u64,
    playing_since: Instant,
    turn_started: Instant,
    ttfa_logged_gen: u64,
    turn_ttfa: Option<Duration>,
    turn_ttft: Option<Duration>,

    // barge-in
    paused: bool,
    paused_at: Instant,
    speaking_since: Instant,
    speech_run_start: Option<Instant>,
    silence_until: Option<Instant>,
    resume_seq: u64,
    // replay of the interrupted sentence after a false interruption
    sent_frames: Vec<Frame>,
    sent_gen: u64,
    sent_req: u64,
    replay_pending: bool,
    replaying: bool,
    replay_idx: usize,
    held: Vec<Held>,

    // caller timing
    caller_speaking: bool,
    utt_start: Option<Instant>,
    speech_end_at: Option<Instant>,
    prev_speech_end_at: Option<Instant>,
    agent_quiet_since: Instant,

    // idle / ending
    idle_task: Option<tokio::task::AbortHandle>,
    idle_seq: u64,
    idle_count: u32,
    idle_armed: bool,
    ending: bool,
    closing: bool,
    hung_up: bool,
    /// Why the agent hung up, for its log line (the Dograh disposition is in `log`).
    end_label: String,
}

/// Runs the agent for one call until the call ends (`cancel`, far-end hangup).
pub async fn run(cfg: AgentConfig, io: AgentIo, cancel: CancellationToken) -> (Summary, Finisher) {
    let (ev_tx, mut events) = mpsc::unbounded_channel();
    let AgentIo { mut media, stt, mut stt_events } = io;
    let now = Instant::now();
    let node = cfg.workflow.start;
    let gate = cfg.stt_gate.map(|g| SttGate::new(g, now));
    let llm_usage_key = format!("LLM|||{}", cfg.llm.model);
    let mut a = Agent {
        id: cfg.call_id.clone(),
        cfg,
        ev_tx,
        stt,
        cancel: cancel.clone(),
        log: CallLog::default(),
        recorder: MixRecorder::default(),
        stt_frames: 0,
        gate,
        flushed_utt: None,
        llm_usage_key,
        turn_speech_end: None,
        extracting: vec![],
        hold: None,
        state: State::Listening,
        node,
        history: vec![],
        generation: 0,
        turn: 0,
        turn_cancel: None,
        llm_failures: Arc::new(AtomicU32::new(0)),
        tool_running: Arc::new(AtomicBool::new(false)),
        transferred: Arc::new(AtomicBool::new(false)),
        tts: None,
        next_conn: 0,
        tts_buf: vec![],
        out_q: VecDeque::new(),
        tts_delivery_done: false,
        empty_ticks: 0,
        drop_warned_gen: 0,
        turn_chunks: vec![],
        turn_tool_msgs: vec![],
        hangup_after_turn: false,
        playing_req: 0,
        playing_since: now,
        turn_started: now,
        ttfa_logged_gen: 0,
        turn_ttfa: None,
        turn_ttft: None,
        paused: false,
        paused_at: now,
        speaking_since: now,
        speech_run_start: None,
        silence_until: None,
        resume_seq: 0,
        sent_frames: vec![],
        sent_gen: 0,
        sent_req: 0,
        replay_pending: false,
        replaying: false,
        replay_idx: 0,
        held: vec![],
        caller_speaking: false,
        utt_start: None,
        speech_end_at: None,
        prev_speech_end_at: None,
        agent_quiet_since: now,
        idle_task: None,
        idle_seq: 0,
        idle_count: 0,
        idle_armed: false,
        ending: false,
        closing: false,
        hung_up: false,
        end_label: String::new(),
    };

    if a.cfg.max_duration > Duration::ZERO {
        let (tx, d, c) = (a.ev_tx.clone(), a.cfg.max_duration, cancel.clone());
        tokio::spawn(async move {
            tokio::select! { _ = c.cancelled() => {}, _ = tokio::time::sleep(d) => { let _ = tx.send(Event::MaxDuration); } }
        });
    }

    a.start();

    let mut tick = new_ticker();
    let mut backlog: Vec<Frame> = Vec::new(); // reused: allocates only the first time frames arrive late
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            _ = media.closed.cancelled() => break,
            _ = tick.tick() => {
                // Everything the caller sent since the last tick. Normally one frame; several after the
                // call's start-up or a clump from Asterisk. The older ones are heard (STT, VAD, recording)
                // and the newest takes the tick: taking one per tick could never catch up, and the queue's
                // overflow was the caller's words.
                let mut pcm = media.inbound.try_recv().unwrap_or(SILENCE);
                let mut late = 0;
                while let Ok(next) = media.inbound.try_recv() {
                    a.hear_late(&pcm);
                    backlog.push(pcm);
                    pcm = next;
                    late += 1;
                }
                if late > 0 {
                    metrics::MEDIA_INBOUND_LATE.fetch_add(late, Relaxed);
                    a.recorder.late(&backlog);
                    backlog.clear();
                }
                let out = a.process_frame(&pcm);
                a.recorder.frame(&pcm, out.as_ref());
                if let Some(out) = out
                    && media.outbound.try_send(out).is_err()
                {
                    metrics::MEDIA_QUEUE_DROPPED.fetch_add(1, Relaxed); // queue full: dropped, never blocks the tick
                }
            }
            Some(ev) = stt_events.recv() => a.on_stt(ev),
            Some(ev) = events.recv() => a.on_event(ev),
        }
    }

    // The call ended mid-reply (usually the caller hanging up): log what they
    // heard of it, which would otherwise be lost.
    if !a.turn_chunks.is_empty() {
        a.record_reply(true);
    }
    a.stop_idle_timer();
    if let Some(t) = a.tts.take() {
        t.client.cancel();
    }
    a.cancel_turn();
    if a.transferred.load(Relaxed) {
        a.log.ending(calllog::TRANSFER);
    }
    a.stt_frames_to_log();
    // Dograh extracts the last node's variables as the call ends, after any still
    // running from earlier nodes: handed back, to run after teardown.
    let last = a.node().extraction.clone().map(|x| (a.node().name.clone(), (*x).clone(), std::mem::take(&mut a.history)));
    let finisher = Finisher { call_id: a.id.clone(), llm: a.cfg.llm.clone(), pending: std::mem::take(&mut a.extracting), last };
    let truncated = a.recorder.truncated;
    if truncated {
        metrics::RECORDING_TRUNCATED.fetch_add(1, Relaxed);
    }
    let mut summary = a.log.into_summary();
    summary.recording_truncated = truncated;
    summary.recording = a.recorder.finish();
    (summary, finisher)
}

impl Agent {
    fn node(&self) -> &Node {
        &self.cfg.workflow.nodes[self.node]
    }

    fn agent_error(&mut self, stage: &str) {
        self.log.error(stage);
    }

    /// The caller audio handed to the STT is what Sarvam bills: every frame of the
    /// call, or with the gate only the speech (and its pre-roll).
    fn stt_frames_to_log(&mut self) {
        self.log.stt_seconds = match &self.gate {
            Some(g) => g.sent_seconds(),
            None => self.stt_frames as f64 * crate::media::FRAME_INTERVAL.as_secs_f64(),
        };
        self.log.stt_gated = self.gate.is_some();
        metrics::add_stt_seconds(self.log.stt_seconds);
    }

    /// Hands the caller's frame to the STT: all of them, or with the gate only
    /// the speech (the gate keeps the audio before it).
    fn feed_stt(&mut self, pcm: &Frame) {
        let Some(gate) = self.gate.as_mut() else {
            self.stt_frames += 1;
            if !self.stt.feed(pcm) {
                self.log.error("stt");
            }
            return;
        };
        let now = Instant::now();
        let why = gate.cfg().voiced(self.cfg.vad.last_speech_at(), now, pcm);
        let (stt, mut failed) = (&self.stt, false);
        if gate.frame(pcm, why, now, &mut |f| failed |= !stt.feed(f)) {
            tracing::debug!(call_id = %self.id, component = "stt", event = "stt.gate_open", reason = ?why, preroll_ms = 0, "stt gate open");
        }
        if failed {
            self.log.error("stt");
        }
    }

    /// Asks the STT to finalize the current utterance once the VAD has heard
    /// `stt_flush_after` of quiet after it -- at most once per utterance.
    fn maybe_flush_stt(&mut self) {
        /// The VAD's last speech must be no older than this before the STT's onset.
        const ONSET_SLACK: Duration = Duration::from_millis(500);
        let Some(utt) = self.utt_start.filter(|u| self.caller_speaking && self.flushed_utt != Some(*u)) else { return };
        // The VAD didn't hear this utterance: no flush, the STT's own end-of-speech decides.
        let Some(last) = self.cfg.vad.last_speech_at().filter(|l| *l >= utt.checked_sub(ONSET_SLACK).unwrap_or(utt)) else { return };
        let quiet = last.elapsed();
        if quiet < self.cfg.stt_flush_after {
            return;
        }
        self.flushed_utt = Some(utt);
        if self.stt.flush() {
            tracing::debug!(call_id = %self.id, event = "stt.flush", quiet_ms = quiet.as_millis() as u64, "stt flush sent");
        } else {
            self.log.error("stt");
        }
    }

    fn interruptible(&self) -> bool {
        self.cfg.barge_in_enabled && self.node().allow_interrupt && !self.closing && !self.node().end
    }

    fn post_after(&self, after: Duration, ev: Event) {
        let (tx, c) = (self.ev_tx.clone(), self.cancel.clone());
        tokio::spawn(async move {
            tokio::select! { _ = c.cancelled() => {}, _ = tokio::time::sleep(after) => { let _ = tx.send(ev); } }
        });
    }

    // ------------------------------------------------------------- start ---

    fn start(&mut self) {
        let n = self.node();
        let (id, name, allow) = (n.id.clone(), n.name.clone(), n.allow_interrupt);
        self.log.node_entered(&id, &name, allow);
        let n = self.node();
        let greeting = match (&n.greeting_clip, n.greeting.is_empty()) {
            (Some(clip), _) => Some(Greeting::Clip(clip.clone())),
            (None, false) => Some(Greeting::Text(n.greeting.clone())),
            (None, true) => None,
        };
        match greeting {
            Some(g) => self.start_greeting(g),
            None => {
                tracing::debug!(call_id = %self.id, event = "agent.opening", "llm speaks first");
                self.start_turn();
            }
        }
    }

    // ------------------------------------------------------- frame handling ---

    /// One 20ms tick: feed the caller's frame onward, return the agent's frame
    /// (None = nothing to play; the writer sends silence).
    fn process_frame(&mut self, pcm: &Frame) -> Option<Frame> {
        // The caller's audio always reaches the STT, whatever the agent is
        // doing: feeding only while listening clipped the first syllable of
        // answers given over the agent. What to do with each transcript is
        // decided when it arrives.
        // During a transfer the caller is waiting: already-queued agent speech (the
        // "please stay on the line" message) plays first, then the hold audio.
        // Nothing reaches the STT and barge-in is off.
        if self.hold.is_some() {
            return self.process_hold_frame();
        }
        self.feed_stt(pcm);
        if !self.cfg.stt_flush_after.is_zero() {
            self.maybe_flush_stt();
        }
        match self.state {
            State::Speaking => self.process_speaking_frame(pcm),
            _ => {
                // Same frame the STT just got, so the VAD's "last speech" lines
                // up with what Sarvam endpoints. Timing only; verdict unused.
                self.cfg.vad.detect(pcm);
                None
            }
        }
    }

    /// A caller frame that arrived behind others in one tick: it reaches the STT and the VAD like any
    /// frame, but takes no tick of its own (nothing is played for it, and it decides no barge-in).
    fn hear_late(&mut self, pcm: &Frame) {
        if self.hold.is_some() {
            return; // during a transfer nothing reaches the STT
        }
        self.feed_stt(pcm);
        self.cfg.vad.detect(pcm);
    }

    fn process_hold_frame(&mut self) -> Option<Frame> {
        if let Some(f) = self.out_q.pop_front() {
            self.playing_req = f.req;
            self.hold.as_mut()?.saw_message = true;
            return Some(f.pcm);
        }
        let h = self.hold.as_mut()?;
        if h.wait_for_message && !h.saw_message && h.waited < HOLD_MESSAGE_WAIT_TICKS {
            h.waited += 1;
            return None;
        }
        h.next()
    }

    fn process_speaking_frame(&mut self, pcm: &Frame) -> Option<Frame> {
        // Paused on a possible interruption: play nothing, leave the queued
        // reply where it is. The STT is still being fed; the VAD keeps timing.
        if self.paused {
            self.cfg.vad.detect(pcm);
            return None;
        }

        // Runs on every Speaking frame, cuts enabled or not (observe-only mode).
        let detected = self.cfg.vad.detect(pcm);
        let now = Instant::now();
        if !detected {
            self.speech_run_start = None;
        } else if self.speech_run_start.is_none() {
            self.speech_run_start = Some(now);
        }

        let run = self.speech_run_start.map(|s| now - s).unwrap_or_default();
        if detected && run >= self.cfg.barge_in_min_speech && now - self.speaking_since >= self.cfg.barge_in_guard && self.interruptible() {
            // Pause, don't cut: whether this is the caller or noise isn't known yet.
            self.paused = true;
            self.log.pauses += 1;
            metrics::AGENT_INTERRUPTION_PAUSES.fetch_add(1, Relaxed);
            self.paused_at = now;
            self.cfg.vad.reset();
            self.speech_run_start = None;
            self.silence_until = Some(now + self.cfg.post_cut_silence);
            self.empty_ticks = 0;
            tracing::info!(call_id = %self.id, event = "agent.barge_in.detected", "agent barge-in detected; pausing the reply");
            self.schedule_resume_check(FALSE_INTERRUPTION);
            return None;
        }

        // After a false interruption: replay the interrupted sentence from its start.
        if self.replay_pending {
            self.replay_pending = false;
            self.replay_idx = 0;
            self.replaying = !self.sent_frames.is_empty();
        }
        if self.replaying {
            if let Some(f) = self.sent_frames.get(self.replay_idx) {
                self.replay_idx += 1;
                self.empty_ticks = 0;
                return Some(*f);
            }
            self.replaying = false;
        }

        if let Some(f) = self.out_q.pop_front() {
            self.empty_ticks = 0;
            if f.generation != self.sent_gen || f.req != self.sent_req {
                (self.sent_gen, self.sent_req) = (f.generation, f.req);
                self.sent_frames.clear(); // a new sentence starts playing
                self.playing_since = now;
            }
            self.playing_req = f.req;
            self.sent_frames.push(f.pcm);
            return Some(f.pcm);
        }

        // Nothing queued. If TTS finished delivering this generation too, the
        // reply is fully played: end the turn.
        if self.tts_delivery_done {
            self.tts_delivery_done = false;
            self.empty_ticks = 0;
            self.finish_turn();
            return None;
        }

        if self.tool_running.load(Relaxed) {
            self.empty_ticks = 0; // waiting on a tool, not a dead TTS connection
            return None;
        }

        // Dead-call safeguard: TTS went silent mid-turn without delivery-done.
        self.empty_ticks += 1;
        if self.empty_ticks >= SPEAKING_DRAIN_TIMEOUT_TICKS {
            self.empty_ticks = 0;
            self.agent_error("tts_drain_timeout");
            tracing::warn!(call_id = %self.id, "speaking with empty outbound queue and no tts delivery-done for 10s; forcing turn end (tts connection likely dead)");
            self.state = State::Listening;
            self.tts_buf.clear();
            self.agent_quiet_since = now;
            self.record_reply(true);
            if self.ending {
                self.hangup();
            }
        }
        None
    }

    // ------------------------------------------------------------- events ---

    fn on_event(&mut self, ev: Event) {
        match ev {
            Event::TtsOpened { conn, client } => {
                if conn == self.next_conn && self.tts.is_none() {
                    self.tts = Some(TtsHandle { client, conn });
                } else {
                    client.cancel(); // a cut superseded the turn that opened it
                }
            }
            Event::TtsOpenFailed { generation } => {
                if generation == self.generation {
                    self.agent_error("tts_open");
                    self.state = State::Listening;
                    self.cancel_turn();
                    tracing::error!(call_id = %self.id, event = "tts.error", generation, "tts open failed");
                }
            }
            Event::Tts { conn, ev } => match ev {
                TtsEvent::Audio { pcm, generation, req, text } => self.on_tts_audio(&pcm, generation, req, text),
                TtsEvent::Done(g) => self.on_tts_done(g),
                TtsEvent::Closed => self.on_tts_closed(conn),
            },
            Event::NodeEntered { generation, idx } => {
                if generation == self.generation {
                    self.node = idx;
                    let n = self.node();
                    let (id, name, allow) = (n.id.clone(), n.name.clone(), n.allow_interrupt);
                    self.log.node_entered(&id, &name, allow);
                }
            }
            Event::Closing { generation, reason } => {
                self.log.ending(reason);
                if generation == self.generation {
                    self.closing = true; // no interrupting the goodbye, as in Dograh
                }
            }
            Event::StartExtraction { node, history } => {
                if let Some(x) = self.cfg.workflow.nodes[node].extraction.clone() {
                    let (id, llm, name) = (self.id.clone(), self.cfg.llm.clone(), self.cfg.workflow.nodes[node].name.clone());
                    self.extracting.push(tokio::spawn(async move { extract::extract(&id, &name, &llm, &x, &history).await }));
                }
            }
            Event::Function { name, call_id, result } => match result {
                None => self.log.function_started(&name, &call_id),
                Some(r) => self.log.function_ended(&name, &call_id, &r),
            },
            Event::Hold(cmd) => {
                let hold = |pcm: &Arc<Vec<u8>>, looped, wait_for_message, done| Hold {
                    pcm: pcm.clone(),
                    looped,
                    wait_for_message,
                    pos: 0,
                    waited: 0,
                    saw_message: false,
                    done,
                };
                self.hold = match cmd {
                    HoldCmd::Ring { wait_for_message } => Some(hold(&self.cfg.hold_audio, true, wait_for_message, None)),
                    HoldCmd::Beep { done } => Some(hold(&self.cfg.beep_audio, false, false, Some(done))),
                    HoldCmd::Off => None,
                };
            }
            Event::LlmUsage(u) => {
                let key = self.llm_usage_key.clone();
                self.log.llm_used(&key, u);
            }
            Event::TtsChars(n) => self.log.tts_chars += n,
            Event::LlmDone(e) => self.on_llm_done(e),
            Event::Resume(seq) => self.on_resume_check(seq),
            Event::Idle(seq) => self.on_idle(seq),
            Event::MaxDuration => {
                tracing::info!(call_id = %self.id, event = "call.max_duration", "max call duration reached; ending the call");
                self.log.ending(calllog::MAX_DURATION);
                self.end_call("max_call_duration");
            }
            Event::EndCallTool => self.end_call("end_call"),
        }
    }

    // ---------------------------------------------------------------- STT ---

    fn on_stt(&mut self, ev: SttEvent) {
        match ev {
            SttEvent::SpeechStarted => self.on_caller_speech(true),
            SttEvent::SpeechEnded => self.on_caller_speech(false),
            SttEvent::Final(text) => self.on_final_transcript(text),
        }
    }

    /// The caller started speaking (stop the silence clock) or stopped. When
    /// they stopped without it becoming a turn, the clock restarts, so noise
    /// that never became words can't switch silence detection off for good.
    fn on_caller_speech(&mut self, started: bool) {
        self.caller_speaking = started;
        let now = Instant::now();
        if started {
            self.utt_start = Some(now);
            self.speech_end_at = None;
            self.stop_idle_timer();
            self.idle_count = 0;
            tracing::debug!(call_id = %self.id, event = "stt.speech_started", state = ?self.state, "stt speech started");
            return;
        }
        self.speech_end_at = Some(now);
        tracing::debug!(call_id = %self.id, event = "stt.speech_ended", "stt speech ended");
        self.prev_speech_end_at = Some(now);
        if self.idle_armed && self.state == State::Listening {
            self.start_idle_timer();
        }
    }

    fn on_final_transcript(&mut self, text: String) {
        let speech_end = self.speech_end_at.take();
        let ignore = |a: &Self, why: &str| {
            tracing::info!(call_id = %a.id, event = "stt.transcript.ignored", why, text = %text, "transcript ignored");
        };

        if self.silence_until.is_some_and(|t| Instant::now() < t) {
            // Arrived too soon after the pause to be what the caller says now:
            // Sarvam needs ~0.7s after they stop, so this one was already on its
            // way. It neither confirms the interruption nor starts a turn.
            return ignore(self, "post_cut");
        }

        if self.paused {
            if junk_transcript(&text) {
                return ignore(self, "junk"); // a lone letter is what noise transcribes to
            }
            if self.closing {
                ignore(self, "closing"); // the caller is muted while the goodbye plays
                return self.resume_reply("call ending");
            }
            if self.cfg.ack_filter && backchannel(&text) {
                // "जी", "हाँ", "ok" over a paused reply: not "stop". Held, not
                // dropped: said during the reply's last sentence it's an answer.
                self.hold_speech(text, "an acknowledgement over a paused reply");
                return self.resume_reply("backchannel");
            }
            self.confirm_interruption(); // real: cut, and this becomes the next turn
        }

        // Said entirely while the agent was preparing or speaking, with no
        // interruption: the caller is muted, as in Dograh (its mute discards
        // transcripts, not audio). An answer that began over the agent but
        // ended after it arrives in Listening instead, whole.
        match self.state {
            State::Speaking if self.cfg.ack_filter && self.interruptible() && !junk_transcript(&text) => {
                return self.hold_speech(text, "said over the agent, too short to pause it");
            }
            State::Speaking => return ignore(self, "agent_speaking"),
            State::Thinking => return ignore(self, "agent_thinking"),
            _ => {}
        }
        let started = self.utterance_start();
        self.accept_user_turn(text, started, speech_end);
    }

    fn utterance_start(&self) -> Option<Instant> {
        self.utt_start.filter(|t| t.elapsed() < MAX_UTTERANCE_AGE)
    }

    fn accept_user_turn(&mut self, text: String, started: Option<Instant>, speech_end: Option<Instant>) {
        self.history.push(Message::new("user", text.clone()));
        self.turn += 1;
        self.log.user_said(&text, started);
        self.turn_speech_end = speech_end;
        let endpoint_ms = speech_end.map(|t| t.elapsed().as_millis() as u64);
        if let Some(t) = speech_end {
            // Sarvam's end-of-speech -> its transcript: Dograh's STT TTFB.
            self.log.ttfb("stt", t.elapsed());
        }
        tracing::info!(call_id = %self.id, event = "turn.user", turn = self.turn, node = %self.node().name,
            generation = self.generation + 1, text = %text, endpoint_ms, "caller turn");
        self.idle_count = 0;
        self.start_turn();
    }

    // -------------------------------------------------------- held speech ---

    /// Keeps what the caller said over the agent until the reply finishes,
    /// instead of dropping it ("interrupting the agent is recoverable,
    /// discarding a real user turn is not").
    fn hold_speech(&mut self, text: String, why: &str) {
        let at = self.utterance_start().unwrap_or_else(Instant::now);
        tracing::info!(call_id = %self.id, event = "agent.speech.held", why, text = %text, generation = self.generation, "caller speech held until the reply ends");
        self.log.held_speech += 1;
        self.held.push(Held { text, generation: self.generation, at });
    }

    fn drop_held(&mut self, why: &str) {
        for h in self.held.drain(..) {
            tracing::info!(call_id = %self.id, event = "agent.speech.dropped", why, text = %h.text, "held caller speech dropped");
        }
    }

    /// Once reply `generation` has fully played: what the caller said during
    /// its last sentence (from `ack_end_margin` before it began) answers what
    /// the agent was finishing and becomes their turn now; earlier is dropped.
    fn settle_held(&mut self, generation: u64) -> bool {
        let held = std::mem::take(&mut self.held);
        let from = self.playing_since.checked_sub(self.cfg.ack_end_margin).unwrap_or(self.playing_since);
        let mut answer = vec![];
        let mut at: Option<Instant> = None;
        for h in held {
            let drop_why = if h.generation != generation {
                Some("said over an earlier reply")
            } else if h.at < from {
                Some("said before the reply's last sentence")
            } else {
                None
            };
            match drop_why {
                Some(why) => {
                    tracing::info!(call_id = %self.id, event = "agent.speech.dropped", why, text = %h.text, "held caller speech dropped")
                }
                None => {
                    at.get_or_insert(h.at);
                    answer.push(h.text);
                }
            }
        }
        if answer.is_empty() {
            return false;
        }
        let text = answer.join(" ");
        self.log.held_delivered += 1;
        tracing::info!(call_id = %self.id, event = "agent.speech.delivered", text = %text, "held caller speech delivered as their turn");
        self.accept_user_turn(text, at, None);
        true
    }

    // --------------------------------------------------------------- idle ---

    fn start_idle_timer(&mut self) {
        self.stop_idle_timer();
        if self.cfg.idle_timeout.is_zero() || self.ending {
            return;
        }
        let (tx, d, seq, c) = (self.ev_tx.clone(), self.cfg.idle_timeout, self.idle_seq, self.cancel.clone());
        let h = tokio::spawn(async move {
            tokio::select! { _ = c.cancelled() => {}, _ = tokio::time::sleep(d) => { let _ = tx.send(Event::Idle(seq)); } }
        });
        self.idle_task = Some(h.abort_handle());
    }

    fn stop_idle_timer(&mut self) {
        if let Some(h) = self.idle_task.take() {
            h.abort();
        }
        self.idle_seq += 1; // a timer that already fired is now stale
    }

    /// The caller said nothing for `idle_timeout` since the agent last spoke.
    /// First time the LLM checks they're still there; the second time in a row
    /// it says goodbye and the call ends after that.
    fn on_idle(&mut self, seq: u64) {
        if seq != self.idle_seq || self.ending || self.state != State::Listening {
            return;
        }
        self.idle_task = None;
        self.idle_count += 1;
        let mut prompt = IDLE_FIRST;
        if self.idle_count >= 2 {
            prompt = IDLE_FINAL;
            self.ending = true;
            self.log.ending(calllog::CALLER_SILENT);
            self.closing = true;
        }
        tracing::info!(call_id = %self.id, event = "caller.silent", times = self.idle_count, ending_call = self.ending, "caller silent");
        self.history.push(Message::new("user", prompt));
        self.start_turn();
    }

    // ------------------------------------------------------------- ending ---

    /// Ends the call for `reason`: right away when no reply is in progress,
    /// otherwise once the current one has played (`finish_turn`).
    fn end_call(&mut self, reason: &str) {
        self.ending = true;
        self.end_label = reason.into();
        self.closing = true;
        self.stop_idle_timer();
        if matches!(self.state, State::Listening | State::Transcribing) {
            self.hangup();
        }
    }

    fn hangup(&mut self) {
        if self.hung_up {
            return;
        }
        self.hung_up = true;
        let reason = if self.end_label.is_empty() { "end_call" } else { &self.end_label };
        tracing::info!(call_id = %self.id, event = "call.ended_by_agent", reason, node = %self.node().name, "agent ending call");
        (self.cfg.hangup)();
    }

    // -------------------------------------------------------------- turns ---

    fn turn_ctx(&mut self, greeting: Option<Greeting>) -> TurnCtx {
        self.generation += 1;
        self.turn_started = Instant::now();
        self.start_turn_chunks();
        let cancel = self.cancel.child_token();
        self.turn_cancel = Some(cancel.clone());
        if self.tts.is_none() {
            self.next_conn += 1;
        }
        TurnCtx {
            wf: self.cfg.workflow.clone(),
            node: self.node,
            history: self.history.clone(),
            llm: self.cfg.llm.clone(),
            tts_cfg: self.cfg.tts.clone(),
            tts: self.tts.as_ref().map(|t| t.client.clone()),
            conn: self.next_conn,
            generation: self.generation,
            cancel,
            call_cancel: self.cancel.clone(),
            ev: self.ev_tx.clone(),
            call_id: self.id.clone(),
            failures: self.llm_failures.clone(),
            tool_running: self.tool_running.clone(),
            transferred: self.transferred.clone(),
            fallback: FailurePolicy { max_failed: self.cfg.llm_max_failed_turns, text: self.cfg.llm_fallback_text.clone() },
            started: self.turn_started,
            greeting,
            transfer: self.cfg.transfer.clone(),
        }
    }

    /// Speaks the start node's fixed greeting before the caller says anything,
    /// bypassing the LLM. It reaches history like any reply (via `record_reply`,
    /// once it has played) so the LLM doesn't greet again.
    fn start_greeting(&mut self, g: Greeting) {
        self.state = State::Thinking;
        tracing::debug!(call_id = %self.id, event = "agent.greeting", text = %g.text(), "agent greeting");
        let ctx = self.turn_ctx(Some(g));
        tokio::spawn(turn_task(ctx));
    }

    /// One LLM turn over the current history: after the caller spoke, at call
    /// start when the LLM opens, or when the caller has gone silent.
    fn start_turn(&mut self) {
        self.log.agent_replies += 1;
        metrics::AGENT_TURNS.fetch_add(1, Relaxed);
        self.stop_idle_timer();
        self.state = State::Thinking;
        let ctx = self.turn_ctx(None);
        tokio::spawn(turn_task(ctx));
    }

    fn start_turn_chunks(&mut self) {
        self.turn_chunks.clear();
        self.turn_tool_msgs.clear();
        self.hangup_after_turn = false;
        self.turn_ttfa = None;
        self.turn_ttft = None;
        self.playing_req = 0;
    }

    fn cancel_turn(&mut self) {
        if let Some(c) = self.turn_cancel.take() {
            c.cancel();
        }
    }

    fn on_llm_done(&mut self, e: LlmDone) {
        if e.failed {
            self.agent_error("llm");
        }
        if e.generation != self.generation {
            return; // a barge-in already moved on; that turn's tools don't matter now
        }
        self.turn_tool_msgs = e.tool_msgs;
        self.hangup_after_turn = e.end_call;
        self.turn_ttft = e.ttft;
        // Listening follows via on_tts_done once the TTS yields this
        // generation: end_generation fires it at once if nothing was said.
    }

    // ---------------------------------------------------------------- TTS ---

    fn on_tts_audio(&mut self, pcm: &[u8], generation: u64, req: u64, text: String) {
        if generation != self.generation {
            return; // stale audio from an interrupted turn
        }
        if !text.is_empty() && self.turn_chunks.last().is_none_or(|c| c.req != req) {
            self.turn_chunks.push(SpokenChunk { req, text });
        }
        if self.ttfa_logged_gen != generation {
            self.ttfa_logged_gen = generation;
            let ttfa = self.turn_started.elapsed();
            self.turn_ttfa = Some(ttfa);
            tracing::debug!(call_id = %self.id, event = "tts.first_audio", generation, latency_ms = ttfa.as_millis() as u64, "tts first audio");
            if let Some(t) = self.turn_speech_end.take() {
                // The caller's wait, from the STT's end-of-speech to the reply's first audio.
                self.log.reply_latency(t.elapsed());
            }
            // Start draining to the caller only once real audio has arrived,
            // not when text is merely sent: flipping earlier left a silence gap
            // as long as Sarvam's synthesis + network round trip.
            if self.state == State::Thinking {
                self.state = State::Speaking;
                self.speaking_since = Instant::now();
            } else {
                tracing::warn!(call_id = %self.id, generation, state = ?self.state, "tts audio arrived but state was not Thinking; Speaking transition skipped");
            }
        }
        self.tts_buf.extend_from_slice(pcm);
        while self.tts_buf.len() >= FRAME_SIZE {
            let mut f = [0u8; FRAME_SIZE];
            f.copy_from_slice(&self.tts_buf[..FRAME_SIZE]);
            self.tts_buf.drain(..FRAME_SIZE);
            if self.out_q.len() >= OUTBOUND_BUFFER_FRAMES {
                self.agent_error("tts_outbound_full");
                if self.drop_warned_gen != generation {
                    self.drop_warned_gen = generation;
                    tracing::warn!(call_id = %self.id, generation, "tts outbound buffer full, dropping frames");
                }
                continue;
            }
            self.out_q.push_back(OutFrame { pcm: f, generation, req });
        }
    }

    /// Sarvam has finished *sending* this generation's audio -- not the caller
    /// finished *hearing* it. It delivers far faster than real time, so ending
    /// the turn here would cut most replies off early; while Speaking this only
    /// records delivery, and the tick that drains the queue ends the turn.
    fn on_tts_done(&mut self, generation: u64) {
        if generation != self.generation {
            return;
        }
        tracing::debug!(call_id = %self.id, event = "tts.delivered", generation, "tts delivery complete");
        match self.state {
            // No audio was ever sent (e.g. the LLM produced no output): nothing to drain.
            State::Thinking => self.finish_turn(),
            State::Speaking => self.tts_delivery_done = true,
            _ => {} // Transcribing: a barge-in already cut this turn
        }
    }

    /// TTS connection `conn` is gone. If it's still the call's TTS, forget it so
    /// the next turn opens a fresh one, and end the turn instead of waiting for
    /// audio that can't come: what already arrived still plays out first.
    fn on_tts_closed(&mut self, conn: u64) {
        if self.tts.as_ref().is_none_or(|t| t.conn != conn) {
            return;
        }
        self.tts = None;
        match self.state {
            State::Thinking => {
                tracing::warn!(call_id = %self.id, event = "tts.lost", generation = self.generation, "tts connection lost before the reply's audio; ending the turn");
                self.agent_error("tts_closed");
                self.cancel_turn();
                self.finish_turn();
            }
            State::Speaking => {
                tracing::warn!(call_id = %self.id, event = "tts.lost", generation = self.generation, "tts connection lost mid-reply; playing what arrived, then ending the turn");
                self.agent_error("tts_closed");
                self.cancel_turn();
                self.tts_delivery_done = true; // no more audio is coming for this generation
            }
            _ => {}
        }
    }

    /// Ends the turn: back to Listening, record the reply, then hang up,
    /// deliver a held answer, or start the caller-silence clock.
    fn finish_turn(&mut self) {
        if matches!(self.state, State::Speaking | State::Thinking) {
            self.state = State::Listening;
        }
        tracing::debug!(call_id = %self.id, event = "agent.turn.completed", generation = self.generation,
            total_latency_ms = self.turn_started.elapsed().as_millis() as u64, "turn complete");
        self.agent_quiet_since = Instant::now();
        self.record_reply(false);
        self.tts_buf.clear();

        if self.hangup_after_turn || self.ending {
            self.drop_held("the call is ending");
            self.hangup_after_turn = false;
            return self.hangup();
        }
        if self.settle_held(self.generation) {
            return; // the caller's held answer is the next turn: no silence clock
        }
        // The agent has finished speaking: now the caller-silence clock runs.
        self.idle_armed = true;
        self.start_idle_timer();
        // The TTS connection stays open for the next turn (billed once per call).
    }

    /// Appends the reply to history as the agent's own turn. Played in full
    /// (`cut` false): every sentence. Cut short (barge-in, dead TTS): only up to
    /// the sentence playing at the cut -- what the caller actually started
    /// hearing -- so the LLM's memory matches the conversation they had. No
    /// "(interrupted)" marker: the LLM imitates markers and anything it writes
    /// gets spoken.
    fn record_reply(&mut self, cut: bool) {
        let played = self.playing_req;
        let mut text = String::new();
        let mut last = '\0';
        for c in &self.turn_chunks {
            if cut && (played == 0 || c.req > played) {
                break;
            }
            // LLM chunks carry their own spacing and pieces spoken on their own
            // are sent with a trailing space; the one gap is between two LLM
            // rounds ("नमस्ते।Thank you"): add a space after a sentence end or
            // comma only, as a chunk can end mid-word.
            if (matches!(last, '.' | '!' | '?' | '\n' | '।' | '॥' | ',')) && c.text.chars().next().is_some_and(|f| !f.is_whitespace()) {
                text.push(' ');
            }
            text.push_str(&c.text);
            last = c.text.chars().last().unwrap_or(last);
        }
        self.turn_chunks.clear();

        // Tool calls and results go in first, even when cut: they really ran.
        self.history.append(&mut self.turn_tool_msgs);

        let text = text.trim().to_string();
        if text.is_empty() {
            return;
        }
        self.history.push(Message::new("assistant", text.clone()));
        if let Some(d) = self.turn_ttft {
            self.log.ttfb("llm", d);
        }
        if let Some(d) = self.turn_ttfa {
            self.log.ttfb("tts", d);
        }
        // The LLM's own history keeps "● <id>" (it needs the format it's asked to
        // follow); the call's log and transcript get only what was said.
        let (spoken, recordings) = strip_recording_refs(&text);
        if !spoken.is_empty() {
            self.log.agent_said(&spoken, Some(self.turn_started));
        }
        tracing::info!(call_id = %self.id, event = "turn.agent", turn = self.turn, node = %self.node().name,
            generation = self.generation, cut, text = %spoken, recordings = ?recordings,
            llm_ttft_ms = self.turn_ttft.map(|d| d.as_millis() as u64),
            tts_ttfa_ms = self.turn_ttfa.map(|d| d.as_millis() as u64), "agent turn");
    }

    // ----------------------------------------------------------- barge-in ---

    fn schedule_resume_check(&mut self, after: Duration) {
        self.resume_seq += 1;
        self.post_after(after, Event::Resume(self.resume_seq));
    }

    /// Resumes a paused reply once the caller has been quiet for
    /// `FALSE_INTERRUPTION` with no transcript confirming the interruption.
    fn on_resume_check(&mut self, seq: u64) {
        if seq != self.resume_seq || !self.paused {
            return;
        }
        let paused = self.paused_at.elapsed();
        let mut quiet_since = self.paused_at;
        if let Some(last) = self.cfg.vad.last_speech_at().filter(|l| *l > quiet_since) {
            quiet_since = last;
        }
        if let Some(p) = self.prev_speech_end_at.filter(|p| *p > quiet_since) {
            quiet_since = p;
        }
        if self.caller_speaking {
            quiet_since = Instant::now(); // the STT still has them mid-utterance
        }
        let quiet = quiet_since.elapsed();
        if quiet < FALSE_INTERRUPTION && paused < FALSE_INTERRUPTION_MAX_PAUSE {
            let wait = (FALSE_INTERRUPTION - quiet).min(FALSE_INTERRUPTION_MAX_PAUSE - paused);
            return self.schedule_resume_check(wait);
        }
        self.resume_reply("no transcript");
    }

    /// Lets the paused reply play on; it restarts from the start of the
    /// sentence it was paused in.
    fn resume_reply(&mut self, reason: &str) {
        self.resume_seq += 1; // any pending check is now stale
        self.replay_pending = true;
        self.paused = false;
        let msg = if matches!(reason, "no transcript" | "backchannel") {
            self.log.false_interruptions += 1;
            metrics::AGENT_FALSE_INTERRUPTIONS.fetch_add(1, Relaxed);
            "false interruption; resuming the reply"
        } else {
            "resuming the reply"
        };
        tracing::info!(call_id = %self.id, generation = self.generation, paused_ms = self.paused_at.elapsed().as_millis() as u64, reason, "{msg}");
    }

    /// A transcript arrived while paused: the caller really is talking. Cut the
    /// reply (the five cuts) so their words become the next turn.
    fn confirm_interruption(&mut self) {
        self.resume_seq += 1;
        // State before `paused`: never "not paused" while still Speaking, or a
        // frame of the reply being cut would play.
        self.state = State::Transcribing;
        self.paused = false;
        self.log.barge_ins += 1;
        metrics::AGENT_BARGEIN.fetch_add(1, Relaxed);
        self.drop_held("a real interruption followed"); // its words carry the meaning
        tracing::info!(call_id = %self.id, event = "agent.barge_in.confirmed", generation = self.generation,
            paused_ms = self.paused_at.elapsed().as_millis() as u64, "agent barge-in confirmed; reply cut");
        self.cut_reply();
    }

    /// The cuts: LLM and TTS cancelled, queued audio dropped, only what the
    /// caller heard recorded. (Cut #1, "pacer stop", is leaving the queue
    /// unfed: the write tick keeps emitting silence per invariants #1/#9.)
    fn cut_reply(&mut self) {
        self.cancel_turn(); // cut: LLM
        // Recorded before the generation advances, under its own turn's number.
        // `playing_req` is stable: playback stopped when the reply was paused.
        self.record_reply(true);
        self.generation += 1; // invalidates the interrupted turn's in-flight audio/done events
        self.agent_quiet_since = Instant::now();
        if let Some(t) = self.tts.take() {
            t.client.cancel(); // cut: TTS connection (the provider has no per-request cancel)
        }
        self.tts_buf.clear();
        // If delivery had just finished for the interrupted generation, leaving
        // this set would end the next turn on its first transient empty tick.
        self.tts_delivery_done = false;
        self.out_q.clear(); // cut: queue flush
        self.sent_frames.clear();
        self.replaying = false;
    }
}

/// A transcript with no digit and fewer than two letters/vowel signs: a lone
/// letter like "ह" or punctuation only. Deliberately narrow: real one-word
/// answers like "जी", "ना", "हाँ" (letter + vowel sign) or "5" still count.
pub fn junk_transcript(text: &str) -> bool {
    let mut n = 0;
    for c in text.chars() {
        if is_digit(c) {
            return false;
        }
        if is_letter(c) || is_mark(c) {
            n += 1;
        }
    }
    n < 2
}

// ------------------------------------------------------------ turn tasks ---

struct FailurePolicy {
    max_failed: u32,
    text: String,
}

impl FailurePolicy {
    /// What to say after the n-th consecutive failed turn, and whether to hang up.
    fn line(&self, n: u32) -> (String, bool) {
        if self.max_failed > 0 && n >= self.max_failed {
            ("I'm sorry, I'm having trouble right now. Goodbye.".into(), true)
        } else if self.text.is_empty() {
            ("Sorry, could you say that again?".into(), false)
        } else {
            (self.text.clone(), false)
        }
    }
}

struct TurnCtx {
    wf: Arc<Workflow>,
    node: usize,
    history: Vec<Message>,
    llm: LlmClient,
    tts_cfg: TtsConfig,
    tts: Option<Arc<TtsClient>>,
    conn: u64,
    generation: u64,
    cancel: CancellationToken,
    call_cancel: CancellationToken,
    ev: UnboundedSender<Event>,
    call_id: String,
    failures: Arc<AtomicU32>,
    tool_running: Arc<AtomicBool>,
    /// Set just before the handover: connecting cancels the agent, so the end reason can't wait for an event.
    transferred: Arc<AtomicBool>,
    fallback: FailurePolicy,
    started: Instant,
    greeting: Option<Greeting>,
    transfer: Option<TransferFn>,
}

/// The start node's opening, spoken before the caller says anything.
enum Greeting {
    Text(String),
    /// A recording, played as-is; recorded in history as its transcript.
    Clip(Arc<Clip>),
}

impl Greeting {
    fn text(&self) -> &str {
        match self {
            Greeting::Text(t) => t,
            Greeting::Clip(c) => &c.transcript,
        }
    }
}

impl TurnCtx {
    fn speak(&self, tts: &TtsClient, text: &str) {
        if self.cancel.is_cancelled() {
            return; // turn already cancelled
        }
        match tts.speak(text, self.generation) {
            // Dograh bills TTS per character.
            Ok(()) => {
                let _ = self.ev.send(Event::TtsChars(text.chars().count() as u64));
            }
            Err(e) if !self.cancel.is_cancelled() => {
                tracing::warn!(call_id = %self.call_id, event = "tts.error", generation = self.generation, error = %e, "tts speak failed");
            }
            Err(_) => {}
        }
    }

    /// Queues `clip` in place of synthesized speech, after whatever this turn
    /// already sent to TTS; `text` is what the turn records for it (history, call
    /// log). Unlike `speak` it costs no TTS characters.
    fn play_clip(&self, tts: &TtsClient, clip: &Clip, text: &str) {
        if self.cancel.is_cancelled() {
            return;
        }
        match tts.play(clip.pcm.clone(), text, self.generation) {
            Ok(()) => {
                tracing::debug!(call_id = %self.call_id, event = "tts.clip", generation = self.generation, recording_id = %clip.id, audio_ms = clip.pcm.len() / 32, "recorded clip queued")
            }
            Err(e) if !self.cancel.is_cancelled() => {
                tracing::warn!(call_id = %self.call_id, event = "tts.error", generation = self.generation, recording_id = %clip.id, error = %e, "recorded clip failed")
            }
            Err(_) => {}
        }
    }

    /// Plays the recording the LLM asked for with "● <id>": what the turn records
    /// is the marker, id and transcript, so the LLM's history shows the reply
    /// format it's asked to keep, as Dograh's context does. Reports whether the
    /// recording exists; if not, nothing was played and the caller falls back to
    /// speaking the text the LLM wrote (`ClipRouter::speak_rest`).
    fn play_recording(&self, tts: &TtsClient, clips: &std::collections::HashMap<String, Arc<Clip>>, id: &str) -> bool {
        let Some(clip) = clips.get(id) else {
            tracing::warn!(call_id = %self.call_id, generation = self.generation, event = "tts.clip_missing", recording_id = %id,
                "llm asked for a recording that isn't available; speaking the text it wrote instead");
            return false;
        };
        self.play_clip(tts, clip, &format!("{RECORDING_MARKER} {} {} ", clip.id, clip.transcript));
        true
    }
}

/// Opens the TTS (lazily, once per call -- it's billed on open), then speaks
/// the greeting or runs the LLM turn. Runs off the agent task so a slow
/// handshake never stalls the 20ms tick.
async fn turn_task(c: TurnCtx) {
    let tts = match c.tts.clone() {
        Some(t) => t,
        None => {
            let res = tokio::select! {
                _ = c.cancel.cancelled() => return,
                r = TtsClient::connect(&c.call_id, c.tts_cfg.clone(), &c.call_cancel) => r,
            };
            match res {
                Ok((client, mut rx)) => {
                    let client = Arc::new(client);
                    let _ = c.ev.send(Event::TtsOpened { conn: c.conn, client: client.clone() });
                    let (ev, conn) = (c.ev.clone(), c.conn);
                    tokio::spawn(async move {
                        while let Some(e) = rx.recv().await {
                            if ev.send(Event::Tts { conn, ev: e }).is_err() {
                                break;
                            }
                        }
                    });
                    client
                }
                Err(e) => {
                    tracing::error!(call_id = %c.call_id, event = "tts.error", error = %e, "tts open failed");
                    let _ = c.ev.send(Event::TtsOpenFailed { generation: c.generation });
                    return;
                }
            }
        }
    };

    match &c.greeting {
        Some(Greeting::Clip(clip)) => {
            // A recorded greeting goes into history as its transcript, as Dograh's does.
            c.play_clip(&tts, clip, &clip.transcript);
            tts.end_generation(c.generation);
            return;
        }
        Some(Greeting::Text(g)) => {
            let mut chunker = SentenceChunker::default();
            for chunk in chunker.feed(g) {
                c.speak(&tts, &chunk);
            }
            let rest = chunker.flush();
            if !rest.is_empty() {
                c.speak(&tts, &rest);
            }
            tts.end_generation(c.generation);
            return;
        }
        None => {}
    }
    run_llm_turn(&c, &tts).await;
}

#[derive(PartialEq, Clone, Copy)]
enum Action {
    None,
    EndCall,
    /// The caller is now with the transfer destination.
    Transferred,
}

/// Streams the LLM's reply for one turn, speaking it sentence by sentence as it
/// arrives. When the model calls tools, any text it wrote first ("ek second,
/// main check kar raha hoon") is spoken at once, the tools run, their results go
/// back to the model, and it streams again -- up to `MAX_TOOL_ROUNDS`. An
/// end_call tool stops the loop; the call hangs up once the reply has played.
async fn run_llm_turn(c: &TurnCtx, tts: &Arc<TtsClient>) {
    let wf = &c.wf;
    let mut node = c.node;
    let mut history = c.history.clone();
    let (mut tool_msgs, mut end_call, mut spoke, mut transferred) = (vec![], false, false, false);
    let mut err: Option<LlmError> = None;
    let mut ttft: Option<Duration> = None;
    let mut chunker = SentenceChunker::default();

    for round in 0..MAX_TOOL_ROUNDS {
        let n = &wf.nodes[node];
        let tools = n.tool_defs();
        let mut msgs = request_messages(n, &history);
        if tools.is_empty() {
            msgs = plain_history(&msgs);
        }
        let mut said = String::new();
        // A node with recordings answers "● <id>" (play it) or "▸ <text>". The
        // clips are the round's node's, even if a tool call moves `node` below.
        let round_clips = n.clips.clone();
        let mut router = round_clips.as_ref().map(|_| ClipRouter::default());
        let res = c
            .llm
            .stream(&c.cancel, &msgs, &tools, &mut |tok| {
                ttft.get_or_insert_with(|| c.started.elapsed());
                said.push_str(tok);
                let speak = match (router.as_mut(), round_clips.as_ref()) {
                    (Some(r), Some(clips)) => {
                        let (mut speak, id) = r.feed(tok);
                        if let Some(id) = id
                            && !c.play_recording(tts, clips, &id)
                        {
                            speak += &r.speak_rest();
                        }
                        speak
                    }
                    _ => tok.to_string(),
                };
                for chunk in chunker.feed(&speak) {
                    c.speak(tts, &chunk);
                }
            })
            .await;
        spoke |= !said.is_empty();

        if c.cancel.is_cancelled() {
            // Cancelled by barge-in or teardown; a barge-in already cancelled
            // this TTS, so no further speak for the generation is possible.
            tracing::debug!(call_id = %c.call_id, event = "llm.cancelled", generation = c.generation, "llm turn cancelled");
            return;
        }
        let calls = match res {
            Ok(out) => {
                if let Some(u) = out.usage {
                    let _ = c.ev.send(Event::LlmUsage(u));
                }
                out.calls
            }
            Err(e) => {
                err = Some(e);
                break;
            }
        };
        if let (Some(r), Some(clips)) = (router.as_mut(), round_clips.as_ref()) {
            let (speak, id) = r.flush();
            if let Some(id) = id {
                c.play_recording(tts, clips, &id);
            }
            for chunk in chunker.feed(&speak) {
                c.speak(tts, &chunk);
            }
        }
        // Speak the tail now, before any tool runs, so the caller isn't left in silence.
        let rest = chunker.flush();
        if !rest.is_empty() {
            c.speak(tts, &rest);
        }
        if calls.is_empty() {
            break;
        }
        if round == MAX_TOOL_ROUNDS - 1 {
            tracing::warn!(call_id = %c.call_id, event = "llm.tool_round_limit", rounds = MAX_TOOL_ROUNDS, "llm tool round limit reached; ending turn");
            break;
        }

        // Within this turn the model sees what it said alongside its calls. The
        // copy kept for long-term history drops that text: record_reply stores
        // everything actually spoken as one message.
        let mut am = Message::new("assistant", said);
        am.tool_calls = calls.clone();
        history.push(am);
        let mut tm = Message::new("assistant", "");
        tm.tool_calls = calls.clone();
        tool_msgs.push(tm);

        for call in &calls {
            let _ = c.ev.send(Event::Function { name: call.function.name.clone(), call_id: call.id.clone(), result: None });
            let (result, action) = if let Some(e) = wf.nodes[node].edge(&call.function.name) {
                // Dograh extracts the node's variables as the conversation leaves it.
                if wf.nodes[node].extraction.is_some() {
                    let _ = c.ev.send(Event::StartExtraction { node, history: history.clone() });
                }
                let r = take_edge(c, tts, node, e);
                node = e.to;
                (r, Action::None)
            } else {
                run_tool(c, tts, node, call).await
            };
            let _ = c.ev.send(Event::Function { name: call.function.name.clone(), call_id: call.id.clone(), result: Some(result.clone()) });
            let mut m = Message::new("tool", result);
            m.tool_call_id = call.id.clone();
            history.push(m.clone());
            tool_msgs.push(m);
            end_call |= action == Action::EndCall;
            transferred |= action == Action::Transferred;
        }

        if c.cancel.is_cancelled() {
            // end_call already ran (the call is recorded as ending and the
            // caller muted): the call must still end, not carry on half-closed
            // because an interruption cut this turn.
            if end_call && !c.call_cancel.is_cancelled() {
                let _ = c.ev.send(Event::EndCallTool);
            }
            return;
        }
        // Neither needs another LLM round: end_call's goodbye has been said, and
        // after a transfer the caller is talking to a person.
        if end_call || transferred {
            break;
        }
    }

    // A failed turn with nothing said would leave the caller in silence: say a
    // fixed line (not through the failing LLM), and after enough in a row, say
    // goodbye and hang up.
    if err.is_some() && !spoke {
        let n = c.failures.fetch_add(1, Relaxed) + 1;
        let (text, hangup) = c.fallback.line(n);
        c.speak(tts, &format!("{text} "));
        metrics::llm_fallback(if hangup { "hangup" } else { "apology" });
        tracing::warn!(call_id = %c.call_id, event = "llm.fallback", consecutive_failures = n, "llm failed; spoke fallback");
        if hangup {
            let _ = c.ev.send(Event::Closing { generation: c.generation, reason: calllog::LLM_DOWN });
        }
        end_call |= hangup;
    } else if err.is_none() {
        c.failures.store(0, Relaxed);
    }

    // At an end node the reply just generated is the closing line.
    end_call |= wf.nodes[node].end;

    match &err {
        None => tracing::debug!(call_id = %c.call_id, event = "llm.done", generation = c.generation,
            duration_ms = c.started.elapsed().as_millis() as u64, "llm turn complete"),
        Some(e) => tracing::error!(call_id = %c.call_id, event = "llm.error", generation = c.generation, error = %e, "llm turn error"),
    }

    // Sent BEFORE end_generation: that can emit Done at once (nothing pending),
    // and the agent must already know end_call/tool_msgs when it does. Both go
    // through the same ordered channel path, so order is kept.
    let _ = c.ev.send(Event::LlmDone(LlmDone { generation: c.generation, failed: err.is_some(), tool_msgs, end_call, ttft }));
    tts.end_generation(c.generation);
}

/// The LLM called an edge's function: speak the transition, switch node.
/// Returns Dograh's transition result.
fn take_edge(c: &TurnCtx, tts: &TtsClient, from: usize, e: &Edge) -> String {
    let (to, via) = (e.to, e.def.name.as_str());
    // Trailing space: a piece spoken on its own must not run into the LLM's
    // reply in the recorded text.
    match (&e.speech_clip, e.speech.is_empty()) {
        (Some(clip), _) => c.play_clip(tts, clip, &format!("{} ", clip.transcript)),
        (None, false) => c.speak(tts, &format!("{} ", e.speech)),
        (None, true) => {}
    }
    let (f, t) = (&c.wf.nodes[from], &c.wf.nodes[to]);
    tracing::info!(call_id = %c.call_id, event = "workflow.node_transition", generation = c.generation,
        from = %f.name, to = %t.name, via, allow_interrupt = t.allow_interrupt, "node transition");
    let _ = c.ev.send(Event::NodeEntered { generation: c.generation, idx: to });
    if t.end {
        let _ = c.ev.send(Event::Closing { generation: c.generation, reason: calllog::END_NODE }); // Dograh mutes the caller at the end node
    }
    r#"{"status":"done"}"#.to_string()
}

fn tool_error(e: impl std::fmt::Display) -> String {
    serde_json::json!({"status": "error", "error": e.to_string()}).to_string()
}

/// Runs one tool call of `node`. The tool's message is spoken first. Unknown
/// tools and failures become error results the LLM can react to.
async fn run_tool(c: &TurnCtx, tts: &TtsClient, node: usize, call: &ToolCall) -> (String, Action) {
    let name = &call.function.name;
    let Some(t) = c.wf.nodes[node].tool(name) else {
        tracing::warn!(call_id = %c.call_id, event = "tool.unknown", tool = %name, "llm called an unknown tool");
        return (tool_error(format!("unknown tool {name:?}")), Action::None);
    };
    tracing::info!(call_id = %c.call_id, event = "tool.call", tool = %name, args = %truncate(&call.function.arguments, 200), "tool call");
    match (&t.message_clip, t.message.is_empty()) {
        (Some(clip), _) => c.play_clip(tts, clip, &format!("{} ", clip.transcript)),
        (None, false) => c.speak(tts, &format!("{} ", t.message)),
        (None, true) => {}
    }
    match &t.kind {
        ToolKind::EndCall => {
            let reason = serde_json::from_str::<serde_json::Value>(&call.function.arguments)
                .ok()
                .and_then(|v| v.get("reason")?.as_str().map(String::from))
                .unwrap_or_default();
            tracing::info!(call_id = %c.call_id, event = "tool.end_call", reason, "end call requested");
            let _ = c.ev.send(Event::Closing { generation: c.generation, reason: calllog::END_CALL_TOOL }); // no interrupting the goodbye
            (r#"{"status":"success","action":"ending_call"}"#.to_string(), Action::EndCall)
        }
        ToolKind::Transfer { destination, timeout } => transfer(c, t, destination, *timeout).await,
        ToolKind::Function(run) => {
            c.tool_running.store(true, Relaxed);
            let args = serde_json::from_str(&call.function.arguments).unwrap_or(serde_json::Value::Null);
            let start = Instant::now();
            let res = tokio::time::timeout(TOOL_TIMEOUT, run(args)).await;
            c.tool_running.store(false, Relaxed);
            let ms = start.elapsed().as_millis() as u64;
            metrics::tool_run(name, matches!(res, Ok(Ok(_))), start.elapsed().as_secs_f64() * 1000.0);
            match res {
                Ok(Ok(r)) => {
                    tracing::info!(call_id = %c.call_id, event = "tool.result", tool = %name, duration_ms = ms, result = %truncate(&r, 200), "tool result");
                    (r, Action::None)
                }
                Ok(Err(e)) => {
                    tracing::error!(call_id = %c.call_id, event = "tool.error", tool = %name, duration_ms = ms, error = %e, "tool failed");
                    (tool_error(e), Action::None)
                }
                Err(_) => (tool_error("tool timed out"), Action::None),
            }
        }
    }
}

fn transfer_failed(reason: &str) -> String {
    serde_json::json!({"status": "failed", "reason": reason}).to_string()
}

/// Runs a Transfer tool: ring the destination with hold music playing to the
/// caller; on answer play the beep, then hand over (Dograh's transfer flow: hold
/// loop while waiting, beep, then bridge swap).
async fn transfer(c: &TurnCtx, t: &Tool, destination: &str, timeout: Duration) -> (String, Action) {
    let Some(dial) = &c.transfer else { return (transfer_failed("call transfer is not available"), Action::None) };
    tracing::info!(call_id = %c.call_id, event = "transfer.dialing", generation = c.generation, destination, timeout_s = timeout.as_secs(), "transfer dialing");
    let _ = c.ev.send(Event::Hold(HoldCmd::Ring { wait_for_message: !t.message.is_empty() || t.message_clip.is_some() }));
    let started = Instant::now();
    let after_ms = || started.elapsed().as_millis() as u64;

    let connect = match dial(destination.to_string(), timeout).await {
        Ok(connect) => connect,
        Err(e) => {
            let _ = c.ev.send(Event::Hold(HoldCmd::Off));
            tracing::warn!(call_id = %c.call_id, event = "transfer.failed", generation = c.generation, destination, after_ms = after_ms(), error = %format!("{e:#}"), "transfer failed");
            return (transfer_failed(&format!("{e:#}")), Action::None);
        }
    };
    tracing::info!(call_id = %c.call_id, event = "transfer.answered", generation = c.generation, destination, after_ms = after_ms(), "transfer answered");

    // Beep, and let it finish playing before the handover cuts this leg off.
    let (done, finished) = tokio::sync::oneshot::channel();
    let _ = c.ev.send(Event::Hold(HoldCmd::Beep { done }));
    let _ = tokio::time::timeout(Duration::from_secs(2), finished).await;

    c.transferred.store(true, Relaxed);
    if let Err(e) = connect().await {
        c.transferred.store(false, Relaxed);
        let _ = c.ev.send(Event::Hold(HoldCmd::Off));
        tracing::warn!(call_id = %c.call_id, event = "transfer.failed", generation = c.generation, error = %format!("{e:#}"), "transfer handover failed");
        return (transfer_failed(&format!("{e:#}")), Action::None);
    }
    tracing::info!(call_id = %c.call_id, event = "transfer.connected", generation = c.generation, destination, "call transferred");
    let _ = c.ev.send(Event::Closing { generation: c.generation, reason: calllog::TRANSFER });
    (r#"{"status":"success","action":"transferred"}"#.to_string(), Action::Transferred)
}

fn truncate(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((i, _)) => format!("{}…", &s[..i]),
        None => s.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn junk_transcripts() {
        for j in ["ह", "म", ".", "", "?!", "a"] {
            assert!(junk_transcript(j), "{j:?}");
        }
        for ok in ["जी", "ना", "हाँ", "5", "ok", "no"] {
            assert!(!junk_transcript(ok), "{ok:?}");
        }
    }

    #[test]
    fn failure_policy_falls_back_then_hangs_up() {
        let p = FailurePolicy { max_failed: 3, text: String::new() };
        assert!(!p.line(1).1 && !p.line(2).1);
        assert!(p.line(3).1);
        let never = FailurePolicy { max_failed: 0, text: "custom".into() };
        assert_eq!(never.line(99), ("custom".to_string(), false));
    }

    #[test]
    fn truncate_is_char_safe() {
        assert_eq!(truncate("नमस्ते दुनिया", 3), "नमस…");
        assert_eq!(truncate("ab", 5), "ab");
    }
}

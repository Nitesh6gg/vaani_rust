//! Process counters and a tiny Prometheus-text endpoint (`/metrics`,
//! `/healthz`). Hand-rolled on purpose: four numbers don't need a dependency.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

pub static CALLS_ACTIVE: AtomicU64 = AtomicU64::new(0);
/// `vaani_calls_started_total{direction}`: calls bridged to the media plane (answered).
pub static CALLS_STARTED: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];
const DIRECTIONS: [&str; 2] = ["inbound", "outbound"];

pub fn call_started(direction: &str) {
    if let Some(i) = DIRECTIONS.iter().position(|d| *d == direction) {
        CALLS_STARTED[i].fetch_add(1, Relaxed);
    }
}
pub static MEDIA_PORTS_IN_USE: AtomicU64 = AtomicU64::new(0);
pub static ARI_CONNECTED: AtomicBool = AtomicBool::new(false);

// Outbound campaigns.
pub static CAMPAIGN_DIALS: AtomicU64 = AtomicU64::new(0);
pub static CAMPAIGN_RETRIES: AtomicU64 = AtomicU64::new(0);
pub static CAMPAIGNS_COMPLETED: AtomicU64 = AtomicU64::new(0);
pub static CAMPAIGN_CALLS_IN_FLIGHT: AtomicU64 = AtomicU64::new(0);
/// `vaani_campaign_outcomes_total{status}`: connected, busy, no-answer, rejected, failed.
pub static CAMPAIGN_OUTCOMES: [AtomicU64; 5] = [const { AtomicU64::new(0) }; 5];
const OUTCOME_LABELS: [&str; 5] = ["connected", "busy", "no-answer", "rejected", "failed"];
/// `vaani_campaign_pauses_total{reason}`: breaker, wallet, provider.
pub static CAMPAIGN_PAUSES: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];
const PAUSE_LABELS: [&str; 3] = ["breaker", "wallet", "provider"];

pub fn campaign_outcome(status: &str) {
    if let Some(i) = OUTCOME_LABELS.iter().position(|l| *l == status) {
        CAMPAIGN_OUTCOMES[i].fetch_add(1, Relaxed);
    }
}

pub fn campaign_pause(reason: &str) {
    if let Some(i) = PAUSE_LABELS.iter().position(|l| *l == reason) {
        CAMPAIGN_PAUSES[i].fetch_add(1, Relaxed);
    }
}

// Load and call setup.
/// The machine's CPU use over the last 5 seconds, in thousandths (`vaani_cpu_load_ratio`).
pub static CPU_LOAD_PERMILLE: AtomicU64 = AtomicU64::new(0);
pub static OVERLOADED: AtomicBool = AtomicBool::new(false);
/// `vaani_calls_rejected_total{reason,direction}`: calls refused or held back.
pub static CALLS_REJECTED: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];
const REJECTED_LABELS: [(&str, &str); 3] = [("cpu", "inbound"), ("cpu", "outbound"), ("setup", "outbound")];
/// Answered calls staged and not yet bridged.
pub static CALLS_SETTING_UP: AtomicU64 = AtomicU64::new(0);
pub static CALLS_SETUP_TIMEOUT: AtomicU64 = AtomicU64::new(0);
pub static ORPHAN_CHANNELS: AtomicU64 = AtomicU64::new(0);
/// ARI events read from the WebSocket and not yet handled.
pub static ARI_EVENT_QUEUE: AtomicU64 = AtomicU64::new(0);

pub fn call_rejected(reason: &str, direction: &str) {
    if let Some(i) = REJECTED_LABELS.iter().position(|l| *l == (reason, direction)) {
        CALLS_REJECTED[i].fetch_add(1, Relaxed);
    }
}

// Same names and buckets as the Go server's, so one set of dashboards and queries reads both engines.

/// A fixed-bucket Prometheus histogram on atomics. `N` is the bucket count: the bounds plus +Inf.
pub struct Histogram<const N: usize> {
    bounds: &'static [f64],
    counts: [AtomicU64; N],
    /// The sum in thousandths of a unit (a millisecond histogram keeps microseconds).
    sum_milli: AtomicU64,
}

impl<const N: usize> Histogram<N> {
    pub const fn new(bounds: &'static [f64]) -> Self {
        assert!(bounds.len() + 1 == N, "N is the number of bounds plus one");
        Self { bounds, counts: [const { AtomicU64::new(0) }; N], sum_milli: AtomicU64::new(0) }
    }

    pub fn observe(&self, v: f64) {
        let i = self.bounds.iter().position(|b| v <= *b).unwrap_or(self.bounds.len());
        self.counts[i].fetch_add(1, Relaxed);
        self.sum_milli.fetch_add((v.max(0.0) * 1000.0).round() as u64, Relaxed);
    }

    /// How many values have been observed.
    #[cfg(test)]
    pub fn count(&self) -> u64 {
        self.counts.iter().map(|c| c.load(Relaxed)).sum()
    }

    fn render(&self, out: &mut String, name: &str, labels: &str) {
        let counts: Vec<u64> = self.counts.iter().map(|c| c.load(Relaxed)).collect();
        render_histogram(out, name, labels, self.bounds, &counts, self.sum_milli.load(Relaxed) as f64 / 1000.0);
    }
}

/// `name_bucket{le=..}` (cumulative), `_sum` and `_count` for `counts` (one per bound, then +Inf).
fn render_histogram(out: &mut String, name: &str, labels: &str, bounds: &[f64], counts: &[u64], sum: f64) {
    let sep = if labels.is_empty() { "" } else { "," };
    let mut cum = 0;
    for (b, c) in bounds.iter().zip(counts) {
        cum += c;
        out.push_str(&format!("{name}_bucket{{{labels}{sep}le=\"{b}\"}} {cum}\n"));
    }
    cum += counts.last().copied().unwrap_or(0);
    out.push_str(&format!("{name}_bucket{{{labels}{sep}le=\"+Inf\"}} {cum}\n"));
    let l = if labels.is_empty() { String::new() } else { format!("{{{labels}}}") };
    out.push_str(&format!("{name}_sum{l} {sum}\n{name}_count{l} {cum}\n"));
}

/// 50ms..25.6s: a fast TTS first chunk through a slow LLM first token on a cold connection.
const TTFB_MS: [f64; 10] = [50.0, 100.0, 200.0, 400.0, 800.0, 1600.0, 3200.0, 6400.0, 12800.0, 25600.0];
/// 1s..~34m.
const CALL_SECONDS: [f64; 12] = [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 2048.0];
/// 10ms..~20s.
const TOOL_MS: [f64; 12] = [10.0, 20.0, 40.0, 80.0, 160.0, 320.0, 640.0, 1280.0, 2560.0, 5120.0, 10240.0, 20480.0];
const PACER_MS: [f64; 8] = [0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0];

/// Time from the transcript accepted to the first LLM token.
pub static LLM_TTFT_MS: Histogram<11> = Histogram::new(&TTFB_MS);
/// Time from the transcript accepted to the first TTS audio.
pub static TTS_TTFA_MS: Histogram<11> = Histogram::new(&TTFB_MS);
/// Time from Sarvam's END_SPEECH to its final transcript.
pub static STT_ENDPOINT_MS: Histogram<11> = Histogram::new(&TTFB_MS);
/// Bridged calls, from staging (answer) to teardown.
pub static CALL_DURATION_SECONDS: Histogram<13> = Histogram::new(&CALL_SECONDS);
/// How far each 20ms tick of a call's writer was from its nominal interval.
pub static PACER_DRIFT_MS: Histogram<9> = Histogram::new(&PACER_MS);

/// The service a time-to-first-byte is for: "llm", "tts" or "stt" (Dograh's names).
pub fn ttfb(service: &str, d: std::time::Duration) {
    let ms = d.as_secs_f64() * 1000.0;
    match service {
        "llm" => LLM_TTFT_MS.observe(ms),
        "tts" => TTS_TTFA_MS.observe(ms),
        "stt" => STT_ENDPOINT_MS.observe(ms),
        _ => {}
    }
}

// Agent.
pub static AGENT_BARGEIN: AtomicU64 = AtomicU64::new(0);
pub static AGENT_INTERRUPTION_PAUSES: AtomicU64 = AtomicU64::new(0);
pub static AGENT_FALSE_INTERRUPTIONS: AtomicU64 = AtomicU64::new(0);
pub static AGENT_TURNS: AtomicU64 = AtomicU64::new(0);
pub static RECORDING_TRUNCATED: AtomicU64 = AtomicU64::new(0);
/// Seconds of caller audio sent to the STT (what Sarvam bills), in microseconds; added when a call ends.
pub static STT_AUDIO_MICROS: AtomicU64 = AtomicU64::new(0);

pub fn add_stt_seconds(s: f64) {
    STT_AUDIO_MICROS.fetch_add((s.max(0.0) * 1e6) as u64, Relaxed);
}

/// `vaani_agent_errors_total{stage}`.
const ERROR_STAGES: [&str; 8] =
    ["stt", "llm", "tts_open", "tts_speak", "tts_cancel", "tts_outbound_full", "tts_closed", "tts_drain_timeout"];
static AGENT_ERRORS: [AtomicU64; 8] = [const { AtomicU64::new(0) }; 8];

pub fn agent_error(stage: &str) {
    if let Some(i) = ERROR_STAGES.iter().position(|s| *s == stage) {
        AGENT_ERRORS[i].fetch_add(1, Relaxed);
    }
}

/// `vaani_llm_retries_total{result}`: recovered, exhausted.
static LLM_RETRIES: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];
/// `vaani_llm_fallback_total{kind}`: apology, hangup.
static LLM_FALLBACK: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];

pub fn llm_retry(result: &str) {
    LLM_RETRIES[usize::from(result == "exhausted")].fetch_add(1, Relaxed);
}

pub fn llm_fallback(kind: &str) {
    LLM_FALLBACK[usize::from(kind == "hangup")].fetch_add(1, Relaxed);
}

/// Per tool: runs ok, runs failed, and a duration histogram's counts and sum.
#[derive(Default)]
struct ToolStats {
    ok: u64,
    err: u64,
    counts: [u64; 13],
    sum_ms: f64,
}
static TOOLS: std::sync::Mutex<std::collections::BTreeMap<String, ToolStats>> = std::sync::Mutex::new(std::collections::BTreeMap::new());

/// One tool run (an http_api function): its outcome and how long it took.
pub fn tool_run(tool: &str, ok: bool, ms: f64) {
    let mut t = TOOLS.lock().unwrap();
    let s = t.entry(tool.to_string()).or_default();
    if ok {
        s.ok += 1;
    } else {
        s.err += 1;
    }
    s.counts[TOOL_MS.iter().position(|b| ms <= *b).unwrap_or(TOOL_MS.len())] += 1;
    s.sum_ms += ms;
}

// Media (AudioSocket).
pub static AUDIOSOCKET_FRAMES_IN: AtomicU64 = AtomicU64::new(0);
pub static AUDIOSOCKET_FRAMES_OUT: AtomicU64 = AtomicU64::new(0);
pub static AUDIOSOCKET_BYTES_IN: AtomicU64 = AtomicU64::new(0);
pub static AUDIOSOCKET_SILENCE_SENT: AtomicU64 = AtomicU64::new(0);
pub static AUDIOSOCKET_SEND_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static AUDIOSOCKET_MALFORMED: AtomicU64 = AtomicU64::new(0);
/// Agent output frames dropped because the outbound queue was full (the writer not keeping up).
pub static MEDIA_QUEUE_DROPPED: AtomicU64 = AtomicU64::new(0);
/// Caller frames dropped because the agent's tick was behind (the reader never blocks).
pub static MEDIA_INBOUND_DROPPED: AtomicU64 = AtomicU64::new(0);
/// Caller frames that arrived more than one to a tick (start-up, or a clump from Asterisk) and were
/// still heard: they would have been dropped before the agent took every waiting frame.
pub static MEDIA_INBOUND_LATE: AtomicU64 = AtomicU64::new(0);
/// Calls whose media ended with no inbound audio for MEDIA_DEAD_TIMEOUT_SECONDS (the Go server's watchdog
/// counts at 5 s and hangs up at the timeout; here the one event is the hangup).
pub static MEDIA_WATCHDOG_TIMEOUTS: AtomicU64 = AtomicU64::new(0);
pub static ARI_RECONNECTS: AtomicU64 = AtomicU64::new(0);
/// Call recordings held in memory right now (calls in progress, and finished ones until their upload starts).
pub static RECORDING_BYTES: AtomicU64 = AtomicU64::new(0);

/// Received and transmitted bytes of every network interface, from /proc/self/net/dev, as the Go server's
/// process collector reads them (the process's whole network namespace: loopback included).
pub fn parse_net_dev(text: &str) -> (u64, u64) {
    text.lines()
        .filter_map(|l| l.split_once(':'))
        .map(|(_, v)| v.split_whitespace().map(|n| n.parse::<u64>().unwrap_or(0)).collect::<Vec<_>>())
        .filter(|f| f.len() >= 9)
        .fold((0, 0), |(rx, tx), f| (rx + f[0], tx + f[8]))
}

/// What the kernel says about this process, in the units of Prometheus' standard `process_*` metrics
/// (and of the Go server's, so `docs/BENCHMARK.md`'s method works unchanged on either engine).
#[derive(Debug, Default, PartialEq)]
pub struct ProcStats {
    pub cpu_seconds: f64,
    pub resident_bytes: u64,
    pub resident_peak_bytes: u64,
    pub virtual_bytes: u64,
    pub threads: u64,
}

/// Linux's USER_HZ: the unit of /proc/<pid>/stat's CPU times (100 on every mainstream build).
const CLOCK_TICKS: f64 = 100.0;

/// Parses /proc/self/stat (CPU time) and /proc/self/status (memory, threads).
pub fn parse_proc(stat: &str, status: &str) -> ProcStats {
    // Fields after the parenthesised command name (which may itself contain spaces): utime and stime are the 12th and 13th.
    let after: Vec<&str> = stat.rsplit_once(") ").map_or("", |(_, r)| r).split_whitespace().collect();
    let tick = |i: usize| after.get(i).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
    let kb = |key: &str| status.lines().find_map(|l| l.strip_prefix(key)?.split_whitespace().next()?.parse::<u64>().ok()).unwrap_or(0);
    ProcStats {
        cpu_seconds: (tick(11) + tick(12)) / CLOCK_TICKS,
        resident_bytes: kb("VmRSS:") * 1024,
        resident_peak_bytes: kb("VmHWM:") * 1024,
        virtual_bytes: kb("VmSize:") * 1024,
        threads: kb("Threads:"),
    }
}

fn proc_stats() -> ProcStats {
    let read = |p| std::fs::read_to_string(p).unwrap_or_default();
    parse_proc(&read("/proc/self/stat"), &read("/proc/self/status"))
}

pub fn render() -> String {
    let mut out = format!(
        "# TYPE vaani_calls_active gauge\nvaani_calls_active {}\n\
         # TYPE vaani_media_ports_in_use gauge\nvaani_media_ports_in_use {}\n\
         # TYPE vaani_ari_connected gauge\nvaani_ari_connected {}\n\
         # TYPE vaani_campaign_dials_total counter\nvaani_campaign_dials_total {}\n\
         # TYPE vaani_campaign_retries_total counter\nvaani_campaign_retries_total {}\n\
         # TYPE vaani_campaigns_completed_total counter\nvaani_campaigns_completed_total {}\n\
         # TYPE vaani_campaign_calls_in_flight gauge\nvaani_campaign_calls_in_flight {}\n",
        CALLS_ACTIVE.load(Relaxed),
        MEDIA_PORTS_IN_USE.load(Relaxed),
        u8::from(ARI_CONNECTED.load(Relaxed)),
        CAMPAIGN_DIALS.load(Relaxed),
        CAMPAIGN_RETRIES.load(Relaxed),
        CAMPAIGNS_COMPLETED.load(Relaxed),
        CAMPAIGN_CALLS_IN_FLIGHT.load(Relaxed),
    );
    let p = proc_stats();
    let open_fds = std::fs::read_dir("/proc/self/fd").map_or(0, |d| d.count());
    let tasks = tokio::runtime::Handle::try_current().map_or(0, |h| h.metrics().num_alive_tasks());
    out.push_str(&format!(
        "# TYPE process_cpu_seconds_total counter\nprocess_cpu_seconds_total {:.2}\n\
         # TYPE process_resident_memory_bytes gauge\nprocess_resident_memory_bytes {}\n\
         # TYPE process_resident_memory_peak_bytes gauge\nprocess_resident_memory_peak_bytes {}\n\
         # TYPE process_virtual_memory_bytes gauge\nprocess_virtual_memory_bytes {}\n\
         # TYPE process_threads gauge\nprocess_threads {}\n\
         # TYPE process_open_fds gauge\nprocess_open_fds {open_fds}\n\
         # TYPE vaani_tokio_alive_tasks gauge\nvaani_tokio_alive_tasks {tasks}\n",
        p.cpu_seconds, p.resident_bytes, p.resident_peak_bytes, p.virtual_bytes, p.threads
    ));
    out.push_str(&format!(
        "# TYPE vaani_cpu_load_ratio gauge\nvaani_cpu_load_ratio {:.3}\n\
         # TYPE vaani_overloaded gauge\nvaani_overloaded {}\n\
         # TYPE vaani_calls_setting_up gauge\nvaani_calls_setting_up {}\n\
         # TYPE vaani_calls_setup_timeout_total counter\nvaani_calls_setup_timeout_total {}\n\
         # TYPE vaani_orphan_channels_hung_up_total counter\nvaani_orphan_channels_hung_up_total {}\n\
         # TYPE vaani_ari_event_queue_length gauge\nvaani_ari_event_queue_length {}\n",
        CPU_LOAD_PERMILLE.load(Relaxed) as f64 / 1000.0,
        u8::from(OVERLOADED.load(Relaxed)),
        CALLS_SETTING_UP.load(Relaxed),
        CALLS_SETUP_TIMEOUT.load(Relaxed),
        ORPHAN_CHANNELS.load(Relaxed),
        ARI_EVENT_QUEUE.load(Relaxed),
    ));
    out.push_str("# TYPE vaani_calls_rejected_total counter\n");
    for ((reason, direction), c) in REJECTED_LABELS.iter().zip(&CALLS_REJECTED) {
        out.push_str(&format!("vaani_calls_rejected_total{{reason=\"{reason}\",direction=\"{direction}\"}} {}\n", c.load(Relaxed)));
    }
    out.push_str("# TYPE vaani_campaign_outcomes_total counter\n");
    for (l, c) in OUTCOME_LABELS.iter().zip(&CAMPAIGN_OUTCOMES) {
        out.push_str(&format!("vaani_campaign_outcomes_total{{status=\"{l}\"}} {}\n", c.load(Relaxed)));
    }
    out.push_str("# TYPE vaani_campaign_pauses_total counter\n");
    for (l, c) in PAUSE_LABELS.iter().zip(&CAMPAIGN_PAUSES) {
        out.push_str(&format!("vaani_campaign_pauses_total{{reason=\"{l}\"}} {}\n", c.load(Relaxed)));
    }
    let (rx, tx) = parse_net_dev(&std::fs::read_to_string("/proc/self/net/dev").unwrap_or_default());
    out.push_str(&format!(
        "# TYPE process_network_receive_bytes_total counter\nprocess_network_receive_bytes_total {rx}\n\
         # TYPE process_network_transmit_bytes_total counter\nprocess_network_transmit_bytes_total {tx}\n"
    ));
    out.push_str("# TYPE vaani_calls_started_total counter\n");
    for (d, c) in DIRECTIONS.iter().zip(&CALLS_STARTED) {
        out.push_str(&format!("vaani_calls_started_total{{direction=\"{d}\"}} {}\n", c.load(Relaxed)));
    }
    for (name, h) in [("vaani_llm_ttft_ms", &LLM_TTFT_MS), ("vaani_tts_ttfa_ms", &TTS_TTFA_MS), ("vaani_stt_endpoint_ms", &STT_ENDPOINT_MS)]
    {
        out.push_str(&format!("# TYPE {name} histogram\n"));
        h.render(&mut out, name, "");
    }
    out.push_str("# TYPE vaani_call_duration_seconds histogram\n");
    CALL_DURATION_SECONDS.render(&mut out, "vaani_call_duration_seconds", "");
    out.push_str("# TYPE vaani_pacer_drift_ms histogram\n");
    PACER_DRIFT_MS.render(&mut out, "vaani_pacer_drift_ms", "");
    let c = |a: &AtomicU64| a.load(Relaxed);
    out.push_str(&format!(
        "# TYPE vaani_agent_bargein_total counter\nvaani_agent_bargein_total {}\n\
         # TYPE vaani_agent_interruption_pauses_total counter\nvaani_agent_interruption_pauses_total {}\n\
         # TYPE vaani_agent_false_interruptions_total counter\nvaani_agent_false_interruptions_total {}\n\
         # TYPE vaani_agent_turns_total counter\nvaani_agent_turns_total {}\n\
         # TYPE vaani_recording_truncated_total counter\nvaani_recording_truncated_total {}\n\
         # TYPE vaani_stt_audio_seconds_total counter\nvaani_stt_audio_seconds_total {}\n\
         # TYPE vaani_audiosocket_frames_in_total counter\nvaani_audiosocket_frames_in_total {}\n\
         # TYPE vaani_audiosocket_frames_out_total counter\nvaani_audiosocket_frames_out_total {}\n\
         # TYPE vaani_audiosocket_bytes_in_total counter\nvaani_audiosocket_bytes_in_total {}\n\
         # TYPE vaani_audiosocket_silence_sent_total counter\nvaani_audiosocket_silence_sent_total {}\n\
         # TYPE vaani_audiosocket_send_errors_total counter\nvaani_audiosocket_send_errors_total {}\n\
         # TYPE vaani_audiosocket_malformed_total counter\nvaani_audiosocket_malformed_total {}\n\
         # TYPE vaani_media_queue_dropped_total counter\nvaani_media_queue_dropped_total {}\n\
         # TYPE vaani_media_inbound_dropped_total counter\nvaani_media_inbound_dropped_total {}\n\
         # TYPE vaani_media_inbound_late_total counter\nvaani_media_inbound_late_total {}\n\
         # TYPE vaani_media_watchdog_timeouts_total counter\nvaani_media_watchdog_timeouts_total {}\n\
         # TYPE vaani_ari_reconnects_total counter\nvaani_ari_reconnects_total {}\n\
         # TYPE vaani_recording_bytes gauge\nvaani_recording_bytes {}\n",
        c(&AGENT_BARGEIN),
        c(&AGENT_INTERRUPTION_PAUSES),
        c(&AGENT_FALSE_INTERRUPTIONS),
        c(&AGENT_TURNS),
        c(&RECORDING_TRUNCATED),
        c(&STT_AUDIO_MICROS) as f64 / 1e6,
        c(&AUDIOSOCKET_FRAMES_IN),
        c(&AUDIOSOCKET_FRAMES_OUT),
        c(&AUDIOSOCKET_BYTES_IN),
        c(&AUDIOSOCKET_SILENCE_SENT),
        c(&AUDIOSOCKET_SEND_ERRORS),
        c(&AUDIOSOCKET_MALFORMED),
        c(&MEDIA_QUEUE_DROPPED),
        c(&MEDIA_INBOUND_DROPPED),
        c(&MEDIA_INBOUND_LATE),
        c(&MEDIA_WATCHDOG_TIMEOUTS),
        c(&ARI_RECONNECTS),
        c(&RECORDING_BYTES),
    ));
    out.push_str("# TYPE vaani_agent_errors_total counter\n");
    for (stage, n) in ERROR_STAGES.iter().zip(&AGENT_ERRORS) {
        out.push_str(&format!("vaani_agent_errors_total{{stage=\"{stage}\"}} {}\n", c(n)));
    }
    out.push_str("# TYPE vaani_llm_retries_total counter\n");
    for (r, n) in ["recovered", "exhausted"].iter().zip(&LLM_RETRIES) {
        out.push_str(&format!("vaani_llm_retries_total{{result=\"{r}\"}} {}\n", c(n)));
    }
    out.push_str("# TYPE vaani_llm_fallback_total counter\n");
    for (k, n) in ["apology", "hangup"].iter().zip(&LLM_FALLBACK) {
        out.push_str(&format!("vaani_llm_fallback_total{{kind=\"{k}\"}} {}\n", c(n)));
    }
    {
        let tools = TOOLS.lock().unwrap();
        out.push_str("# TYPE vaani_tool_calls_total counter\n");
        for (t, s) in tools.iter() {
            out.push_str(&format!(
                "vaani_tool_calls_total{{tool=\"{t}\",outcome=\"ok\"}} {}\nvaani_tool_calls_total{{tool=\"{t}\",outcome=\"error\"}} {}\n",
                s.ok, s.err
            ));
        }
        out.push_str("# TYPE vaani_tool_call_duration_ms histogram\n");
        for (t, s) in tools.iter() {
            render_histogram(&mut out, "vaani_tool_call_duration_ms", &format!("tool=\"{t}\""), &TOOL_MS, &s.counts, s.sum_ms);
        }
    }
    out
}

/// Serves until the process exits; a bind failure is logged, never fatal.
pub async fn serve(addr: String) {
    let lis = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => return tracing::error!(addr, error = %e, "metrics listener failed"),
    };
    loop {
        let Ok((mut s, _)) = lis.accept().await else { continue };
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = s.read(&mut buf).await.unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);
            let (code, body) = match req.split_whitespace().nth(1) {
                Some("/metrics") => ("200 OK", render()),
                // 503 while ARI is down: the process is up but can't take calls.
                Some("/healthz") if ARI_CONNECTED.load(Relaxed) => ("200 OK", "ok\n".into()),
                Some("/healthz") => ("503 Service Unavailable", "ari disconnected\n".into()),
                _ => ("404 Not Found", String::new()),
            };
            let resp =
                format!("HTTP/1.1 {code}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            let _ = s.write_all(resp.as_bytes()).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cpu_time_and_memory_like_the_kernel_writes_them() {
        // The command name can hold spaces and parentheses; utime=250, stime=50 ticks -> 3.00s.
        let stat = "123 (va ani) x) S 1 123 123 0 -1 4194304 100 0 0 0 250 50 0 0 20 0 9 0 5000 1000000 500 18446744073709551615";
        let status = "Name:\tvaani\nVmPeak:\t  900000 kB\nVmSize:\t  800000 kB\nVmHWM:\t   60000 kB\nVmRSS:\t   51200 kB\nThreads:\t17\n";
        assert_eq!(
            parse_proc(stat, status),
            ProcStats {
                cpu_seconds: 3.0,
                resident_bytes: 51200 * 1024,
                resident_peak_bytes: 60000 * 1024,
                virtual_bytes: 800000 * 1024,
                threads: 17
            }
        );
        assert_eq!(parse_proc("", ""), ProcStats::default(), "unreadable input is zeros, not a panic");
    }

    #[tokio::test]
    async fn the_endpoint_reports_this_processs_own_numbers() {
        let m = render();
        let value = |name: &str| {
            m.lines().find_map(|l| l.strip_prefix(name)?.trim().parse::<f64>().ok()).unwrap_or_else(|| panic!("{name} missing from\n{m}"))
        };
        if cfg!(target_os = "linux") {
            assert!(value("process_resident_memory_bytes ") > 1_000_000.0, "a running test binary holds more than 1 MB");
            assert!(value("process_threads ") >= 1.0 && value("process_open_fds ") >= 3.0);
            assert!(value("process_resident_memory_peak_bytes ") >= value("process_resident_memory_bytes "));
        }
        assert!(value("vaani_tokio_alive_tasks ") >= 0.0 && value("process_cpu_seconds_total ") >= 0.0);
        for name in [
            "vaani_cpu_load_ratio ",
            "vaani_overloaded ",
            "vaani_calls_setting_up ",
            "vaani_calls_setup_timeout_total ",
            "vaani_orphan_channels_hung_up_total ",
            "vaani_ari_event_queue_length ",
        ] {
            value(name);
        }
        for (reason, direction) in REJECTED_LABELS {
            assert!(m.contains(&format!("vaani_calls_rejected_total{{reason=\"{reason}\",direction=\"{direction}\"}} ")), "{m}");
        }
    }

    #[test]
    fn histograms_render_cumulative_buckets_like_prometheus() {
        static H: Histogram<3> = Histogram::new(&[10.0, 100.0]);
        for v in [5.0, 50.0, 100.0, 500.0] {
            H.observe(v); // 100 is inside the 100 bucket: bounds are inclusive
        }
        let mut out = String::new();
        H.render(&mut out, "h", "");
        assert_eq!(out, "h_bucket{le=\"10\"} 1\nh_bucket{le=\"100\"} 3\nh_bucket{le=\"+Inf\"} 4\nh_sum 655\nh_count 4\n", "{out}");
        let mut labelled = String::new();
        render_histogram(&mut labelled, "t", "tool=\"x\"", &[10.0], &[1, 2], 7.5);
        assert_eq!(
            labelled,
            "t_bucket{tool=\"x\",le=\"10\"} 1\nt_bucket{tool=\"x\",le=\"+Inf\"} 3\nt_sum{tool=\"x\"} 7.5\nt_count{tool=\"x\"} 3\n"
        );
    }

    #[test]
    fn network_bytes_sum_every_interface_like_the_go_collector() {
        let dev = "Inter-|   Receive                                                |  Transmit\n face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n    lo: 1000 10 0 0 0 0 0 0 2000 10 0 0 0 0 0 0\n  eth0: 5000 50 0 0 0 0 0 0 7000 40 0 0 0 0 0 0\n";
        assert_eq!(parse_net_dev(dev), (6000, 9000));
        assert_eq!(parse_net_dev(""), (0, 0));
    }

    /// Every series the Go server exports that matters for comparing the two engines is here too, under the same name.
    #[test]
    fn the_engine_comparison_series_exist_under_the_go_names() {
        ttfb("llm", std::time::Duration::from_millis(300));
        tool_run("lookup", true, 25.0);
        agent_error("llm");
        let m = render();
        for name in [
            "vaani_call_duration_seconds_bucket{le=\"1\"}",
            "vaani_call_duration_seconds_count",
            "vaani_llm_ttft_ms_bucket{le=\"50\"}",
            "vaani_tts_ttfa_ms_sum",
            "vaani_stt_endpoint_ms_count",
            "vaani_pacer_drift_ms_bucket{le=\"0.5\"}",
            "vaani_calls_started_total{direction=\"inbound\"}",
            "vaani_calls_started_total{direction=\"outbound\"}",
            "vaani_agent_errors_total{stage=\"tts_closed\"}",
            "vaani_agent_turns_total",
            "vaani_agent_bargein_total",
            "vaani_agent_false_interruptions_total",
            "vaani_agent_interruption_pauses_total",
            "vaani_stt_audio_seconds_total",
            "vaani_llm_retries_total{result=\"recovered\"}",
            "vaani_llm_fallback_total{kind=\"hangup\"}",
            "vaani_tool_calls_total{tool=\"lookup\",outcome=\"ok\"}",
            "vaani_tool_call_duration_ms_bucket{tool=\"lookup\",le=\"40\"}",
            "vaani_audiosocket_frames_in_total",
            "vaani_audiosocket_frames_out_total",
            "vaani_audiosocket_bytes_in_total",
            "vaani_audiosocket_silence_sent_total",
            "vaani_audiosocket_send_errors_total",
            "vaani_audiosocket_malformed_total",
            "vaani_media_queue_dropped_total",
            "vaani_media_watchdog_timeouts_total",
            "vaani_recording_truncated_total",
            "vaani_ari_reconnects_total",
            "vaani_recording_bytes",
            "process_network_receive_bytes_total",
            "process_network_transmit_bytes_total",
        ] {
            assert!(m.contains(name), "{name} missing from\n{m}");
        }
        assert!(LLM_TTFT_MS.count() >= 1);
    }
}

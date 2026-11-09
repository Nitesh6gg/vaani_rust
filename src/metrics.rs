//! Process counters and a tiny Prometheus-text endpoint (`/metrics`,
//! `/healthz`). Hand-rolled on purpose: four numbers don't need a dependency.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

pub static CALLS_ACTIVE: AtomicU64 = AtomicU64::new(0);
pub static CALLS_STARTED: AtomicU64 = AtomicU64::new(0);
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
         # TYPE vaani_calls_started_total counter\nvaani_calls_started_total {}\n\
         # TYPE vaani_media_ports_in_use gauge\nvaani_media_ports_in_use {}\n\
         # TYPE vaani_ari_connected gauge\nvaani_ari_connected {}\n\
         # TYPE vaani_campaign_dials_total counter\nvaani_campaign_dials_total {}\n\
         # TYPE vaani_campaign_retries_total counter\nvaani_campaign_retries_total {}\n\
         # TYPE vaani_campaigns_completed_total counter\nvaani_campaigns_completed_total {}\n\
         # TYPE vaani_campaign_calls_in_flight gauge\nvaani_campaign_calls_in_flight {}\n",
        CALLS_ACTIVE.load(Relaxed),
        CALLS_STARTED.load(Relaxed),
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
}

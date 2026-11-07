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

//! Process counters and a tiny Prometheus-text endpoint (`/metrics`,
//! `/healthz`). Hand-rolled on purpose: four numbers don't need a dependency.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener};

pub static CALLS_ACTIVE: AtomicU64 = AtomicU64::new(0);
pub static CALLS_STARTED: AtomicU64 = AtomicU64::new(0);
pub static MEDIA_PORTS_IN_USE: AtomicU64 = AtomicU64::new(0);
pub static ARI_CONNECTED: AtomicBool = AtomicBool::new(false);

pub fn render() -> String {
    format!(
        "# TYPE vaani_calls_active gauge\nvaani_calls_active {}\n\
         # TYPE vaani_calls_started_total counter\nvaani_calls_started_total {}\n\
         # TYPE vaani_media_ports_in_use gauge\nvaani_media_ports_in_use {}\n\
         # TYPE vaani_ari_connected gauge\nvaani_ari_connected {}\n",
        CALLS_ACTIVE.load(Relaxed),
        CALLS_STARTED.load(Relaxed),
        MEDIA_PORTS_IN_USE.load(Relaxed),
        u8::from(ARI_CONNECTED.load(Relaxed)),
    )
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
            let resp = format!("HTTP/1.1 {code}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            let _ = s.write_all(resp.as_bytes()).await;
        });
    }
}

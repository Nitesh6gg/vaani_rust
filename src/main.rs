//! Vaani (Rust): real-time voice agent. Asterisk ARI + AudioSocket media plane,
//! Sarvam STT/TTS, an OpenAI-compatible LLM, Earshot VAD for barge-in, and agent
//! configuration read per call from a self-hosted Dograh's Postgres.

mod agent;
mod ai;
mod ari;
mod assets;
mod callagent;
mod config;
mod dograh;
mod media;
mod metrics;
#[cfg(test)]
mod testkit;

use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

fn init_logging() {
    use tracing_subscriber::{EnvFilter, fmt};
    let level = std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".into());
    let filter = EnvFilter::try_new(&level).unwrap_or_else(|_| EnvFilter::new("info"));
    if std::env::var("LOG_FORMAT").is_ok_and(|f| f == "json") {
        fmt().json().with_env_filter(filter).init();
    } else {
        fmt().with_env_filter(filter).init();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    config::load_dotenv();
    init_logging();
    // reqwest and tokio-tungstenite both use rustls; pick the provider explicitly.
    rustls::crypto::ring::default_provider().install_default().map_err(|_| anyhow::anyhow!("rustls crypto provider already installed"))?;

    let cfg = Arc::new(config::Config::load()?);
    let cancel = CancellationToken::new();

    let mut store = dograh::Store::open(&cfg.dograh_db_url, cfg.dograh_db_max_conns).await?;
    match cfg.storage() {
        Some(storage) => store.use_storage(storage),
        None => tracing::warn!("MINIO_ENDPOINT not set: recordings and transcripts won't be uploaded and recorded clips can't be played"),
    }
    tracing::info!(workflow_id = cfg.dograh_workflow_id, "connected to dograh's database");

    tokio::spawn(metrics::serve(cfg.metrics_addr.clone()));

    let ari = ari::Ari::new(&cfg.ari_url, &cfg.ari_user, &cfg.ari_pass, &cfg.ari_app);
    let url = ari::events_url(&cfg.ari_url, &cfg.ari_app, &cfg.ari_user, &cfg.ari_pass).context("ari")?;
    let (ev_tx, ev_rx) = mpsc::unbounded_channel();
    let (conn_tx, mut conn_rx) = watch::channel(false);
    tokio::spawn(ari::run_events(url, ev_tx, conn_tx, cancel.clone()));
    tokio::spawn(async move {
        while conn_rx.changed().await.is_ok() {
            metrics::ARI_CONNECTED.store(*conn_rx.borrow(), std::sync::atomic::Ordering::Relaxed);
        }
    });

    let manager = ari::manager::Manager::new(ari, cfg.clone(), store);
    let run = tokio::spawn(manager.run(ev_rx, cancel.clone()));

    tracing::info!(ari = %cfg.ari_url, app = %cfg.ari_app, media_ip = %cfg.media_ip,
        ports = format!("{}..{}", cfg.media_port_base, cfg.media_port_base + cfg.media_port_count), "vaani ready");
    shutdown_signal().await;
    tracing::info!("shutting down");
    cancel.cancel();
    run.await.context("manager")?;
    Ok(())
}

async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

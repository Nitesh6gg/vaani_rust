//! Process configuration from the environment (a `.env` in the working dir or
//! any parent is loaded first; real env vars win). Same variable names as the
//! Go server. Model/provider settings are NOT here: they come from Dograh.

use anyhow::{bail, Context, Result};
use std::{env, time::Duration};

#[derive(Debug, Clone)]
pub struct Config {
    pub ari_url: String,
    pub ari_user: String,
    pub ari_pass: String,
    pub ari_app: String,
    pub media_ip: String,
    pub media_port_base: u16,
    pub media_port_count: u16,
    pub dograh_db_url: String,
    pub dograh_db_max_conns: usize,
    pub dograh_workflow_id: i32,
    pub barge_in_enabled: bool,
    pub barge_in_min_speech: Duration,
    pub barge_in_guard: Duration,
    pub post_cut_silence: Duration,
    pub ack_filter: bool,
    pub ack_end_margin: Duration,
    /// Earshot's speech probability threshold (0..1).
    pub vad_threshold: f32,
    pub metrics_addr: String,
    /// Override Sarvam's WebSocket endpoints (mock servers in tests and load runs).
    pub stt_ws_url: String,
    pub tts_ws_url: String,
    pub max_call_duration: Duration,
    pub media_dead_timeout: Duration,
    pub llm_retries: u32,
    pub llm_retry_max_wait: Duration,
    pub llm_max_failed_turns: u32,
    pub llm_fallback_text: String,
}

fn get(key: &str, default: &str) -> String {
    env::var(key).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| default.to_string())
}

fn get_num<T: std::str::FromStr>(key: &str, default: T) -> Result<T>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match env::var(key).ok().filter(|v| !v.is_empty()) {
        None => Ok(default),
        Some(v) => v.trim().parse().with_context(|| format!("config: {key}={v:?} is not a valid number")),
    }
}

fn ms(key: &str, default: u64) -> Result<Duration> {
    Ok(Duration::from_millis(get_num(key, default)?))
}

fn secs(key: &str, default: u64) -> Result<Duration> {
    Ok(Duration::from_secs(get_num(key, default)?))
}

/// Minimal `.env` loader: KEY=VALUE lines, `#` comments, optional quotes.
/// Never overrides a variable that is already set.
pub fn load_dotenv() {
    let Ok(mut dir) = env::current_dir() else { return };
    loop {
        if let Ok(data) = std::fs::read_to_string(dir.join(".env")) {
            for line in data.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                if let Some((k, v)) = line.split_once('=') {
                    let (k, v) = (k.trim(), v.trim().trim_matches(['"', '\'']));
                    if env::var_os(k).is_none() {
                        // SAFETY: called once at startup, before any other thread exists.
                        unsafe { env::set_var(k, v) };
                    }
                }
            }
            return;
        }
        if !dir.pop() {
            return;
        }
    }
}

impl Config {
    pub fn load() -> Result<Self> {
        let c = Config {
            ari_url: get("ARI_URL", "http://localhost:8088/ari"),
            ari_user: get("ARI_USER", "vaani"),
            ari_pass: get("ARI_PASS", "vaani"),
            ari_app: get("ARI_APP", "vaani"),
            media_ip: get("MEDIA_IP", "127.0.0.1"),
            media_port_base: get_num("MEDIA_PORT_BASE", 20000)?,
            media_port_count: get_num("MEDIA_PORT_COUNT", 256)?,
            dograh_db_url: get("DOGRAH_DB_URL", ""),
            dograh_db_max_conns: get_num("DOGRAH_DB_MAX_CONNS", 50)?,
            dograh_workflow_id: get_num("DOGRAH_WORKFLOW_ID", 0)?,
            barge_in_enabled: get("BARGE_IN_ENABLED", "1") == "1",
            barge_in_min_speech: ms("BARGE_IN_MIN_SPEECH_MS", 200)?,
            barge_in_guard: ms("BARGE_IN_GUARD_MS", 300)?,
            post_cut_silence: ms("BARGE_IN_POST_CUT_SILENCE_MS", 1200)?,
            ack_filter: get("BARGE_IN_ACK_FILTER", "1") == "1",
            ack_end_margin: ms("ACK_END_MARGIN_MS", 300)?,
            metrics_addr: get("METRICS_ADDR", "0.0.0.0:9091"),
            stt_ws_url: get("SARVAM_STT_WS_URL", ""),
            tts_ws_url: get("SARVAM_TTS_WS_URL", ""),
            vad_threshold: get_num("VAD_THRESHOLD", 0.5)?,
            max_call_duration: secs("MAX_CALL_DURATION_SECONDS", 0)?,
            media_dead_timeout: secs("MEDIA_DEAD_TIMEOUT_SECONDS", 30)?,
            llm_retries: get_num("LLM_RETRIES", 2)?,
            llm_retry_max_wait: ms("LLM_RETRY_MAX_WAIT_MS", 2000)?,
            llm_max_failed_turns: get_num("LLM_MAX_FAILED_TURNS", 3)?,
            llm_fallback_text: get("LLM_FALLBACK_TEXT", ""),
        };
        c.validate()?;
        Ok(c)
    }

    fn validate(&self) -> Result<()> {
        if self.media_port_count == 0 {
            bail!("config: MEDIA_PORT_COUNT must be positive");
        }
        if self.media_port_base < 1024 {
            bail!("config: MEDIA_PORT_BASE must be >= 1024, got {}", self.media_port_base);
        }
        if u32::from(self.media_port_base) + u32::from(self.media_port_count) > 65535 {
            bail!("config: MEDIA_PORT_BASE + MEDIA_PORT_COUNT must be <= 65535");
        }
        if self.dograh_db_url.is_empty() {
            bail!("config: DOGRAH_DB_URL is required (agent config is read from Dograh's Postgres)");
        }
        if self.dograh_db_max_conns == 0 {
            bail!("config: DOGRAH_DB_MAX_CONNS must be at least 1");
        }
        Ok(())
    }
}

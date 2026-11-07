//! Process configuration from the environment (a `.env` in the working dir or
//! any parent is loaded first; real env vars win). Same variable names as the
//! Go server. Model/provider settings are NOT here: they come from Dograh.

use anyhow::{Context, Result, bail};
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
    /// Ask the STT to finalize once the VAD has heard this much quiet after the caller
    /// spoke, instead of waiting for Sarvam's own end-of-speech (~0.6-0.7s). 0 = off.
    pub stt_flush_after: Duration,
    /// Send the STT only the caller's speech (Sarvam bills the audio sent).
    pub stt_speech_only: bool,
    pub stt_preroll: Duration,
    pub stt_hangover: Duration,
    /// A frame at least this loud also opens the gate (media::rms units). 0 = off.
    pub stt_gate_rms_floor: f64,
    pub metrics_addr: String,
    /// Dial Dograh's campaigns for organizations whose media engine is "vaani" (needs `redis_url`).
    pub campaigns_enabled: bool,
    /// **Dograh's** Redis: the organizations' rate and concurrency counters live there, so limits hold across both engines.
    pub redis_url: String,
    /// Campaign calls this Vaani may have up at once (its own key, not Dograh's fleet cap). 0 = org limits only.
    pub outbound_max_calls: u32,
    /// The concurrent-call limit of an organization with none set (Dograh's default).
    pub outbound_default_org_concurrency: i32,
    pub campaign_poll: Duration,
    pub outbound_ring_timeout: Duration,
    /// "<number>,<org id>,<workflow id>": dial once at startup, outside any campaign.
    pub outbound_test_call: String,
    /// Dograh's MinIO (its own MINIO_* settings, which live in its environment, not its database);
    /// without an endpoint recordings and transcripts aren't uploaded.
    pub minio_endpoint: String,
    pub minio_access_key: String,
    pub minio_secret_key: String,
    pub minio_bucket: String,
    pub minio_secure: bool,
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
    /// Dograh's MinIO, if configured.
    pub fn storage(&self) -> Option<crate::dograh::storage::Storage> {
        (!self.minio_endpoint.is_empty()).then(|| crate::dograh::storage::Storage {
            endpoint: self.minio_endpoint.clone(),
            access_key: self.minio_access_key.clone(),
            secret_key: self.minio_secret_key.clone(),
            bucket: self.minio_bucket.clone(),
            secure: self.minio_secure,
        })
    }

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
            campaigns_enabled: get("CAMPAIGNS_ENABLED", "0") == "1",
            redis_url: get("REDIS_URL", ""),
            outbound_max_calls: get_num("OUTBOUND_MAX_CALLS", 20)?,
            outbound_default_org_concurrency: get_num("OUTBOUND_DEFAULT_ORG_CONCURRENCY", 2)?,
            campaign_poll: secs("CAMPAIGN_POLL_SECONDS", 5)?,
            outbound_ring_timeout: secs("OUTBOUND_RING_TIMEOUT_SECONDS", 30)?,
            outbound_test_call: get("OUTBOUND_TEST_CALL", ""),
            minio_endpoint: get("MINIO_ENDPOINT", ""),
            minio_access_key: get("MINIO_ACCESS_KEY", ""),
            minio_secret_key: get("MINIO_SECRET_KEY", ""),
            minio_bucket: get("MINIO_BUCKET", "voice-audio"),
            minio_secure: get("MINIO_SECURE", "false").eq_ignore_ascii_case("true"),
            stt_ws_url: get("SARVAM_STT_WS_URL", ""),
            tts_ws_url: get("SARVAM_TTS_WS_URL", ""),
            vad_threshold: get_num("VAD_THRESHOLD", 0.5)?,
            stt_flush_after: ms("STT_FLUSH_AFTER_MS", 400)?,
            stt_speech_only: get("STT_SPEECH_ONLY", "0") == "1",
            stt_preroll: ms("STT_PREROLL_MS", 1000)?,
            stt_hangover: ms("STT_HANGOVER_MS", 700)?,
            stt_gate_rms_floor: get_num("STT_GATE_RMS_FLOOR", 400.0)?,
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
        // A hangover shorter than the flush wait would close the gate before the flush's reply.
        if self.stt_speech_only && !self.stt_flush_after.is_zero() && self.stt_hangover <= self.stt_flush_after {
            bail!(
                "config: STT_HANGOVER_MS ({}) must exceed STT_FLUSH_AFTER_MS ({}), or the gate closes before the flush's reply",
                self.stt_hangover.as_millis(),
                self.stt_flush_after.as_millis()
            );
        }
        if self.campaigns_enabled && self.redis_url.is_empty() {
            bail!("config: CAMPAIGNS_ENABLED=1 needs REDIS_URL (Dograh's Redis)");
        }
        if !(5..=300).contains(&self.outbound_ring_timeout.as_secs()) {
            bail!("config: OUTBOUND_RING_TIMEOUT_SECONDS must be 5-300, got {}", self.outbound_ring_timeout.as_secs());
        }
        if self.outbound_default_org_concurrency < 1 || self.campaign_poll.is_zero() {
            bail!("config: OUTBOUND_DEFAULT_ORG_CONCURRENCY and CAMPAIGN_POLL_SECONDS must be at least 1");
        }
        if !self.outbound_test_call.is_empty() {
            crate::campaign::testcall::parse(&self.outbound_test_call)?;
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

#[cfg(test)]
impl Config {
    /// A config for tests: loopback media on a small port range, no caps.
    pub fn for_test(db_url: &str) -> Self {
        Config {
            ari_url: "http://127.0.0.1:1/ari".into(),
            ari_user: "u".into(),
            ari_pass: "p".into(),
            ari_app: "vaani".into(),
            media_ip: "127.0.0.1".into(),
            media_port_base: 41000,
            media_port_count: 4,
            dograh_db_url: db_url.into(),
            dograh_db_max_conns: 4,
            dograh_workflow_id: 42,
            barge_in_enabled: true,
            barge_in_min_speech: Duration::from_millis(200),
            barge_in_guard: Duration::from_millis(300),
            post_cut_silence: Duration::from_millis(1200),
            ack_filter: true,
            ack_end_margin: Duration::from_millis(300),
            metrics_addr: String::new(),
            campaigns_enabled: false,
            redis_url: String::new(),
            outbound_max_calls: 20,
            outbound_default_org_concurrency: 2,
            campaign_poll: Duration::from_secs(5),
            outbound_ring_timeout: Duration::from_secs(30),
            outbound_test_call: String::new(),
            minio_endpoint: String::new(),
            minio_access_key: String::new(),
            minio_secret_key: String::new(),
            minio_bucket: "voice-audio".into(),
            minio_secure: false,
            stt_ws_url: String::new(),
            tts_ws_url: String::new(),
            vad_threshold: 0.5,
            stt_flush_after: Duration::ZERO,
            stt_speech_only: false,
            stt_preroll: Duration::from_millis(1000),
            stt_hangover: Duration::from_millis(700),
            stt_gate_rms_floor: 400.0,
            max_call_duration: Duration::ZERO,
            media_dead_timeout: Duration::from_secs(5),
            llm_retries: 0,
            llm_retry_max_wait: Duration::from_millis(50),
            llm_max_failed_turns: 3,
            llm_fallback_text: String::new(),
        }
    }
}

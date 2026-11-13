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
    /// How long a caller waits for a free Postgres connection before failing (DOGRAH_DB_WAIT_MS, default 5000).
    pub dograh_db_wait: Duration,
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
    /// Contacts of one campaign dialed at the same time (CAMPAIGN_DIAL_WORKERS, default 10; 1 = one at a time).
    pub campaign_dial_workers: usize,
    /// The dialer places no call while this many answered calls are still being set up (staged, not yet
    /// bridged): it never dials faster than Vaani and Asterisk can set calls up. 0 = no cap.
    pub outbound_max_setup: usize,
    /// How often Asterisk's channels are listed and those of this ARI app that Vaani lost track of are
    /// hung up (ORPHAN_SWEEP_SECONDS, default 30; 0 = off). Needs this Vaani to be the only one on its ARI app name.
    pub orphan_sweep: Duration,
    /// How many ended calls finish their Dograh record at once (POST_CALL_WORKERS, default 32): the
    /// last extraction, the run's completion, the recording and transcript uploads, the cost. The rest wait
    /// (their recordings stay in memory), so a burst of hang-ups can't open hundreds of uploads and LLM calls at once.
    pub post_call_workers: usize,
    /// How long a shutdown (SIGTERM) waits for calls still cleaning up and records still being written before
    /// giving up (SHUTDOWN_WAIT_SECONDS, default 60). It returns as soon as they are done.
    pub shutdown_wait: Duration,
    /// Connections to Redis for the campaign dialer (REDIS_POOL_SIZE, default 4, 1 to 64): commands are spread
    /// over them, so one stalled reply doesn't hold up every dial worker.
    pub redis_pool_size: usize,
    /// How long an inbound caller may wait to be answered (ANSWER_WAIT_SECONDS, default 10, 0 = no limit): the time
    /// to load its workflow, open its STT and get a turn at Asterisk. Past it the caller is hung up as congestion
    /// (SIP 503, so the trunk can try another server) instead of being answered late -- by then they have often gone.
    pub answer_wait: Duration,
    /// Call setups (answer, externalMedia, bridge) running against Asterisk at once (SETUP_CONCURRENCY, default 16): a burst
    /// waits its turn in Vaani instead of piling onto Asterisk.
    pub setup_concurrency: usize,
    /// Workflow loads + STT handshakes for callers not yet answered, at once (PREPARE_CONCURRENCY, default 32): the rest of a
    /// burst waits, inside its answer deadline, instead of hitting Postgres and Sarvam all together.
    pub prepare_concurrency: usize,
    /// A staged call whose media channel doesn't enter Stasis within this long is hung up (SETUP_TIMEOUT_SECONDS, default 15).
    pub setup_timeout: Duration,
    /// While the machine's CPU (a 5-second average, Asterisk included) is at or over this percent, new
    /// calls are refused -- inbound ones hung up as congestion before they are answered, and campaigns
    /// don't dial -- so the calls already up don't get jitter. Calls in progress are never dropped.
    /// 0 = off. It must stay under for 10 seconds below `load_resume_percent` before calls are taken again.
    pub load_shed_cpu_percent: u32,
    pub load_resume_percent: u32,
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
            dograh_db_max_conns: get_num("DOGRAH_DB_MAX_CONNS", 40)?,
            dograh_db_wait: ms("DOGRAH_DB_WAIT_MS", 5000)?,
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
            campaign_dial_workers: get_num("CAMPAIGN_DIAL_WORKERS", 10)?,
            outbound_max_setup: get_num("OUTBOUND_MAX_SETUP", 30)?,
            orphan_sweep: secs("ORPHAN_SWEEP_SECONDS", 30)?,
            post_call_workers: get_num("POST_CALL_WORKERS", 32)?,
            shutdown_wait: secs("SHUTDOWN_WAIT_SECONDS", 60)?,
            redis_pool_size: get_num("REDIS_POOL_SIZE", 4)?,
            setup_concurrency: get_num("SETUP_CONCURRENCY", 16)?,
            prepare_concurrency: get_num("PREPARE_CONCURRENCY", 32)?,
            setup_timeout: secs("SETUP_TIMEOUT_SECONDS", 15)?,
            answer_wait: secs("ANSWER_WAIT_SECONDS", 10)?,
            load_shed_cpu_percent: get_num("LOAD_SHED_CPU_PERCENT", 90)?,
            load_resume_percent: get_num("LOAD_SHED_RESUME_PERCENT", 80)?,
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
        if self.post_call_workers < 1 || self.shutdown_wait.is_zero() || !(1..=64).contains(&self.redis_pool_size) {
            bail!(
                "config: POST_CALL_WORKERS and SHUTDOWN_WAIT_SECONDS must be at least 1, REDIS_POOL_SIZE 1 to 64, got {}, {}, {}",
                self.post_call_workers,
                self.shutdown_wait.as_secs(),
                self.redis_pool_size
            );
        }
        if self.campaign_dial_workers < 1 {
            bail!("config: CAMPAIGN_DIAL_WORKERS must be at least 1, got {}", self.campaign_dial_workers);
        }
        if self.load_shed_cpu_percent > 100
            || (self.load_shed_cpu_percent > 0 && !(1..self.load_shed_cpu_percent).contains(&self.load_resume_percent))
        {
            bail!(
                "config: LOAD_SHED_CPU_PERCENT must be 0 (off) to 100, and LOAD_SHED_RESUME_PERCENT from 1 to below it, got {} and {}",
                self.load_shed_cpu_percent,
                self.load_resume_percent
            );
        }
        if !self.outbound_test_call.is_empty() {
            crate::campaign::testcall::parse(&self.outbound_test_call)?;
        }
        if self.dograh_db_url.is_empty() {
            bail!("config: DOGRAH_DB_URL is required (agent config is read from Dograh's Postgres)");
        }
        if self.setup_concurrency == 0 || self.prepare_concurrency == 0 || self.setup_timeout.is_zero() {
            bail!("config: SETUP_CONCURRENCY, PREPARE_CONCURRENCY and SETUP_TIMEOUT_SECONDS must be at least 1");
        }
        if self.dograh_db_wait.is_zero() {
            bail!("config: DOGRAH_DB_WAIT_MS must be at least 1");
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
            // Tests run in parallel and bind real sockets: each config gets its own range.
            media_port_base: 30000 + (rand::random::<u16>() % 2000) * 16,
            media_port_count: 8,
            dograh_db_url: db_url.into(),
            dograh_db_max_conns: 4,
            dograh_db_wait: Duration::from_secs(5),
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
            campaign_dial_workers: 1,
            outbound_max_setup: 0,
            orphan_sweep: Duration::ZERO,
            post_call_workers: 32,
            shutdown_wait: Duration::from_secs(10),
            redis_pool_size: 4,
            setup_concurrency: 16,
            prepare_concurrency: 32,
            setup_timeout: Duration::from_secs(15),
            answer_wait: Duration::from_secs(10),
            load_shed_cpu_percent: 0,
            load_resume_percent: 80,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> Config {
        Config::for_test("postgres://x")
    }

    #[test]
    fn the_cpu_guard_and_the_dial_workers_are_validated() {
        let mut c = valid();
        assert!(c.validate().is_ok());
        c.load_shed_cpu_percent = 90;
        c.load_resume_percent = 80;
        assert!(c.validate().is_ok());
        for (limit, resume) in [(101, 80), (90, 90), (90, 0), (90, 95)] {
            (c.load_shed_cpu_percent, c.load_resume_percent) = (limit, resume);
            assert!(c.validate().is_err(), "{limit}/{resume}");
        }
        c.load_shed_cpu_percent = 0; // off: the resume level doesn't matter
        c.load_resume_percent = 0;
        assert!(c.validate().is_ok());
        c.campaign_dial_workers = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn the_post_call_pool_shutdown_wait_and_redis_pool_are_validated() {
        let mut c = valid();
        assert!(c.validate().is_ok());
        for (workers, wait, pool) in [(0, 60, 4), (32, 0, 4), (32, 60, 0), (32, 60, 65)] {
            (c.post_call_workers, c.shutdown_wait, c.redis_pool_size) = (workers, Duration::from_secs(wait), pool);
            assert!(c.validate().is_err(), "{workers}/{wait}/{pool}");
        }
        (c.post_call_workers, c.shutdown_wait, c.redis_pool_size) = (8, Duration::from_secs(120), 64);
        assert!(c.validate().is_ok());
    }
}

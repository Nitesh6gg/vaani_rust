//! Builds one call's agent: the Dograh workflow (read fresh per call, so a
//! publish in the editor applies to the next call), the models it configures,
//! and the STT connection.

use crate::{
    agent::{
        handler::{AgentConfig, AgentIo, TransferFn},
        sttgate::GateConfig,
        vad::VoteDetector,
    },
    ai::{
        llm::LlmClient,
        stt::{SttClient, SttConfig, SttEvent},
        tts::TtsConfig,
    },
    config::Config,
    dograh::{LoadedWorkflow, Store, run::RunMeta},
    media::call::MediaIo,
};
use anyhow::{Context, Result};
use serde_json::{Map, Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_util::sync::CancellationToken;

/// Bounds the Dograh read: a stuck database must not hold a ringing call forever.
const LOAD_TIMEOUT: Duration = Duration::from_secs(10);

pub struct CallInfo {
    pub id: String,
    pub caller_number: String,
    pub called_number: String,
    /// "inbound", or "outbound" for a call Vaani dialed.
    pub direction: &'static str,
    /// An outbound campaign call has its own workflow, a Dograh run the dialer created before dialing,
    /// and the contact's template variables; an inbound call uses DOGRAH_WORKFLOW_ID and gets a run created.
    pub workflow_id: Option<i32>,
    pub run_id: Option<i64>,
    pub variables: serde_json::Map<String, Value>,
}

/// Everything a call needs before its media connection exists.
pub struct Prepared {
    wf: LoadedWorkflow,
    stt: SttClient,
    stt_events: UnboundedReceiver<SttEvent>,
}

/// Loads the workflow and opens the STT. Runs concurrently with Asterisk
/// connecting the media socket, so neither waits for the other.
pub async fn prepare(cfg: &Config, store: &Store, info: &CallInfo, cancel: &CancellationToken) -> Result<Prepared> {
    // The contact's variables, over which the call's own numbers are laid.
    let mut vars = info.variables.clone();
    vars.insert("caller_number".into(), json!(info.caller_number));
    vars.insert("called_number".into(), json!(info.called_number));
    let wf = tokio::time::timeout(LOAD_TIMEOUT, store.workflow(info.workflow_id.unwrap_or(cfg.dograh_workflow_id), &vars))
        .await
        .context("dograh: workflow load timed out")??;
    for w in &wf.warnings {
        tracing::warn!(call_id = %info.id, component = "dograh", event = "workflow.warning", workflow_id = wf.id, "agent: workflow: {w}");
    }
    tracing::info!(call_id = %info.id, component = "dograh", event = "workflow.loaded", workflow_id = wf.id, definition_id = wf.definition_id,
        nodes = wf.workflow.nodes.len(), llm_model = %wf.services.llm.model, "agent workflow loaded");

    let (stt, stt_events) = SttClient::connect(
        &info.id,
        SttConfig {
            ws_url: cfg.stt_ws_url.clone(),
            api_key: wf.services.stt.api_key.clone(),
            model: wf.services.stt.model.clone(),
            language: wf.services.stt.language.clone(),
            flush_signal: !cfg.stt_flush_after.is_zero(),
        },
        cancel.clone(),
    )
    .await?;
    Ok(Prepared { wf, stt, stt_events })
}

impl Prepared {
    /// What the call's Dograh run needs to remember about its workflow.
    pub fn run_meta(&self) -> RunMeta {
        self.wf.run_meta()
    }

    /// Assembles the agent once the media connection is up. `hangup` hangs the
    /// caller's channel up (which fires StasisEnd and the normal teardown).
    pub fn into_agent(
        self,
        cfg: &Config,
        call_id: &str,
        media: MediaIo,
        hangup: Arc<dyn Fn() + Send + Sync>,
        transfer: Option<TransferFn>,
    ) -> (AgentConfig, AgentIo) {
        let Prepared { wf, stt, stt_events } = self;
        let s = &wf.services;
        let mut llm = LlmClient::new(&s.llm.base_url, &s.llm.api_key, &s.llm.model);
        llm.retries = cfg.llm_retries;
        llm.retry_max_wait = cfg.llm_retry_max_wait;
        let agent = AgentConfig {
            call_id: call_id.to_string(),
            workflow: Arc::new(wf.workflow),
            llm,
            tts: TtsConfig {
                ws_url: cfg.tts_ws_url.clone(),
                api_key: s.tts.api_key.clone(),
                model: s.tts.model.clone(),
                voice: s.tts.voice.clone(),
                language: s.tts.language.clone(),
            },
            vad: Box::new(VoteDetector::earshot(cfg.vad_threshold)),
            barge_in_enabled: cfg.barge_in_enabled,
            barge_in_min_speech: cfg.barge_in_min_speech,
            barge_in_guard: cfg.barge_in_guard,
            post_cut_silence: cfg.post_cut_silence,
            stt_flush_after: cfg.stt_flush_after,
            stt_gate: cfg.stt_speech_only.then_some(GateConfig {
                preroll: cfg.stt_preroll,
                hangover: cfg.stt_hangover,
                rms_floor: cfg.stt_gate_rms_floor,
            }),
            ack_filter: cfg.ack_filter,
            ack_end_margin: cfg.ack_end_margin,
            idle_timeout: wf.idle_timeout,
            max_duration: wf.max_duration,
            llm_max_failed_turns: cfg.llm_max_failed_turns,
            llm_fallback_text: cfg.llm_fallback_text.clone(),
            hangup,
            transfer,
            hold_audio: crate::assets::transfer_hold_ring(),
            beep_audio: crate::assets::beep(),
        };
        (agent, AgentIo { media, stt, stt_events })
    }
}

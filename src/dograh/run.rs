//! Each call is recorded in Dograh's `workflow_runs`, so it shows in Dograh's
//! call history like one of its own: created when the call starts
//! (ari_manager.py, inbound ARI call), completed when the agent leaves it
//! (event_handlers.py on_pipeline_finished, plus the recording and transcript
//! uploads its process_workflow_completion job makes).

use super::{
    services::Services,
    storage::Storage,
    store::{LoadedWorkflow, Store},
};
use crate::agent::calllog::{self, LogEvent, Summary};
use anyhow::{Context, Result, anyhow};
use serde_json::{Map, Value, json};
use std::time::Duration;

/// Each `finish_run` step gets its own time budget, so a slow one can't use up
/// the next one's (a slow MinIO used to leave runs stuck at 'running').
const DB_TIMEOUT: Duration = Duration::from_secs(10);
const UPLOAD_TIMEOUT: Duration = Duration::from_secs(20);

// Keys of Dograh's usage_info ("<processor>|||<model>"); its cost job prices by the model part only.
const USAGE_KEY_TTS: &str = "SarvamTTSService|||";
const USAGE_KEY_STT: &str = "SarvamSTTService|||";

/// What a run needs to remember about its workflow while the call is live.
#[derive(Debug, Clone)]
pub struct RunMeta {
    pub workflow_id: i32,
    pub definition_id: i64,
    pub org_id: Option<i64>,
    pub stt_model: String,
    pub tts_model: String,
}

impl LoadedWorkflow {
    pub fn run_meta(&self) -> RunMeta {
        let Services { stt, tts, .. } = &self.services;
        RunMeta {
            workflow_id: self.id,
            definition_id: self.definition_id,
            org_id: self.org_id,
            stt_model: stt.model.clone(),
            tts_model: tts.model.clone(),
        }
    }
}

/// The call a run is created for.
pub struct RunCall {
    /// The caller's channel id.
    pub call_id: String,
    pub caller_number: String,
    pub called_number: String,
}

/// What a finished call leaves behind.
pub struct RunEnd<'a> {
    pub summary: &'a Summary,
    pub duration: Duration,
    /// A WAV file; empty when there's none.
    pub recording: Vec<u8>,
}

impl Store {
    /// Creates the call's run and returns its id, trying a second time after a
    /// failure: one dropped connection must not cost the call its place in
    /// Dograh's history.
    pub async fn start_run(&self, m: &RunMeta, c: &RunCall) -> Result<i64> {
        match self.try_start_run(m, c).await {
            Ok(id) => Ok(id),
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(500)).await;
                self.try_start_run(m, c).await
            }
        }
    }

    async fn try_start_run(&self, m: &RunMeta, c: &RunCall) -> Result<i64> {
        let initial =
            json!({"caller_number": c.caller_number, "called_number": c.called_number, "direction": "inbound", "provider": "ari"})
                .to_string();
        let gathered = json!({"call_id": c.call_id}).to_string();
        let name = format!("ARI Inbound {}", c.caller_number);
        let db = self.db().await?;
        let row = tokio::time::timeout(
            DB_TIMEOUT,
            db.query_one(
                "INSERT INTO workflow_runs (name, workflow_id, definition_id, mode, call_type, state,
                    is_completed, usage_info, cost_info, initial_context, gathered_context, logs,
                    annotations, created_at)
                 VALUES ($1, $2::int, $3::bigint, 'ari', 'inbound', 'running', false, '{}', '{}',
                    $4::text::json, $5::text::json, '{}', '{}', now())
                 RETURNING id::bigint",
                &[&name, &m.workflow_id, &m.definition_id, &initial, &gathered],
            ),
        )
        .await
        .map_err(|_| anyhow!("timed out"))?
        .context("dograh: create run")?;
        Ok(row.get(0))
    }

    /// Completes run `run_id`: what the call gathered, its length, its
    /// conversation (the run page's events), and -- with `storage` -- its
    /// recording and transcript, uploaded where Dograh keeps them.
    ///
    /// The run is marked completed first and the files attached after, as
    /// Dograh's own completion job does: a slow or failing upload, or the
    /// process being stopped meanwhile, then costs only the files, never leaves
    /// the run at 'running'. Every failure is in the returned error.
    pub async fn finish_run(&self, m: &RunMeta, run_id: i64, end: RunEnd<'_>, storage: Option<&Storage>) -> Result<()> {
        let sum = end.summary;
        let mut errs: Vec<String> = vec![];

        let (g, mapped) = {
            let org = m.org_id;
            let mapping = self.disposition_mapping(org).await;
            gathered_context(sum, sum.extracted.as_ref(), &sum.extracted_keys, &|v| {
                mapping.as_ref().and_then(|m| m.get(v)).and_then(Value::as_str).unwrap_or(v).to_string()
            })
        };

        // The call's own usage now, so it's there even if pricing never runs.
        // call_duration_seconds goes into cost_info too: that's where Dograh's
        // call list reads the duration.
        let usage = usage_info(m, sum, end.duration);
        let cost = json!({"call_duration_seconds": usage["call_duration_seconds"]});
        let logs = if sum.events.is_empty() { "{}".to_string() } else { json!({"realtime_feedback_events": sum.events}).to_string() };

        let step = |what: &'static str, r: std::result::Result<Result<u64, tokio_postgres::Error>, tokio::time::error::Elapsed>| match r {
            Ok(Ok(_)) => None,
            Ok(Err(e)) => Some(format!("dograh: {what}: {e}")),
            Err(_) => Some(format!("dograh: {what}: timed out")),
        };

        let db = self.db().await?;
        errs.extend(step(
            "complete run",
            tokio::time::timeout(
                DB_TIMEOUT,
                db.execute(
                    "UPDATE workflow_runs SET
                        usage_info = $2::text::json,
                        cost_info = (cost_info::jsonb || $3::text::jsonb)::json,
                        gathered_context = (gathered_context::jsonb || $4::text::jsonb)::json,
                        logs = $5::text::json,
                        is_completed = true,
                        state = 'completed'
                     WHERE id = $1::bigint",
                    &[&run_id, &usage.to_string(), &cost.to_string(), &Value::Object(g).to_string(), &logs],
                ),
            )
            .await,
        ));

        // Lets Dograh's call list filter by this disposition code.
        if !mapped.is_empty() {
            errs.extend(step(
                "add disposition code",
                tokio::time::timeout(
                    DB_TIMEOUT,
                    db.execute(
                        "UPDATE workflows SET call_disposition_codes = jsonb_build_object('disposition_codes',
                            COALESCE(call_disposition_codes::jsonb->'disposition_codes', '[]'::jsonb) || to_jsonb($2::text))::json
                         WHERE id = $1::int
                           AND NOT COALESCE(call_disposition_codes::jsonb->'disposition_codes', '[]'::jsonb) ? $2::text",
                        &[&m.workflow_id, &mapped],
                    ),
                )
                .await,
            ));
        }

        if let Some(storage) = storage {
            let id = run_id.to_string();
            let put = |key: String, ctype: &'static str, body: Vec<u8>| {
                let st = storage.clone();
                async move {
                    match tokio::time::timeout(UPLOAD_TIMEOUT, st.put(&key, ctype, body)).await {
                        Ok(Ok(())) => (Some(key), None),
                        Ok(Err(e)) => (None, Some(e.to_string())),
                        Err(_) => (None, Some(format!("minio put {key}: timed out"))),
                    }
                }
            };
            let (mut recording_url, mut transcript_url) = (None, None);
            if !end.recording.is_empty() {
                let (k, e) = put(format!("recordings/{id}.wav"), "audio/wav", end.recording).await;
                (recording_url, errs) = (k, errs.into_iter().chain(e).collect());
            }
            let text = transcript_text(&sum.events);
            if !text.is_empty() {
                let (k, e) = put(format!("transcripts/{id}.txt"), "text/plain; charset=utf-8", text.into_bytes()).await;
                (transcript_url, errs) = (k, errs.into_iter().chain(e).collect());
            }
            if recording_url.is_some() || transcript_url.is_some() {
                errs.extend(step(
                    "attach files to run",
                    tokio::time::timeout(
                        DB_TIMEOUT,
                        db.execute(
                            "UPDATE workflow_runs SET
                                recording_url = COALESCE($2::text, recording_url),
                                transcript_url = COALESCE($3::text, transcript_url),
                                storage_backend = 'minio'
                             WHERE id = $1::bigint",
                            &[&run_id, &recording_url, &transcript_url],
                        ),
                    )
                    .await,
                ));
            }
        }

        if errs.is_empty() { Ok(()) } else { Err(anyhow!(errs.join("; "))) }
    }

    /// The organization's DISPOSITION_CODE_MAPPING ({"user_hangup": "HU", ...}), if it has one.
    async fn disposition_mapping(&self, org: Option<i64>) -> Option<Map<String, Value>> {
        let org = org?;
        let db = self.db().await.ok()?;
        let row = tokio::time::timeout(
            DB_TIMEOUT,
            db.query_opt(
                "SELECT value::text FROM organization_configurations
                 WHERE organization_id = $1::bigint AND key = 'DISPOSITION_CODE_MAPPING' ORDER BY id LIMIT 1",
                &[&org],
            ),
        )
        .await
        .ok()?
        .ok()??;
        serde_json::from_str::<Value>(&row.get::<_, String>(0)).ok()?.as_object().cloned()
    }
}

/// The call's usage_info in Dograh's shape (get_all_usage_metrics_serialized):
/// LLM tokens by key, TTS characters, and STT billed as the whole call, as
/// Dograh bills streaming STT.
fn usage_info(m: &RunMeta, sum: &Summary, duration: Duration) -> Value {
    let seconds = duration.as_secs_f64().round() as i64;
    let llm: Map<String, Value> = sum
        .llm_usage
        .iter()
        .map(|(k, u)| {
            (
                k.clone(),
                json!({"prompt_tokens": u.prompt, "completion_tokens": u.completion, "total_tokens": u.total,
                       "cache_read_input_tokens": u.cached, "cache_creation_input_tokens": null}),
            )
        })
        .collect();
    let tts = if sum.tts_chars > 0 { json!({format!("{USAGE_KEY_TTS}{}", m.tts_model): sum.tts_chars}) } else { json!({}) };
    json!({
        "llm": llm,
        "tts": tts,
        "stt": {format!("{USAGE_KEY_STT}{}", m.stt_model): seconds as f64},
        "call_duration_seconds": seconds,
    })
}

/// What the call adds to the run's gathered_context, as Dograh's engine builds
/// it: nodes_visited; the extracted variables, at the top level and under
/// extracted_variables; the disposition (end_call_with_reason) -- an extracted
/// call_disposition if there is one, else why the call ended -- and its mapping;
/// and the call tags: the disposition, user_speech if the caller said anything,
/// then (on_pipeline_finished) the value of every tag_* variable.
pub fn gathered_context(
    sum: &Summary,
    extracted: Option<&Map<String, Value>>,
    extracted_keys: &[String],
    map_disposition: &dyn Fn(&str) -> String,
) -> (Map<String, Value>, String) {
    let mut g = Map::new();
    g.insert("nodes_visited".into(), json!(sum.nodes_visited));
    if let Some(ex) = extracted {
        g.extend(ex.clone());
        g.insert("extracted_variables".into(), Value::Object(ex.clone()));
    }
    // An extracted one counts only as non-empty text (Dograh: any truthy value).
    let extracted_disp = extracted.and_then(|e| e.get("call_disposition")).and_then(Value::as_str).filter(|d| !d.is_empty());
    if let Some(ed) = extracted_disp {
        g.insert("extracted_call_disposition".into(), json!(ed));
    }
    let d = extracted_disp.unwrap_or_else(|| sum.end_reason.unwrap_or(calllog::USER_HANGUP)).to_string();
    let mapped = if d.is_empty() { d.clone() } else { map_disposition(&d) };
    g.insert("call_disposition".into(), json!(d));
    g.insert("mapped_call_disposition".into(), json!(mapped));

    let mut tags = vec![json!(d)];
    if sum.user_spoke {
        tags.push(json!("user_speech"));
    }
    // Dograh's own check compares the key, not the value, with the tags.
    for k in extracted_keys {
        if k.starts_with("tag_") && !tags.iter().any(|t| t == &json!(k)) {
            tags.push(extracted.and_then(|e| e.get(k)).cloned().unwrap_or(Value::Null));
        }
    }
    g.insert("call_tags".into(), Value::Array(tags));
    (g, mapped)
}

/// Dograh's generate_transcript_text: one line per caller turn and agent reply,
/// "[timestamp] User: ..." / "[timestamp] Agent: ...".
pub fn transcript_text(events: &[LogEvent]) -> String {
    let mut out = String::new();
    for e in events {
        let who = match e.kind.as_str() {
            "rtf-user-transcription" if e.payload.get("final") == Some(&Value::Bool(true)) => "User",
            "rtf-bot-text" => "Agent",
            _ => continue,
        };
        let ts = e.payload.get("timestamp").and_then(Value::as_str).filter(|t| !t.is_empty()).unwrap_or(&e.timestamp);
        if !ts.is_empty() {
            out.push_str(&format!("[{ts}] "));
        }
        out.push_str(&format!("{who}: {}\n", e.payload.get("text").and_then(Value::as_str).unwrap_or("")));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::llm::Usage;

    fn ev(kind: &str, ts: &str, payload: Value) -> LogEvent {
        LogEvent {
            kind: kind.into(),
            payload: payload.as_object().cloned().unwrap(),
            timestamp: ts.into(),
            turn: 0,
            node_id: String::new(),
            node_name: String::new(),
        }
    }

    #[test]
    fn transcript_matches_dograh() {
        let events = vec![
            ev("rtf-node-transition", "t0", json!({"node_name": "Start"})),
            ev("rtf-bot-text", "t1", json!({"text": "नमस्ते", "timestamp": "2026-10-05T12:00:00.000+00:00"})),
            ev("rtf-user-transcription", "t2", json!({"text": "हाँ", "final": true})),
            ev("rtf-function-call-start", "t3", json!({"function_name": "x"})),
        ];
        assert_eq!(transcript_text(&events), "[2026-10-05T12:00:00.000+00:00] Agent: नमस्ते\n[t2] User: हाँ\n");
        assert_eq!(transcript_text(&[]), "");
    }

    fn map(v: Value) -> Map<String, Value> {
        v.as_object().cloned().unwrap()
    }

    #[test]
    fn gathered_context_matches_dograh() {
        let mapping = |v: &str| match v {
            "user_hangup" => "HU".to_string(),
            "interested" => "INT".to_string(),
            o => o.to_string(),
        };

        // No extraction, caller hung up.
        let sum = Summary { nodes_visited: vec!["Start".into()], ..Default::default() };
        let (g, mapped) = gathered_context(&sum, None, &[], &mapping);
        assert_eq!(
            Value::Object(g),
            json!({"nodes_visited": ["Start"], "call_disposition": "user_hangup", "mapped_call_disposition": "HU", "call_tags": ["user_hangup"]})
        );
        assert_eq!(mapped, "HU");

        // Extracted variables, disposition and tags.
        let sum = Summary {
            nodes_visited: vec!["Start".into(), "End".into()],
            end_reason: Some("user_qualified"),
            user_spoke: true,
            ..Default::default()
        };
        let ex = map(json!({"party": "AAP", "call_disposition": "interested", "tag_x": "voter"}));
        let keys: Vec<String> = ["party", "call_disposition", "tag_x"].map(String::from).to_vec();
        let (g, mapped) = gathered_context(&sum, Some(&ex), &keys, &mapping);
        assert_eq!(
            Value::Object(g),
            json!({
                "nodes_visited": ["Start", "End"], "party": "AAP", "call_disposition": "interested", "tag_x": "voter",
                "extracted_variables": {"party": "AAP", "call_disposition": "interested", "tag_x": "voter"},
                "extracted_call_disposition": "interested", "mapped_call_disposition": "INT",
                "call_tags": ["interested", "user_speech", "voter"]
            })
        );
        assert_eq!(mapped, "INT");

        // An empty extracted disposition falls back to why the call ended.
        let sum = Summary { end_reason: Some("end_call_tool"), ..Default::default() };
        let ex = map(json!({"call_disposition": ""}));
        let (g, mapped) = gathered_context(&sum, Some(&ex), &["call_disposition".into()], &mapping);
        assert_eq!(g["call_disposition"], "end_call_tool");
        assert_eq!((g["call_tags"].clone(), mapped.as_str()), (json!(["end_call_tool"]), "end_call_tool"));
    }

    #[test]
    fn usage_info_has_dograhs_shape() {
        let m = RunMeta { workflow_id: 1, definition_id: 2, org_id: None, stt_model: "saaras:v4".into(), tts_model: "bulbul:v3".into() };
        let mut sum = Summary { tts_chars: 120, ..Default::default() };
        sum.llm_usage.insert("LLM|||gpt".into(), Usage { prompt: 10, completion: 5, total: 15, cached: 2 });
        let u = usage_info(&m, &sum, Duration::from_millis(71_400));
        assert_eq!(u["call_duration_seconds"], 71);
        assert_eq!(u["stt"]["SarvamSTTService|||saaras:v4"], 71.0);
        assert_eq!(u["tts"]["SarvamTTSService|||bulbul:v3"], 120);
        assert_eq!(u["llm"]["LLM|||gpt"]["cache_read_input_tokens"], 2);
        assert_eq!(usage_info(&m, &Summary::default(), Duration::ZERO)["tts"], json!({}));
    }
}

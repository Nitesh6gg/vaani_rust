//! An outbound call is dialed as Dograh's ARI provider does
//! (api/services/telephony/providers/ari_provider.py), from the organization's
//! TELEPHONY_CONFIGURATION. Only the dial-string fields are read here: Vaani has
//! its own ARI connection, so the config's ARI password is never touched.

use super::store::Cached;
use super::{
    Store,
    campaign::{Campaign, QueuedRun},
};
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::{collections::HashMap, time::Duration};

const DB_TIMEOUT: Duration = Duration::from_secs(10);

/// The organization's ARI dial-string settings.
#[derive(Debug, Clone, Default)]
pub struct DialSettings {
    /// Must be "ari" for Vaani to dial.
    pub provider: String,
    /// "PJSIP" or "SIP".
    pub technology: String,
    pub trunk: String,
    pub strip_prefix: String,
    pub from_numbers: Vec<String>,
    /// The caller ID mapped to a workflow (number_workflow_map is stored {phone: workflow_id}).
    pub workflow_cli: HashMap<i32, String>,
}

impl DialSettings {
    /// The Asterisk dial string for `number`: used as given when it already names a technology (SIP/ or
    /// PJSIP/), else "<technology>/<number minus the strip prefix>[@<trunk>]".
    pub fn endpoint(&self, number: &str) -> String {
        if number.starts_with("SIP/") || number.starts_with("PJSIP/") {
            return number.to_string();
        }
        let number = if self.strip_prefix.is_empty() { number } else { number.strip_prefix(self.strip_prefix.as_str()).unwrap_or(number) };
        let tech = if self.technology.is_empty() { "PJSIP" } else { &self.technology };
        if self.trunk.is_empty() { format!("{tech}/{number}") } else { format!("{tech}/{number}@{}", self.trunk) }
    }

    /// The number shown to the callee: the one mapped to the workflow, else the first configured number, else none.
    pub fn caller_id(&self, workflow_id: i32) -> String {
        self.workflow_cli.get(&workflow_id).filter(|c| !c.is_empty()).or_else(|| self.from_numbers.first()).cloned().unwrap_or_default()
    }
}

/// Statuses a call that never connected can end with (Dograh's names).
pub const STATUS_BUSY: &str = "busy";
pub const STATUS_NO_ANSWER: &str = "no-answer";
pub const STATUS_REJECTED: &str = "rejected";
pub const STATUS_FAILED: &str = "failed";

/// The status an Asterisk hangup cause of an unanswered call stands for, as Dograh does: 17 busy, 19 no
/// answer, 21 rejected, anything else failed.
pub fn status_for_cause(cause: i64) -> &'static str {
    match cause {
        17 => STATUS_BUSY,
        19 => STATUS_NO_ANSWER,
        21 => STATUS_REJECTED,
        _ => STATUS_FAILED,
    }
}

/// Dograh's mark_workflow_run_not_connected mapping.
fn not_connected_disposition(status: &str) -> String {
    match status {
        "no-answer" => "NO_ANSWER".into(),
        "busy" => "BUSY".into(),
        "failed" | "error" => "FAILED".into(),
        "canceled" => "CANCELED".into(),
        "rejected" => "REJECTED".into(),
        other => other.replace('-', "_").to_uppercase(),
    }
}

impl DialSettings {
    /// A telephony configuration read as its columns: the provider, SIP technology, trunk and
    /// strip prefix as text, and from_numbers / number_workflow_map as JSON text.
    pub(crate) fn from_columns(
        provider: String,
        technology: String,
        trunk: String,
        strip_prefix: String,
        from_json: &str,
        map_json: &str,
    ) -> Self {
        let mut d = DialSettings {
            provider,
            technology: technology.trim().to_uppercase(),
            trunk: trunk.trim().to_string(),
            strip_prefix: strip_prefix.trim().to_string(),
            ..Default::default()
        };
        if d.technology.is_empty() {
            d.technology = "PJSIP".into();
        }
        // from_numbers is a list, or one string (Dograh accepts both).
        d.from_numbers = match serde_json::from_str::<Value>(from_json) {
            Ok(Value::Array(a)) => a.iter().filter_map(|v| v.as_str().map(String::from)).collect(),
            Ok(Value::String(s)) if !s.is_empty() => vec![s],
            _ => vec![],
        };
        if let Ok(Value::Object(map)) = serde_json::from_str::<Value>(map_json) {
            for (phone, w) in map {
                let id = match w {
                    Value::Number(n) => n.as_i64().map(|n| n as i32),
                    Value::String(s) => s.parse().ok(),
                    _ => None,
                };
                if let Some(id) = id {
                    d.workflow_cli.insert(id, phone);
                }
            }
        }
        d
    }
}

impl Store {
    /// Organization `org`'s telephony configuration.
    pub async fn dial_settings(&self, org: i32) -> Result<DialSettings> {
        let db = self.db().await?;
        let row = db
            .query_opt(
                "SELECT COALESCE(v->>'provider', 'twilio'), COALESCE(v->>'sip_technology', 'PJSIP'),
                        COALESCE(v->>'outbound_trunk', ''), COALESCE(v->>'strip_number_prefix', ''),
                        COALESCE((v->'from_numbers')::text, '[]'), COALESCE((v->'number_workflow_map')::text, '{}')
                 FROM (SELECT value::text::jsonb AS v FROM organization_configurations
                       WHERE organization_id = $1::int AND key = 'TELEPHONY_CONFIGURATION') t",
                &[&org],
            )
            .await
            .with_context(|| format!("dograh: telephony configuration of organization {org}"))?;
        let Some(r) = row else { bail!("dograh: organization {org} has no telephony configuration") };

        Ok(DialSettings::from_columns(r.get(0), r.get(1), r.get(2), r.get(3), &r.get::<_, String>(4), &r.get::<_, String>(5)))
    }

    /// Records a campaign call before it is dialed, as Dograh's dispatcher does, so even a call nobody
    /// answers leaves a run in Dograh. The run starts 'initialized'; `mark_run_running` moves it on when
    /// answered. initial_context is the contact's variables plus the campaign's, as Dograh writes it. A
    /// campaign with id 0 is a call outside any campaign: no campaign or queued-run link.
    pub async fn create_outbound_run(&self, c: &Campaign, q: &QueuedRun, called: &str, caller: &str) -> Result<i64> {
        let mut initial = q.variables.clone();
        let name = if c.id > 0 {
            initial.insert("campaign_id".into(), json!(c.id));
            format!("WR-CAMPAIGN-{}-{}", c.id, q.id)
        } else {
            format!("ARI Outbound {called}") // a call placed outside a campaign
        };
        initial.insert("provider".into(), json!("ari"));
        initial.insert("source_uuid".into(), json!(q.source_uuid));
        initial.insert("caller_number".into(), json!(caller));
        initial.insert("called_number".into(), json!(called));

        let db = self.db().await?;
        let row = tokio::time::timeout(
            DB_TIMEOUT,
            db.query_opt(
                "INSERT INTO workflow_runs (name, workflow_id, definition_id, mode, call_type, state,
                    is_completed, usage_info, cost_info, initial_context, gathered_context, logs,
                    annotations, created_at, campaign_id, queued_run_id)
                 SELECT $1, w.id, COALESCE(w.released_definition_id,
                        (SELECT id FROM workflow_definitions WHERE workflow_id = w.id AND is_current LIMIT 1)),
                    'ari', 'outbound', 'initialized', false, '{}', '{}', $3::text::json, '{}', '{}', '{}',
                    now(), NULLIF($4::int, 0), NULLIF($5::int, 0)
                 FROM workflows w WHERE w.id = $2::int
                 RETURNING id::bigint",
                &[&name, &c.workflow_id, &Value::Object(initial).to_string(), &c.id, &q.id],
            ),
        )
        .await
        .map_err(|_| anyhow::anyhow!("dograh: create outbound run: timed out"))?
        .context("dograh: create outbound run")?;
        match row {
            Some(r) => Ok(r.get(0)),
            None => bail!("dograh: workflow {} not found", c.workflow_id),
        }
    }

    /// Notes that the callee answered and the agent is on the call.
    pub async fn mark_run_running(&self, run: i64) -> Result<()> {
        let db = self.db().await?;
        tokio::time::timeout(DB_TIMEOUT, db.execute("UPDATE workflow_runs SET state = 'running' WHERE id = $1::bigint", &[&run]))
            .await
            .map_err(|_| anyhow::anyhow!("dograh: run {run} running: timed out"))?
            .with_context(|| format!("dograh: run {run} running"))?;
        Ok(())
    }

    /// Completes the run of a call that never connected, with the tags and disposition Dograh gives it, so
    /// it doesn't stay 'initialized' and shows in the call list: tags "not_connected" and
    /// "telephony_<status>" added to any it has, call_disposition BUSY / NO_ANSWER / ..., and the
    /// organization's mapping of it.
    pub async fn mark_not_connected(&self, run: i64, org: i32, status: &str) -> Result<()> {
        self.complete_run(
            run,
            org,
            &["not_connected".to_string(), format!("telephony_{}", status.to_lowercase())],
            &not_connected_disposition(status),
        )
        .await
    }

    /// Completes the run of a call Vaani lost (it was restarted, or the agent never started), so it
    /// doesn't stay 'running' for ever.
    pub async fn mark_interrupted(&self, run: i64, org: i32) -> Result<()> {
        self.complete_run(run, org, &["interrupted".to_string()], "INTERRUPTED").await
    }

    async fn complete_run(&self, run: i64, org: i32, tags: &[String], disposition: &str) -> Result<()> {
        let mapped = self
            .disposition_mapping(Some(i64::from(org)))
            .await
            .and_then(|m| m.get(disposition).and_then(Value::as_str).map(String::from))
            .unwrap_or_else(|| disposition.to_string());
        let db = self.db().await?;
        tokio::time::timeout(
            DB_TIMEOUT,
            db.execute(
                "UPDATE workflow_runs SET is_completed = true, state = 'completed',
                    gathered_context = (COALESCE(gathered_context::jsonb, '{}'::jsonb) || jsonb_build_object(
                        'call_tags', COALESCE(gathered_context::jsonb->'call_tags', '[]'::jsonb) || $2::text::jsonb,
                        'call_disposition', $3::text, 'mapped_call_disposition', $4::text))::json
                 WHERE id = $1::bigint",
                &[&run, &json!(tags).to_string(), &disposition, &mapped],
            ),
        )
        .await
        .map_err(|_| anyhow::anyhow!("dograh: complete run {run}: timed out"))?
        .with_context(|| format!("dograh: complete run {run}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> DialSettings {
        DialSettings {
            provider: "ari".into(),
            technology: "PJSIP".into(),
            trunk: "carrier".into(),
            strip_prefix: "+91".into(),
            from_numbers: vec!["0731111".into(), "0732222".into()],
            workflow_cli: HashMap::from([(7, "0739999".to_string())]),
        }
    }

    #[test]
    fn dial_strings_and_caller_ids() {
        let s = settings();
        assert_eq!(s.endpoint("+919876543210"), "PJSIP/9876543210@carrier");
        assert_eq!(s.endpoint("PJSIP/100@pbx"), "PJSIP/100@pbx", "a dial string is used as given");
        assert_eq!(DialSettings { technology: "SIP".into(), ..Default::default() }.endpoint("123"), "SIP/123", "no trunk, no prefix");
        assert_eq!(s.caller_id(7), "0739999", "the number mapped to the workflow");
        assert_eq!(s.caller_id(8), "0731111", "else the first configured number");
        assert_eq!(DialSettings::default().caller_id(1), "");
    }

    #[test]
    fn hangup_causes_map_to_dograhs_statuses_and_dispositions() {
        assert_eq!(
            [17, 19, 21, 1, 38].map(status_for_cause),
            [STATUS_BUSY, STATUS_NO_ANSWER, STATUS_REJECTED, STATUS_FAILED, STATUS_FAILED]
        );
        assert_eq!(not_connected_disposition("no-answer"), "NO_ANSWER");
        assert_eq!(not_connected_disposition("rejected"), "REJECTED");
        assert_eq!(not_connected_disposition("some-thing"), "SOME_THING");
    }
}

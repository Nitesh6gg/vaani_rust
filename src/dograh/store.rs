//! Reads agent configuration straight from a self-hosted Dograh's Postgres, so
//! Dograh's visual editor stays the single place agents are configured. Read
//! fresh per call, so a publish in the editor applies to the next call.

use super::{
    services::{self, Services},
    tools::{Credential, ToolRow},
    workflow::{self as wfjson, WorkflowJson},
};
use crate::agent::workflow::Workflow;
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use deadpool_postgres::{Pool, Runtime};
use serde_json::{Map, Value};
use std::time::Duration;
use tokio_postgres::NoTls;

/// Dograh's backend defaults (api/services/pipecat/run_pipeline.py), used when
/// the workflow's settings were never saved.
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_DURATION: Duration = Duration::from_secs(300);

/// What one call runs.
pub struct LoadedWorkflow {
    pub id: i32,
    /// The definition version this call runs.
    pub definition_id: i64,
    #[allow(dead_code)] // for the call's run record (next milestone)
    pub org_id: Option<i64>,
    pub workflow: Workflow,
    pub services: Services,
    /// From the workflow's settings; zero disables.
    pub idle_timeout: Duration,
    pub max_duration: Duration,
    pub warnings: Vec<String>,
}

/// A connection pool to Dograh's database, shared by every call.
#[derive(Clone)]
pub struct Store {
    pool: Pool,
}

impl Store {
    /// Connects and verifies the connection. ponytail: no TLS to Postgres
    /// (Dograh's DB is on the private network); add tokio-postgres-rustls if not.
    pub async fn open(url: &str, max_conns: usize) -> Result<Self> {
        let mut cfg = deadpool_postgres::Config::new();
        cfg.url = Some(url.to_string());
        cfg.pool = Some(deadpool_postgres::PoolConfig::new(max_conns));
        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).context("dograh: connect")?;
        pool.get().await.context("dograh: ping")?.simple_query("SELECT 1").await.context("dograh: ping")?;
        Ok(Self { pool })
    }

    /// Loads workflow `workflow_id`. `call_vars` (caller_number, called_number)
    /// layer over the workflow's template_context_variables.
    ///
    /// The definition is resolved like Dograh does for a real call: the
    /// published one (released_definition_id), else the legacy is_current row;
    /// its template variables, else the workflow's. The models come from the
    /// workflow owner's user_configurations (ari_manager.py: workflow.user_id).
    /// Tools are scoped to the workflow's organization and must be active.
    pub async fn workflow(&self, workflow_id: i32, call_vars: &Map<String, Value>) -> Result<LoadedWorkflow> {
        let db = self.pool.get().await.context("dograh: get connection")?;
        let row = db
            .query_opt(
                "SELECT d.id::bigint, d.workflow_json::text, COALESCE(d.template_context_variables::text, ''),
                        COALESCE(w.template_context_variables::text, ''),
                        COALESCE(d.workflow_configurations::text, ''), w.organization_id::bigint,
                        COALESCE((SELECT configuration::text FROM user_configurations
                                  WHERE user_id = w.user_id ORDER BY id LIMIT 1), '')
                 FROM workflows w
                 JOIN workflow_definitions d ON d.id = COALESCE(w.released_definition_id,
                      (SELECT id FROM workflow_definitions WHERE workflow_id = w.id AND is_current LIMIT 1))
                 WHERE w.id = $1::int",
                &[&workflow_id],
            )
            .await
            .with_context(|| format!("dograh: query workflow {workflow_id}"))?
            .ok_or_else(|| anyhow!("dograh: workflow {workflow_id} not found or has no published definition"))?;

        let def_id: i64 = row.get(0);
        let (def_json, def_vars, wf_vars, def_config): (String, String, String, String) = (row.get(1), row.get(2), row.get(3), row.get(4));
        let org_id: Option<i64> = row.get(5);
        let user_config: String = row.get(6);

        let wf = WorkflowJson::parse(&def_json).with_context(|| format!("dograh: workflow {workflow_id}"))?;

        let mut vars = template_vars(&def_vars);
        if vars.is_empty() {
            vars = template_vars(&wf_vars);
        }
        vars.extend(call_vars.clone());

        let tool_rows = self.tools(&db, &wf.tool_uuids(), org_id).await?;
        let (workflow, mut warnings) =
            wfjson::build(&wf, &vars, &tool_rows, Utc::now()).with_context(|| format!("dograh: workflow {workflow_id}"))?;
        let (services, w) = services::resolve(&user_config, &def_config).with_context(|| format!("dograh: workflow {workflow_id}: models"))?;
        warnings.extend(w);
        let (idle_timeout, max_duration, w) = call_limits(&def_config);
        warnings.extend(w);

        Ok(LoadedWorkflow { id: workflow_id, definition_id: def_id, org_id, workflow, services, idle_timeout, max_duration, warnings })
    }

    /// The active tools among `uuids` that belong to `org_id`, each with its
    /// credential (same organization and active, as Dograh's lookup does).
    async fn tools(&self, db: &deadpool_postgres::Object, uuids: &[String], org_id: Option<i64>) -> Result<Vec<ToolRow>> {
        if uuids.is_empty() {
            return Ok(vec![]);
        }
        let rows = db
            .query(
                "SELECT t.tool_uuid::text, t.name, COALESCE(t.description, ''), t.category::text, t.definition::text,
                        c.credential_type::text, c.credential_data::text
                 FROM tools t
                 LEFT JOIN external_credentials c
                   ON c.credential_uuid::text = t.definition->'config'->>'credential_uuid'
                  AND c.organization_id IS NOT DISTINCT FROM $2::bigint AND c.is_active
                 WHERE t.tool_uuid::text = ANY($1::text[]) AND t.organization_id IS NOT DISTINCT FROM $2::bigint
                   AND t.status::text = 'active'",
                &[&uuids, &org_id],
            )
            .await
            .context("dograh: query tools")?;
        rows.iter()
            .map(|r| {
                let def: String = r.get(4);
                let (kind, data): (Option<String>, Option<String>) = (r.get(5), r.get(6));
                Ok(ToolRow {
                    uuid: r.get(0),
                    name: r.get(1),
                    description: r.get(2),
                    category: r.get(3),
                    definition: serde_json::from_str(&def).unwrap_or(Value::Null),
                    credential: kind.map(|kind| Credential { kind, data: data.and_then(|d| serde_json::from_str(&d).ok()).unwrap_or(Value::Null) }),
                })
            })
            .collect()
    }
}

/// `max_user_idle_timeout` and `max_call_duration` (seconds) from a
/// definition's workflow_configurations, as Dograh reads them. Idle 0 disables
/// silence handling (pipecat's rule); a max duration <= 0 means no limit.
/// Unreadable settings fall back to the defaults, with a warning.
pub fn call_limits(config_json: &str) -> (Duration, Duration, Option<String>) {
    let secs = |v: f64| if v <= 0.0 { Duration::ZERO } else { Duration::from_secs_f64(v) };
    let (mut idle, mut max) = (DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_DURATION);
    if config_json.is_empty() {
        return (idle, max, None);
    }
    match serde_json::from_str::<Value>(config_json) {
        Err(e) => return (idle, max, Some(format!("unreadable workflow_configurations, using default call limits: {e}"))),
        Ok(v) => {
            if let Some(i) = v.get("max_user_idle_timeout").and_then(Value::as_f64) {
                idle = secs(i);
            }
            if let Some(m) = v.get("max_call_duration").and_then(Value::as_f64) {
                max = secs(m);
            }
        }
    }
    (idle, max, None)
}

/// A template_context_variables JSON object; anything else (empty, null,
/// malformed) is no variables.
fn template_vars(s: &str) -> Map<String, Value> {
    serde_json::from_str::<Value>(s).ok().and_then(|v| v.as_object().cloned()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_limits_defaults_overrides_and_garbage() {
        let (i, m, w) = call_limits("");
        assert_eq!((i, m, w), (DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_DURATION, None));
        let (i, m, _) = call_limits(r#"{"max_user_idle_timeout": 7.5, "max_call_duration": 120}"#);
        assert_eq!((i, m), (Duration::from_millis(7500), Duration::from_secs(120)));
        let (i, m, _) = call_limits(r#"{"max_user_idle_timeout": 0, "max_call_duration": -1}"#);
        assert_eq!((i, m), (Duration::ZERO, Duration::ZERO));
        let (i, _, w) = call_limits("{nope");
        assert_eq!(i, DEFAULT_IDLE_TIMEOUT);
        assert!(w.unwrap().contains("unreadable"));
    }

    #[test]
    fn template_vars_tolerate_junk() {
        assert!(template_vars("").is_empty() && template_vars("null").is_empty() && template_vars("[1]").is_empty());
        assert_eq!(template_vars(r#"{"a":1}"#)["a"], 1);
    }

    /// Runs against a real Postgres when `DOGRAH_TEST_DB_URL` is set (it
    /// recreates the handful of Dograh tables this module reads, with Dograh's
    /// column types: uuid tool ids, enum category/status, jsonb documents).
    #[tokio::test]
    async fn loads_a_workflow_from_postgres() {
        let Ok(url) = std::env::var("DOGRAH_TEST_DB_URL") else {
            eprintln!("skipped: DOGRAH_TEST_DB_URL not set");
            return;
        };
        let store = Store::open(&url, 2).await.unwrap();
        let db = store.pool.get().await.unwrap();
        db.batch_execute(
            r#"
            DROP TABLE IF EXISTS external_credentials, tools, user_configurations, workflow_definitions, workflows CASCADE;
            DROP TYPE IF EXISTS tool_category, tool_status;
            CREATE TYPE tool_category AS ENUM ('http_api', 'end_call', 'transfer_call');
            CREATE TYPE tool_status AS ENUM ('active', 'archived');
            CREATE TABLE workflows (id serial PRIMARY KEY, user_id int, organization_id int,
                released_definition_id int, template_context_variables jsonb);
            CREATE TABLE workflow_definitions (id serial PRIMARY KEY, workflow_id int, workflow_json jsonb,
                template_context_variables jsonb, workflow_configurations jsonb, is_current bool);
            CREATE TABLE user_configurations (id serial PRIMARY KEY, user_id int, configuration jsonb);
            CREATE TABLE tools (id serial PRIMARY KEY, tool_uuid uuid, organization_id int, name text, description text,
                category tool_category, status tool_status, definition jsonb);
            CREATE TABLE external_credentials (id serial PRIMARY KEY, credential_uuid varchar, organization_id int,
                credential_type text, credential_data jsonb, is_active bool);

            INSERT INTO user_configurations (user_id, configuration) VALUES (7, '{
              "llm": {"provider":"openai","api_key":"sk","model":"gpt-4.1"},
              "stt": {"provider":"sarvam","api_key":"k","model":"saaras:v4"},
              "tts": {"provider":"sarvam","api_key":"k","model":"bulbul:v3","voice":"shubh"}}');
            INSERT INTO workflows (id, user_id, organization_id, released_definition_id, template_context_variables)
              VALUES (42, 7, 3, NULL, '{"company": "Acme"}');
            INSERT INTO workflow_definitions (workflow_id, workflow_json, template_context_variables, workflow_configurations, is_current)
              VALUES (42, '{"nodes":[
                  {"id":"1","type":"startCall","data":{"name":"Start","prompt":"You work at {{company}}; caller {{caller_number}}.","is_start":true,
                     "greeting":"Hello from {{company}}","tool_uuids":["11111111-1111-1111-1111-111111111111","22222222-2222-2222-2222-222222222222"]}},
                  {"id":"2","type":"endCall","data":{"name":"End","prompt":"Bye","is_end":true}}],
                "edges":[{"source":"1","target":"2","data":{"label":"Finish","condition":"done"}}]}',
                NULL, '{"max_user_idle_timeout": 6, "max_call_duration": 90}', true);
            INSERT INTO tools (tool_uuid, organization_id, name, description, category, status, definition) VALUES
              ('11111111-1111-1111-1111-111111111111', 3, 'Lookup', 'find it', 'http_api', 'active',
               '{"config":{"url":"http://x/y","method":"GET","credential_uuid":"cred-1","parameters":[{"name":"q","type":"string"}]}}'),
              ('22222222-2222-2222-2222-222222222222', 3, 'Old', '', 'end_call', 'archived', '{}'),
              ('33333333-3333-3333-3333-333333333333', 99, 'Other org', '', 'end_call', 'active', '{}');
            INSERT INTO external_credentials (credential_uuid, organization_id, credential_type, credential_data, is_active)
              VALUES ('cred-1', 3, 'bearer_token', '{"token":"t0k"}', true);
            "#,
        )
        .await
        .unwrap();
        drop(db);

        let vars = serde_json::json!({"caller_number": "98765"});
        let w = store.workflow(42, vars.as_object().unwrap()).await.unwrap();
        assert_eq!((w.id, w.org_id), (42, Some(3)));
        assert_eq!((w.idle_timeout, w.max_duration), (Duration::from_secs(6), Duration::from_secs(90)));
        assert_eq!(w.services.llm.model, "gpt-4.1");
        assert_eq!(w.services.tts.voice, "shubh");
        let start = &w.workflow.nodes[w.workflow.start];
        assert_eq!(start.prompt, "You work at Acme; caller 98765.");
        assert_eq!(start.greeting, "Hello from Acme");
        assert_eq!(start.tools.len(), 1, "the archived tool is not offered");
        assert_eq!(start.tools[0].def.name, "lookup");
        // The archived tool isn't returned by the active-only query, so it reads as missing.
        assert_eq!(w.warnings, [r#"node "Start": tool 22222222-2222-2222-2222-222222222222 not found or not active"#]);
        let err = store.workflow(999, &Map::new()).await.err().unwrap();
        assert!(err.to_string().contains("not found"), "{err}");
    }
}

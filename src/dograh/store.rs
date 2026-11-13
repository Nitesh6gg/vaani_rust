//! Reads agent configuration straight from a self-hosted Dograh's Postgres, so
//! Dograh's visual editor stays the single place agents are configured. Read
//! fresh per call, so a publish in the editor applies to the next call.

use super::{
    services::{self, Services},
    tools::{Credential, ToolRow},
    workflow::{self as wfjson, WorkflowJson},
};
use crate::{agent::workflow::Workflow, metrics};
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use deadpool_postgres::{Pool, Runtime};
use serde_json::{Map, Value};
use std::{sync::Arc, time::Duration};
use tokio_postgres::{NoTls, Row, types::ToSql};

/// Dograh's backend defaults (api/services/pipecat/run_pipeline.py), used when
/// the workflow's settings were never saved.
/// How long a caller waits for a free Postgres connection by default (DOGRAH_DB_WAIT_MS).
#[cfg(test)]
pub const DB_WAIT: Duration = Duration::from_secs(5);
/// Connections opened at start.
const WARM: usize = 4;
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_DURATION: Duration = Duration::from_secs(300);

/// The URL of a service the tests need, or `None` (the test skips) when it is unset. With `REQUIRE_TEST_SERVICES` set (CI),
/// a missing URL panics instead, so a run can't pass by skipping everything.
#[cfg(test)]
pub(crate) fn test_service_url(var: &str) -> Option<String> {
    let url = std::env::var(var).ok();
    assert!(url.is_some() || std::env::var_os("REQUIRE_TEST_SERVICES").is_none(), "REQUIRE_TEST_SERVICES is set but {var} is not");
    url
}

/// What one call runs.
pub struct LoadedWorkflow {
    pub id: i32,
    /// The definition version this call runs.
    pub definition_id: i64,
    pub org_id: Option<i64>,
    pub workflow: Workflow,
    pub services: Services,
    /// From the workflow's settings; zero disables.
    pub idle_timeout: Duration,
    pub max_duration: Duration,
    pub warnings: Vec<String>,
}

/// The queries of a pooled connection, with their statements prepared once per
/// connection and cached. tokio-postgres's own `query(&str)` prepares the
/// statement and then runs it: two round trips every time, and Dograh's
/// Postgres is on another server. This trait is in scope wherever `db.query(...)`
/// is called, and (being found before the deref to the plain client) takes its place.
pub(crate) trait Cached {
    async fn query(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, tokio_postgres::Error>;
    async fn query_one(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Row, tokio_postgres::Error>;
    async fn query_opt(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Option<Row>, tokio_postgres::Error>;
    async fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64, tokio_postgres::Error>;
}

impl Cached for deadpool_postgres::Object {
    async fn query(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, tokio_postgres::Error> {
        let st = self.prepare_cached(sql).await?;
        let client: &tokio_postgres::Client = self;
        client.query(&st, params).await
    }
    async fn query_one(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Row, tokio_postgres::Error> {
        let st = self.prepare_cached(sql).await?;
        let client: &tokio_postgres::Client = self;
        client.query_one(&st, params).await
    }
    async fn query_opt(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Option<Row>, tokio_postgres::Error> {
        let st = self.prepare_cached(sql).await?;
        let client: &tokio_postgres::Client = self;
        client.query_opt(&st, params).await
    }
    async fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64, tokio_postgres::Error> {
        let st = self.prepare_cached(sql).await?;
        let client: &tokio_postgres::Client = self;
        client.execute(&st, params).await
    }
}

/// A connection pool to Dograh's database, shared by every call.
#[derive(Clone)]
pub struct Store {
    pool: Pool,
    /// Set by `use_storage`: without it recordings can't be played.
    pub(super) clips: Option<super::recordings::ClipCache>,
}

impl Store {
    /// Lets the store play Dograh's recordings, read from `storage` (its MinIO).
    pub fn use_storage(&mut self, storage: super::storage::Storage) {
        self.clips = Some(super::recordings::ClipCache::new(storage));
    }

    /// A pooled connection. Hold it only for statements: never across a MinIO transfer, an LLM call or any
    /// other wait, and never while asking for a second one (see `finish_run`, `workflow`): a pool whose
    /// connections are all held by tasks that want another one never recovers.
    pub(crate) async fn db(&self) -> Result<deadpool_postgres::Object> {
        self.pool.get().await.map_err(|e| {
            if matches!(e, deadpool_postgres::PoolError::Timeout(_)) {
                metrics::DB_POOL_TIMEOUTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            anyhow::Error::new(e).context("dograh: get connection")
        })
    }

    /// Connects and verifies the connection, with the default wait for a free connection (`DB_WAIT`).
    #[cfg(test)]
    pub async fn open(url: &str, max_conns: usize) -> Result<Self> {
        Self::open_with(url, max_conns, DB_WAIT).await
    }

    /// Connects and verifies the connection. A caller waits at most `wait` for a free connection (and a new
    /// one is made, or an old one checked, in at most `wait`): an exhausted pool fails the caller fast
    /// instead of queueing it for ever. TCP keepalive every 30 s, so a firewall or PgBouncer that drops idle
    /// connections is noticed. A few connections are opened up front. ponytail: no TLS to Postgres
    /// (Dograh's DB is on the private network); add tokio-postgres-rustls if not.
    pub async fn open_with(url: &str, max_conns: usize, wait: Duration) -> Result<Self> {
        let mut cfg = deadpool_postgres::Config::new();
        cfg.url = Some(url.to_string());
        cfg.keepalives_idle = Some(Duration::from_secs(30));
        let mut pool_cfg = deadpool_postgres::PoolConfig::new(max_conns);
        pool_cfg.timeouts = deadpool_postgres::Timeouts { wait: Some(wait), create: Some(wait), recycle: Some(wait) };
        cfg.pool = Some(pool_cfg);
        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).context("dograh: connect")?;
        pool.get().await.context("dograh: ping")?.simple_query("SELECT 1").await.context("dograh: ping")?;
        // Warm: the first calls after a start don't each pay for a connection.
        let warm = futures_util::future::join_all((0..WARM.min(max_conns)).map(|_| pool.get())).await;
        drop(warm);
        let gauge = pool.clone();
        metrics::set_db_pool(Arc::new(move || {
            let s = gauge.status();
            (s.size.saturating_sub(s.available), s.waiting, s.max_size)
        }));
        Ok(Self { pool, clips: None })
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
        let db = self.db().await?;
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
        // The connection goes back before the clips load: `load_clips` takes its own for the recordings query,
        // and downloads and converts clips (up to 30 s on a cold cache) that must not pin one.
        drop(db);
        let (pks, by_id) = wf.recording_needs(&tool_rows);
        let (clips, mut warnings) = self.load_clips(org_id, &pks, by_id).await;
        let (workflow, build_warnings) =
            wfjson::build(&wf, &vars, &tool_rows, &clips, Utc::now()).with_context(|| format!("dograh: workflow {workflow_id}"))?;
        warnings.extend(build_warnings);
        let (services, w) =
            services::resolve(&user_config, &def_config).with_context(|| format!("dograh: workflow {workflow_id}: models"))?;
        warnings.extend(w);
        let (idle_timeout, max_duration, w) = call_limits(&def_config);
        warnings.extend(w);

        Ok(LoadedWorkflow { id: workflow_id, definition_id: def_id, org_id, workflow, services, idle_timeout, max_duration, warnings })
    }

    /// A store on its own fresh schema of the Postgres named by
    /// `DOGRAH_TEST_DB_URL` (tests run in parallel and must not share tables);
    /// `None` when the variable is unset. `sql` creates and fills the fixtures.
    #[cfg(test)]
    pub async fn for_test(schema: &str, sql: &str) -> Option<(Self, String)> {
        Self::for_test_pool(schema, sql, 4, DB_WAIT).await
    }

    /// `for_test` with a pool of `max_conns` connections and a `wait` for a free one: a small pool makes a
    /// connection held too long, or two held at once, show up at once.
    #[cfg(test)]
    pub async fn for_test_pool(schema: &str, sql: &str, max_conns: usize, wait: Duration) -> Option<(Self, String)> {
        let url = test_service_url("DOGRAH_TEST_DB_URL")?;
        let admin = Self::open(&url, 1).await.unwrap();
        let db = admin.pool.get().await.unwrap();
        db.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema};")).await.unwrap();
        let scoped = format!("{url}?options=-csearch_path%3D{schema}");
        let store = Self::open_with(&scoped, max_conns, wait).await.unwrap();
        store.pool.get().await.unwrap().batch_execute(sql).await.unwrap();
        Some((store, scoped))
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
                    credential: kind
                        .map(|kind| Credential { kind, data: data.and_then(|d| serde_json::from_str(&d).ok()).unwrap_or(Value::Null) }),
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

    /// Runs against a real Postgres when `DOGRAH_TEST_DB_URL` is set. Recreates
    /// the handful of Dograh tables this module reads, with Dograh's column
    /// types: uuid tool ids, enum category/status, jsonb documents.
    #[tokio::test]
    async fn loads_a_workflow_from_postgres() {
        let Some((store, _)) = Store::for_test("t_store", r#"
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
            "#).await else {
            eprintln!("skipped: DOGRAH_TEST_DB_URL not set");
            return;
        };

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

    /// Recordings end to end: rows in Postgres, objects in (a fake) MinIO, ffmpeg
    /// conversion and silence trimming, then the clips wired into the workflow.
    /// Needs `DOGRAH_TEST_DB_URL` and ffmpeg.
    #[tokio::test]
    async fn recordings_become_clips_in_the_workflow() {
        use crate::{agent::recorder::wav, testkit::mock_minio_objects};
        let tone = |ms: usize, v: i16| -> Vec<u8> { (0..16 * ms).flat_map(|_| v.to_le_bytes()).collect() };
        // 100ms of silence, 200ms of sound, 100ms of silence: trimming keeps the 200ms.
        let file = wav(&[tone(100, 0), tone(200, 5000), tone(100, 0)].concat());
        let endpoint = mock_minio_objects(vec![
            ("/voice-audio/recordings/greet.wav".into(), file.clone()),
            ("/voice-audio/recordings/offer.wav".into(), file),
        ])
        .await;

        let Some((mut store, _)) = Store::for_test(
            "t_recordings",
            r#"
            CREATE TABLE workflows (id serial PRIMARY KEY, user_id int, organization_id int, released_definition_id int, template_context_variables jsonb);
            CREATE TABLE workflow_definitions (id serial PRIMARY KEY, workflow_id int, workflow_json jsonb, template_context_variables jsonb, workflow_configurations jsonb, is_current bool);
            CREATE TABLE user_configurations (id serial PRIMARY KEY, user_id int, configuration jsonb);
            CREATE TABLE tools (tool_uuid uuid, organization_id int, name text, description text, category text, status text, definition jsonb);
            CREATE TABLE external_credentials (credential_uuid varchar, organization_id int, credential_type text, credential_data jsonb, is_active bool);
            CREATE TABLE workflow_recordings (id serial PRIMARY KEY, organization_id int, recording_id text, transcript text, storage_key text, storage_backend text, is_active bool);
            INSERT INTO workflow_recordings (id, organization_id, recording_id, transcript, storage_key, storage_backend, is_active) VALUES
              (5, 3, 'greet', 'Namaste!', 'recordings/greet.wav', 'minio', true),
              (6, 3, 'offer', 'Our offer.', 'recordings/offer.wav', 'minio', true),
              (7, 3, 'old', 'Old.', 'recordings/old.wav', 'minio', false);
            INSERT INTO user_configurations (user_id, configuration) VALUES (7, '{
              "llm": {"provider":"openai","api_key":"sk","model":"m"},
              "stt": {"provider":"sarvam","api_key":"k"}, "tts": {"provider":"sarvam","api_key":"k"}}');
            INSERT INTO workflows (id, user_id, organization_id) VALUES (42, 7, 3);
            INSERT INTO workflow_definitions (workflow_id, workflow_json, is_current) VALUES (42, '{"nodes":[
                {"id":"1","type":"startCall","data":{"name":"Start","prompt":"Reply with RECORDING_ID: offer when asked.","is_start":true,
                   "greeting_type":"audio","greeting_recording_id":"5"}},
                {"id":"2","type":"agentNode","data":{"name":"Main","prompt":"Plain prompt."}}],
              "edges":[{"source":"1","target":"2","data":{"label":"Go","condition":"c","transition_speech_type":"audio","transition_speech_recording_id":6}},
                       {"source":"2","target":"1","data":{"label":"Back","condition":"c","transition_speech_type":"audio","transition_speech_recording_id":7}}]}', true);
            "#,
        )
        .await
        else {
            eprintln!("skipped: DOGRAH_TEST_DB_URL not set");
            return;
        };
        store.use_storage(crate::dograh::storage::Storage {
            endpoint,
            access_key: "a".into(),
            secret_key: "b".into(),
            bucket: "voice-audio".into(),
            secure: false,
        });

        let w = store.workflow(42, &Map::new()).await.unwrap();
        let start = &w.workflow.nodes[w.workflow.start];
        let greeting = start.greeting_clip.as_ref().expect("the audio greeting is a clip");
        assert_eq!((greeting.id.as_str(), greeting.transcript.as_str()), ("greet", "Namaste!"));
        assert_eq!(greeting.pcm.len(), 200 * 32, "leading and trailing silence trimmed, 200ms kept");
        assert!(start.greeting.is_empty());

        // The start prompt names RECORDING_ID: -> response-mode instructions and the by-id clips.
        assert!(start.prompt.contains("RESPONSE MODE INSTRUCTIONS - MANDATORY FORMAT"));
        let clips = start.clips.as_ref().expect("recording response mode");
        assert!(clips.contains_key("offer") && clips.contains_key("greet") && !clips.contains_key("old"), "active recordings only");
        assert!(w.workflow.nodes[1].clips.is_none() && !w.workflow.nodes[1].prompt.contains("RESPONSE MODE"));

        // Transitions: an available recording plays; an inactive one says nothing, with a warning.
        assert_eq!(start.edges[0].speech_clip.as_ref().map(|c| c.id.as_str()), Some("offer"));
        assert!(w.workflow.nodes[1].edges[0].speech_clip.is_none());
        assert_eq!(
            w.warnings,
            ["recording 7 not found or not active", r#"edge "Back": transition recording unavailable; nothing is said"#]
        );
    }

    /// A workflow load must never hold a connection while it loads the clips, or wait for a second one while
    /// holding the first: with every connection held by a load that wants another, nothing moves until the wait
    /// runs out. Pool of 2, six calls starting at once, a cold clip cache (a slow download): all six get their clips.
    #[tokio::test]
    async fn loading_workflows_with_clips_never_needs_a_second_connection_while_holding_one() {
        use crate::{agent::recorder::wav, testkit::mock_minio_objects_slow};
        let tone = |ms: usize, v: i16| -> Vec<u8> { (0..16 * ms).flat_map(|_| v.to_le_bytes()).collect() };
        let file = wav(&[tone(100, 0), tone(200, 5000), tone(100, 0)].concat());
        // Keys no earlier run cached on disk: the clips are downloaded now, slowly.
        let tag = format!("{:x}", rand::random::<u64>());
        let (greet, offer) = (format!("recordings/greet-{tag}.wav"), format!("recordings/offer-{tag}.wav"));
        let endpoint = mock_minio_objects_slow(
            vec![(format!("/voice-audio/{greet}"), file.clone()), (format!("/voice-audio/{offer}"), file)],
            Duration::from_millis(300),
        )
        .await;

        let Some((mut store, _)) = Store::for_test_pool(
            "t_pool_nesting",
            &format!(
                r#"
            CREATE TABLE workflows (id serial PRIMARY KEY, user_id int, organization_id int, released_definition_id int, template_context_variables jsonb);
            CREATE TABLE workflow_definitions (id serial PRIMARY KEY, workflow_id int, workflow_json jsonb, template_context_variables jsonb, workflow_configurations jsonb, is_current bool);
            CREATE TABLE user_configurations (id serial PRIMARY KEY, user_id int, configuration jsonb);
            CREATE TABLE tools (tool_uuid uuid, organization_id int, name text, description text, category text, status text, definition jsonb);
            CREATE TABLE external_credentials (credential_uuid varchar, organization_id int, credential_type text, credential_data jsonb, is_active bool);
            CREATE TABLE workflow_recordings (id serial PRIMARY KEY, organization_id int, recording_id text, transcript text, storage_key text, storage_backend text, is_active bool);
            INSERT INTO workflow_recordings (id, organization_id, recording_id, transcript, storage_key, storage_backend, is_active) VALUES
              (5, 3, 'greet', 'Namaste!', '{greet}', 'minio', true), (6, 3, 'offer', 'Our offer.', '{offer}', 'minio', true);
            INSERT INTO user_configurations (user_id, configuration) VALUES (7, '{{
              "llm": {{"provider":"openai","api_key":"sk","model":"m"}},
              "stt": {{"provider":"sarvam","api_key":"k"}}, "tts": {{"provider":"sarvam","api_key":"k"}}}}');
            INSERT INTO workflows (id, user_id, organization_id) VALUES (42, 7, 3);
            INSERT INTO workflow_definitions (workflow_id, workflow_json, is_current) VALUES (42, '{{"nodes":[
                {{"id":"1","type":"startCall","data":{{"name":"Start","prompt":"Reply with RECORDING_ID: offer when asked.","is_start":true,
                   "greeting_type":"audio","greeting_recording_id":"5"}}}}]}}', true);
            "#
            ),
            2,
            Duration::from_secs(2),
        )
        .await
        else {
            return eprintln!("skipped: DOGRAH_TEST_DB_URL not set");
        };
        store.use_storage(crate::dograh::storage::Storage {
            endpoint,
            access_key: "a".into(),
            secret_key: "b".into(),
            bucket: "voice-audio".into(),
            secure: false,
        });

        let started = std::time::Instant::now();
        let loads: Vec<_> = (0..6)
            .map(|_| {
                let store = store.clone();
                tokio::spawn(async move { store.workflow(42, &Map::new()).await })
            })
            .collect();
        for l in loads {
            let w = l.await.unwrap().expect("the workflow loads");
            assert!(w.warnings.iter().all(|x| !x.contains("recordings unavailable")), "{:?}", w.warnings);
            assert!(w.workflow.nodes[w.workflow.start].greeting_clip.is_some(), "the clip was loaded");
        }
        assert!(started.elapsed() < Duration::from_millis(1800), "no load waited for the pool: {:?}", started.elapsed());
    }
}

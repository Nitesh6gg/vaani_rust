//! Dograh's campaign tables (`campaigns`, `queued_runs`). Vaani dials the campaigns
//! of organizations whose MEDIA_ENGINE setting is "vaani" and Dograh leaves those
//! alone (api/services/campaign/media_engine.py). The defaults below are Dograh's,
//! so a campaign behaves the same whichever engine dials.

use super::{Store, outbound::DialSettings, store::Cached};
use anyhow::{Context, Result, bail};
use jiff::{Timestamp, tz::TimeZone};
use serde::Deserialize;
use serde_json::{Map, Value};
use std::time::Duration;

/// A running campaign Vaani dials.
#[derive(Debug, Clone, Default)]
pub struct Campaign {
    pub id: i32,
    pub organization_id: i32,
    pub workflow_id: i32,
    pub rate_per_second: i32,
    /// Lowers the organization's limit for this campaign; 0 = none.
    pub max_concurrency: i32,
    pub retry: RetryConfig,
    pub schedule: Schedule,
    pub breaker: BreakerConfig,
}

/// campaigns.retry_config.
#[derive(Debug, Clone, Default)]
pub struct RetryConfig {
    pub enabled: bool,
    pub max_retries: i32,
    pub delay: Duration,
    pub on_busy: bool,
    pub on_no_answer: bool,
    pub on_voicemail: bool,
}

impl RetryConfig {
    /// Whether a call that ended for `reason` ("busy", "no_answer", "voicemail") is one the campaign retries at all.
    pub fn eligible(&self, reason: &str) -> bool {
        self.enabled
            && match reason {
                "busy" => self.on_busy,
                "no_answer" => self.on_no_answer,
                "voicemail" => self.on_voicemail,
                _ => false,
            }
    }
}

/// orchestrator_metadata.circuit_breaker.
#[derive(Debug, Clone, Copy, Default)]
pub struct BreakerConfig {
    pub enabled: bool,
    pub threshold: f64,
    pub window: Duration,
    pub min_calls: i64,
}

/// orchestrator_metadata.schedule_config: when calls may be placed.
#[derive(Debug, Clone, Default)]
pub struct Schedule {
    pub enabled: bool,
    /// `None` (an unknown timezone) allows every time, as Dograh does.
    pub tz: Option<TimeZone>,
    pub slots: Vec<ScheduleSlot>,
}

/// One weekly window; `day` is Python's weekday (0 = Monday), `start`/`end` are "HH:MM", compared as text as Dograh does.
#[derive(Debug, Clone)]
pub struct ScheduleSlot {
    pub day: u32,
    pub start: String,
    pub end: String,
}

impl Schedule {
    /// No schedule, no slots or an unknown timezone allow every time.
    pub fn allows(&self, now: Timestamp) -> bool {
        let (true, Some(tz), false) = (self.enabled, &self.tz, self.slots.is_empty()) else { return true };
        let local = now.to_zoned(tz.clone());
        let (day, hm) = (local.weekday().to_monday_zero_offset() as u32, format!("{:02}:{:02}", local.hour(), local.minute()));
        self.slots.iter().any(|s| s.day == day && s.start <= hm && hm < s.end)
    }
}

/// One contact to call, a row of `queued_runs`.
#[derive(Debug, Clone, Default)]
pub struct QueuedRun {
    pub id: i32,
    pub campaign_id: i32,
    pub source_uuid: String,
    pub variables: Map<String, Value>,
    pub retry_count: i32,
}

#[derive(Deserialize, Default)]
struct RetryJson {
    enabled: Option<bool>,
    max_retries: Option<i32>,
    retry_delay_seconds: Option<u64>,
    retry_on_busy: Option<bool>,
    retry_on_no_answer: Option<bool>,
    retry_on_voicemail: Option<bool>,
}

#[derive(Deserialize, Default)]
struct MetaJson {
    max_concurrency: Option<i32>,
    schedule_config: Option<ScheduleJson>,
    circuit_breaker: Option<BreakerJson>,
}

#[derive(Deserialize)]
struct ScheduleJson {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    timezone: String,
    #[serde(default)]
    slots: Vec<SlotJson>,
}

#[derive(Deserialize)]
struct SlotJson {
    day_of_week: u32,
    start_time: String,
    end_time: String,
}

#[derive(Deserialize)]
struct BreakerJson {
    enabled: Option<bool>,
    failure_threshold: Option<f64>,
    window_seconds: Option<u64>,
    min_calls_in_window: Option<i64>,
}

/// Fills `c`'s retry, schedule and breaker settings from the two JSON columns, with Dograh's defaults for anything missing.
pub fn parse_campaign(c: &mut Campaign, retry_json: &str, meta_json: &str) -> Result<()> {
    let r: RetryJson = if retry_json.is_empty() { Default::default() } else { serde_json::from_str(retry_json).context("retry_config")? };
    let m: MetaJson =
        if meta_json.is_empty() { Default::default() } else { serde_json::from_str(meta_json).context("orchestrator_metadata")? };

    c.retry = RetryConfig {
        enabled: r.enabled.unwrap_or(true),
        max_retries: r.max_retries.unwrap_or(1),
        delay: Duration::from_secs(r.retry_delay_seconds.unwrap_or(120)),
        on_busy: r.retry_on_busy.unwrap_or(true),
        on_no_answer: r.retry_on_no_answer.unwrap_or(true),
        on_voicemail: r.retry_on_voicemail.unwrap_or(true),
    };
    c.max_concurrency = m.max_concurrency.unwrap_or(0);

    c.breaker = BreakerConfig { enabled: true, threshold: 0.5, window: Duration::from_secs(120), min_calls: 5 };
    if let Some(b) = m.circuit_breaker {
        c.breaker.enabled = b.enabled.unwrap_or(true);
        c.breaker.threshold = b.failure_threshold.unwrap_or(c.breaker.threshold);
        c.breaker.window = Duration::from_secs(b.window_seconds.unwrap_or(120));
        c.breaker.min_calls = b.min_calls_in_window.unwrap_or(5);
    }

    if let Some(sc) = m.schedule_config {
        c.schedule.enabled = sc.enabled;
        let tz = if sc.timezone.is_empty() { "UTC" } else { &sc.timezone };
        c.schedule.tz = TimeZone::get(tz).ok();
        c.schedule.slots =
            sc.slots.into_iter().map(|s| ScheduleSlot { day: s.day_of_week, start: s.start_time, end: s.end_time }).collect();
    }
    Ok(())
}

/// The states a claimed row can be moved to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    /// The call was placed.
    Processed,
    /// It never will be.
    Failed,
    /// Give it back.
    Queued,
}

impl RunState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Processed => "processed",
            Self::Failed => "failed",
            Self::Queued => "queued",
        }
    }
}

/// An organization's Vaani media engine, as a SQL condition on campaigns aliased `c`.
const VAANI_ORGS: &str = "EXISTS (SELECT 1 FROM organization_configurations oc
    WHERE oc.organization_id = c.organization_id AND oc.key = 'MEDIA_ENGINE'
      AND oc.value::text::jsonb ->> 'value' = 'vaani')";

impl Store {
    /// The running campaigns of organizations whose media engine is "vaani". A campaign with
    /// unreadable settings is skipped (and reported in the error) rather than blocking the others.
    pub async fn vaani_campaigns(&self) -> Result<(Vec<Campaign>, Vec<String>)> {
        let db = self.db().await?;
        let rows = db
            .query(
                "SELECT c.id::int, c.organization_id::int, c.workflow_id::int, COALESCE(c.rate_limit_per_second, 1)::int,
                        COALESCE(c.retry_config::text, ''), COALESCE(c.orchestrator_metadata::text, '')
                 FROM campaigns c
                 JOIN organization_configurations oc ON oc.organization_id = c.organization_id AND oc.key = 'MEDIA_ENGINE'
                 WHERE c.state = 'running' AND oc.value::text::jsonb ->> 'value' = 'vaani'
                 ORDER BY c.id",
                &[],
            )
            .await
            .context("dograh: list campaigns")?;
        let (mut out, mut bad) = (vec![], vec![]);
        for r in rows {
            let mut c = Campaign {
                id: r.get(0),
                organization_id: r.get(1),
                workflow_id: r.get(2),
                rate_per_second: r.get::<_, i32>(3).max(1),
                ..Default::default()
            };
            match parse_campaign(&mut c, &r.get::<_, String>(4), &r.get::<_, String>(5)) {
                Ok(()) => out.push(c),
                Err(e) => bad.push(format!("campaign {}: {e:#}", c.id)),
            }
        }
        Ok((out, bad))
    }

    /// Everything the dialer needs before dialing a contact of campaign `campaign`, in one round trip.
    /// The campaign's state is read fresh every time (not cached), so a Pause in Dograh's UI stops
    /// dialing at the next contact. A campaign that doesn't exist is not running.
    pub async fn dial_check(&self, campaign: i32, org: i32) -> Result<DialCheck> {
        let db = self.db().await?;
        let row = db
            .query_opt(
                "SELECT c.state::text = 'running',
                    (SELECT value::text FROM organization_configurations WHERE organization_id = $2::int AND key = 'CONCURRENT_CALL_LIMIT' LIMIT 1),
                    (SELECT value::text FROM organization_configurations WHERE organization_id = $2::int AND key = 'DIAL_RATE_LIMIT_PER_SECOND' LIMIT 1),
                    o.billing_mode::text, o.billing_rate_per_min_inr::float8, o.wallet_balance_inr::float8, o.credit_limit_inr::float8,
                    t.v IS NOT NULL,
                    COALESCE(t.v->>'provider', 'twilio'), COALESCE(t.v->>'sip_technology', 'PJSIP'),
                    COALESCE(t.v->>'outbound_trunk', ''), COALESCE(t.v->>'strip_number_prefix', ''),
                    COALESCE((t.v->'from_numbers')::text, '[]'), COALESCE((t.v->'number_workflow_map')::text, '{}')
                 FROM campaigns c
                 LEFT JOIN organizations o ON o.id = $2::int
                 LEFT JOIN LATERAL (SELECT value::text::jsonb AS v FROM organization_configurations
                                    WHERE organization_id = $2::int AND key = 'TELEPHONY_CONFIGURATION' LIMIT 1) t ON true
                 WHERE c.id = $1::int",
                &[&campaign, &org],
            )
            .await
            .with_context(|| format!("dograh: campaign {campaign} dial check"))?;
        let Some(r) = row else { return Ok(DialCheck::default()) };
        Ok(DialCheck {
            running: r.get(0),
            concurrent_max: r.get::<_, Option<String>>(1).as_deref().and_then(config_int),
            dial_rate: r.get::<_, Option<String>>(2).as_deref().and_then(config_int),
            wallet_blocks: wallet_blocks(r.get::<_, Option<String>>(3).as_deref(), r.get(4), r.get(5), r.get(6)),
            settings: r.get::<_, bool>(7).then(|| {
                DialSettings::from_columns(r.get(8), r.get(9), r.get(10), r.get(11), &r.get::<_, String>(12), &r.get::<_, String>(13))
            }),
        })
    }

    /// Takes up to `limit` runs that are due, scheduled retries first (the earliest first), then the
    /// rest in the order they were loaded, and marks them 'processing'. FOR UPDATE SKIP LOCKED means two
    /// claimers never get the same row. Dograh picks the regular rows at random; loading order is kept
    /// here so a CSV is called top to bottom.
    pub async fn claim_runs(&self, campaign: i32, limit: i64, now: Timestamp) -> Result<Vec<QueuedRun>> {
        let db = self.db().await?;
        let rows = db
            .query(
                "WITH due AS (
                    SELECT id FROM queued_runs
                    WHERE campaign_id = $1::int AND state = 'queued' AND (scheduled_for IS NULL OR scheduled_for <= $2::text::timestamptz)
                    ORDER BY (scheduled_for IS NULL), scheduled_for, id
                    LIMIT $3
                    FOR UPDATE SKIP LOCKED)
                 UPDATE queued_runs q SET state = 'processing'
                 FROM due WHERE q.id = due.id
                 RETURNING q.id::int, q.campaign_id::int, q.source_uuid, COALESCE(q.context_variables::text, '{}'), COALESCE(q.retry_count, 0)::int",
                &[&campaign, &now.to_string(), &limit],
            )
            .await
            .context("dograh: claim runs")?;
        rows.iter()
            .map(|r| {
                let vars: String = r.get(3);
                let variables = match serde_json::from_str::<Value>(&vars) {
                    Ok(Value::Object(m)) => m,
                    Ok(_) | Err(_) if vars.trim().is_empty() || vars.trim() == "null" => Map::new(),
                    _ => bail!("dograh: run {} variables aren't a JSON object", r.get::<_, i32>(0)),
                };
                Ok(QueuedRun { id: r.get(0), campaign_id: r.get(1), source_uuid: r.get(2), variables, retry_count: r.get(4) })
            })
            .collect()
    }

    /// Moves a claimed run to `state`. 'processed' also counts it in the campaign (processed_rows) and
    /// stamps processed_at; 'failed' counts it in failed_rows. Both counters are incremented in SQL, so
    /// concurrent updates can't lose one.
    pub async fn set_run_state(&self, run: i32, state: RunState) -> Result<()> {
        let db = self.db().await?;
        let s = state.as_str();
        // One statement, so one round trip: the run's new state and, for a contact that was handled, the
        // campaign's counter.
        let mut sql = String::from(
            "WITH u AS (UPDATE queued_runs SET state = $2::text::queued_run_state,
                    processed_at = CASE WHEN $2::text = 'queued' THEN NULL ELSE now() END
                 WHERE id = $1::int RETURNING campaign_id)",
        );
        let column = match state {
            RunState::Processed => Some("processed_rows"),
            RunState::Failed => Some("failed_rows"),
            RunState::Queued => None,
        };
        if let Some(col) = column {
            sql.push_str(&format!(", c AS (UPDATE campaigns SET {col} = {col} + 1 FROM u WHERE campaigns.id = u.campaign_id)"));
        }
        sql.push_str(" SELECT count(*)::int FROM u");
        let n: i32 = db.query_one(&sql, &[&run, &s]).await.with_context(|| format!("dograh: run {run} to {s}"))?.get(0);
        if n == 0 {
            bail!("dograh: run {run} to {s}: no such run");
        }
        Ok(())
    }

    /// Adds a new run for `run`'s contact, due after `delay`, as Dograh's orchestrator does: the same
    /// variables plus is_retry / retry_attempt / retry_reason, source_uuid "<original>_retry_<n>", the
    /// attempt counted in retry_count. A retry that already exists (same campaign, source and attempt)
    /// is left alone, so a repeated call can't queue two.
    pub async fn schedule_retry(&self, run: &QueuedRun, reason: &str, delay: Duration, now: Timestamp) -> Result<()> {
        let mut vars = run.variables.clone();
        vars.insert("is_retry".into(), Value::Bool(true));
        vars.insert("retry_attempt".into(), Value::from(run.retry_count + 1));
        vars.insert("retry_reason".into(), Value::from(reason));
        let (attempt, source) = (run.retry_count + 1, format!("{}_retry_{}", run.source_uuid, run.retry_count + 1));
        let due = now.checked_add(delay).unwrap_or(now).to_string();
        let db = self.db().await?;
        db.execute(
            "INSERT INTO queued_runs (campaign_id, source_uuid, context_variables, state, retry_count, parent_queued_run_id, scheduled_for, retry_reason, created_at)
             VALUES ($1::int, $2, $3::text::json, 'queued', $4::int, $5::int, $6::text::timestamptz, $7, now())
             ON CONFLICT (campaign_id, source_uuid, retry_count) DO NOTHING",
            &[&run.campaign_id, &source, &Value::Object(vars).to_string(), &attempt, &run.id, &due, &reason],
        )
        .await
        .with_context(|| format!("dograh: schedule retry of run {}", run.id))?;
        Ok(())
    }

    /// Moves a campaign from one state to another, only if it is still in `from` (a Pause or Resume made
    /// meanwhile in Dograh's UI wins). Says whether it moved.
    pub async fn set_campaign_state(&self, id: i32, from: &str, to: &str) -> Result<bool> {
        let db = self.db().await?;
        let n = db
            .execute("UPDATE campaigns SET state = $3::text::campaign_state WHERE id = $1::int AND state::text = $2", &[&id, &from, &to])
            .await
            .with_context(|| format!("dograh: campaign {id} {from} -> {to}"))?;
        Ok(n == 1)
    }

    /// Counts a contact that failed for good (retries used up).
    pub async fn add_failed_row(&self, campaign: i32) -> Result<()> {
        let db = self.db().await?;
        db.execute("UPDATE campaigns SET failed_rows = failed_rows + 1 WHERE id = $1::int", &[&campaign])
            .await
            .with_context(|| format!("dograh: campaign {campaign} failed_rows"))?;
        Ok(())
    }

    /// Counts campaign `id`'s contacts still to be called ('queued', retries due later included) and
    /// those being handled right now ('processing').
    pub async fn open_work(&self, campaign: i32) -> Result<(i64, i64)> {
        let db = self.db().await?;
        let r = db
            .query_one(
                "SELECT count(*) FILTER (WHERE state::text = 'queued'), count(*) FILTER (WHERE state::text = 'processing') FROM queued_runs WHERE campaign_id = $1::int",
                &[&campaign],
            )
            .await
            .with_context(|| format!("dograh: campaign {campaign} work"))?;
        Ok((r.get(0), r.get(1)))
    }

    /// Marks a running campaign completed. Says whether it did (a Pause or Resume made meanwhile wins).
    pub async fn complete_campaign(&self, id: i32) -> Result<bool> {
        let db = self.db().await?;
        let n = db
            .execute(
                "UPDATE campaigns SET state = 'completed', completed_at = now() WHERE id = $1::int AND state::text = 'running'",
                &[&id],
            )
            .await
            .with_context(|| format!("dograh: complete campaign {id}"))?;
        Ok(n == 1)
    }

    /// Settles the contacts a stopped Vaani left 'processing' in campaigns it dials: one with no Dograh
    /// run was never dialed and goes back to the queue; one with a run may have been called, so it is
    /// marked processed and never redialed. Run once at startup, with no other Vaani running.
    pub async fn recover_contacts(&self) -> Result<(i64, i64)> {
        let db = self.db().await?;
        let r = db
            .query_one(
                &format!(
                    "WITH moved AS (
                        UPDATE queued_runs q SET state = CASE WHEN EXISTS (SELECT 1 FROM workflow_runs r WHERE r.queued_run_id = q.id)
                                THEN 'processed' ELSE 'queued' END::text::queued_run_state
                        FROM campaigns c
                        WHERE c.id = q.campaign_id AND q.state::text = 'processing' AND {VAANI_ORGS}
                        RETURNING q.state::text AS state)
                     SELECT count(*) FILTER (WHERE state = 'queued'), count(*) FILTER (WHERE state = 'processed') FROM moved"
                ),
                &[],
            )
            .await
            .context("dograh: recover contacts")?;
        Ok((r.get(0), r.get(1)))
    }

    /// Lists the unfinished outbound runs of campaigns Vaani dials that no live call accounts for
    /// (`live`: their ids). An 'initialized' run is stale once created before `initialized_before` (the
    /// call never reached the agent). A 'running' run is stale only when `include_running`: right after a
    /// restart, when nothing of this Vaani's is alive -- later, a finished call's record is still being
    /// written while its run reads 'running'.
    pub async fn stale_runs(&self, initialized_before: Timestamp, include_running: bool, live: &[i64]) -> Result<Vec<StaleRun>> {
        let db = self.db().await?;
        let rows = db
            .query(
                &format!(
                    "SELECT r.id::bigint, c.organization_id::int, r.state::text
                     FROM workflow_runs r JOIN campaigns c ON c.id = r.campaign_id
                     WHERE r.call_type::text = 'outbound' AND NOT r.is_completed AND NOT (r.id = ANY($1::bigint[])) AND {VAANI_ORGS}
                       AND ((r.state::text = 'initialized' AND r.created_at < $2::text::timestamptz) OR ($3::bool AND r.state::text = 'running'))
                     ORDER BY r.id"
                ),
                &[&live, &initialized_before.to_string(), &include_running],
            )
            .await
            .context("dograh: stale runs")?;
        Ok(rows.iter().map(|r| StaleRun { id: r.get(0), org_id: r.get(1), state: r.get(2) }).collect())
    }
}

/// A call of a Vaani campaign whose Dograh run was never completed.
#[derive(Debug, Clone)]
pub struct StaleRun {
    pub id: i64,
    pub org_id: i32,
    /// "initialized" or "running".
    pub state: String,
}

/// What the dialer reads before dialing a contact (see `Store::dial_check`).
#[derive(Debug, Clone, Default)]
pub struct DialCheck {
    /// The campaign is still 'running'.
    pub running: bool,
    /// The organization's CONCURRENT_CALL_LIMIT; `None` when it has none set (the caller uses its default).
    pub concurrent_max: Option<i32>,
    /// The organization's DIAL_RATE_LIMIT_PER_SECOND; `None` when it has none set (the campaign's own rate applies).
    pub dial_rate: Option<i32>,
    pub wallet_blocks: bool,
    /// The organization's telephony configuration; `None` when it has none.
    pub settings: Option<DialSettings>,
}

/// The integer in an organization_configurations value of the form {"value": n}.
fn config_int(raw: &str) -> Option<i32> {
    serde_json::from_str::<Value>(raw).ok()?.get("value")?.as_i64().map(|n| n as i32)
}

/// Dograh's can_start_call, inverted: prepaid needs at least a minute of balance; postpaid needs to be
/// above its credit limit, if it has one.
fn wallet_blocks(mode: Option<&str>, rate: Option<f64>, balance: Option<f64>, credit: Option<f64>) -> bool {
    let (Some(mode), Some(rate)) = (mode.filter(|m| !m.is_empty()), rate.filter(|r| *r != 0.0)) else { return false };
    let bal = balance.unwrap_or(0.0);
    if mode == "prepaid" {
        return bal < rate;
    }
    credit.is_some_and(|c| bal <= -c)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::CAMPAIGN_SCHEMA;

    fn campaign(retry: &str, meta: &str) -> Campaign {
        let mut c = Campaign::default();
        parse_campaign(&mut c, retry, meta).unwrap();
        c
    }

    #[test]
    fn dograhs_defaults_apply_to_a_campaign_with_no_settings() {
        let c = campaign("", "");
        assert!(c.retry.enabled && c.retry.on_busy && c.retry.on_no_answer && c.retry.on_voicemail);
        assert_eq!((c.retry.max_retries, c.retry.delay), (1, Duration::from_secs(120)));
        assert_eq!(
            (c.breaker.enabled, c.breaker.threshold, c.breaker.window, c.breaker.min_calls),
            (true, 0.5, Duration::from_secs(120), 5)
        );
        assert_eq!(c.max_concurrency, 0);
        assert!(!c.schedule.enabled && c.schedule.allows(Timestamp::now()));
    }

    #[test]
    fn settings_override_the_defaults_and_retry_eligibility_follows_them() {
        let c = campaign(
            r#"{"max_retries": 3, "retry_delay_seconds": 30, "retry_on_busy": false}"#,
            r#"{"max_concurrency": 4, "circuit_breaker": {"failure_threshold": 0.8, "window_seconds": 60, "min_calls_in_window": 10}}"#,
        );
        assert_eq!((c.retry.max_retries, c.retry.delay, c.max_concurrency), (3, Duration::from_secs(30), 4));
        assert_eq!((c.breaker.threshold, c.breaker.window, c.breaker.min_calls), (0.8, Duration::from_secs(60), 10));
        assert!(!c.retry.eligible("busy") && c.retry.eligible("no_answer") && !c.retry.eligible("failed"));
        assert!(!campaign(r#"{"enabled": false}"#, "").retry.eligible("no_answer"));
        let mut bad = Campaign::default();
        assert!(parse_campaign(&mut bad, "{nope", "").is_err());
    }

    #[test]
    fn the_schedule_is_a_weekly_window_in_the_campaigns_timezone() {
        let c = campaign(
            "",
            r#"{"schedule_config": {"enabled": true, "timezone": "Asia/Kolkata",
            "slots": [{"day_of_week": 0, "start_time": "09:00", "end_time": "18:00"}]}}"#,
        );
        // Monday 2026-11-02, 10:00 IST = 04:30 UTC
        let at = |h, m| -> Timestamp { format!("2026-11-02T{h:02}:{m:02}:00Z").parse().unwrap() };
        assert!(c.schedule.allows(at(4, 30)));
        assert!(!c.schedule.allows(at(12, 30)), "18:00 IST is the end: exclusive");
        assert!(!c.schedule.allows(at(3, 0)), "08:30 IST is before the window");
        assert!(!c.schedule.allows("2026-11-03T04:30:00Z".parse::<Timestamp>().unwrap()), "Tuesday has no slot");
        let unknown = campaign(
            "",
            r#"{"schedule_config": {"enabled": true, "timezone": "Nowhere/Land", "slots": [{"day_of_week": 0, "start_time": "09:00", "end_time": "10:00"}]}}"#,
        );
        assert!(unknown.schedule.allows(at(0, 0)), "an unknown timezone allows every time");
    }

    #[test]
    fn the_wallet_blocks_like_dograhs_can_start_call() {
        assert!(!wallet_blocks(None, Some(1.0), Some(0.0), None), "billing off");
        assert!(!wallet_blocks(Some("prepaid"), Some(0.0), Some(0.0), None), "no rate");
        assert!(wallet_blocks(Some("prepaid"), Some(5.0), Some(4.9), None), "less than a minute of balance");
        assert!(!wallet_blocks(Some("prepaid"), Some(5.0), Some(5.0), None));
        assert!(wallet_blocks(Some("postpaid"), Some(5.0), Some(-100.0), Some(100.0)), "at the credit limit");
        assert!(!wallet_blocks(Some("postpaid"), Some(5.0), Some(-50.0), Some(100.0)));
        assert!(!wallet_blocks(Some("postpaid"), Some(5.0), Some(-1e9), None), "no limit set");
    }

    /// The campaign tables against a real Postgres (`DOGRAH_TEST_DB_URL`), with Dograh's column types.
    #[tokio::test]
    async fn campaigns_contacts_and_runs_in_postgres() {
        use crate::testkit::CAMPAIGN_SCHEMA;
        let Some((store, _)) = Store::for_test(
            "t_campaign",
            &format!(
                "{CAMPAIGN_SCHEMA}
                INSERT INTO organization_configurations (organization_id, key, value) VALUES
                  (3, 'MEDIA_ENGINE', '{{\"value\": \"vaani\"}}'), (4, 'MEDIA_ENGINE', '{{\"value\": \"pipecat\"}}'),
                  (3, 'CONCURRENT_CALL_LIMIT', '{{\"value\": 7}}'), (3, 'DIAL_RATE_LIMIT_PER_SECOND', '{{\"value\": 5}}'),
                  (3, 'DISPOSITION_CODE_MAPPING', '{{\"BUSY\": \"B1\"}}'),
                  (3, 'TELEPHONY_CONFIGURATION', '{{\"provider\": \"ari\", \"sip_technology\": \"pjsip\", \"outbound_trunk\": \"carrier\",
                     \"strip_number_prefix\": \"+91\", \"from_numbers\": [\"0731111\"], \"number_workflow_map\": {{\"0739999\": 42}}}}');
                INSERT INTO workflows (id, user_id, organization_id) VALUES (42, 7, 3);
                INSERT INTO workflow_definitions (workflow_id, workflow_json, is_current) VALUES (42, '{{}}', true);
                INSERT INTO campaigns (id, organization_id, workflow_id, state, rate_limit_per_second, retry_config) VALUES
                  (1, 3, 42, 'running', 0, '{{\"max_retries\": 2}}'), (2, 3, 42, 'paused', 1, NULL), (3, 4, 42, 'running', 1, NULL);
                INSERT INTO queued_runs (campaign_id, source_uuid, context_variables, scheduled_for) VALUES
                  (1, 'a', '{{\"phone_number\": \"+911\"}}', NULL),
                  (1, 'b', '{{\"phone_number\": \"+912\"}}', NULL),
                  (1, 'retry-late', '{{\"phone_number\": \"+913\"}}', now() - interval '1 minute'),
                  (1, 'retry-early', '{{\"phone_number\": \"+914\"}}', now() - interval '5 minutes'),
                  (1, 'future', '{{\"phone_number\": \"+915\"}}', now() + interval '1 hour');"
            ),
        )
        .await
        else {
            eprintln!("skipped: DOGRAH_TEST_DB_URL not set");
            return;
        };

        // Only the running campaigns of organizations whose engine is vaani.
        let (cs, bad) = store.vaani_campaigns().await.unwrap();
        assert!(bad.is_empty());
        assert_eq!(
            cs.iter().map(|c| (c.id, c.organization_id, c.workflow_id, c.rate_per_second, c.retry.max_retries)).collect::<Vec<_>>(),
            [(1, 3, 42, 1, 2)],
            "rate 0 -> at least 1"
        );
        // The dialer's pre-dial read, in one query: the organization's own keys, not each other's, and none
        // where it has none; the telephony settings; a paused or missing campaign is not running.
        let chk = store.dial_check(1, 3).await.unwrap();
        assert_eq!((chk.running, chk.concurrent_max, chk.dial_rate, chk.wallet_blocks), (true, Some(7), Some(5), false));
        let st = chk.settings.expect("org 3 has a telephony configuration");
        assert_eq!((st.provider.as_str(), st.endpoint("+919876543210").as_str()), ("ari", "PJSIP/9876543210@carrier"));
        let chk = store.dial_check(3, 4).await.unwrap();
        assert_eq!((chk.running, chk.concurrent_max, chk.dial_rate), (true, None, None));
        assert!(chk.settings.is_none(), "org 4 has no telephony configuration");
        assert!(!store.dial_check(2, 3).await.unwrap().running, "paused: seen at once");
        assert!(!store.dial_check(99, 3).await.unwrap().running, "no such campaign");

        // Scheduled retries first (earliest first), then loading order; a future retry isn't due.
        let now = Timestamp::now();
        let first = store.claim_runs(1, 3, now).await.unwrap();
        assert_eq!(first.iter().map(|q| q.source_uuid.as_str()).collect::<Vec<_>>(), ["retry-early", "retry-late", "a"]);
        assert_eq!(first[2].variables["phone_number"], "+911");
        assert_eq!(
            store.claim_runs(1, 5, now).await.unwrap().iter().map(|q| q.source_uuid.clone()).collect::<Vec<_>>(),
            ["b"],
            "claimed rows aren't claimed again; the future one isn't due"
        );
        assert_eq!(store.open_work(1).await.unwrap(), (1, 4), "queued includes the retry due later; four are being handled");

        // Row states update the campaign's counters.
        store.set_run_state(first[0].id, RunState::Processed).await.unwrap();
        store.set_run_state(first[1].id, RunState::Failed).await.unwrap();
        store.set_run_state(first[2].id, RunState::Queued).await.unwrap();
        let db = store.db().await.unwrap();
        let row = db.query_one("SELECT processed_rows, failed_rows FROM campaigns WHERE id = 1", &[]).await.unwrap();
        assert_eq!((row.get::<_, i32>(0), row.get::<_, i32>(1)), (1, 1));
        assert!(
            db.query_one("SELECT processed_at IS NULL FROM queued_runs WHERE id = $1", &[&first[2].id]).await.unwrap().get::<_, bool>(0),
            "giving a row back clears processed_at"
        );

        // A retry is a new row, once.
        let q = QueuedRun {
            id: first[0].id,
            campaign_id: 1,
            source_uuid: "retry-early".into(),
            variables: first[0].variables.clone(),
            retry_count: 0,
        };
        for _ in 0..2 {
            store.schedule_retry(&q, "busy", Duration::from_secs(120), now).await.unwrap();
        }
        let r = db.query_one("SELECT count(*), min(source_uuid), min(retry_count), min(context_variables::text) FROM queued_runs WHERE parent_queued_run_id = $1", &[&q.id]).await.unwrap();
        assert_eq!((r.get::<_, i64>(0), r.get::<_, String>(1), r.get::<_, i32>(2)), (1, "retry-early_retry_1".to_string(), 1));
        assert!(
            r.get::<_, String>(3).contains("\"is_retry\":true") && r.get::<_, String>(3).contains("retry_reason"),
            "{}",
            r.get::<_, String>(3)
        );

        // Telephony settings and the outbound run's whole life.
        let d = store.dial_settings(3).await.unwrap();
        assert_eq!(
            (d.provider.as_str(), d.endpoint("+919876543210").as_str(), d.caller_id(42).as_str()),
            ("ari", "PJSIP/9876543210@carrier", "0739999")
        );
        assert!(store.dial_settings(99).await.unwrap_err().to_string().contains("no telephony configuration"));
        let c = &cs[0];
        let run = store.create_outbound_run(c, &first[0], "+911", "0739999").await.unwrap();
        let r = db
            .query_one(
                "SELECT name, call_type, state, campaign_id, queued_run_id, initial_context::text FROM workflow_runs WHERE id = $1",
                &[&run],
            )
            .await
            .unwrap();
        assert_eq!(
            (r.get::<_, String>(0), r.get::<_, String>(1), r.get::<_, String>(2), r.get::<_, i32>(3), r.get::<_, i32>(4)),
            (format!("WR-CAMPAIGN-1-{}", first[0].id), "outbound".into(), "initialized".into(), 1, first[0].id)
        );
        assert!(r.get::<_, String>(5).contains("\"called_number\":\"+911\"") && r.get::<_, String>(5).contains("\"campaign_id\":1"));
        assert!(
            store
                .create_outbound_run(&Campaign { workflow_id: 999, ..c.clone() }, &first[0], "+911", "")
                .await
                .unwrap_err()
                .to_string()
                .contains("workflow 999 not found")
        );

        // An initialized run nobody is running goes stale; one a live call accounts for doesn't.
        let later = Timestamp::now() + jiff::SignedDuration::from_mins(10);
        assert!(store.stale_runs(later, false, &[run]).await.unwrap().is_empty());
        let stale = store.stale_runs(later, false, &[]).await.unwrap();
        assert_eq!((stale.len(), stale[0].id, stale[0].org_id, stale[0].state.as_str()), (1, run, 3, "initialized"));
        store.mark_run_running(run).await.unwrap();
        assert!(store.stale_runs(later, false, &[]).await.unwrap().is_empty(), "a running call isn't stale unless after a restart");
        assert_eq!(store.stale_runs(later, true, &[]).await.unwrap()[0].state, "running");
        store.mark_not_connected(run, 3, "busy").await.unwrap();
        let r = db.query_one("SELECT is_completed, state, gathered_context::text FROM workflow_runs WHERE id = $1", &[&run]).await.unwrap();
        assert_eq!((r.get::<_, bool>(0), r.get::<_, String>(1).as_str()), (true, "completed"));
        let g: Value = serde_json::from_str(&r.get::<_, String>(2)).unwrap();
        assert_eq!(
            (g["call_disposition"].as_str(), g["mapped_call_disposition"].as_str()),
            (Some("BUSY"), Some("B1")),
            "the organization's mapping"
        );
        assert_eq!(g["call_tags"], serde_json::json!(["not_connected", "telephony_busy"]));
        assert!(store.stale_runs(later, true, &[]).await.unwrap().is_empty());

        // A restart: a contact left 'processing' with no run was never dialed and goes back to the
        // queue; one that has a run (b: dialed, ringing when Vaani stopped) stays processed.
        let b_id = db.query_one("SELECT id FROM queued_runs WHERE source_uuid = 'b'", &[]).await.unwrap().get::<_, i32>(0);
        store.create_outbound_run(c, &QueuedRun { id: b_id, ..first[0].clone() }, "+912", "").await.unwrap();
        db.execute(
            "INSERT INTO queued_runs (campaign_id, source_uuid, context_variables, state) VALUES (1, 'never-dialed', '{}', 'processing')",
            &[],
        )
        .await
        .unwrap();
        assert_eq!(store.recover_contacts().await.unwrap(), (1, 1));
        let states: Vec<String> = db
            .query("SELECT state::text FROM queued_runs WHERE source_uuid IN ('b', 'never-dialed') ORDER BY source_uuid", &[])
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(states, ["processed", "queued"]);
        assert_eq!(store.recover_contacts().await.unwrap(), (0, 0), "nothing left to settle");

        // The campaign is complete once nothing is queued or being handled.
        assert!(!store.complete_campaign(2).await.unwrap(), "a paused campaign isn't completed");
        assert!(store.dial_check(1, 3).await.unwrap().running);
        assert!(store.set_campaign_state(1, "running", "paused").await.unwrap());
        assert!(!store.set_campaign_state(1, "running", "paused").await.unwrap(), "only from the state it is in");
        assert!(store.set_campaign_state(1, "paused", "running").await.unwrap());
        assert!(store.complete_campaign(1).await.unwrap() && !store.complete_campaign(1).await.unwrap());
    }

    /// The wallet rules reach the dialer through the same one query as Dograh's can_start_call.
    #[tokio::test]
    async fn dial_check_reads_the_wallet_like_dograhs_can_start_call() {
        let Some((store, _)) = Store::for_test(
            "t_dial_check_wallet",
            &format!(
                "{CAMPAIGN_SCHEMA}
                 INSERT INTO organizations (id, billing_mode, billing_rate_per_min_inr, credit_limit_inr, wallet_balance_inr) VALUES
                   (1, NULL, NULL, NULL, 0), (2, 'prepaid', 1.5, NULL, 1.4), (3, 'prepaid', 1.5, NULL, 1.5),
                   (4, 'postpaid', 1.5, 100, -100), (5, 'postpaid', 1.5, 100, -99.99), (6, 'postpaid', 1.5, NULL, -5000);
                 INSERT INTO campaigns (id, organization_id, workflow_id, state) VALUES (1, 1, 1, 'running');"
            ),
        )
        .await
        else {
            return eprintln!("skipped: DOGRAH_TEST_DB_URL not set");
        };
        for (org, want) in [(1, false), (2, true), (3, false), (4, true), (5, false), (6, false), (99, false)] {
            assert_eq!(store.dial_check(1, org).await.unwrap().wallet_blocks, want, "org {org}");
        }
    }

    /// Dial workers finish contacts at the same time: no counter increment is lost.
    #[tokio::test]
    async fn concurrent_run_state_updates_lose_no_count() {
        let Some((store, _)) = Store::for_test(
            "t_run_state_concurrent",
            &format!(
                "{CAMPAIGN_SCHEMA}
                 INSERT INTO campaigns (id, organization_id, workflow_id, state) VALUES (1, 1, 1, 'running');
                 INSERT INTO queued_runs (campaign_id, source_uuid, state) SELECT 1, 'r' || g, 'processing' FROM generate_series(1, 40) g;"
            ),
        )
        .await
        else {
            return eprintln!("skipped: DOGRAH_TEST_DB_URL not set");
        };
        let ids: Vec<i32> = store
            .db()
            .await
            .unwrap()
            .query("SELECT id FROM queued_runs ORDER BY id", &[])
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        let tasks: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| {
                let (store, id) = (store.clone(), *id);
                tokio::spawn(async move { store.set_run_state(id, if i % 4 == 3 { RunState::Failed } else { RunState::Processed }).await })
            })
            .collect();
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        let row = store.db().await.unwrap().query_one("SELECT processed_rows, failed_rows FROM campaigns WHERE id = 1", &[]).await.unwrap();
        assert_eq!((row.get::<_, i32>(0), row.get::<_, i32>(1)), (30, 10));
        assert!(store.set_run_state(9999, RunState::Processed).await.is_err(), "no such run");
    }
}

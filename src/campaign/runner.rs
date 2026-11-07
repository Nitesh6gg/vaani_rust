//! The dialer: it claims contacts from Dograh's queue one at a time, each under the
//! organization's rate and concurrency limits, places the call, and decides retries
//! and the circuit breaker from how it ends. Dograh's UI starts, pauses and resumes
//! campaigns; the Runner only follows their state.

use super::limiter::{Limiter, Slot};
use crate::{
    ari::outbound::{DialRequest, Outcome},
    dograh::{
        Store,
        campaign::{Campaign, QueuedRun, RunState},
        outbound::{STATUS_BUSY, STATUS_FAILED, STATUS_NO_ANSWER, STATUS_REJECTED},
    },
    metrics,
};
use anyhow::Result;
use chrono::Utc;
use futures_util::future::BoxFuture;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, atomic::Ordering::Relaxed},
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Places a call; `ari::manager::Manager` is one.
pub trait CallPlacer: Send + Sync {
    fn dial(&self, req: DialRequest) -> BoxFuture<'_, Result<String>>;
}

impl CallPlacer for Arc<crate::ari::manager::Manager> {
    fn dial(&self, req: DialRequest) -> BoxFuture<'_, Result<String>> {
        Box::pin(async move { crate::ari::manager::Manager::dial(self, req).await })
    }
}

#[derive(Debug, Clone)]
pub struct RunnerConfig {
    /// How often Dograh's database is scanned for campaigns to dial.
    pub poll: Duration,
    /// The concurrent-call limit of an organization that has none set (Dograh's
    /// DEFAULT_ORG_CONCURRENCY_LIMIT, 2).
    pub default_org_limit: i32,
    pub ring_timeout: Duration,
}

/// What one attempt to dial a contact came to.
enum Step {
    /// A contact was handled: try the next.
    Dialed,
    /// The rate window is full: wait, then try again.
    Wait(Duration),
    /// Nothing more to do now: wait for the next scan.
    Stop,
}

#[derive(Default)]
struct State {
    /// Campaigns a drain task is working on.
    draining: HashSet<i32>,
    /// Calls placed and not yet over, per campaign.
    inflight: HashMap<i32, i64>,
    live_runs: HashSet<i64>,
}

pub struct Runner {
    store: Store,
    lim: Limiter,
    placer: Arc<dyn CallPlacer>,
    cfg: RunnerConfig,
    /// The process's token. Call results are written even while it is being cancelled.
    cancel: CancellationToken,
    state: Mutex<State>,
    wake: Notify,
}

fn phone_of(vars: &serde_json::Map<String, Value>) -> String {
    match vars.get("phone_number") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        _ => String::new(),
    }
}

/// The retry setting a status stands for; `None` = not retried.
fn retry_reason(status: &str) -> Option<&'static str> {
    match status {
        STATUS_BUSY => Some("busy"),
        STATUS_NO_ANSWER => Some("no_answer"),
        _ => None,
    }
}

impl Runner {
    pub fn new(store: Store, lim: Limiter, placer: Arc<dyn CallPlacer>, cfg: RunnerConfig, cancel: CancellationToken) -> Arc<Self> {
        Arc::new(Self { store, lim, placer, cfg, cancel, state: Mutex::default(), wake: Notify::new() })
    }

    /// Scans for campaigns until the runner's token is cancelled, then waits for the dialing tasks to stop.
    /// Calls already placed carry on (the ARI manager ends them at shutdown).
    pub async fn run(self: Arc<Self>) {
        self.recover().await;
        let (mut poll, mut sweep) = (tokio::time::interval(self.cfg.poll), tokio::time::interval(Duration::from_secs(60)));
        poll.tick().await;
        sweep.tick().await;
        let tracker = tokio_util::task::TaskTracker::new();
        loop {
            self.scan(&tracker).await;
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = poll.tick() => {}
                _ = self.wake.notified() => {}
                _ = sweep.tick() => self.sweep(false).await,
            }
        }
        tracker.close();
        tracker.wait().await;
    }

    /// Makes the next scan happen now (a call ended, so a slot is free).
    fn wake(&self) {
        self.wake.notify_one();
    }

    async fn scan(self: &Arc<Self>, tracker: &tokio_util::task::TaskTracker) {
        let campaigns = match self.store.vaani_campaigns().await {
            Ok((c, bad)) => {
                for b in bad {
                    tracing::error!(component = "campaign", event = "campaign.scan_failed", error = %b, "campaign scan: a campaign could not be read");
                }
                c
            }
            Err(e) => {
                if !self.cancel.is_cancelled() {
                    tracing::error!(component = "campaign", event = "campaign.scan_failed", error = %format!("{e:#}"), "campaign scan failed");
                }
                return;
            }
        };
        for c in campaigns {
            if !self.state.lock().unwrap().draining.insert(c.id) {
                continue; // a drain task is already on it
            }
            let me = self.clone();
            tracker.spawn(async move {
                me.drain(&c).await;
                me.state.lock().unwrap().draining.remove(&c.id);
            });
        }
    }

    /// Dials campaign `c`'s contacts until it runs out of work or room.
    async fn drain(self: &Arc<Self>, c: &Campaign) {
        while !self.cancel.is_cancelled() {
            match self.dial_one(c).await {
                Step::Dialed => {}
                Step::Wait(d) => {
                    tokio::select! {
                        _ = tokio::time::sleep(d.clamp(Duration::from_millis(10), Duration::from_secs(1))) => {}
                        _ = self.cancel.cancelled() => {}
                    }
                }
                Step::Stop => return,
            }
        }
    }

    fn log(&self, c: &Campaign, event: &str, msg: &str) {
        tracing::info!(component = "campaign", event, campaign_id = c.id, org_id = c.organization_id, "{msg}");
    }

    /// Moves a running campaign to paused (unless someone changed it meanwhile).
    async fn pause(&self, c: &Campaign, kind: &str, why: &str) {
        match self.store.set_campaign_state(c.id, "running", "paused").await {
            Ok(true) => {
                metrics::campaign_pause(kind);
                tracing::warn!(
                    component = "campaign",
                    event = "campaign.paused",
                    campaign_id = c.id,
                    org_id = c.organization_id,
                    reason = why,
                    "campaign paused"
                );
            }
            Ok(false) => {}
            Err(e) => {
                tracing::error!(component = "campaign", event = "campaign.pause_failed", campaign_id = c.id, error = %format!("{e:#}"), "campaign: pausing failed")
            }
        }
    }

    /// How many calls campaign `c`'s organization may have at once: its CONCURRENT_CALL_LIMIT or the
    /// default, lowered by the campaign's own max_concurrency.
    async fn org_limit(&self, c: &Campaign) -> Result<i32> {
        let limit = self.store.org_concurrency_limit(c.organization_id).await?.unwrap_or(self.cfg.default_org_limit);
        Ok(if c.max_concurrency > 0 && c.max_concurrency < limit { c.max_concurrency } else { limit })
    }

    /// How many calls per second campaign `c`'s organization may start: its DIAL_RATE_LIMIT_PER_SECOND,
    /// which overrides the campaign's own rate_limit_per_second, or that rate when the organization has none set.
    async fn dial_rate(&self, c: &Campaign) -> Result<i32> {
        Ok(self.store.org_dial_rate(c.organization_id).await?.map_or(c.rate_per_second, |r| r.max(1)))
    }

    /// Takes the next contact of campaign `c` and places the call, if the campaign is running, inside its
    /// window, healthy, paid for, and under the organization's rate and concurrency limits.
    async fn dial_one(self: &Arc<Self>, c: &Campaign) -> Step {
        if !self.store.campaign_running(c.id).await.unwrap_or(false) || !c.schedule.allows(Utc::now()) {
            return Step::Stop;
        }
        if let Ok((true, stats)) = self.lim.breaker_open(c.id, &c.breaker).await {
            self.pause(c, "breaker", &format!("circuit breaker: {} of {} calls failed", stats.failures, stats.total())).await;
            return Step::Stop;
        }
        if self.store.wallet_blocks(c.organization_id).await {
            self.pause(c, "wallet", "the organization's wallet is out of credit").await;
            return Step::Stop;
        }
        let limit = match self.org_limit(c).await {
            Ok(l) => l,
            Err(e) => return self.limit_failed(c, "organization limit", e),
        };
        let rate = match self.dial_rate(c).await {
            Ok(r) => r,
            Err(e) => return self.limit_failed(c, "organization dial rate", e),
        };
        match self.lim.acquire_token(c.organization_id, rate).await {
            Ok(true) => {}
            Ok(false) => return Step::Wait(self.lim.next_token_wait(c.organization_id).await),
            Err(e) => return self.limit_failed(c, "rate limiter", e),
        }
        let slot = match self.lim.acquire_slot(c.organization_id, limit).await {
            Ok(Some(s)) => s,
            Ok(None) => return Step::Stop, // full: a call ending wakes the next scan
            Err(e) => return self.limit_failed(c, "slot limiter", e),
        };

        match self.store.claim_runs(c.id, 1, Utc::now()).await {
            Ok(mut runs) if !runs.is_empty() => self.place(c, runs.remove(0), slot).await,
            Ok(_) => {
                self.release(&slot).await;
                self.maybe_complete(c).await;
                Step::Stop
            }
            Err(e) => {
                self.release(&slot).await;
                tracing::error!(component = "campaign", event = "campaign.claim_failed", campaign_id = c.id, error = %format!("{e:#}"), "campaign: claiming a contact failed");
                Step::Stop
            }
        }
    }

    fn limit_failed(&self, c: &Campaign, what: &str, e: anyhow::Error) -> Step {
        tracing::error!(component = "campaign", event = "campaign.limit_failed", campaign_id = c.id, error = %format!("{e:#}"), "campaign: {what} failed");
        Step::Stop
    }

    async fn release(&self, slot: &Slot) {
        if let Err(e) = self.lim.release_slot(slot).await {
            tracing::error!(component = "campaign", event = "campaign.release_failed", slot = %slot.id, error = %format!("{e:#}"), "campaign: releasing a slot failed");
        }
    }

    /// Dials claimed contact `q`. It owns `slot` from here: released when the call is over, or at once if
    /// it was never placed.
    async fn place(self: &Arc<Self>, c: &Campaign, q: QueuedRun, slot: Slot) -> Step {
        let phone = phone_of(&q.variables);
        if phone.is_empty() {
            tracing::warn!(
                component = "campaign",
                event = "campaign.no_number",
                campaign_id = c.id,
                queued_run_id = q.id,
                "campaign: contact has no phone_number; failing it"
            );
            self.release(&slot).await;
            self.set_run_state(&q, RunState::Failed).await;
            return Step::Dialed;
        }

        // Anything that stops the call from being placed gives the contact back, to be tried again at the next scan.
        let give_back = |why: &'static str, e: String| {
            tracing::error!(component = "campaign", event = "campaign.place_failed", campaign_id = c.id, queued_run_id = q.id, error = %e, "campaign: {why}");
        };

        let settings = match self.store.dial_settings(c.organization_id).await {
            Ok(s) => s,
            Err(e) => {
                give_back("the organization's telephony settings are unreadable", format!("{e:#}"));
                return self.give_back(&q, &slot).await;
            }
        };
        if settings.provider != "ari" {
            self.pause(c, "provider", &format!("the organization's telephony provider is {:?}, not ari", settings.provider)).await;
            give_back("cannot dial over ARI", format!("provider {:?}", settings.provider));
            return self.give_back(&q, &slot).await;
        }
        let caller = settings.caller_id(c.workflow_id);

        let run_id = match self.store.create_outbound_run(c, &q, &phone, &caller).await {
            Ok(id) => id,
            Err(e) => {
                give_back("creating the call's Dograh run failed", format!("{e:#}"));
                return self.give_back(&q, &slot).await;
            }
        };

        self.add_inflight(c.id, 1);
        self.track(run_id, true);

        let (me, camp, queued, held) = (self.clone(), c.clone(), q.clone(), slot.clone());
        let req = DialRequest {
            endpoint: settings.endpoint(&phone),
            caller_id: caller,
            called: phone.clone(),
            ring_timeout: self.cfg.ring_timeout,
            workflow_id: c.workflow_id,
            run_id,
            variables: q.variables.clone(),
            // On its own task, so the ARI event loop never waits on the database.
            done: Box::new(move |out| {
                tokio::spawn(async move { me.finished(&camp, &queued, run_id, &held, out).await });
            }),
        };
        if let Err(e) = self.placer.dial(req).await {
            // The ARI call itself failed: nothing is ringing, and Done won't come.
            tracing::error!(component = "campaign", event = "campaign.dial_failed", campaign_id = c.id, queued_run_id = q.id, run_id, error = %format!("{e:#}"), "campaign: dialing failed");
            self.add_inflight(c.id, -1);
            self.track(run_id, false);
            self.release(&slot).await;
            metrics::campaign_outcome(STATUS_FAILED);
            self.record_outcome(c, true).await;
            self.mark_not_connected(c, run_id, STATUS_FAILED).await;
            self.set_run_state(&q, RunState::Failed).await;
            return Step::Dialed;
        }

        metrics::CAMPAIGN_DIALS.fetch_add(1, Relaxed);
        self.set_run_state(&q, RunState::Processed).await;
        tracing::info!(component = "campaign", event = "campaign.dialed", campaign_id = c.id, org_id = c.organization_id, queued_run_id = q.id, run_id,
            called_number = %phone, retry_count = q.retry_count, "campaign call placed");
        Step::Dialed
    }

    async fn give_back(&self, q: &QueuedRun, slot: &Slot) -> Step {
        self.release(slot).await;
        self.set_run_state(q, RunState::Queued).await;
        Step::Stop
    }

    async fn set_run_state(&self, q: &QueuedRun, state: RunState) {
        if let Err(e) = self.store.set_run_state(q.id, state).await {
            tracing::error!(component = "campaign", event = "campaign.row_update_failed", queued_run_id = q.id, state = ?state, error = %format!("{e:#}"), "campaign: updating a contact failed");
        }
    }

    async fn mark_not_connected(&self, c: &Campaign, run_id: i64, status: &str) {
        if let Err(e) = self.store.mark_not_connected(run_id, c.organization_id, status).await {
            tracing::error!(component = "campaign", event = "campaign.run_update_failed", run_id, error = %format!("{e:#}"), "campaign: completing an unconnected call's run failed");
        }
    }

    /// Adds a finished call to the circuit breaker's window and pauses the campaign if that trips it.
    async fn record_outcome(&self, c: &Campaign, failure: bool) {
        match self.lim.record_outcome(c.id, failure, &c.breaker).await {
            Ok((true, stats)) => {
                self.pause(c, "breaker", &format!("circuit breaker: {} of {} calls failed", stats.failures, stats.total())).await
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(component = "campaign", event = "campaign.breaker_failed", campaign_id = c.id, error = %format!("{e:#}"), "campaign: circuit breaker failed")
            }
        }
    }

    fn add_inflight(&self, campaign: i32, n: i64) {
        let mut st = self.state.lock().unwrap();
        if n > 0 {
            metrics::CAMPAIGN_CALLS_IN_FLIGHT.fetch_add(n as u64, Relaxed);
        } else {
            // Every decrement follows its own increment (place -> finished), so this can't underflow.
            metrics::CAMPAIGN_CALLS_IN_FLIGHT.fetch_sub(n.unsigned_abs(), Relaxed);
        }
        let e = st.inflight.entry(campaign).or_default();
        *e += n;
        if *e <= 0 {
            st.inflight.remove(&campaign);
        }
    }

    fn track(&self, run_id: i64, live: bool) {
        let mut st = self.state.lock().unwrap();
        if live {
            st.live_runs.insert(run_id);
        } else {
            st.live_runs.remove(&run_id);
        }
    }

    /// How many placed calls of campaign `id` are not over yet.
    pub fn in_flight(&self, id: i32) -> i64 {
        self.state.lock().unwrap().inflight.get(&id).copied().unwrap_or(0)
    }

    /// The end of a placed call: free the slot, tell the circuit breaker, and for a call that never
    /// connected decide its retry and complete its run.
    async fn finished(self: &Arc<Self>, c: &Campaign, q: &QueuedRun, run_id: i64, slot: &Slot, out: Outcome) {
        self.release(slot).await;
        self.finish_call(c, q, run_id, out).await;
        self.add_inflight(c.id, -1);
        self.track(run_id, false);
        self.wake();
    }

    async fn finish_call(&self, c: &Campaign, q: &QueuedRun, run_id: i64, out: Outcome) {
        if out.connected {
            metrics::campaign_outcome("connected");
            self.log(c, "campaign.finished", "campaign call finished");
            self.record_outcome(c, false).await;
            return;
        }
        metrics::campaign_outcome(out.status);
        tracing::info!(
            component = "campaign",
            event = "campaign.not_connected",
            campaign_id = c.id,
            queued_run_id = q.id,
            run_id,
            status = out.status,
            cause = out.cause,
            "campaign call did not connect"
        );

        // As Dograh: only a failed or rejected call counts against the breaker.
        self.record_outcome(c, out.status == STATUS_FAILED || out.status == STATUS_REJECTED).await;

        if let Some(reason) = retry_reason(out.status).filter(|r| c.retry.eligible(r)) {
            let res = if q.retry_count >= c.retry.max_retries {
                self.store.add_failed_row(c.id).await // retries used up
            } else {
                let r = self.store.schedule_retry(q, reason, c.retry.delay, Utc::now()).await;
                if r.is_ok() {
                    metrics::CAMPAIGN_RETRIES.fetch_add(1, Relaxed);
                }
                r
            };
            if let Err(e) = res {
                tracing::error!(component = "campaign", event = "campaign.retry_failed", campaign_id = c.id, queued_run_id = q.id, error = %format!("{e:#}"), "campaign: retry bookkeeping failed");
            }
        }
        self.mark_not_connected(c, run_id, out.status).await;
    }

    /// Completes campaign `c` when nothing is left to do: no contact queued (a retry due later counts),
    /// none being handled, no call in flight. Dograh completes as soon as nothing is queued, even with
    /// calls still ringing, whose busy or no-answer would then find no campaign to retry in.
    async fn maybe_complete(&self, c: &Campaign) {
        if self.in_flight(c.id) > 0 {
            return;
        }
        let Ok((queued, processing)) = self.store.open_work(c.id).await else { return };
        // Re-checked: a call may have been placed since the count was taken.
        if queued > 0 || processing > 0 || self.in_flight(c.id) > 0 {
            return;
        }
        match self.store.complete_campaign(c.id).await {
            Ok(true) => {
                metrics::CAMPAIGNS_COMPLETED.fetch_add(1, Relaxed);
                self.log(c, "campaign.completed", "campaign completed");
            }
            Ok(false) => {}
            Err(e) => {
                tracing::error!(component = "campaign", event = "campaign.complete_failed", campaign_id = c.id, error = %format!("{e:#}"), "campaign: completing it failed")
            }
        }
    }

    /// Settles what a previous Vaani left behind, once, before dialing: contacts claimed but never dialed
    /// go back to the queue, contacts that were dialed stay processed, and runs of calls that died with it
    /// are completed. Assumes no other Vaani is dialing the same campaigns.
    async fn recover(&self) {
        match self.store.recover_contacts().await {
            Ok((requeued, processed)) if requeued + processed > 0 => {
                tracing::warn!(
                    component = "campaign",
                    event = "campaign.recovered",
                    requeued,
                    already_dialed = processed,
                    "campaign recovery: contacts a previous run left in progress"
                );
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(component = "campaign", event = "campaign.recover_failed", error = %format!("{e:#}"), "campaign recovery: contacts")
            }
        }
        self.sweep(true).await;
    }

    /// Completes the Dograh runs of calls nothing is running any more: a call that never reached the agent
    /// after the ring timeout, and -- right after a restart only -- calls the restart cut off.
    async fn sweep(&self, startup: bool) {
        let live: Vec<i64> = self.state.lock().unwrap().live_runs.iter().copied().collect();
        let before = Utc::now() - chrono::Duration::from_std(self.cfg.ring_timeout + Duration::from_secs(60)).unwrap_or_default();
        let stale = match self.store.stale_runs(before, startup, &live).await {
            Ok(s) => s,
            Err(e) => {
                return tracing::error!(component = "campaign", event = "campaign.sweep_failed", error = %format!("{e:#}"), "campaign sweep failed");
            }
        };
        for run in stale {
            let res = if run.state == "running" {
                self.store.mark_interrupted(run.id, run.org_id).await
            } else {
                self.store.mark_not_connected(run.id, run.org_id, STATUS_FAILED).await
            };
            match res {
                Ok(()) => {
                    tracing::warn!(component = "campaign", event = "campaign.swept", run_id = run.id, was = %run.state, "campaign sweep: completed a run nothing is running")
                }
                Err(e) => {
                    tracing::error!(component = "campaign", event = "campaign.sweep_failed", run_id = run.id, error = %format!("{e:#}"), "campaign sweep: completing a run failed")
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::CAMPAIGN_SCHEMA;
    use serde_json::json;

    type Placed = Arc<Mutex<Vec<(String, String, String, i32, i64, serde_json::Map<String, Value>, crate::ari::outbound::Done)>>>;

    /// Records every call it is asked to place and hands the test each call's `done`.
    #[derive(Clone, Default)]
    struct FakePlacer {
        placed: Placed,
        refuse: Arc<std::sync::atomic::AtomicBool>,
    }

    impl CallPlacer for FakePlacer {
        fn dial(&self, req: DialRequest) -> BoxFuture<'_, Result<String>> {
            Box::pin(async move {
                if self.refuse.load(Relaxed) {
                    anyhow::bail!("asterisk refused");
                }
                let n = self.placed.lock().unwrap().len();
                self.placed.lock().unwrap().push((
                    req.endpoint,
                    req.caller_id,
                    req.called,
                    req.workflow_id,
                    req.run_id,
                    req.variables,
                    req.done,
                ));
                Ok(format!("chan-{n}"))
            })
        }
    }

    impl FakePlacer {
        fn count(&self) -> usize {
            self.placed.lock().unwrap().len()
        }
        /// Ends the n-th placed call.
        fn end(&self, n: usize, out: Outcome) {
            let done = std::mem::replace(&mut self.placed.lock().unwrap()[n].6, Box::new(|_| {}));
            done(out);
        }
    }

    struct Rig {
        store: Store,
        placer: FakePlacer,
        cancel: CancellationToken,
        campaign: i32,
        task: tokio::task::JoinHandle<()>,
    }

    /// A runner over a real Postgres and Redis (`DOGRAH_TEST_DB_URL`, `REDIS_TEST_URL`); `None` when either is unset.
    /// `contacts` are phone numbers; `retry` and `meta` are the campaign's JSON settings; `extra` more config rows/SQL.
    async fn rig(schema: &str, contacts: &[&str], retry: &str, meta: &str, extra: &str, org_limit: Option<i32>) -> Option<Rig> {
        let redis = std::env::var("REDIS_TEST_URL").ok()?;
        // Ids no other run shares, so Redis counters from earlier runs can't interfere.
        let (org, campaign) = ((rand::random::<u32>() >> 8) as i32 + 1000, (rand::random::<u32>() >> 8) as i32 + 1000);
        let rows: String = contacts
            .iter()
            .enumerate()
            .map(|(i, p)| format!("({campaign}, 'src{i}', '{{\"phone_number\": \"{p}\", \"name\": \"C{i}\"}}'),"))
            .collect::<String>();
        let limit = org_limit.map(|l| format!("({org}, 'CONCURRENT_CALL_LIMIT', '{{\"value\": {l}}}'),")).unwrap_or_default();
        let sql = format!(
            "{CAMPAIGN_SCHEMA}
            INSERT INTO organization_configurations (organization_id, key, value) VALUES {limit}
              ({org}, 'MEDIA_ENGINE', '{{\"value\": \"vaani\"}}'), ({org}, 'DIAL_RATE_LIMIT_PER_SECOND', '{{\"value\": 100}}'),
              ({org}, 'TELEPHONY_CONFIGURATION', '{{\"provider\": \"ari\", \"outbound_trunk\": \"carrier\", \"from_numbers\": [\"0731111\"]}}');
            INSERT INTO workflows (id, user_id, organization_id) VALUES (42, 7, {org});
            INSERT INTO workflow_definitions (workflow_id, workflow_json, is_current) VALUES (42, '{{}}', true);
            INSERT INTO campaigns (id, organization_id, workflow_id, state, retry_config, orchestrator_metadata)
              VALUES ({campaign}, {org}, 42, 'running', '{retry}', '{meta}');
            INSERT INTO queued_runs (campaign_id, source_uuid, context_variables) VALUES {rows};
            {extra}"
        )
        .replace("),;", ");");
        let (store, _) = Store::for_test(schema, &sql).await?;
        let placer = FakePlacer::default();
        let cancel = CancellationToken::new();
        let lim = Limiter::connect(&redis, 0).await.unwrap();
        let cfg = RunnerConfig { poll: Duration::from_millis(50), default_org_limit: 2, ring_timeout: Duration::from_secs(30) };
        let runner = Runner::new(store.clone(), lim, Arc::new(placer.clone()), cfg, cancel.clone());
        let task = tokio::spawn(runner.run());
        Some(Rig { store, placer, cancel, campaign, task })
    }

    async fn wait_for(what: &str, cond: impl Fn() -> bool) {
        for _ in 0..300 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("timed out waiting for {what}");
    }

    async fn campaign_state(r: &Rig) -> String {
        let db = r.store.db().await.unwrap();
        db.query_one("SELECT state::text FROM campaigns WHERE id = $1", &[&r.campaign]).await.unwrap().get(0)
    }

    fn out(connected: bool, status: &'static str, cause: i64) -> Outcome {
        Outcome { connected, status, cause }
    }

    #[tokio::test]
    async fn a_campaign_is_dialed_under_the_limit_retried_and_completed() {
        let sql_retry = r#"{"max_retries": 1, "retry_delay_seconds": 1}"#;
        let Some(r) = rig("t_runner_flow", &["+911", "+912", "+913"], sql_retry, "{}", "", Some(2)).await else {
            return eprintln!("skipped: DOGRAH_TEST_DB_URL / REDIS_TEST_URL not set");
        };

        // The organization's limit is 2: two calls go out, the third waits for a slot.
        wait_for("two calls", || r.placer.count() == 2).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(r.placer.count(), 2, "a third call would exceed the organization's concurrent limit");
        {
            let placed = r.placer.placed.lock().unwrap();
            let (endpoint, caller, called, workflow, _, vars, _) = &placed[0];
            assert_eq!((endpoint.as_str(), caller.as_str(), called.as_str(), *workflow), ("PJSIP/+911@carrier", "0731111", "+911", 42));
            assert_eq!(vars["name"], "C0", "the contact's variables go to the agent");
        }
        let db = r.store.db().await.unwrap();
        let q = |sql: &'static str| async { db.query_one(sql, &[]).await.unwrap().get::<_, i64>(0) };
        assert_eq!(
            q("SELECT count(*) FROM workflow_runs WHERE call_type = 'outbound' AND state = 'initialized'").await,
            2,
            "the dialer creates each call's run before dialing"
        );
        assert_eq!(q("SELECT count(*) FROM queued_runs WHERE state::text = 'processed'").await, 2);

        // A call ends -> its slot frees -> the third goes out at once (no waiting for the next poll).
        r.placer.end(0, out(true, "", 0));
        wait_for("the third call", || r.placer.count() == 3).await;

        // Busy: retried once (due after the campaign's delay) and the run is completed as not connected.
        r.placer.end(1, out(false, "busy", 17));
        let run1: i64 = r.placer.placed.lock().unwrap()[1].4;
        wait_for("a retry to be scheduled", || futures_util::FutureExt::now_or_never(async { 0 }).is_some()).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        let g: Value = serde_json::from_str(
            &db.query_one("SELECT gathered_context::text FROM workflow_runs WHERE id = $1", &[&run1]).await.unwrap().get::<_, String>(0),
        )
        .unwrap();
        assert_eq!((g["call_disposition"].as_str(), &g["call_tags"]), (Some("BUSY"), &json!(["not_connected", "telephony_busy"])));
        r.placer.end(2, out(true, "", 0));
        wait_for("the retry to be dialed", || r.placer.count() == 4).await;
        {
            let placed = r.placer.placed.lock().unwrap();
            assert_eq!(
                (placed[3].2.as_str(), &placed[3].5["is_retry"], &placed[3].5["retry_reason"]),
                ("+912", &json!(true), &json!("busy"))
            );
        }
        assert_eq!(campaign_state(&r).await, "running", "a retry is still to be called: not complete");

        // No answer on the retry: retries used up -> failed_rows; then nothing is left, the campaign completes.
        r.placer.end(3, out(false, "no-answer", 19));
        for _ in 0..200 {
            if campaign_state(&r).await == "completed" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(campaign_state(&r).await, "completed");
        let row = db.query_one("SELECT processed_rows, failed_rows FROM campaigns WHERE id = $1", &[&r.campaign]).await.unwrap();
        assert_eq!(
            (row.get::<_, i32>(0), row.get::<_, i32>(1)),
            (4, 1),
            "4 calls placed; the contact that never answered counts as failed"
        );
        r.cancel.cancel();
        r.task.await.unwrap();
    }

    #[tokio::test]
    async fn failures_trip_the_breaker_and_pause_the_campaign() {
        let meta = r#"{"circuit_breaker": {"failure_threshold": 0.5, "window_seconds": 60, "min_calls_in_window": 2}}"#;
        let Some(r) = rig("t_runner_breaker", &["+911", "+912", "+913", "+914"], "{}", meta, "", Some(2)).await else {
            return eprintln!("skipped: DOGRAH_TEST_DB_URL / REDIS_TEST_URL not set");
        };
        wait_for("two calls", || r.placer.count() == 2).await;
        // Busy doesn't count against the breaker (as in Dograh); a failed or rejected call does.
        r.placer.end(0, out(false, "busy", 17));
        wait_for("a third call", || r.placer.count() == 3).await;
        r.placer.end(1, out(false, "rejected", 21));
        r.placer.end(2, out(false, "failed", 38));
        for _ in 0..200 {
            if campaign_state(&r).await == "paused" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(campaign_state(&r).await, "paused", "2 of 3 calls failed (>= 50%, at least 2 calls)");
        let dialed = r.placer.count();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(r.placer.count(), dialed, "a paused campaign dials no more");
        r.cancel.cancel();
        r.task.await.unwrap();
    }

    #[tokio::test]
    async fn pausing_in_dograhs_ui_and_the_schedule_stop_dialing() {
        // A schedule with no slot today: nothing is dialed, though the campaign is running.
        let none_today = r#"{"schedule_config": {"enabled": true, "timezone": "UTC", "slots": [{"day_of_week": 9, "start_time": "00:00", "end_time": "23:59"}]}}"#;
        let Some(r) = rig("t_runner_schedule", &["+911"], "{}", none_today, "", Some(5)).await else {
            return eprintln!("skipped: DOGRAH_TEST_DB_URL / REDIS_TEST_URL not set");
        };
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(r.placer.count(), 0, "outside the schedule window");
        assert_eq!(campaign_state(&r).await, "running");
        r.cancel.cancel();
        r.task.await.unwrap();

        // Paused by the user: the next call to be placed is not.
        let Some(r) = rig("t_runner_paused", &["+911", "+912", "+913"], "{}", "{}", "", Some(1)).await else { return };
        wait_for("the first call", || r.placer.count() == 1).await;
        let db = r.store.db().await.unwrap();
        db.execute("UPDATE campaigns SET state = 'paused'", &[]).await.unwrap();
        r.placer.end(0, out(true, "", 0));
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(r.placer.count(), 1, "Pause in the UI takes effect at the next call");
        r.cancel.cancel();
        r.task.await.unwrap();
    }

    #[tokio::test]
    async fn a_non_ari_provider_a_refused_dial_and_a_contact_without_a_number() {
        // The organization's telephony isn't ARI: the campaign is paused and the contact is given back, not lost.
        let extra = "UPDATE organization_configurations SET value = '{\"provider\": \"twilio\"}' WHERE key = 'TELEPHONY_CONFIGURATION';";
        let Some(r) = rig("t_runner_provider", &["+911"], "{}", "{}", extra, Some(5)).await else {
            return eprintln!("skipped: DOGRAH_TEST_DB_URL / REDIS_TEST_URL not set");
        };
        for _ in 0..200 {
            if campaign_state(&r).await == "paused" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(campaign_state(&r).await, "paused");
        let db = r.store.db().await.unwrap();
        assert_eq!(r.placer.count(), 0);
        assert_eq!(
            db.query_one("SELECT state::text FROM queued_runs", &[]).await.unwrap().get::<_, String>(0),
            "queued",
            "the contact goes back to the queue"
        );
        r.cancel.cancel();
        r.task.await.unwrap();

        // Asterisk refuses the dial: the run is completed as failed, the contact is failed, nothing is left ringing.
        let Some(r) = rig("t_runner_refused", &["+911", ""], "{}", "{}", "", Some(5)).await else { return };
        r.placer.refuse.store(true, Relaxed);
        let db = r.store.db().await.unwrap();
        for _ in 0..200 {
            if db.query_one("SELECT count(*) FROM queued_runs WHERE state::text = 'failed'", &[]).await.unwrap().get::<_, i64>(0) == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let rows = db.query("SELECT state::text FROM queued_runs ORDER BY id", &[]).await.unwrap();
        assert_eq!(
            rows.iter().map(|r| r.get::<_, String>(0)).collect::<Vec<_>>(),
            ["failed", "failed"],
            "a refused dial and a contact with no number both fail for good"
        );
        let run = db.query_one("SELECT is_completed, gathered_context::text FROM workflow_runs", &[]).await.unwrap();
        assert!(run.get::<_, bool>(0) && run.get::<_, String>(1).contains("telephony_failed"), "{}", run.get::<_, String>(1));
        assert_eq!(db.query_one("SELECT failed_rows FROM campaigns", &[]).await.unwrap().get::<_, i32>(0), 2);
        r.cancel.cancel();
        r.task.await.unwrap();
    }

    #[tokio::test]
    async fn a_restart_requeues_contacts_that_were_never_dialed() {
        let extra = "UPDATE queued_runs SET state = 'processing';";
        let Some(r) = rig("t_runner_recover", &["+911"], "{}", "{}", extra, Some(5)).await else {
            return eprintln!("skipped: DOGRAH_TEST_DB_URL / REDIS_TEST_URL not set");
        };
        wait_for("the recovered contact to be dialed", || r.placer.count() == 1).await;
        assert_eq!(r.placer.placed.lock().unwrap()[0].2, "+911");
        r.cancel.cancel();
        r.task.await.unwrap();
    }
}

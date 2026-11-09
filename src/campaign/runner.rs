//! The dialer: it claims contacts from Dograh's queue one at a time, each under the
//! organization's rate and concurrency limits, places the call, and decides retries
//! and the circuit breaker from how it ends. Dograh's UI starts, pauses and resumes
//! campaigns; the Runner only follows their state.

use super::limiter::{Limiter, Slot};
use crate::{
    ari::outbound::{DialRequest, Outcome},
    dograh::{
        Store,
        campaign::{Campaign, DialCheck, QueuedRun, RunState},
        outbound::{DialSettings, STATUS_BUSY, STATUS_FAILED, STATUS_NO_ANSWER, STATUS_REJECTED},
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

/// A reading the dialer takes before each dial (the CPU guard, the setup backlog).
pub type Probe<T> = Arc<dyn Fn() -> T + Send + Sync>;

#[derive(Clone)]
pub struct RunnerConfig {
    /// How often Dograh's database is scanned for campaigns to dial.
    pub poll: Duration,
    /// The concurrent-call limit of an organization that has none set (Dograh's
    /// DEFAULT_ORG_CONCURRENCY_LIMIT, 2).
    pub default_org_limit: i32,
    pub ring_timeout: Duration,
    /// How many contacts of one campaign are dialed at the same time (CAMPAIGN_DIAL_WORKERS); 0 means 1.
    /// Dialing one contact takes ~10 round trips to Postgres, Redis and Asterisk, so one worker tops out
    /// near 5 calls a second whatever the dial rate; the organization's rate and concurrency limits still
    /// decide the real speed. At most twice this many workers run across all campaigns, so they can't
    /// take the whole database pool from the calls being set up.
    pub workers: usize,
    /// Says the machine is too busy for another call: the dialer then places none and claims no
    /// contact (LOAD_SHED_CPU_PERCENT).
    pub overloaded: Option<Probe<bool>>,
    /// How many answered calls are staged and not yet bridged, and the most the dialer lets pile up
    /// (OUTBOUND_MAX_SETUP; 0 = no cap): it places no call while that many wait, so it never dials
    /// faster than Vaani and Asterisk can set calls up.
    pub setting_up: Option<Probe<usize>>,
    pub max_setting_up: usize,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            poll: Duration::from_secs(5),
            default_org_limit: 2,
            ring_timeout: Duration::from_secs(30),
            workers: 1,
            overloaded: None,
            setting_up: None,
            max_setting_up: 0,
        }
    }
}

/// How many calls per second campaign `c`'s organization may start: its DIAL_RATE_LIMIT_PER_SECOND,
/// which overrides the campaign's own rate_limit_per_second, or that rate when the organization has none set.
fn dial_rate(c: &Campaign, chk: &DialCheck) -> i32 {
    chk.dial_rate.map_or(c.rate_per_second, |r| r.max(1))
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
    /// Dial workers running, per campaign, and in all.
    workers: HashMap<i32, usize>,
    total_workers: usize,
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
    tracker: tokio_util::task::TaskTracker,
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
        Arc::new(Self { store, lim, placer, cfg, cancel, state: Mutex::default(), wake: Notify::new(), tracker: Default::default() })
    }

    /// Scans for campaigns until the runner's token is cancelled, then waits for the dialing tasks to stop.
    /// Calls already placed carry on (the ARI manager ends them at shutdown).
    pub async fn run(self: Arc<Self>) {
        self.recover().await;
        let (mut poll, mut sweep) = (tokio::time::interval(self.cfg.poll), tokio::time::interval(Duration::from_secs(60)));
        poll.tick().await;
        sweep.tick().await;
        loop {
            self.scan().await;
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = poll.tick() => {}
                _ = self.wake.notified() => {}
                _ = sweep.tick() => self.sweep(false).await,
            }
        }
        self.tracker.close();
        self.tracker.wait().await;
    }

    /// Makes the next scan happen now (a call ended, so a slot is free).
    fn wake(&self) {
        self.wake.notify_one();
    }

    async fn scan(self: &Arc<Self>) {
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
            self.start_worker(&c, false);
        }
    }

    /// Starts a dial worker for campaign `c`, unless it has its share already. A scan starts one; each
    /// worker that dials a contact starts the next (`grow`), up to `workers`, so a campaign with no room
    /// (a full slot limit) costs one failed attempt per scan, not `workers` of them.
    fn start_worker(self: &Arc<Self>, c: &Campaign, grow: bool) {
        let workers = self.cfg.workers.max(1);
        {
            let mut st = self.state.lock().unwrap();
            let n = st.workers.get(&c.id).copied().unwrap_or(0);
            if n >= workers || st.total_workers >= 2 * workers || (!grow && n > 0) {
                return;
            }
            *st.workers.entry(c.id).or_default() += 1;
            st.total_workers += 1;
        }
        let (me, c) = (self.clone(), c.clone());
        self.tracker.spawn(async move {
            me.drain(&c).await;
            let mut st = me.state.lock().unwrap();
            if let Some(n) = st.workers.get_mut(&c.id) {
                *n -= 1;
                if *n == 0 {
                    st.workers.remove(&c.id);
                }
            }
            st.total_workers -= 1;
        });
    }

    /// Dials campaign `c`'s contacts until it runs out of work or room.
    async fn drain(self: &Arc<Self>, c: &Campaign) {
        while !self.cancel.is_cancelled() {
            match self.dial_one(c).await {
                Step::Dialed => self.start_worker(c, true),
                Step::Wait(d) => {
                    // Workers waiting for the same token don't all ask again at once.
                    let d = d.clamp(Duration::from_millis(10), Duration::from_secs(1));
                    let d = d + Duration::from_micros(rand::random_range(0..=(d.as_micros() as u64 / 4)));
                    tokio::select! {
                        _ = tokio::time::sleep(d) => {}
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
    fn org_limit(&self, c: &Campaign, chk: &DialCheck) -> i32 {
        let limit = chk.concurrent_max.unwrap_or(self.cfg.default_org_limit);
        if c.max_concurrency > 0 && c.max_concurrency < limit { c.max_concurrency } else { limit }
    }

    /// Takes the next contact of campaign `c` and places the call, if the campaign is running, inside its
    /// window, healthy, paid for, and under the organization's rate and concurrency limits.
    async fn dial_one(self: &Arc<Self>, c: &Campaign) -> Step {
        // Before anything is read, taken or claimed: nothing to give back, and the campaign carries on by
        // itself once the CPU is back under the limit or the calls waiting in setup are bridged.
        if self.cfg.overloaded.as_ref().is_some_and(|f| f()) {
            metrics::call_rejected("cpu", "outbound");
            return Step::Wait(Duration::from_secs(1));
        }
        if self.cfg.max_setting_up > 0 && self.cfg.setting_up.as_ref().is_some_and(|f| f() >= self.cfg.max_setting_up) {
            metrics::call_rejected("setup", "outbound");
            return Step::Wait(Duration::from_millis(200));
        }

        // The database checks and the breaker (Redis) don't depend on each other: one round trip each, at once.
        let (chk, breaker) = tokio::join!(self.store.dial_check(c.id, c.organization_id), self.lim.breaker_open(c.id, &c.breaker));
        let Ok(chk) = chk else { return Step::Stop };
        if !chk.running || !c.schedule.allows(Utc::now()) {
            return Step::Stop;
        }
        if let Ok((true, stats)) = breaker {
            self.pause(c, "breaker", &format!("circuit breaker: {} of {} calls failed", stats.failures, stats.total())).await;
            return Step::Stop;
        }
        if chk.wallet_blocks {
            self.pause(c, "wallet", "the organization's wallet is out of credit").await;
            return Step::Stop;
        }
        // Checked before a contact is claimed, so there is nothing to give back.
        let Some(settings) = chk.settings.clone() else {
            tracing::error!(component = "campaign", event = "campaign.place_failed", campaign_id = c.id,
                error = %format!("organization {} has no telephony configuration", c.organization_id), "campaign: the organization's telephony settings are unreadable");
            return Step::Stop;
        };
        if settings.provider != "ari" {
            self.pause(c, "provider", &format!("the organization's telephony provider is {:?}, not ari", settings.provider)).await;
            tracing::error!(component = "campaign", event = "campaign.place_failed", campaign_id = c.id,
                error = %format!("provider {:?}", settings.provider), "campaign: cannot dial over ARI");
            return Step::Stop;
        }
        let (limit, rate) = (self.org_limit(c, &chk), dial_rate(c, &chk));
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
            Ok(mut runs) if !runs.is_empty() => self.place(c, runs.remove(0), settings, slot).await,
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
    async fn place(self: &Arc<Self>, c: &Campaign, q: QueuedRun, settings: DialSettings, slot: Slot) -> Step {
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
        /// How long placing a call takes.
        delay: Duration,
        /// Placements going on now, and at most at once.
        cur: Arc<std::sync::atomic::AtomicUsize>,
        peak: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl CallPlacer for FakePlacer {
        fn dial(&self, req: DialRequest) -> BoxFuture<'_, Result<String>> {
            Box::pin(async move {
                let now = self.cur.fetch_add(1, Relaxed) + 1;
                self.peak.fetch_max(now, Relaxed);
                tokio::time::sleep(self.delay).await;
                self.cur.fetch_sub(1, Relaxed);
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
        rig_with(schema, contacts, retry, meta, extra, org_limit, Duration::ZERO, |_| {}).await
    }

    /// `rig` with the placer's delay and a say in the runner's config.
    #[allow(clippy::too_many_arguments)]
    async fn rig_with(
        schema: &str,
        contacts: &[&str],
        retry: &str,
        meta: &str,
        extra: &str,
        org_limit: Option<i32>,
        delay: Duration,
        tweak: impl FnOnce(&mut RunnerConfig),
    ) -> Option<Rig> {
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
        let placer = FakePlacer { delay, ..Default::default() };
        let cancel = CancellationToken::new();
        let lim = Limiter::connect(&redis, 0).await.unwrap();
        let mut cfg = RunnerConfig {
            poll: Duration::from_millis(50),
            default_org_limit: 2,
            ring_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        tweak(&mut cfg);
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

    fn many(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("+9{i:05}")).collect()
    }

    async fn queued(r: &Rig) -> i64 {
        let db = r.store.db().await.unwrap();
        db.query_one("SELECT count(*) FROM queued_runs WHERE state::text = 'queued'", &[]).await.unwrap().get(0)
    }

    #[tokio::test]
    async fn workers_dial_every_contact_exactly_once() {
        let nums = many(100);
        let refs: Vec<&str> = nums.iter().map(String::as_str).collect();
        let Some(r) = rig_with("t_runner_workers", &refs, "{}", "{}", "", Some(100), Duration::from_millis(20), |c| c.workers = 10).await
        else {
            return eprintln!("skipped: DOGRAH_TEST_DB_URL / REDIS_TEST_URL not set");
        };
        wait_for("100 calls", || r.placer.count() == 100).await;
        let run_ids: HashSet<i64> = r.placer.placed.lock().unwrap().iter().map(|p| p.4).collect();
        assert_eq!(run_ids.len(), 100, "every call has its own run: no contact was dialed twice");
        let db = r.store.db().await.unwrap();
        let processed: i64 = db.query_one("SELECT count(*) FROM queued_runs WHERE state::text = 'processed'", &[]).await.unwrap().get(0);
        assert_eq!(processed, 100);
        assert_eq!(r.placer.peak.load(Relaxed), 10, "ramped up to the 10 workers, and no further");
        r.cancel.cancel();
        r.task.await.unwrap();
    }

    #[tokio::test]
    async fn workers_stay_under_the_org_limit_and_rate() {
        let nums = many(100);
        let refs: Vec<&str> = nums.iter().map(String::as_str).collect();
        let Some(r) =
            rig_with("t_runner_workers_limit", &refs, "{}", "{}", "", Some(30), Duration::from_millis(5), |c| c.workers = 10).await
        else {
            return eprintln!("skipped: DOGRAH_TEST_DB_URL / REDIS_TEST_URL not set");
        };
        wait_for("30 calls", || r.placer.count() == 30).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(r.placer.count(), 30, "30 slots, however many workers");
        assert_eq!(queued(&r).await, 70, "a contact is claimed only with a slot in hand");
        r.cancel.cancel();
        r.task.await.unwrap();

        // The organization's dial rate is shared by the workers: 5 a second in all, not per worker.
        let rate = "UPDATE organization_configurations SET value = '{\"value\": 5}' WHERE key = 'DIAL_RATE_LIMIT_PER_SECOND';";
        let Some(r) =
            rig_with("t_runner_workers_rate", &refs, "{}", "{}", rate, Some(100), Duration::from_millis(5), |c| c.workers = 10).await
        else {
            return;
        };
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(r.placer.count(), 5, "within one rate window");
        r.cancel.cancel();
        r.task.await.unwrap();
    }

    #[tokio::test]
    async fn a_pause_stops_every_worker() {
        let nums = many(200);
        let refs: Vec<&str> = nums.iter().map(String::as_str).collect();
        let Some(r) =
            rig_with("t_runner_workers_pause", &refs, "{}", "{}", "", Some(200), Duration::from_millis(20), |c| c.workers = 10).await
        else {
            return eprintln!("skipped: DOGRAH_TEST_DB_URL / REDIS_TEST_URL not set");
        };
        wait_for("5 calls", || r.placer.count() >= 5).await;
        assert!(r.store.set_campaign_state(r.campaign, "running", "paused").await.unwrap());
        tokio::time::sleep(Duration::from_millis(300)).await;
        let n = r.placer.count();
        assert!(n < 40, "the 10 workers finish the contact in hand, no more: {n}");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(r.placer.count(), n, "nothing more is dialed");
        r.cancel.cancel();
        r.task.await.unwrap();
    }

    #[tokio::test]
    async fn nothing_is_dialed_or_claimed_while_overloaded_or_while_calls_pile_up_in_setup() {
        let over = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let setting = Arc::new(std::sync::atomic::AtomicUsize::new(30));
        let (o, s2) = (over.clone(), setting.clone());
        let Some(r) = rig_with("t_runner_guard", &["+911", "+912", "+913"], "{}", "{}", "", Some(2), Duration::ZERO, move |c| {
            c.overloaded = Some(Arc::new(move || o.load(Relaxed)));
            c.setting_up = Some(Arc::new(move || s2.load(Relaxed)));
            c.max_setting_up = 30;
        })
        .await
        else {
            return eprintln!("skipped: DOGRAH_TEST_DB_URL / REDIS_TEST_URL not set");
        };
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(r.placer.count(), 0, "over the CPU limit");
        assert_eq!(queued(&r).await, 3, "no contact claimed");
        assert_eq!(campaign_state(&r).await, "running", "not a reason to pause the campaign");

        over.store(false, Relaxed);
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(r.placer.count(), 0, "30 answered calls are still waiting in setup");
        assert_eq!(queued(&r).await, 3);

        setting.store(29, Relaxed);
        wait_for("dialing resumes (org limit 2)", || r.placer.count() == 2).await;
        r.cancel.cancel();
        r.task.await.unwrap();
    }

    #[tokio::test]
    async fn an_organization_without_telephony_settings_keeps_its_contacts() {
        let gone = "DELETE FROM organization_configurations WHERE key = 'TELEPHONY_CONFIGURATION';";
        let Some(r) = rig("t_runner_no_telephony", &["+911", "+912"], "{}", "{}", gone, Some(2)).await else {
            return eprintln!("skipped: DOGRAH_TEST_DB_URL / REDIS_TEST_URL not set");
        };
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(r.placer.count(), 0);
        assert_eq!(queued(&r).await, 2, "checked before a contact is claimed");
        assert_eq!(campaign_state(&r).await, "running", "an error to fix, not a reason to pause");
        r.cancel.cancel();
        r.task.await.unwrap();
    }
}

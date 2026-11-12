//! The Stasis event loop: answers a call, wires it to an AudioSocket
//! externalMedia channel and a bridge, runs the agent for its duration, and
//! tears everything down on hangup.
//!
//! Per-call flow:
//! 1. `StasisStart` (caller) -> answer; take a port from the pool; BIND and
//!    LISTEN on it (invariant #9: before Asterisk is told the address -- an
//!    address with nothing listening broke production once); create the
//!    externalMedia channel pointed at `MEDIA_IP:port`.
//! 2. That channel's own `StasisStart` -> create a mixing bridge, add both.
//! 3. Accept Asterisk's TCP connection while loading the Dograh workflow and
//!    opening the STT; run the agent until the call ends.
//! 4. `StasisEnd` on either leg -> teardown (idempotent: resources are taken
//!    exactly once, so a double StasisEnd can't double-free a port).
//!
//! Every ARI-facing step runs off the event loop in its own task, so a slow
//! REST call or database read for one call never stalls another's events.

use super::{
    Ari, Event,
    outbound::{OUTBOUND_APP_ARG, OutboundCall},
    uuid_v4,
};
use crate::{
    agent::{
        calllog, extract,
        handler::{self, ConnectFn, TransferFn},
    },
    callagent::{self, CallInfo},
    campaign::runner::Probe,
    config::Config,
    dograh::{
        Store,
        run::{RunCall, RunEnd, RunMeta},
    },
    media::{call as media_call, ports::PortAllocator},
    metrics,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock, atomic::Ordering::Relaxed},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, mpsc},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

/// The first Stasis argument on a transfer destination's channel, so `on_event` can tell it
/// apart from a new inbound call. (Dograh tags its ARI transfer legs the same way.)
const TRANSFER_APP_ARG: &str = "transfer";
/// Added to the ring timeout for our own wait, so Asterisk's timeout (which hangs the
/// ringing channel up and yields a clear ChannelDestroyed cause) normally fires first.
const TRANSFER_ANSWER_SLACK: Duration = Duration::from_secs(5);

/// How long to wait for Asterisk to open the AudioSocket TCP connection after the bridge is up.
const ACCEPT_TIMEOUT: Duration = Duration::from_secs(10);

/// The call setups (answer, externalMedia, bridge) running against Asterisk at once; a burst waits its
/// turn here, in Vaani, instead of piling onto Asterisk.
const MAX_CONCURRENT_SETUPS: usize = 16;

#[derive(Default)]
struct Res {
    port: Option<u16>,
    listener: Option<TcpListener>,
    bridge: Option<String>,
    external_requested: bool,
    counted_active: bool,
    ended: bool,
    /// The transfer destination's channel once a transfer has connected: from then
    /// on the call is caller <-> destination, and teardown hangs that channel up too.
    transferred: Option<String>,
}

struct Call {
    info: CallInfo,
    external_id: String,
    cancel: CancellationToken,
    /// Set for a call Vaani dialed.
    out: Option<Arc<OutboundCall>>,
    /// Scopes the media plane and the agent (a child of `cancel`). A transfer
    /// cancels it to take the agent out of a call that carries on without it.
    media_cancel: CancellationToken,
    /// Cancelled when the externalMedia channel enters Stasis: the setup timeout stops waiting for it.
    arrived: CancellationToken,
    /// When the call was staged (answered): `vaani_call_duration_seconds` runs from here to teardown.
    started: std::time::Instant,
    res: Mutex<Res>,
}

/// What a finished call hands to `record_run`.
struct CallEnd {
    meta: RunMeta,
    run: tokio::task::JoinHandle<anyhow::Result<i64>>,
    summary: calllog::Summary,
    finisher: extract::Finisher,
    duration: Duration,
}

/// The call's one summary line: what to chart and alert on (duration, turns,
/// reply latency, interruptions, errors, outcome).
fn log_summary(id: &str, duration: Duration, s: &calllog::Summary, stats: &media_call::MediaStats) {
    let (p50, p95) = percentiles(&s.reply_latencies_ms);
    tracing::info!(call_id = %id, component = "agent", event = "call.summary", duration_s = duration.as_secs_f64().round() as u64,
        disposition = s.end_reason.unwrap_or(calllog::USER_HANGUP), user_turns = s.user_turns, agent_replies = s.agent_replies,
        bargeins = s.barge_ins, pauses = s.pauses, false_interruptions = s.false_interruptions,
        held_speech = s.held_speech, held_delivered = s.held_delivered, nodes = ?s.nodes_visited,
        recording_bytes = s.recording.pcm_len(), stt_audio_s = (s.stt_seconds * 10.0).round() / 10.0,
        reply_latency_p50_ms = p50, reply_latency_p95_ms = p95, errors = ?s.errors,
        packets_in = stats.packets_in.load(Relaxed), packets_out = stats.packets_out.load(Relaxed),
        silence_out = stats.silence_out.load(Relaxed), inbound_dropped = stats.inbound_dropped.load(Relaxed), "call summary");
}

/// 50th and 95th percentile (nearest rank) of `v`, `None` when empty.
fn percentiles(v: &[u64]) -> (Option<u64>, Option<u64>) {
    if v.is_empty() {
        return (None, None);
    }
    let mut s = v.to_vec();
    s.sort_unstable();
    let rank = |p: f64| s[((p * s.len() as f64).ceil() as usize).saturating_sub(1).min(s.len() - 1)];
    (Some(rank(0.50)), Some(rank(0.95)))
}

pub struct Manager {
    pub(super) ari: Ari,
    cfg: Arc<Config>,
    ports: PortAllocator,
    store: Store,
    /// Keyed by BOTH channel ids, so a hangup of either leg finds the call.
    calls: Mutex<HashMap<String, Arc<Call>>>,
    /// Transfer destinations that are ringing, by channel id.
    transfers: Mutex<HashMap<String, mpsc::Sender<TransferSignal>>>,
    /// Calls Vaani dialed, by channel id, from dialing until over.
    pub(super) outbound: Mutex<HashMap<String, Arc<OutboundCall>>>,
    pub(super) tracker: TaskTracker,
    /// Says the machine is too busy for another call (the CPU guard); set once, before `run`.
    overloaded: OnceLock<Probe<bool>>,
    setup_sem: Arc<Semaphore>,
    /// Ended calls finishing their Dograh record at once (`POST_CALL_WORKERS`).
    post_call_sem: Arc<Semaphore>,
}

/// What happened to a ringing transfer destination.
enum TransferSignal {
    Answered,
    /// Destroyed without answering: busy, rejected, no answer (the hangup cause).
    Failed(String),
}

impl Manager {
    pub fn new(ari: Ari, cfg: Arc<Config>, store: Store) -> Arc<Self> {
        let ports = PortAllocator::new(cfg.media_port_base, cfg.media_port_count);
        let cfg_post_call_workers = cfg.post_call_workers;
        Arc::new(Self {
            ari,
            cfg,
            ports,
            store,
            calls: Mutex::new(HashMap::new()),
            transfers: Mutex::new(HashMap::new()),
            outbound: Mutex::new(HashMap::new()),
            tracker: TaskTracker::new(),
            overloaded: OnceLock::new(),
            setup_sem: Arc::new(Semaphore::new(MAX_CONCURRENT_SETUPS)),
            post_call_sem: Arc::new(Semaphore::new(cfg_post_call_workers)),
        })
    }

    /// Refuses new inbound calls while `f` says the machine is overloaded (the CPU guard).
    pub fn set_overloaded(&self, f: Probe<bool>) {
        let _ = self.overloaded.set(f);
    }

    /// How many answered calls are staged and not yet bridged.
    pub fn setting_up(&self) -> usize {
        let calls = self.calls.lock().unwrap();
        // Each call is in the map under both its channel ids: count it once, under its caller's.
        calls
            .iter()
            .filter(|(id, c)| {
                **id == c.info.id && {
                    let r = c.res.lock().unwrap();
                    !r.ended && !r.counted_active
                }
            })
            .count()
    }

    /// Whether Vaani knows channel `id`: a call being set up or bridged (either leg, or a transfer
    /// destination), a transfer ringing, or a call it dialed.
    pub(super) fn tracks(&self, id: &str) -> bool {
        self.calls.lock().unwrap().contains_key(id)
            || self.transfers.lock().unwrap().contains_key(id)
            || self.outbound.lock().unwrap().contains_key(id)
    }

    /// Every `ORPHAN_SWEEP_SECONDS`, hangs up the channels of this ARI app that Vaani lost track of.
    async fn sweep_loop(self: Arc<Self>, cancel: CancellationToken) {
        let mut tick = tokio::time::interval(self.cfg.orphan_sweep);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await;
        loop {
            tokio::select! { _ = cancel.cancelled() => return, _ = tick.tick() => {} }
            if !metrics::ARI_CONNECTED.load(Relaxed) {
                continue;
            }
            if let Err(e) = super::sweep::sweep_once(&self.ari, &self.cfg.ari_app, chrono::Utc::now(), |id| self.tracks(id)).await {
                tracing::warn!(event = "channel.sweep_failed", error = %format!("{e:#}"), "orphan channel sweep failed");
            }
        }
    }

    /// Dispatches events until `cancel`, then tears every call down and waits
    /// (bounded) for the calls' cleanup.
    pub async fn run(self: Arc<Self>, mut events: mpsc::UnboundedReceiver<Event>, cancel: CancellationToken) {
        if !self.cfg.orphan_sweep.is_zero() {
            self.tracker.spawn(self.clone().sweep_loop(cancel.clone()));
        }
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                ev = events.recv() => match ev {
                    Some(ev) => {
                        metrics::ARI_EVENT_QUEUE.store(events.len() as u64, Relaxed);
                        self.on_event(ev);
                        metrics::CALLS_SETTING_UP.store(self.setting_up() as u64, Relaxed);
                    }
                    None => break,
                },
            }
        }
        self.hangup_ringing_outbound();
        self.teardown_all();
        self.tracker.close();
        // Calls ending now are still writing their Dograh records (recordings upload, costs): wait for them, as
        // long as SHUTDOWN_WAIT_SECONDS allows, and no longer than they need.
        let wait = self.cfg.shutdown_wait;
        if tokio::time::timeout(wait, self.tracker.wait()).await.is_err() {
            tracing::warn!(
                event = "shutdown.incomplete",
                waited_s = wait.as_secs(),
                records_running = metrics::POST_CALL_RUNNING.load(Relaxed),
                records_waiting = metrics::POST_CALL_WAITING.load(Relaxed),
                "shutdown: calls and records still being written after the wait; giving up on them"
            );
        }
    }

    fn on_event(self: &Arc<Self>, ev: Event) {
        match ev {
            Event::StasisStart { channel, args } => {
                // A transfer destination we dialed has answered; it enters this same Stasis
                // app, so it must not be mistaken for a new inbound call.
                if args.first().is_some_and(|a| a == TRANSFER_APP_ARG) {
                    return self.on_transfer_answered(&channel.id);
                }
                // A call we dialed has been answered.
                if args.first().is_some_and(|a| a == OUTBOUND_APP_ARG) {
                    return self.on_outbound_answered(channel);
                }
                let found = self.calls.lock().unwrap().get(&channel.id).cloned();
                match found {
                    // The externalMedia leg arriving: complete the bridge.
                    Some(call) if call.external_id == channel.id => {
                        call.arrived.cancel();
                        let m = self.clone();
                        self.tracker.spawn(async move { m.complete_bridge(call).await });
                    }
                    Some(_) => {} // the caller leg again; nothing to do
                    // A media channel of ours that no call is waiting for (its call was already torn down or
                    // timed out) is not a new caller: hang it up.
                    None if channel.name.starts_with("AudioSocket/") => {
                        tracing::warn!(event = "call.media_orphan", channel_id = %channel.id, name = %channel.name,
                            "media channel entered Stasis but its call is gone; hanging it up");
                        let (m, id) = (self.clone(), channel.id);
                        self.tracker.spawn(async move {
                            let _ = m.ari.hangup(&id).await;
                        });
                    }
                    None => self.start_call(channel),
                }
            }
            Event::StasisEnd { channel } => {
                let call = self.calls.lock().unwrap().get(&channel.id).cloned();
                if let Some(call) = call {
                    let leg = if channel.id == call.info.id { "caller" } else { "externalMedia" };
                    tracing::info!(call_id = %call.info.id, event = "call.hangup", leg, channel_id = %channel.id, "call ended by leg");
                    self.teardown(&call);
                }
            }
            // StasisEnd alone doesn't say WHY a channel left; the hangup cause
            // tells a normal clearing (16) from carrier drops and Asterisk-side kicks.
            Event::ChannelDestroyed { channel, cause, cause_txt } => {
                let call_id = self.calls.lock().unwrap().get(&channel.id).map(|c| c.info.id.clone()).unwrap_or_else(|| channel.id.clone());
                tracing::info!(call_id = %call_id, event = "channel.destroyed", channel_id = %channel.id, cause, cause_txt, "channel destroyed");
                // A ringing transfer destination that went away without answering.
                if let Some(tx) = self.transfers.lock().unwrap().get(&channel.id) {
                    let _ = tx.try_send(TransferSignal::Failed(format!("{cause_txt}, cause {cause}")));
                }
                self.on_outbound_destroyed(&channel.id, cause, &cause_txt);
            }
            // Events during the gap, StasisEnd above all, are unrecoverable: any
            // call spanning it may be over without us knowing -- a billable
            // channel and a leaked port. Tearing down is the conservative
            // recovery (a blip sacrifices healthy calls too).
            Event::Reconnected => {
                if !self.calls.lock().unwrap().is_empty() {
                    tracing::warn!("ari websocket reconnected after a drop; tearing down all calls (events during the gap were lost)");
                    self.teardown_all();
                }
            }
        }
    }

    pub(super) fn start_call(self: &Arc<Self>, channel: super::Channel) {
        // A call we dialed is already answered (it only enters Stasis on answer).
        let out = self.outbound_for(&channel.id);
        // Over the CPU limit, a new caller is refused before it is answered, as congestion (SIP 503): the
        // trunk can try another server, and the calls already up keep their CPU. Calls we placed ourselves
        // are not refused here (the dialer stops placing them while overloaded).
        if out.is_none() && self.overloaded.get().is_some_and(|f| f()) {
            metrics::call_rejected("cpu", "inbound");
            tracing::warn!(call_id = %channel.id, event = "call.rejected", reason = "cpu", "call refused: cpu over the limit");
            let (m, id) = (self.clone(), channel.id);
            self.tracker.spawn(async move {
                if let Err(e) = m.ari.hangup_reason(&id, "congestion").await {
                    tracing::debug!(call_id = %id, error = %e, "hangup failed");
                }
            });
            return;
        }
        let external_id = format!("vaani-ext-{}", &uuid_v4()[..13]);
        let cancel = CancellationToken::new();
        let call = Arc::new(Call {
            info: match &out {
                // The caller ID we showed, and the number we dialed.
                Some(o) => CallInfo {
                    id: channel.id.clone(),
                    caller_number: o.caller_id.clone(),
                    called_number: o.called.clone(),
                    direction: "outbound",
                    workflow_id: Some(o.workflow_id),
                    run_id: Some(o.run_id),
                    variables: o.variables.clone(),
                },
                None => CallInfo {
                    id: channel.id.clone(),
                    caller_number: channel.caller_number(),
                    called_number: channel.called_number(),
                    direction: "inbound",
                    workflow_id: None,
                    run_id: None,
                    variables: Default::default(),
                },
            },
            out,
            external_id: external_id.clone(),
            media_cancel: cancel.child_token(),
            cancel,
            arrived: CancellationToken::new(),
            started: std::time::Instant::now(),
            res: Mutex::new(Res::default()),
        });
        {
            // Registered under both ids before anything else runs, so a hangup
            // during setup can always find (and clean up) the call.
            let mut calls = self.calls.lock().unwrap();
            calls.insert(channel.id.clone(), call.clone());
            calls.insert(external_id, call.clone());
        }

        if self.cfg.max_call_duration > Duration::ZERO {
            // A backstop against a wedged call burning billable telephony time.
            let (m, c) = (self.clone(), call.clone());
            self.tracker.spawn(async move {
                tokio::select! {
                    _ = c.cancel.cancelled() => {}
                    _ = tokio::time::sleep(m.cfg.max_call_duration) => {
                        tracing::warn!(call_id = %c.info.id, event = "call.max_duration_backstop", "max call duration reached; hanging up");
                        m.teardown(&c);
                    }
                }
            });
        }

        // The call's trace (a no-op unless OTLP export is on): every line about it carries its trace id.
        crate::telemetry::start_call(&call.info.id, &[("direction", call.info.direction.to_string())], call.info.run_id);

        // If the media channel never enters Stasis (its event lost, Asterisk wedged) nothing else would
        // ever end this call: hang it up after the setup timeout.
        {
            let (m, c) = (self.clone(), call.clone());
            self.tracker.spawn(async move {
                tokio::select! {
                    _ = c.cancel.cancelled() => {}
                    _ = c.arrived.cancelled() => {}
                    _ = tokio::time::sleep(m.cfg.setup_timeout) => {
                        tracing::warn!(call_id = %c.info.id, external_id = %c.external_id, event = "call.setup_timeout", after_s = m.cfg.setup_timeout.as_secs(),
                            "call setup timed out: its media channel never entered Stasis; hanging it up");
                        metrics::CALLS_SETUP_TIMEOUT.fetch_add(1, Relaxed);
                        m.teardown(&c);
                    }
                }
            });
        }

        let m = self.clone();
        self.tracker.spawn(async move {
            if let Err(e) = m.setup(&call).await {
                tracing::error!(call_id = %call.info.id, event = "call.setup_failed", error = %format!("{e:#}"), "call setup failed");
                m.teardown(&call);
            } else if call.cancel.is_cancelled() {
                m.teardown(&call); // hung up while we were setting up: free what setup created since
            }
        });
    }

    async fn setup(&self, call: &Arc<Call>) -> anyhow::Result<()> {
        use anyhow::Context;
        let id = &call.info.id;
        // Only so many setups run against Asterisk at once; a burst waits here.
        let _permit = self.setup_sem.acquire().await.context("setup limiter closed")?;
        if call.out.is_none() {
            self.ari.answer(id).await.context("answer")?;
        }

        // Listen BEFORE telling Asterisk where to connect (invariant #9). A port whose previous call is
        // still closing it is busy: put it back at the end of the line and take the next one.
        let (port, listener) = {
            let mut tries = 0;
            loop {
                let port = self.ports.alloc().context("no free media port")?;
                metrics::MEDIA_PORTS_IN_USE.store(self.ports.in_use() as u64, Relaxed);
                match TcpListener::bind(("0.0.0.0", port)).await {
                    Ok(l) => break (port, l),
                    Err(e) if e.kind() == std::io::ErrorKind::AddrInUse && tries < 2 => {
                        tries += 1;
                        tracing::warn!(call_id = %id, port, event = "media.port_busy", "media port still in use; trying the next one");
                        self.ports.free(port);
                    }
                    Err(e) => {
                        call.res.lock().unwrap().port = Some(port); // teardown frees it
                        return Err(e).with_context(|| format!("bind media port {port}"));
                    }
                }
            }
        };
        call.res.lock().unwrap().port = Some(port);
        call.res.lock().unwrap().listener = Some(listener);

        call.res.lock().unwrap().external_requested = true;
        self.ari
            .external_media_audiosocket(&call.external_id, &format!("{}:{port}", self.cfg.media_ip), &uuid_v4())
            .await
            .context("externalMedia")?;

        tracing::info!(call_id = %id, event = "call.answered", external_id = %call.external_id, port,
            caller_number = %call.info.caller_number, called_number = %call.info.called_number, direction = call.info.direction,
            "call answered, media socket listening, externalMedia staged");
        Ok(())
    }

    /// The externalMedia channel entered Stasis: bridge the pair and run the call.
    async fn complete_bridge(self: Arc<Self>, call: Arc<Call>) {
        let id = &call.info.id;
        let bridge = format!("vaani-br-{}", &uuid_v4()[..13]);
        let res = async {
            use anyhow::Context;
            let _permit = self.setup_sem.acquire().await.context("setup limiter closed")?;
            self.ari.create_bridge(&bridge).await.context("create bridge")?;
            call.res.lock().unwrap().bridge = Some(bridge.clone());
            self.ari.add_to_bridge(&bridge, &[id, &call.external_id]).await.context("add channels to bridge")
        }
        .await;
        if let Err(e) = res {
            tracing::error!(call_id = %id, event = "call.setup_failed", step = "bridge", error = %format!("{e:#}"), "call setup failed");
            return self.teardown(&call);
        }
        {
            let mut r = call.res.lock().unwrap();
            if r.ended {
                return; // torn down meanwhile; teardown cleaned the bridge it knew of
            }
            r.counted_active = true;
        }
        metrics::CALLS_ACTIVE.fetch_add(1, Relaxed);
        metrics::call_started(call.info.direction);
        tracing::info!(call_id = %id, event = "call.bridged", external_id = %call.external_id, bridge = %bridge, "call bridged");

        let ended = self.run_media(&call).await;
        // After a transfer the call carries on without the agent: caller <-> destination,
        // ended by either hanging up (StasisEnd). Anything else ends the call with the agent.
        if call.res.lock().unwrap().transferred.is_none() {
            self.teardown(&call);
        }
        if let Some(end) = ended {
            self.record_run(id, end).await;
        }
        crate::telemetry::forget_call(id);
    }

    async fn run_media(self: &Arc<Self>, call: &Arc<Call>) -> Option<CallEnd> {
        let id = call.info.id.clone();
        let listener = call.res.lock().unwrap().listener.take()?;

        // Accept Asterisk's connection WHILE loading the workflow and opening the
        // STT: the database read and the Sarvam handshake overlap with it.
        let (conn, prepared) = tokio::join!(
            async { tokio::time::timeout(ACCEPT_TIMEOUT, listener.accept()).await },
            callagent::prepare(&self.cfg, &self.store, &call.info, &call.cancel),
        );
        drop(listener); // one connection is all a call needs
        let stream = match conn {
            Ok(Ok((s, _))) => s,
            other => {
                let why = match other {
                    Ok(Err(e)) => e.to_string(),
                    _ => "timed out".into(),
                };
                tracing::warn!(call_id = %id, component = "media", event = "media.no_connection", error = %why, "audiosocket connection never arrived");
                return None;
            }
        };
        let prepared = match prepared {
            Ok(p) => p,
            // Without its workflow the agent has no prompt and nothing to say.
            Err(e) => {
                tracing::error!(call_id = %id, component = "dograh", event = "workflow.load_failed", error = %format!("{e:#}"), "agent: loading the dograh workflow failed; hanging up");
                return None;
            }
        };
        if call.cancel.is_cancelled() {
            return None;
        }

        // The call's place in Dograh's history, created while the agent runs.
        let meta = prepared.run_meta();
        let run = if let Some(run_id) = call.info.run_id {
            // An outbound call's run exists already (the dialer made it before dialing, so even an
            // unanswered call is recorded): it starts now.
            let store = self.store.clone();
            tokio::spawn(async move { store.mark_run_running(run_id).await.map(|()| run_id) })
        } else {
            let (store, meta, rc) = (
                self.store.clone(),
                meta.clone(),
                RunCall {
                    call_id: id.clone(),
                    caller_number: call.info.caller_number.clone(),
                    called_number: call.info.called_number.clone(),
                },
            );
            tokio::spawn(async move { store.start_run(&meta, &rc).await })
        };

        if let Some(out) = &call.out {
            out.agent_started(); // from here its Dograh run is completed by the agent's own record
        }
        let media = media_call::spawn(id.clone(), stream, self.cfg.media_dead_timeout, call.media_cancel.child_token());
        tracing::info!(call_id = %id, component = "media", event = "media.started", "audiosocket connected, media plane running");
        let stats = media.stats.clone();

        let (ari, hang_id) = (self.ari.clone(), id.clone());
        let hangup: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let (ari, hang_id) = (ari.clone(), hang_id.clone());
            tokio::spawn(async move {
                if let Err(e) = ari.hangup(&hang_id).await {
                    tracing::debug!(call_id = %hang_id, error = %e, "hangup failed");
                }
            });
        });
        let transfer: TransferFn = {
            let (m, c) = (self.clone(), call.clone());
            Arc::new(move |destination, timeout| {
                let (m, c) = (m.clone(), c.clone());
                Box::pin(async move { m.transfer(&c, &destination, timeout).await })
            })
        };
        let (agent_cfg, io) = prepared.into_agent(&self.cfg, &id, media, hangup, Some(transfer));
        let started = std::time::Instant::now();
        let (summary, finisher) = handler::run(agent_cfg, io, call.media_cancel.clone()).await;
        let duration = started.elapsed();

        log_summary(&id, duration, &summary, &stats);
        Some(CallEnd { meta, run, summary, finisher, duration })
    }

    /// Completes the call's run in Dograh: conversation, usage, disposition, and
    /// the recording and transcript uploaded to MinIO. Failures are logged; the
    /// call itself is never affected. Runs AFTER teardown, so a slow upload never holds the call's port.
    /// Writes the ended call's Dograh record, at most `POST_CALL_WORKERS` calls at a time: a burst of hang-ups
    /// would otherwise open an extraction, a pair of uploads and several statements for every one of them at
    /// once. A call waiting for a worker keeps its recording in memory (`vaani_post_call_waiting`).
    async fn record_run(&self, id: &str, end: CallEnd) {
        let waiting = metrics::Counted::new(&metrics::POST_CALL_WAITING);
        let queued = std::time::Instant::now();
        let Ok(_permit) = self.post_call_sem.acquire().await else { return };
        drop(waiting);
        let _running = metrics::Counted::new(&metrics::POST_CALL_RUNNING);
        if queued.elapsed() >= Duration::from_secs(5) {
            tracing::info!(call_id = %id, event = "post_call.queued", waited_ms = queued.elapsed().as_millis() as u64, "this call's record waited for a post-call worker");
        }
        self.record_run_now(id, end).await;
    }

    async fn record_run_now(&self, id: &str, mut end: CallEnd) {
        // Dograh's end-of-call extraction, now that the call is torn down.
        end.finisher.finish(&mut end.summary).await;
        let run_id = match end.run.await {
            Ok(Ok(run_id)) => run_id,
            Ok(Err(e)) => {
                tracing::error!(call_id = %id, component = "dograh", event = "run.create_failed", error = %format!("{e:#}"), "dograh run: not created; this call won't appear in Dograh");
                return;
            }
            Err(_) => return,
        };
        crate::telemetry::set_run(id, run_id);
        tracing::info!(call_id = %id, component = "dograh", event = "run.created", run_id, workflow_id = end.meta.workflow_id, "dograh run created");

        if end.summary.recording_truncated {
            tracing::warn!(call_id = %id, component = "media", event = "recording.truncated", "call recording hit its size cap; the end of the call is missing from it");
        }
        let recording = std::mem::take(&mut end.summary.recording).into_wav();
        let recording_bytes = recording.len();
        let storage = self.cfg.storage();
        let run_end = RunEnd { summary: &end.summary, duration: end.duration, recording };
        match self.store.finish_run(&end.meta, run_id, run_end, storage.as_ref()).await {
            Ok(()) => tracing::info!(call_id = %id, component = "dograh", event = "run.completed", run_id,
                disposition = end.summary.end_reason.unwrap_or(calllog::USER_HANGUP), events = end.summary.events.len(),
                recording_bytes, minio = storage.is_some(), "dograh run completed"),
            Err(e) => {
                tracing::error!(call_id = %id, component = "dograh", event = "run.complete_failed", run_id, error = %format!("{e:#}"), "dograh run: completing it failed");
                return;
            }
        }
        // Dograh's cost job, likewise never triggered for Vaani's calls.
        self.store.record_cost(id, &end.meta, run_id, &end.summary, end.duration).await;
    }

    /// Idempotent: cancels the call and releases what it holds, each resource
    /// taken exactly once, so a second StasisEnd (the other leg) or a racing
    /// setup can't double-free a port or delete a bridge twice.
    fn teardown(self: &Arc<Self>, call: &Arc<Call>) {
        call.cancel.cancel();
        let (port, bridge, ext, active, first, transferred) = {
            let mut r = call.res.lock().unwrap();
            let first = !r.ended;
            r.ended = true;
            r.listener = None; // closes the socket if the media plane never took it
            (
                r.port.take(),
                r.bridge.take(),
                std::mem::take(&mut r.external_requested),
                std::mem::take(&mut r.counted_active),
                first,
                r.transferred.take(),
            )
        };
        {
            let mut calls = self.calls.lock().unwrap();
            calls.remove(&call.info.id);
            calls.remove(&call.external_id);
            if let Some(t) = &transferred {
                calls.remove(t);
            }
        }
        if let Some(p) = port {
            self.ports.free(p);
            metrics::MEDIA_PORTS_IN_USE.store(self.ports.in_use() as u64, Relaxed);
        }
        if active {
            metrics::CALLS_ACTIVE.fetch_sub(1, Relaxed);
            metrics::CALL_DURATION_SECONDS.observe(call.started.elapsed().as_secs_f64());
        }
        if first {
            crate::telemetry::end_call(&call.info.id);
            if !active {
                crate::telemetry::forget_call(&call.info.id); // never bridged: no run to record after it
            }
            tracing::debug!(call_id = %call.info.id, event = "call.teardown", "tearing down call");
            if let Some(out) = &call.out {
                self.finish_outbound(&call.info.id, out);
            }
        }
        if bridge.is_none() && !ext && !first && transferred.is_none() {
            return;
        }
        let (m, call) = (self.clone(), call.clone());
        self.tracker.spawn(async move {
            // Hangups are routine 404s when the far end beat us to it: debug-level.
            // A bridge that survives holds Asterisk resources with no event that
            // would ever clean it up: that one warns.
            if let Some(b) = bridge
                && let Err(e) = m.ari.delete_bridge(&b).await
            {
                tracing::warn!(call_id = %call.info.id, error = %e, "failed to delete bridge");
            }
            if let Err(e) = m.ari.hangup(&call.info.id).await {
                tracing::debug!(call_id = %call.info.id, error = %e, "hangup failed");
            }
            if ext && let Err(e) = m.ari.hangup(&call.external_id).await {
                tracing::debug!(call_id = %call.info.id, error = %e, "hangup of externalMedia channel failed");
            }
            if let Some(t) = transferred
                && let Err(e) = m.ari.hangup(&t).await
            {
                tracing::debug!(call_id = %call.info.id, error = %e, "hangup of the transfer destination failed");
            }
        });
    }

    /// Dials `destination` for `call` and waits until it answers, fails, or times
    /// out. On answer it returns the function that swaps the destination into the
    /// bridge in place of the agent's media leg and stops the agent. Mirrors
    /// Dograh's ARI transfer: originate into the same Stasis app with
    /// "transfer,<id>" args, then swap the bridge on answer.
    async fn transfer(self: &Arc<Self>, call: &Arc<Call>, destination: &str, timeout: Duration) -> anyhow::Result<ConnectFn> {
        use anyhow::Context;
        let dest_id = format!("vaani-xfer-{}", &uuid_v4()[..13]);
        let (tx, mut rx) = mpsc::channel(2);
        self.transfers.lock().unwrap().insert(dest_id.clone(), tx);
        let forget = |m: &Self| {
            m.transfers.lock().unwrap().remove(&dest_id);
        };

        let app_args = format!("{TRANSFER_APP_ARG},{}", call.info.id);
        if let Err(e) = self.ari.originate(destination, &app_args, timeout.as_secs(), &dest_id, &call.info.caller_number).await {
            forget(self);
            return Err(e).with_context(|| format!("could not dial {destination}"));
        }
        tracing::info!(call_id = %call.info.id, transfer_channel = %dest_id, destination, "transfer leg dialing");

        let outcome = tokio::select! {
            s = rx.recv() => s,
            _ = tokio::time::sleep(timeout + TRANSFER_ANSWER_SLACK) => None,
            _ = call.cancel.cancelled() => None,
        };
        forget(self);
        match outcome {
            Some(TransferSignal::Answered) => {
                let (m, c) = (self.clone(), call.clone());
                Ok(Box::new(move || Box::pin(async move { m.connect_transfer(&c, &dest_id).await })))
            }
            Some(TransferSignal::Failed(cause)) => anyhow::bail!("transfer destination did not answer ({cause})"),
            None => {
                let _ = self.ari.hangup(&dest_id).await; // gave up, or the caller hung up while it rang
                if call.cancel.is_cancelled() {
                    anyhow::bail!("the call ended while the transfer rang")
                } else {
                    anyhow::bail!("transfer destination did not answer in time")
                }
            }
        }
    }

    /// Puts the answered destination into the call's bridge, takes the agent's
    /// externalMedia leg out, and stops the agent. From here the call is caller <->
    /// destination; teardown ends both together.
    async fn connect_transfer(self: &Arc<Self>, call: &Arc<Call>, dest_id: &str) -> anyhow::Result<()> {
        let bridge = {
            let r = call.res.lock().unwrap();
            if r.ended { None } else { r.bridge.clone() }
        };
        let Some(bridge) = bridge else {
            let _ = self.ari.hangup(dest_id).await;
            anyhow::bail!("call ended before the transfer could connect");
        };
        // Add the destination before removing the agent, so the caller never sits in a bridge alone.
        if let Err(e) = self.ari.add_to_bridge(&bridge, &[dest_id]).await {
            let _ = self.ari.hangup(dest_id).await;
            return Err(e.context("could not connect the transfer"));
        }
        // The caller can hang up during add_to_bridge's round trip: re-check under the
        // lock teardown reads `transferred` with, so either this sees the teardown and
        // hangs the destination up itself, or teardown's cleanup sees it and does.
        let ended = {
            let mut r = call.res.lock().unwrap();
            if !r.ended {
                r.transferred = Some(dest_id.to_string());
                r.external_requested = false; // hung up on purpose below
                let mut calls = self.calls.lock().unwrap();
                calls.insert(dest_id.to_string(), call.clone());
                // The agent's leg is about to be hung up on purpose; its StasisEnd must not tear the call down.
                calls.remove(&call.external_id);
            }
            r.ended
        };
        if ended {
            let _ = self.ari.hangup(dest_id).await;
            anyhow::bail!("call ended while the transfer was connecting");
        }
        if let Err(e) = self.ari.remove_from_bridge(&bridge, &call.external_id).await {
            tracing::debug!(call_id = %call.info.id, error = %e, "removing the agent leg from the bridge failed");
        }
        call.media_cancel.cancel(); // stops the media plane and the agent (STT/TTS connections close)
        let _ = self.ari.hangup(&call.external_id).await;
        Ok(())
    }

    /// A transfer destination answered (it only enters Stasis once answered).
    fn on_transfer_answered(&self, channel_id: &str) {
        let tx = self.transfers.lock().unwrap().get(channel_id).cloned();
        match tx {
            Some(tx) => {
                let _ = tx.try_send(TransferSignal::Answered);
            }
            // Answered after we gave up (timeout, caller hung up): nobody is waiting
            // for it, so don't leave it connected to nothing.
            None => {
                tracing::warn!(channel_id, "transfer leg answered but no transfer is waiting; hanging it up");
                let (ari, id) = (self.ari.clone(), channel_id.to_string());
                self.tracker.spawn(async move {
                    let _ = ari.hangup(&id).await;
                });
            }
        }
    }

    fn teardown_all(self: &Arc<Self>) {
        let calls: Vec<_> = {
            let mut seen = std::collections::HashSet::new();
            self.calls.lock().unwrap().values().filter(|c| seen.insert(c.info.id.clone())).cloned().collect()
        };
        for c in calls {
            self.teardown(&c);
        }
    }
}

/// Whole-call test: a mock Asterisk (ARI REST + AudioSocket client), a real
/// Postgres holding the Dograh workflow, and mock Sarvam/LLM servers. Runs only
/// with `DOGRAH_TEST_DB_URL` set (see `dograh::store` tests).
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ari::{Channel, Event},
        media::{FRAME_SIZE, audiosocket},
        testkit::*,
    };
    use std::sync::atomic::Ordering::SeqCst;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    /// Mock ARI REST: records "METHOD path" lines and answers 200 to all of them.
    /// The externalMedia request's body is forwarded so the test can play Asterisk.
    async fn mock_ari() -> (String, Arc<Mutex<Vec<String>>>, mpsc::UnboundedReceiver<serde_json::Value>) {
        let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/ari", lis.local_addr().unwrap());
        let log = Arc::new(Mutex::new(vec![]));
        let (tx, rx) = mpsc::unbounded_channel();
        let l = log.clone();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = lis.accept().await.unwrap();
                let (l, tx) = (l.clone(), tx.clone());
                tokio::spawn(async move {
                    let mut buf = vec![];
                    let (line, body) = loop {
                        let mut c = [0u8; 4096];
                        let n = s.read(&mut c).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&c[..n]);
                        let t = String::from_utf8_lossy(&buf).to_string();
                        if let Some((head, body)) = t.split_once("\r\n\r\n") {
                            let len = head
                                .to_lowercase()
                                .split("content-length:")
                                .nth(1)
                                .and_then(|v| v.split("\r\n").next()?.trim().parse::<usize>().ok())
                                .unwrap_or(0);
                            if body.len() >= len {
                                let mut it = head.lines().next().unwrap().split_whitespace();
                                break (format!("{} {}", it.next().unwrap(), it.next().unwrap()), body[..len].to_string());
                            }
                        }
                    };
                    if line.contains("/channels/externalMedia") {
                        let _ = tx.send(serde_json::from_str(&body).unwrap());
                    }
                    l.lock().unwrap().push(line);
                    let _ = s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await;
                });
            }
        });
        (url, log, rx)
    }

    fn channel(id: &str) -> Channel {
        Channel { id: id.into(), ..Default::default() }
    }

    async fn wait_for(what: &str, cond: impl Fn() -> bool) {
        for _ in 0..200 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("timed out waiting for {what}");
    }

    #[tokio::test]
    async fn a_call_from_stasis_start_to_agent_audio_to_teardown() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let (tts_url, _tts) = mock_tts(10).await;
        let (stt_url, _stt_tx, stt_audio, _) = mock_stt().await;
        let (llm_url, llm_reqs) = mock_llm(|_| sse("Namaste, how can I help?")).await;
        let (ari_url, ari_log, mut ext_req) = mock_ari().await;
        let (minio_addr, minio_puts) = mock_minio().await;

        let Some((store, db_url)) = Store::for_test("t_manager", &format!(
            r#"
                CREATE TABLE workflows (id serial PRIMARY KEY, user_id int, organization_id int, released_definition_id int, template_context_variables jsonb, call_disposition_codes json);
                CREATE TABLE workflow_runs (id bigserial PRIMARY KEY, name text, workflow_id int, definition_id bigint, mode text, call_type text, state text,
                    is_completed bool, usage_info json, cost_info json, initial_context json, gathered_context json, logs json, annotations json,
                    created_at timestamptz, recording_url text, transcript_url text, storage_backend text);
                CREATE TABLE users (id serial PRIMARY KEY, selected_organization_id bigint);
                CREATE TABLE organizations (id bigint PRIMARY KEY, price_per_second_usd float8);
                CREATE TABLE organization_usage_cycles (id serial PRIMARY KEY, organization_id bigint, period_start timestamptz, period_end timestamptz,
                    used_dograh_tokens float8 DEFAULT 0, total_duration_seconds bigint DEFAULT 0, used_amount_usd float8);
                INSERT INTO users (id, selected_organization_id) VALUES (7, 3);
                INSERT INTO organizations (id, price_per_second_usd) VALUES (3, 0.001);
                INSERT INTO organization_usage_cycles (organization_id, period_start, period_end, used_amount_usd)
                  VALUES (3, now() - interval '1 day', now() + interval '1 day', 1.0);
                CREATE TABLE organization_configurations (id serial PRIMARY KEY, organization_id bigint, key text, value jsonb);
                INSERT INTO organization_configurations (organization_id, key, value) VALUES (3, 'DISPOSITION_CODE_MAPPING', '{{"user_hangup": "HU"}}');
                CREATE TABLE workflow_definitions (id serial PRIMARY KEY, workflow_id int, workflow_json jsonb, template_context_variables jsonb, workflow_configurations jsonb, is_current bool);
                CREATE TABLE user_configurations (id serial PRIMARY KEY, user_id int, configuration jsonb);
                CREATE TABLE tools (tool_uuid uuid, organization_id int, name text, description text, category text, status text, definition jsonb);
                CREATE TABLE external_credentials (credential_uuid varchar, organization_id int, credential_type text, credential_data jsonb, is_active bool);
                INSERT INTO user_configurations (user_id, configuration) VALUES (7, '{{
                  "llm": {{"provider":"speaches","base_url":"{llm_url}","api_key":"k","model":"gpt-4.1"}},
                  "stt": {{"provider":"sarvam","api_key":"k"}}, "tts": {{"provider":"sarvam","api_key":"k"}}}}');
                INSERT INTO workflows (id, user_id, organization_id) VALUES (42, 7, 3);
                INSERT INTO workflow_definitions (workflow_id, workflow_json, is_current)
                  VALUES (42, '{{"nodes":[{{"id":"1","type":"startCall","data":{{"name":"Start","prompt":"Greet {{{{caller_number}}}}","is_start":true}}}}]}}', true);
                "#)).await else {
            eprintln!("skipped: DOGRAH_TEST_DB_URL not set");
            return;
        };

        let mut cfg = Config::for_test(&db_url);
        cfg.ari_url = ari_url.clone();
        cfg.stt_ws_url = stt_url;
        cfg.tts_ws_url = tts_url;
        cfg.minio_endpoint = minio_addr;
        cfg.minio_access_key = "a".into();
        cfg.minio_secret_key = "b".into();
        let check_store = store.clone();
        let mgr = Manager::new(Ari::new(&ari_url, "u", "p", "vaani"), Arc::new(cfg), store);
        let (ev_tx, ev_rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let run = tokio::spawn(mgr.clone().run(ev_rx, cancel.clone()));

        // 1. The caller arrives: answered, and a media port is bound and listening.
        let mut caller = channel("caller-1");
        caller.caller = Some(crate::ari::Number { number: "98765".into() });
        ev_tx.send(Event::StasisStart { channel: caller, args: vec![] }).unwrap();
        let ext = tokio::time::timeout(Duration::from_secs(5), ext_req.recv()).await.unwrap().unwrap();
        assert_eq!(
            (ext["encapsulation"].as_str(), ext["transport"].as_str(), ext["connection_type"].as_str()),
            (Some("audiosocket"), Some("tcp"), Some("client"))
        );
        assert_eq!(ext["format"], "slin16");
        assert!(ext["data"].as_str().is_some_and(|u| u.len() == 36), "the audiosocket uuid");
        assert_eq!(mgr.ports.in_use(), 1);

        // 2. Playing Asterisk: connect to external_host. It was bound BEFORE the
        //    externalMedia request returned, so this can't be refused (invariant #9).
        let mut asterisk = TcpStream::connect(ext["external_host"].as_str().unwrap()).await.unwrap();
        let mut scratch = [0u8; audiosocket::HEADER_SIZE + FRAME_SIZE];
        audiosocket::write_frame(&mut asterisk, &mut scratch, 0x01, ext["data"].as_str().unwrap().as_bytes()).await.unwrap();

        // 3. The externalMedia leg enters Stasis: the bridge is built, the agent starts.
        ev_tx.send(Event::StasisStart { channel: channel(ext["channelId"].as_str().unwrap()), args: vec![] }).unwrap();
        let (mut rd, mut wr) = asterisk.into_split();
        tokio::spawn(async move {
            let mut scratch = [0u8; audiosocket::HEADER_SIZE + FRAME_SIZE];
            let mut tick = media_call::new_ticker();
            loop {
                tick.tick().await;
                if audiosocket::write_frame(&mut wr, &mut scratch, audiosocket::KIND_SLIN16, &[0u8; FRAME_SIZE]).await.is_err() {
                    break;
                }
            }
        });

        // 4. The agent speaks: tone frames arrive on the AudioSocket.
        let mut tone = 0;
        tokio::time::timeout(Duration::from_secs(8), async {
            while let Ok(audiosocket::Inbound::Audio(f)) = audiosocket::read_frame(&mut rd).await {
                if f[..2] == TONE.to_le_bytes() {
                    tone += 1;
                }
                if tone >= 10 {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(tone, 10, "the agent's reply plays over AudioSocket");
        assert!(stt_audio.load(SeqCst) > 0, "caller audio reaches the STT");
        assert!(metrics::render().contains("vaani_calls_started_total{direction=\"inbound\"} "), "counted when bridged");
        let req = llm_reqs.lock().unwrap()[0].clone();
        assert_eq!(req["messages"][0]["content"], "Greet 98765", "Dograh's prompt, variables filled");
        assert!(metrics::CALLS_ACTIVE.load(SeqCst) >= 1);

        // 5. Hangup: everything is released exactly once.
        ev_tx.send(Event::StasisEnd { channel: channel("caller-1") }).unwrap();
        ev_tx.send(Event::StasisEnd { channel: channel(ext["channelId"].as_str().unwrap()) }).unwrap(); // the other leg's StasisEnd
        wait_for("the port to be freed", || mgr.ports.in_use() == 0).await;
        wait_for("ARI cleanup", || {
            let l = ari_log.lock().unwrap();
            l.iter().any(|x| x.starts_with("DELETE /ari/bridges/")) && l.iter().any(|x| x == "DELETE /ari/channels/caller-1")
        })
        .await;
        let l = ari_log.lock().unwrap().clone();
        assert!(l.contains(&"POST /ari/channels/caller-1/answer".to_string()), "{l:?}");
        assert_eq!(l.iter().filter(|x| x.starts_with("DELETE /ari/bridges/")).count(), 1, "bridge deleted once: {l:?}");
        assert!(mgr.calls.lock().unwrap().is_empty());

        // 6. The call is in Dograh's history: completed, with its conversation,
        //    disposition, usage, and the recording and transcript uploaded.
        let db = check_store.db().await.unwrap();
        let mut row = None;
        for _ in 0..200 {
            row = db.query_opt("SELECT id, is_completed, state, recording_url, transcript_url, gathered_context::text, logs::text, usage_info::text, cost_info::text FROM workflow_runs", &[]).await.unwrap();
            if row.as_ref().is_some_and(|r| r.get::<_, Option<String>>(4).is_some()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let row = row.expect("a workflow_runs row");
        let run_id: i64 = row.get(0);
        assert_eq!((row.get::<_, bool>(1), row.get::<_, String>(2).as_str()), (true, "completed"));
        assert_eq!(row.get::<_, String>(3), format!("recordings/{run_id}.wav"));
        let gathered: serde_json::Value = serde_json::from_str(&row.get::<_, String>(5)).unwrap();
        assert_eq!(gathered["call_id"], "caller-1");
        assert_eq!(
            (gathered["call_disposition"].as_str(), gathered["mapped_call_disposition"].as_str()),
            (Some("user_hangup"), Some("HU"))
        );
        assert_eq!(gathered["nodes_visited"], serde_json::json!(["Start"]));
        let logs: serde_json::Value = serde_json::from_str(&row.get::<_, String>(6)).unwrap();
        let kinds: Vec<_> =
            logs["realtime_feedback_events"].as_array().unwrap().iter().map(|e| e["type"].as_str().unwrap().to_string()).collect();
        assert!(kinds.contains(&"rtf-node-transition".to_string()) && kinds.contains(&"rtf-bot-text".to_string()), "{kinds:?}");
        let usage: serde_json::Value = serde_json::from_str(&row.get::<_, String>(7)).unwrap();
        assert!(
            usage["tts"].as_object().unwrap().values().next().unwrap().as_u64().unwrap() >= 24,
            "TTS characters of the greeting: {usage}"
        );
        assert!(usage["call_duration_seconds"].as_i64().unwrap() >= 0);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(8)).unwrap()["call_duration_seconds"],
            usage["call_duration_seconds"]
        );
        // Priced like Dograh's own cost job: cost_info, and the organization's usage cycle.
        let mut cost_row = None;
        for _ in 0..200 {
            let r = db.query_one("SELECT cost_info::text, usage_info::text FROM workflow_runs WHERE id = $1", &[&run_id]).await.unwrap();
            if r.get::<_, String>(0).contains("cost_breakdown") {
                cost_row = Some(r);
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let cost_row = cost_row.expect("cost_info with a cost_breakdown");
        let cost: serde_json::Value = serde_json::from_str(&cost_row.get::<_, String>(0)).unwrap();
        let usage: serde_json::Value = serde_json::from_str(&cost_row.get::<_, String>(1)).unwrap();
        assert_eq!(usage["llm"]["LLM|||gpt-4.1"]["prompt_tokens"], 100);
        // 100 prompt tokens * $2/M + 20 completion * $8/M
        assert!((cost["cost_breakdown"]["llm_cost"].as_f64().unwrap() - 0.00036).abs() < 1e-9, "{cost}");
        assert!(cost["cost_breakdown"]["tts_cost"].as_f64().unwrap() > 0.0);
        let secs = cost["call_duration_seconds"].as_i64().unwrap();
        assert!((cost["charge_usd"].as_f64().unwrap() - secs as f64 * 0.001).abs() < 1e-9, "{cost}");
        let cyc = db
            .query_one("SELECT total_duration_seconds, used_amount_usd, used_dograh_tokens FROM organization_usage_cycles", &[])
            .await
            .unwrap();
        assert_eq!(cyc.get::<_, i64>(0), secs);
        assert!((cyc.get::<_, f64>(1) - (1.0 + secs as f64 * 0.001)).abs() < 1e-9);
        assert!(cyc.get::<_, f64>(2) > 0.0, "Dograh tokens were charged");
        let puts = minio_puts.lock().unwrap().clone();
        let wav = puts.iter().find(|(p, _)| p == &format!("/voice-audio/recordings/{run_id}.wav")).expect("the recording upload");
        assert_eq!(&wav.1[..4], b"RIFF");
        assert!(wav.1.len() > 44 + FRAME_SIZE * 10, "the recording holds the call's audio");
        let transcript = puts.iter().find(|(p, _)| p == &format!("/voice-audio/transcripts/{run_id}.txt")).expect("the transcript upload");
        assert!(String::from_utf8_lossy(&transcript.1).contains("Agent: Namaste, how can I help?"));

        cancel.cancel();
        run.await.unwrap();
    }

    fn bare_call(id: &str, ext: &str) -> Arc<Call> {
        let cancel = CancellationToken::new();
        Arc::new(Call {
            info: CallInfo {
                id: id.into(),
                caller_number: "98765".into(),
                called_number: "1000".into(),
                direction: "inbound",
                workflow_id: None,
                run_id: None,
                variables: Default::default(),
            },
            out: None,
            external_id: ext.into(),
            media_cancel: cancel.child_token(),
            cancel,
            arrived: CancellationToken::new(),
            started: std::time::Instant::now(),
            res: Mutex::new(Res { bridge: Some("br1".into()), external_requested: true, ..Default::default() }),
        })
    }

    /// The `channelId=` the originate request carried.
    async fn originated_channel(log: &Arc<Mutex<Vec<String>>>, endpoint: &str) -> String {
        for _ in 0..200 {
            let line = log.lock().unwrap().iter().find(|l| l.starts_with("POST /ari/channels?endpoint=") && l.contains(endpoint)).cloned();
            if let Some(l) = line {
                return l.split("channelId=").nth(1).unwrap().split('&').next().unwrap().to_string();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("no originate for {endpoint}");
    }

    #[tokio::test]
    async fn a_transfer_swaps_the_destination_into_the_bridge_and_failures_are_reported() {
        let (ari_url, log, _ext) = mock_ari().await;
        let Some((store, _)) = Store::for_test("t_transfer", "SELECT 1").await else {
            eprintln!("skipped: DOGRAH_TEST_DB_URL not set");
            return;
        };
        let mgr = Manager::new(Ari::new(&ari_url, "u", "p", "vaani"), Arc::new(Config::for_test("")), store);

        // The destination answers: it enters Stasis with the "transfer" argument.
        let call = bare_call("caller-1", "ext-1");
        mgr.calls.lock().unwrap().extend([("caller-1".to_string(), call.clone()), ("ext-1".to_string(), call.clone())]);
        let (m, c) = (mgr.clone(), call.clone());
        let dialing = tokio::spawn(async move { m.transfer(&c, "PJSIP/100@pbx", Duration::from_secs(5)).await });
        let dest = originated_channel(&log, "PJSIP%2F100%40pbx").await;
        assert!(
            log.lock().unwrap().iter().any(|l| l.contains("appArgs=transfer%2Ccaller-1") && l.contains("timeout=5")),
            "{:?}",
            log.lock().unwrap()
        );
        mgr.on_event(Event::StasisStart { channel: channel(&dest), args: vec!["transfer".into(), "caller-1".into()] });
        let connect = dialing.await.unwrap().expect("answered");
        assert!(!call.media_cancel.is_cancelled(), "the agent keeps the call until the handover");

        connect().await.unwrap();
        let l = log.lock().unwrap().clone();
        let pos = |needle: &str| l.iter().position(|x| x.contains(needle)).unwrap_or_else(|| panic!("{needle} missing from {l:?}"));
        assert!(
            pos(&format!("/bridges/br1/addChannel?channel={dest}")) < pos("/bridges/br1/removeChannel?channel=ext-1"),
            "destination added BEFORE the agent leaves: {l:?}"
        );
        assert!(l.contains(&"DELETE /ari/channels/ext-1".to_string()));
        assert!(call.media_cancel.is_cancelled() && !call.cancel.is_cancelled(), "the agent stops; the call goes on");
        {
            let calls = mgr.calls.lock().unwrap();
            assert!(calls.contains_key(&dest) && !calls.contains_key("ext-1"), "the agent leg's StasisEnd no longer ends the call");
        }
        // Either side hanging up now ends everything.
        mgr.on_event(Event::StasisEnd { channel: channel(&dest) });
        wait_for("teardown", || {
            let l = log.lock().unwrap();
            l.contains(&"DELETE /ari/bridges/br1".to_string())
                && l.contains(&"DELETE /ari/channels/caller-1".to_string())
                && l.iter().filter(|x| x.ends_with(&format!("/channels/{dest}"))).count() >= 1
        })
        .await;
        assert!(call.cancel.is_cancelled() && mgr.calls.lock().unwrap().is_empty());

        // The destination is busy: reported with Asterisk's cause, and nothing was swapped.
        let call2 = bare_call("caller-2", "ext-2");
        let (m, c) = (mgr.clone(), call2.clone());
        let dialing = tokio::spawn(async move { m.transfer(&c, "PJSIP/200@pbx", Duration::from_secs(5)).await });
        let dest2 = originated_channel(&log, "PJSIP%2F200%40pbx").await;
        mgr.on_event(Event::ChannelDestroyed { channel: channel(&dest2), cause: 17, cause_txt: "User busy".into() });
        let err = dialing.await.unwrap().err().expect("busy").to_string();
        assert_eq!(err, "transfer destination did not answer (User busy, cause 17)");
        assert!(!call2.media_cancel.is_cancelled() && mgr.transfers.lock().unwrap().is_empty());

        // The caller hangs up while it rings: the destination is hung up, not left ringing.
        let call3 = bare_call("caller-3", "ext-3");
        let (m, c) = (mgr.clone(), call3.clone());
        let dialing = tokio::spawn(async move { m.transfer(&c, "PJSIP/300@pbx", Duration::from_secs(5)).await });
        let dest3 = originated_channel(&log, "PJSIP%2F300%40pbx").await;
        call3.cancel.cancel();
        assert!(dialing.await.unwrap().is_err());
        wait_for("the ringing destination hung up", || log.lock().unwrap().contains(&format!("DELETE /ari/channels/{dest3}"))).await;
    }

    fn dial_request(
        run_id: i64,
        ring: Duration,
    ) -> (super::super::outbound::DialRequest, mpsc::UnboundedReceiver<super::super::outbound::Outcome>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let req = super::super::outbound::DialRequest {
            endpoint: "PJSIP/9876543210@carrier".into(),
            caller_id: "0739999".into(),
            called: "+919876543210".into(),
            ring_timeout: ring,
            workflow_id: 42,
            run_id,
            variables: serde_json::json!({"name": "Asha"}).as_object().cloned().unwrap(),
            done: Box::new(move |o| {
                let _ = tx.send(o);
            }),
        };
        (req, rx)
    }

    #[tokio::test]
    async fn a_dialed_call_that_is_not_answered_reports_its_status_exactly_once() {
        use super::super::outbound::Outcome;
        let (ari_url, log, _ext) = mock_ari().await;
        let Some((store, _)) = Store::for_test("t_outbound_unanswered", "SELECT 1").await else {
            eprintln!("skipped: DOGRAH_TEST_DB_URL not set");
            return;
        };
        let mgr = Manager::new(Ari::new(&ari_url, "u", "p", "vaani"), Arc::new(Config::for_test("")), store);

        // Busy (17), rejected (21), anything else failed -- Dograh's mapping of the hangup cause.
        for (cause, status) in [(17, "busy"), (21, "rejected"), (38, "failed"), (19, "no-answer")] {
            let (req, mut done) = dial_request(77, Duration::from_secs(30));
            let id = mgr.dial(req).await.unwrap();
            let line = log.lock().unwrap().iter().rev().find(|l| l.contains(&id)).cloned().unwrap();
            assert!(line.starts_with("POST /ari/channels?endpoint=PJSIP%2F9876543210%40carrier"), "{line}");
            assert!(line.contains("appArgs=outbound%2C77") && line.contains("timeout=30") && line.contains("callerId=0739999"), "{line}");
            mgr.on_event(Event::ChannelDestroyed { channel: channel(&id), cause, cause_txt: "x".into() });
            mgr.on_event(Event::ChannelDestroyed { channel: channel(&id), cause: 16, cause_txt: "again".into() });
            assert_eq!(done.recv().await.unwrap(), Outcome { connected: false, status, cause });
            assert!(done.try_recv().is_err(), "Done is called exactly once");
            assert!(mgr.outbound.lock().unwrap().is_empty());
        }

        // Asterisk never reports: the watchdog gives up, hangs the channel up, and calls it unanswered.
        let (req, mut done) = dial_request(78, Duration::from_millis(50));
        let id = mgr.dial(req).await.unwrap();
        let out = tokio::time::timeout(Duration::from_secs(3), done.recv()).await.expect("the watchdog fires").unwrap();
        assert_eq!((out.connected, out.status), (false, "no-answer"));
        wait_for("the stuck channel hung up", || log.lock().unwrap().contains(&format!("DELETE /ari/channels/{id}"))).await;

        // A leg answered after we gave up on it is hung up, not left connected to nothing.
        mgr.on_event(Event::StasisStart { channel: channel(&id), args: vec!["outbound".into(), "78".into()] });
        wait_for("the orphan hung up", || log.lock().unwrap().iter().filter(|l| **l == format!("DELETE /ari/channels/{id}")).count() >= 2)
            .await;

        // Nothing was dialed (Asterisk refused): an error, and Done is never called.
        let dead = Manager::new(Ari::new("http://127.0.0.1:1/ari", "u", "p", "vaani"), Arc::new(Config::for_test("")), mgr.store.clone());
        let (req, mut done) = dial_request(79, Duration::from_secs(30));
        assert!(dead.dial(req).await.unwrap_err().to_string().contains("could not dial PJSIP/9876543210@carrier"));
        assert!(done.try_recv().is_err() && dead.outbound.lock().unwrap().is_empty());
    }

    /// A dialed call that IS answered follows the inbound path -- minus the answer, which already
    /// happened -- with the campaign's workflow, the contact's variables and the run the dialer made.
    #[tokio::test]
    async fn an_answered_outbound_call_runs_the_agent_on_the_dialers_run() {
        use super::super::outbound::Outcome;
        use crate::dograh::campaign::{Campaign, QueuedRun};
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (tts_url, _tts) = mock_tts(10).await;
        let (stt_url, _stt_tx, _stt_audio, _) = mock_stt().await;
        let (llm_url, llm_reqs) = mock_llm(|_| sse("Namaste Asha.")).await;
        let (ari_url, log, mut ext_req) = mock_ari().await;
        let Some((store, db_url)) = Store::for_test(
            "t_outbound_answered",
            &format!(
                r#"{CAMPAIGN_SCHEMA}
                CREATE TABLE user_configurations (id serial PRIMARY KEY, user_id int, configuration jsonb);
                INSERT INTO user_configurations (user_id, configuration) VALUES (7, '{{
                  "llm": {{"provider":"speaches","base_url":"{llm_url}","api_key":"k","model":"m"}},
                  "stt": {{"provider":"sarvam","api_key":"k"}}, "tts": {{"provider":"sarvam","api_key":"k"}}}}');
                INSERT INTO workflows (id, user_id, organization_id) VALUES (42, 7, 3);
                INSERT INTO workflow_definitions (workflow_id, workflow_json, is_current)
                  VALUES (42, '{{"nodes":[{{"id":"1","type":"startCall","data":{{"name":"Start","prompt":"Greet {{{{name}}}} at {{{{called_number}}}}","is_start":true}}}}]}}', true);"#
            ),
        )
        .await
        else {
            eprintln!("skipped: DOGRAH_TEST_DB_URL not set");
            return;
        };
        let mut cfg = Config::for_test(&db_url);
        cfg.stt_ws_url = stt_url;
        cfg.tts_ws_url = tts_url;
        let check = store.clone();
        let mgr = Manager::new(Ari::new(&ari_url, "u", "p", "vaani"), Arc::new(cfg), store.clone());

        // The dialer creates the run first, then dials.
        let campaign = Campaign { workflow_id: 42, ..Default::default() };
        let run = store.create_outbound_run(&campaign, &QueuedRun::default(), "+919876543210", "0739999").await.unwrap();
        let (req, mut done) = dial_request(run, Duration::from_secs(30));
        let id = mgr.dial(req).await.unwrap();

        // The callee answers: the channel enters Stasis with the "outbound" argument.
        mgr.on_event(Event::StasisStart { channel: channel(&id), args: vec!["outbound".into(), run.to_string()] });
        let ext = tokio::time::timeout(Duration::from_secs(5), ext_req.recv()).await.unwrap().unwrap();
        assert!(
            !log.lock().unwrap().iter().any(|l| l.ends_with("/answer")),
            "a dialed call is already answered: {:?}",
            log.lock().unwrap()
        );

        let mut asterisk = TcpStream::connect(ext["external_host"].as_str().unwrap()).await.unwrap();
        let mut scratch = [0u8; audiosocket::HEADER_SIZE + FRAME_SIZE];
        audiosocket::write_frame(&mut asterisk, &mut scratch, 0x01, ext["data"].as_str().unwrap().as_bytes()).await.unwrap();
        mgr.on_event(Event::StasisStart { channel: channel(ext["channelId"].as_str().unwrap()), args: vec![] });
        let (mut rd, mut wr) = asterisk.into_split();
        tokio::spawn(async move {
            let mut scratch = [0u8; audiosocket::HEADER_SIZE + FRAME_SIZE];
            let mut tick = media_call::new_ticker();
            loop {
                tick.tick().await;
                if audiosocket::write_frame(&mut wr, &mut scratch, audiosocket::KIND_SLIN16, &[0u8; FRAME_SIZE]).await.is_err() {
                    break;
                }
            }
        });
        let mut tone = 0;
        tokio::time::timeout(Duration::from_secs(8), async {
            while let Ok(audiosocket::Inbound::Audio(f)) = audiosocket::read_frame(&mut rd).await {
                tone += usize::from(f[..2] == TONE.to_le_bytes());
                if tone >= 10 {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(tone, 10, "the agent speaks on the dialed call");
        assert_eq!(
            llm_reqs.lock().unwrap()[0]["messages"][0]["content"],
            "Greet Asha at +919876543210",
            "the contact's variables and the dialed number"
        );

        // The dialer's run goes 'running' (not a second run).
        let db = check.db().await.unwrap();
        assert_eq!(db.query_one("SELECT state FROM workflow_runs WHERE id = $1", &[&run]).await.unwrap().get::<_, String>(0), "running");

        // Hangup: the call is over -> Done(connected); the agent completes the SAME run.
        mgr.on_event(Event::StasisEnd { channel: channel(&id) });
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), done.recv()).await.unwrap().unwrap(),
            Outcome { connected: true, status: "", cause: 0 }
        );
        wait_for("the run to complete", || mgr.calls.lock().unwrap().is_empty()).await;
        let mut row = None;
        for _ in 0..200 {
            let r = db.query_one("SELECT is_completed, state, (SELECT count(*) FROM workflow_runs), initial_context::text FROM workflow_runs WHERE id = $1", &[&run]).await.unwrap();
            if r.get::<_, bool>(0) {
                row = Some(r);
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let row = row.expect("the dialer's run is completed by the agent");
        assert_eq!((row.get::<_, String>(1).as_str(), row.get::<_, i64>(2)), ("completed", 1));
        assert!(row.get::<_, String>(3).contains("\"called_number\":\"+919876543210\""), "the dialer's initial context survives");
    }

    async fn guard_mgr(schema: &str, tweak: impl FnOnce(&mut Config)) -> Option<(Arc<Manager>, Arc<Mutex<Vec<String>>>)> {
        let (ari_url, log, _ext) = mock_ari().await;
        let (store, _) = Store::for_test(schema, "SELECT 1").await?;
        let mut cfg = Config::for_test("");
        tweak(&mut cfg);
        Some((Manager::new(Ari::new(&ari_url, "u", "p", "vaani"), Arc::new(cfg), store), log))
    }

    fn logged(log: &Arc<Mutex<Vec<String>>>, needle: &str) -> bool {
        log.lock().unwrap().iter().any(|l| l.contains(needle))
    }

    #[tokio::test]
    async fn a_new_caller_is_refused_before_it_is_answered_while_overloaded() {
        let Some((mgr, log)) = guard_mgr("t_mgr_overloaded", |_| {}).await else { return eprintln!("skipped: DOGRAH_TEST_DB_URL not set") };
        mgr.set_overloaded(Arc::new(|| true));

        mgr.on_event(Event::StasisStart { channel: channel("caller-9"), args: vec![] });

        wait_for("the hangup", || logged(&log, "DELETE /ari/channels/caller-9?reason=congestion")).await;
        assert!(!logged(&log, "/answer"), "never answered: {:?}", log.lock().unwrap());
        assert!(mgr.calls.lock().unwrap().is_empty());
        assert_eq!(mgr.ports.in_use(), 0, "nothing was allocated for it");
    }

    /// An answered call whose externalMedia channel never enters Stasis is hung up, its port freed.
    #[tokio::test]
    async fn setup_times_out_when_the_media_channel_never_arrives() {
        let Some((mgr, log)) = guard_mgr("t_mgr_setup_timeout", |c| c.setup_timeout = Duration::from_millis(600)).await else {
            return eprintln!("skipped: DOGRAH_TEST_DB_URL not set");
        };
        mgr.on_event(Event::StasisStart { channel: channel("caller-1"), args: vec![] });
        wait_for("externalMedia requested", || logged(&log, "/channels/externalMedia")).await;
        assert_eq!(mgr.setting_up(), 1);
        assert_eq!(mgr.ports.in_use(), 1);

        wait_for("the caller is hung up", || logged(&log, "DELETE /ari/channels/caller-1")).await;
        wait_for("and the media channel", || logged(&log, "DELETE /ari/channels/vaani-ext-")).await;
        assert_eq!(mgr.setting_up(), 0);
        assert_eq!(mgr.ports.in_use(), 0, "the port is free again");
    }

    #[tokio::test]
    async fn the_media_channel_arriving_stops_the_setup_timer() {
        let Some((mgr, log)) = guard_mgr("t_mgr_setup_arrived", |c| c.setup_timeout = Duration::from_millis(600)).await else {
            return eprintln!("skipped: DOGRAH_TEST_DB_URL not set");
        };
        mgr.on_event(Event::StasisStart { channel: channel("caller-1"), args: vec![] });
        wait_for("externalMedia requested", || logged(&log, "/channels/externalMedia")).await;
        let call = mgr.calls.lock().unwrap().get("caller-1").cloned().expect("staged");
        assert!(!call.arrived.is_cancelled());

        mgr.on_event(Event::StasisStart { channel: channel(&call.external_id), args: vec![] });
        assert!(call.arrived.is_cancelled(), "the timer no longer waits for it");
    }

    /// A media channel whose call is already gone is hung up, not taken for a new caller.
    #[tokio::test]
    async fn a_stray_media_channel_is_hung_up() {
        let Some((mgr, log)) = guard_mgr("t_mgr_stray", |_| {}).await else { return eprintln!("skipped: DOGRAH_TEST_DB_URL not set") };
        let stray = Channel { id: "ext-9".into(), name: "AudioSocket/192.168.36.19:20101-x".into(), ..Default::default() };

        mgr.on_event(Event::StasisStart { channel: stray, args: vec![] });

        wait_for("the hangup", || logged(&log, "DELETE /ari/channels/ext-9")).await;
        assert!(!logged(&log, "/answer"), "not a new caller");
        assert_eq!(mgr.ports.in_use(), 0);
        assert!(mgr.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn setting_up_counts_staged_calls_once_until_they_are_bridged() {
        let Some((mgr, _log)) = guard_mgr("t_mgr_setting_up", |_| {}).await else {
            return eprintln!("skipped: DOGRAH_TEST_DB_URL not set");
        };
        let (a, b) = (bare_call("a", "a-ext"), bare_call("b", "b-ext"));
        mgr.calls.lock().unwrap().extend([
            ("a".to_string(), a.clone()),
            ("a-ext".to_string(), a.clone()),
            ("b".to_string(), b.clone()),
            ("b-ext".to_string(), b.clone()),
        ]);
        assert_eq!(mgr.setting_up(), 2, "each call once, though it is in the map under both channel ids");

        a.res.lock().unwrap().counted_active = true; // bridged
        assert_eq!(mgr.setting_up(), 1);
        b.res.lock().unwrap().ended = true;
        assert_eq!(mgr.setting_up(), 0);
        assert!(mgr.tracks("a-ext") && !mgr.tracks("nobody"));
    }

    /// A bridged call's length, from staging to teardown, goes to `vaani_call_duration_seconds`; one never bridged doesn't.
    #[tokio::test]
    async fn a_bridged_calls_duration_is_observed_at_teardown() {
        let Some((mgr, _log)) = guard_mgr("t_mgr_duration", |_| {}).await else { return eprintln!("skipped: DOGRAH_TEST_DB_URL not set") };
        let before = metrics::CALL_DURATION_SECONDS.count();

        let never_bridged = bare_call("n", "n-ext");
        mgr.calls.lock().unwrap().extend([("n".to_string(), never_bridged.clone()), ("n-ext".to_string(), never_bridged.clone())]);
        mgr.teardown(&never_bridged);
        assert_eq!(metrics::CALL_DURATION_SECONDS.count(), before, "a call that never reached the bridge isn't an answered call");

        let bridged = bare_call("b", "b-ext");
        bridged.res.lock().unwrap().counted_active = true;
        metrics::CALLS_ACTIVE.fetch_add(1, Relaxed);
        mgr.calls.lock().unwrap().extend([("b".to_string(), bridged.clone()), ("b-ext".to_string(), bridged.clone())]);
        mgr.teardown(&bridged);
        assert!(metrics::CALL_DURATION_SECONDS.count() > before);
    }

    /// A manager with a slow mock MinIO and Dograh's run tables holding runs 1..=8, for the post-call tests.
    async fn post_call_mgr(
        schema: &str,
        upload_ms: u64,
        tweak: impl FnOnce(&mut Config),
    ) -> Option<(Arc<Manager>, Arc<Mutex<Vec<(String, Vec<u8>)>>>, Arc<std::sync::atomic::AtomicUsize>)> {
        let (ari_url, _log, _ext) = mock_ari().await;
        let (minio, puts, peak) = crate::testkit::mock_minio_peak(Duration::from_millis(upload_ms)).await;
        let (store, _) = Store::for_test(
            schema,
            &format!(
                "{}
                 INSERT INTO workflow_runs (id, name, workflow_id, state, is_completed, usage_info, cost_info, gathered_context, logs)
                   SELECT g, 'r', 42, 'running', false, '{{}}', '{{}}', '{{}}', '{{}}' FROM generate_series(1, 8) g;",
                crate::testkit::CAMPAIGN_SCHEMA
            ),
        )
        .await?;
        let mut cfg = Config::for_test("");
        (cfg.minio_endpoint, cfg.minio_access_key, cfg.minio_secret_key) = (minio, "a".into(), "b".into());
        tweak(&mut cfg);
        Some((Manager::new(Ari::new(&ari_url, "u", "p", "vaani"), Arc::new(cfg), store), puts, peak))
    }

    /// An ended call whose run is `run_id`, with a recording to upload.
    fn call_end(run_id: i64) -> CallEnd {
        let mut rec = crate::agent::recorder::MixRecorder::default();
        rec.frame(&crate::media::SILENCE, None);
        CallEnd {
            meta: RunMeta { workflow_id: 42, definition_id: 1, org_id: None, stt_model: "m".into(), tts_model: "m".into() },
            run: tokio::spawn(async move { Ok(run_id) }),
            summary: calllog::Summary { recording: rec.finish(), ..Default::default() },
            finisher: extract::Finisher {
                call_id: format!("c{run_id}"),
                llm: crate::ai::llm::LlmClient::new("http://127.0.0.1:1", "k", "m"),
                pending: vec![],
                last: None,
            },
            duration: Duration::from_secs(5),
        }
    }

    /// A burst of hang-ups must not open an upload (and an extraction, and a cost update) per call: at most
    /// `POST_CALL_WORKERS` ended calls finish their record at once, the rest wait their turn, and all finish.
    #[tokio::test]
    async fn post_call_work_is_bounded_and_all_of_it_gets_done() {
        let Some((mgr, puts, peak)) = post_call_mgr("t_post_call_bounded", 400, |c| c.post_call_workers = 2).await else {
            return eprintln!("skipped: DOGRAH_TEST_DB_URL not set");
        };
        let started = std::time::Instant::now();
        let jobs: Vec<_> = (1..=6)
            .map(|i| {
                let m = mgr.clone();
                tokio::spawn(async move { m.record_run(&format!("c{i}"), call_end(i)).await })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(mgr.post_call_sem.available_permits(), 0, "both workers are busy while four calls wait");
        for j in jobs {
            j.await.unwrap();
        }
        assert_eq!(puts.lock().unwrap().len(), 6, "every call's recording was uploaded");
        assert!(peak.load(Relaxed) <= 2, "at most 2 uploads at once, saw {}", peak.load(Relaxed));
        assert!(started.elapsed() >= Duration::from_millis(1100), "six 400 ms uploads, two at a time: {:?}", started.elapsed());
        assert_eq!(mgr.post_call_sem.available_permits(), 2, "the workers are free again");
        let rows = mgr
            .store
            .db()
            .await
            .unwrap()
            .query_one("SELECT count(*) FROM workflow_runs WHERE recording_url IS NOT NULL", &[])
            .await
            .unwrap();
        assert_eq!(rows.get::<_, i64>(0), 6, "and every run has its recording attached");
    }

    /// Shutdown waits for the records being written, as long as SHUTDOWN_WAIT_SECONDS allows, and returns as soon as they are done.
    #[tokio::test]
    async fn shutdown_waits_for_the_records_being_written() {
        let Some((mgr, _puts, _)) = post_call_mgr("t_shutdown_waits", 700, |c| c.shutdown_wait = Duration::from_secs(5)).await else {
            return eprintln!("skipped: DOGRAH_TEST_DB_URL not set");
        };
        let (_ev_tx, ev_rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let run = tokio::spawn(mgr.clone().run(ev_rx, cancel.clone()));
        let m = mgr.clone();
        mgr.tracker.spawn(async move { m.record_run("c1", call_end(1)).await });
        tokio::time::sleep(Duration::from_millis(150)).await; // the upload is under way
        cancel.cancel();
        run.await.unwrap();
        let row = mgr.store.db().await.unwrap().query_one("SELECT recording_url FROM workflow_runs WHERE id = 1", &[]).await.unwrap();
        assert_eq!(
            row.get::<_, Option<String>>(0).as_deref(),
            Some("recordings/1.wav"),
            "shutdown did not return before the upload finished"
        );
    }

    /// ... and gives up after SHUTDOWN_WAIT_SECONDS, not a fixed 10 s.
    #[tokio::test]
    async fn shutdown_gives_up_after_the_configured_wait() {
        let Some((mgr, _puts, _)) = post_call_mgr("t_shutdown_gives_up", 3000, |c| c.shutdown_wait = Duration::from_millis(300)).await
        else {
            return eprintln!("skipped: DOGRAH_TEST_DB_URL not set");
        };
        let (_ev_tx, ev_rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let run = tokio::spawn(mgr.clone().run(ev_rx, cancel.clone()));
        let m = mgr.clone();
        mgr.tracker.spawn(async move { m.record_run("c1", call_end(1)).await });
        tokio::time::sleep(Duration::from_millis(150)).await;
        let started = std::time::Instant::now();
        cancel.cancel();
        run.await.unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(1500),
            "returned after the configured wait, not when the 3 s upload finished: {:?}",
            started.elapsed()
        );
    }
}

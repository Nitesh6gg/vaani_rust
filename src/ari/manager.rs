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

use super::{Ari, Event, uuid_v4};
use crate::{
    agent::handler,
    callagent::{self, CallInfo},
    config::Config,
    dograh::Store,
    media::{call as media_call, ports::PortAllocator},
    metrics,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, atomic::Ordering::Relaxed},
    time::Duration,
};
use tokio::{net::TcpListener, sync::mpsc};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

/// How long to wait for Asterisk to open the AudioSocket TCP connection after the bridge is up.
const ACCEPT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct Res {
    port: Option<u16>,
    listener: Option<TcpListener>,
    bridge: Option<String>,
    external_requested: bool,
    counted_active: bool,
    ended: bool,
}

struct Call {
    info: CallInfo,
    external_id: String,
    cancel: CancellationToken,
    res: Mutex<Res>,
}

pub struct Manager {
    ari: Ari,
    cfg: Arc<Config>,
    ports: PortAllocator,
    store: Store,
    /// Keyed by BOTH channel ids, so a hangup of either leg finds the call.
    calls: Mutex<HashMap<String, Arc<Call>>>,
    tracker: TaskTracker,
}

impl Manager {
    pub fn new(ari: Ari, cfg: Arc<Config>, store: Store) -> Arc<Self> {
        let ports = PortAllocator::new(cfg.media_port_base, cfg.media_port_count);
        Arc::new(Self { ari, cfg, ports, store, calls: Mutex::new(HashMap::new()), tracker: TaskTracker::new() })
    }

    /// Dispatches events until `cancel`, then tears every call down and waits
    /// (bounded) for the calls' cleanup.
    pub async fn run(self: Arc<Self>, mut events: mpsc::UnboundedReceiver<Event>, cancel: CancellationToken) {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                ev = events.recv() => match ev {
                    Some(ev) => self.on_event(ev),
                    None => break,
                },
            }
        }
        self.teardown_all();
        self.tracker.close();
        if tokio::time::timeout(Duration::from_secs(10), self.tracker.wait()).await.is_err() {
            tracing::warn!("shutdown: calls still cleaning up after 10s");
        }
    }

    fn on_event(self: &Arc<Self>, ev: Event) {
        match ev {
            Event::StasisStart { channel } => {
                let found = self.calls.lock().unwrap().get(&channel.id).cloned();
                match found {
                    // The externalMedia leg arriving: complete the bridge.
                    Some(call) if call.external_id == channel.id => {
                        let m = self.clone();
                        self.tracker.spawn(async move { m.complete_bridge(call).await });
                    }
                    Some(_) => {} // the caller leg again; nothing to do
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

    fn start_call(self: &Arc<Self>, channel: super::Channel) {
        let external_id = format!("vaani-ext-{}", &uuid_v4()[..13]);
        let call = Arc::new(Call {
            info: CallInfo { id: channel.id.clone(), caller_number: channel.caller_number(), called_number: channel.called_number() },
            external_id: external_id.clone(),
            cancel: CancellationToken::new(),
            res: Mutex::new(Res::default()),
        });
        {
            // Registered under both ids before anything else runs, so a hangup
            // during setup can always find (and clean up) the call.
            let mut calls = self.calls.lock().unwrap();
            calls.insert(channel.id.clone(), call.clone());
            calls.insert(external_id, call.clone());
        }
        metrics::CALLS_STARTED.fetch_add(1, Relaxed);

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
        self.ari.answer(id).await.context("answer")?;

        let port = self.ports.alloc().context("no free media port")?;
        call.res.lock().unwrap().port = Some(port);
        metrics::MEDIA_PORTS_IN_USE.store(self.ports.in_use() as u64, Relaxed);

        // Listen BEFORE telling Asterisk where to connect (invariant #9).
        let listener = TcpListener::bind(("0.0.0.0", port)).await.with_context(|| format!("bind media port {port}"))?;
        call.res.lock().unwrap().listener = Some(listener);

        call.res.lock().unwrap().external_requested = true;
        self.ari
            .external_media_audiosocket(&call.external_id, &format!("{}:{port}", self.cfg.media_ip), &uuid_v4())
            .await
            .context("externalMedia")?;

        tracing::info!(call_id = %id, event = "call.answered", external_id = %call.external_id, port,
            caller_number = %call.info.caller_number, called_number = %call.info.called_number, direction = "inbound",
            "call answered, media socket listening, externalMedia staged");
        Ok(())
    }

    /// The externalMedia channel entered Stasis: bridge the pair and run the call.
    async fn complete_bridge(self: Arc<Self>, call: Arc<Call>) {
        let id = &call.info.id;
        let bridge = format!("vaani-br-{}", &uuid_v4()[..13]);
        let res = async {
            use anyhow::Context;
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
        tracing::info!(call_id = %id, event = "call.bridged", external_id = %call.external_id, bridge = %bridge, "call bridged");

        self.run_media(&call).await;
        self.teardown(&call);
    }

    async fn run_media(&self, call: &Arc<Call>) {
        let id = call.info.id.clone();
        let Some(listener) = call.res.lock().unwrap().listener.take() else { return };

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
                return;
            }
        };
        let prepared = match prepared {
            Ok(p) => p,
            // Without its workflow the agent has no prompt and nothing to say.
            Err(e) => {
                tracing::error!(call_id = %id, component = "dograh", event = "workflow.load_failed", error = %format!("{e:#}"), "agent: loading the dograh workflow failed; hanging up");
                return;
            }
        };
        if call.cancel.is_cancelled() {
            return;
        }

        let media = media_call::spawn(id.clone(), stream, self.cfg.media_dead_timeout, call.cancel.child_token());
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
        let (agent_cfg, io) = prepared.into_agent(&self.cfg, &id, media, hangup);
        let started = std::time::Instant::now();
        let summary = handler::run(agent_cfg, io, call.cancel.clone()).await;

        tracing::info!(call_id = %id, event = "call.summary", duration_s = started.elapsed().as_secs(), turns = summary.turns,
            interruptions = summary.interruptions, false_interruptions = summary.false_interruptions,
            errors = ?summary.errors, end_reason = %summary.end_reason,
            packets_in = stats.packets_in.load(Relaxed), packets_out = stats.packets_out.load(Relaxed),
            silence_out = stats.silence_out.load(Relaxed), inbound_dropped = stats.inbound_dropped.load(Relaxed), "call summary");
    }

    /// Idempotent: cancels the call and releases what it holds, each resource
    /// taken exactly once, so a second StasisEnd (the other leg) or a racing
    /// setup can't double-free a port or delete a bridge twice.
    fn teardown(self: &Arc<Self>, call: &Arc<Call>) {
        call.cancel.cancel();
        let (port, bridge, ext, active, first) = {
            let mut r = call.res.lock().unwrap();
            let first = !r.ended;
            r.ended = true;
            r.listener = None; // closes the socket if the media plane never took it
            (r.port.take(), r.bridge.take(), std::mem::take(&mut r.external_requested), std::mem::take(&mut r.counted_active), first)
        };
        {
            let mut calls = self.calls.lock().unwrap();
            calls.remove(&call.info.id);
            calls.remove(&call.external_id);
        }
        if let Some(p) = port {
            self.ports.free(p);
            metrics::MEDIA_PORTS_IN_USE.store(self.ports.in_use() as u64, Relaxed);
        }
        if active {
            metrics::CALLS_ACTIVE.fetch_sub(1, Relaxed);
        }
        if first {
            tracing::debug!(call_id = %call.info.id, event = "call.teardown", "tearing down call");
        }
        if bridge.is_none() && !ext && !first {
            return;
        }
        let (m, call) = (self.clone(), call.clone());
        self.tracker.spawn(async move {
            // Hangups are routine 404s when the far end beat us to it: debug-level.
            // A bridge that survives holds Asterisk resources with no event that
            // would ever clean it up: that one warns.
            if let Some(b) = bridge && let Err(e) = m.ari.delete_bridge(&b).await {
                tracing::warn!(call_id = %call.info.id, error = %e, "failed to delete bridge");
            }
            if let Err(e) = m.ari.hangup(&call.info.id).await {
                tracing::debug!(call_id = %call.info.id, error = %e, "hangup failed");
            }
            if ext && let Err(e) = m.ari.hangup(&call.external_id).await {
                tracing::debug!(call_id = %call.info.id, error = %e, "hangup of externalMedia channel failed");
            }
        });
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

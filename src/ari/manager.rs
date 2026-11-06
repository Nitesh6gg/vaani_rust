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
    agent::{calllog, extract, handler, recorder},
    callagent::{self, CallInfo},
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
        recording_bytes = s.recording_pcm.len(), stt_audio_s = (s.stt_seconds * 10.0).round() / 10.0,
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

        let ended = self.run_media(&call).await;
        self.teardown(&call);
        if let Some(end) = ended {
            self.record_run(id, end).await;
        }
    }

    async fn run_media(&self, call: &Arc<Call>) -> Option<CallEnd> {
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
        let run = {
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
        let (summary, finisher) = handler::run(agent_cfg, io, call.cancel.clone()).await;
        let duration = started.elapsed();

        log_summary(&id, duration, &summary, &stats);
        Some(CallEnd { meta, run, summary, finisher, duration })
    }

    /// Completes the call's run in Dograh: conversation, usage, disposition, and
    /// the recording and transcript uploaded to MinIO. Failures are logged; the
    /// call itself is never affected. Runs AFTER teardown, so a slow upload never holds the call's port.
    async fn record_run(&self, id: &str, mut end: CallEnd) {
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
        tracing::info!(call_id = %id, component = "dograh", event = "run.created", run_id, workflow_id = end.meta.workflow_id, "dograh run created");

        if end.summary.recording_truncated {
            tracing::warn!(call_id = %id, component = "media", event = "recording.truncated", "call recording hit its size cap; the end of the call is missing from it");
        }
        let recording = if end.summary.recording_pcm.is_empty() { vec![] } else { recorder::wav(&end.summary.recording_pcm) };
        let recording_bytes = recording.len();
        let storage = self.cfg.storage();
        let run_end = RunEnd { summary: &end.summary, duration: end.duration, recording };
        match self.store.finish_run(&end.meta, run_id, run_end, storage.as_ref()).await {
            Ok(()) => tracing::info!(call_id = %id, component = "dograh", event = "run.completed", run_id,
                disposition = end.summary.end_reason.unwrap_or(calllog::USER_HANGUP), events = end.summary.events.len(),
                recording_bytes, minio = storage.is_some(), "dograh run completed"),
            Err(e) => {
                tracing::error!(call_id = %id, component = "dograh", event = "run.complete_failed", run_id, error = %format!("{e:#}"), "dograh run: completing it failed")
            }
        }
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
                CREATE TABLE organization_configurations (id serial PRIMARY KEY, organization_id bigint, key text, value jsonb);
                INSERT INTO organization_configurations (organization_id, key, value) VALUES (3, 'DISPOSITION_CODE_MAPPING', '{{"user_hangup": "HU"}}');
                CREATE TABLE workflow_definitions (id serial PRIMARY KEY, workflow_id int, workflow_json jsonb, template_context_variables jsonb, workflow_configurations jsonb, is_current bool);
                CREATE TABLE user_configurations (id serial PRIMARY KEY, user_id int, configuration jsonb);
                CREATE TABLE tools (tool_uuid uuid, organization_id int, name text, description text, category text, status text, definition jsonb);
                CREATE TABLE external_credentials (credential_uuid varchar, organization_id int, credential_type text, credential_data jsonb, is_active bool);
                INSERT INTO user_configurations (user_id, configuration) VALUES (7, '{{
                  "llm": {{"provider":"speaches","base_url":"{llm_url}","api_key":"k","model":"m"}},
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
        ev_tx.send(Event::StasisStart { channel: caller }).unwrap();
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
        ev_tx.send(Event::StasisStart { channel: channel(ext["channelId"].as_str().unwrap()) }).unwrap();
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
        let puts = minio_puts.lock().unwrap().clone();
        let wav = puts.iter().find(|(p, _)| p == &format!("/voice-audio/recordings/{run_id}.wav")).expect("the recording upload");
        assert_eq!(&wav.1[..4], b"RIFF");
        assert!(wav.1.len() > 44 + FRAME_SIZE * 10, "the recording holds the call's audio");
        let transcript = puts.iter().find(|(p, _)| p == &format!("/voice-audio/transcripts/{run_id}.txt")).expect("the transcript upload");
        assert!(String::from_utf8_lossy(&transcript.1).contains("Agent: Namaste, how can I help?"));

        cancel.cancel();
        run.await.unwrap();
    }
}

//! Outbound calls: `Manager::dial` originates a call and reports how it ended through
//! `DialRequest::done`. Like a transfer leg, a dialed channel enters the Stasis app only
//! once the callee answers, so from there the call follows the inbound path
//! (`start_call`: media socket bound before Asterisk is told its address, externalMedia,
//! bridge, agent).

use super::{Channel, manager::Manager, uuid_v4};
use crate::dograh::outbound::{STATUS_FAILED, STATUS_NO_ANSWER, status_for_cause};
use anyhow::{Context, Result};
use serde_json::{Map, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::task::AbortHandle;

/// The first Stasis argument of a channel Vaani dialed.
pub const OUTBOUND_APP_ARG: &str = "outbound";

/// Added to the ring timeout for our own watchdog, so Asterisk's timeout (which ends the
/// ringing channel with a clear hangup cause) normally fires first. The watchdog is for
/// events that never arrive, such as ones lost while the ARI WebSocket was down.
#[cfg(not(test))]
const ANSWER_SLACK: Duration = Duration::from_secs(5);
#[cfg(test)]
const ANSWER_SLACK: Duration = Duration::from_millis(100);

/// How a dialed call ended up. `done` gets exactly one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Outcome {
    /// The callee answered and the agent ran the call; its Dograh run is completed by the
    /// agent's own record. Otherwise the call never connected and `status` says why (busy,
    /// no-answer, rejected, failed), for the caller to complete the run.
    pub connected: bool,
    pub status: &'static str,
    /// Asterisk's hangup cause, if there was one.
    pub cause: i64,
}

impl Outcome {
    fn unanswered(status: &'static str, cause: i64) -> Self {
        Self { connected: false, status, cause }
    }
}

pub type Done = Box<dyn FnOnce(Outcome) + Send>;

/// An outbound call to place.
pub struct DialRequest {
    /// Asterisk dial string, e.g. PJSIP/9876543210@trunk.
    pub endpoint: String,
    pub caller_id: String,
    /// The dialed number, as the callee is known to Dograh.
    pub called: String,
    pub ring_timeout: Duration,
    /// The call's Dograh workflow and its run, which the caller created before dialing.
    pub workflow_id: i32,
    pub run_id: i64,
    /// The contact's own template variables.
    pub variables: Map<String, Value>,
    /// Called once when the call is over or never connected.
    pub done: Done,
}

#[derive(Default)]
struct State {
    answered: bool,
    agent_started: bool,
    watchdog: Option<AbortHandle>,
}

/// A dialed call from origination until it is over.
pub struct OutboundCall {
    pub caller_id: String,
    pub called: String,
    pub workflow_id: i32,
    pub run_id: i64,
    pub variables: Map<String, Value>,
    done: Mutex<Option<Done>>,
    state: Mutex<State>,
}

impl OutboundCall {
    fn finish(&self, out: Outcome) {
        if let Some(done) = self.done.lock().unwrap().take() {
            done(out);
        }
    }

    /// The agent is on the call: from here the call is "connected" when it ends.
    pub fn agent_started(&self) {
        self.state.lock().unwrap().agent_started = true;
    }
}

impl Manager {
    /// Originates the call and returns its channel id. The result arrives through `req.done`. An error
    /// means nothing was dialed and `done` won't be called.
    pub async fn dial(self: &Arc<Self>, req: DialRequest) -> Result<String> {
        let id = format!("vaani-out-{}", &uuid_v4()[..13]);
        let oc = Arc::new(OutboundCall {
            caller_id: req.caller_id.clone(),
            called: req.called.clone(),
            workflow_id: req.workflow_id,
            run_id: req.run_id,
            variables: req.variables,
            done: Mutex::new(Some(req.done)),
            state: Mutex::default(),
        });
        self.outbound.lock().unwrap().insert(id.clone(), oc.clone());

        let app_args = format!("{OUTBOUND_APP_ARG},{}", req.run_id);
        if let Err(e) = self.ari.originate(&req.endpoint, &app_args, req.ring_timeout.as_secs(), &id, &req.caller_id).await {
            self.outbound.lock().unwrap().remove(&id);
            return Err(e).with_context(|| format!("could not dial {}", req.endpoint));
        }

        {
            let mut st = oc.state.lock().unwrap();
            if !st.answered {
                let (m, wid) = (self.clone(), id.clone());
                let h = self.tracker.spawn(async move {
                    tokio::time::sleep(req.ring_timeout + ANSWER_SLACK).await;
                    m.outbound_timed_out(&wid);
                });
                st.watchdog = Some(h.abort_handle());
            }
        }
        tracing::info!(call_id = %id, event = "outbound.dialing", endpoint = %req.endpoint, called_number = %req.called, caller_id = %req.caller_id,
            workflow_id = req.workflow_id, run_id = req.run_id, ring_timeout_s = req.ring_timeout.as_secs(), "outbound call dialing");
        Ok(id)
    }

    /// The dialed call on channel `id`, if any.
    pub(super) fn outbound_for(&self, id: &str) -> Option<Arc<OutboundCall>> {
        self.outbound.lock().unwrap().get(id).cloned()
    }

    /// Removes and returns the dialed call on channel `id` if it has not been answered; `None` if it's
    /// not ours, answered, or already taken.
    fn take_ringing(&self, id: &str) -> Option<Arc<OutboundCall>> {
        let mut out = self.outbound.lock().unwrap();
        let oc = out.get(id)?.clone();
        let mut st = oc.state.lock().unwrap();
        if st.answered {
            return None;
        }
        if let Some(w) = st.watchdog.take() {
            w.abort();
        }
        drop(st);
        out.remove(id);
        Some(oc)
    }

    /// The StasisStart of a channel we dialed: the callee answered.
    pub(super) fn on_outbound_answered(self: &Arc<Self>, channel: Channel) {
        let id = channel.id.clone();
        let oc = self.outbound_for(&id);
        if let Some(oc) = &oc {
            let mut st = oc.state.lock().unwrap();
            st.answered = true;
            if let Some(w) = st.watchdog.take() {
                w.abort();
            }
        }
        match oc {
            // Answered after we gave up on it (timeout, restart): nobody wants it.
            None => {
                tracing::warn!(call_id = %id, event = "outbound.orphan", "outbound leg answered but no call is waiting; hanging it up");
                let (ari, id) = (self.ari.clone(), id);
                self.tracker.spawn(async move {
                    let _ = ari.hangup(&id).await;
                });
            }
            Some(oc) => {
                tracing::info!(call_id = %id, event = "outbound.answered", called_number = %oc.called, "outbound call answered");
                self.start_call(channel);
            }
        }
    }

    /// A dialed call that ended without being answered, with the status its hangup cause stands for.
    /// Asterisk sends every app the destroyed events of every channel, so only channels in `outbound` count.
    pub(super) fn on_outbound_destroyed(&self, id: &str, cause: i64, cause_txt: &str) {
        let Some(oc) = self.take_ringing(id) else { return };
        let status = status_for_cause(cause);
        tracing::info!(call_id = %id, event = "outbound.unanswered", status, cause, cause_txt, called_number = %oc.called, "outbound call not answered");
        oc.finish(Outcome::unanswered(status, cause));
    }

    /// The ring timeout passed and Asterisk never reported.
    fn outbound_timed_out(self: &Arc<Self>, id: &str) {
        let Some(oc) = self.take_ringing(id) else { return };
        tracing::warn!(call_id = %id, event = "outbound.timeout", called_number = %oc.called, "outbound call timed out without a result; hanging it up");
        let (ari, id) = (self.ari.clone(), id.to_string());
        self.tracker.spawn(async move {
            let _ = ari.hangup(&id).await;
        });
        oc.finish(Outcome::unanswered(STATUS_NO_ANSWER, 0));
    }

    /// The end of an answered outbound call (from teardown): connected if the agent was on it, failed if
    /// the media setup never got that far (the callee heard nothing).
    pub(super) fn finish_outbound(&self, call_id: &str, oc: &OutboundCall) {
        self.outbound.lock().unwrap().remove(call_id);
        if oc.state.lock().unwrap().agent_started {
            oc.finish(Outcome { connected: true, status: "", cause: 0 });
        } else {
            oc.finish(Outcome::unanswered(STATUS_FAILED, 0));
        }
    }

    /// Hangs up every dialed call still ringing (shutdown).
    pub(super) fn hangup_ringing_outbound(self: &Arc<Self>) {
        let ids: Vec<String> =
            self.outbound.lock().unwrap().iter().filter(|(_, oc)| !oc.state.lock().unwrap().answered).map(|(id, _)| id.clone()).collect();
        for id in ids {
            if let Some(oc) = self.take_ringing(&id) {
                let (ari, hid) = (self.ari.clone(), id);
                self.tracker.spawn(async move {
                    let _ = ari.hangup(&hid).await;
                });
                oc.finish(Outcome::unanswered(STATUS_FAILED, 0));
            }
        }
    }
}

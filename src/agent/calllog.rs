//! A call as Dograh's call history shows it: the conversation, node
//! transitions and function calls as `realtime_feedback_events`, the nodes
//! visited, why the call ended, and what it used (tokens, TTS characters, STT
//! audio). Owned by the agent task, so no locking.

use crate::ai::llm::Usage;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::{collections::HashMap, time::Instant};

/// Dograh's end-of-call reasons (pipecat EndTaskReason) Vaani can produce;
/// saved as the run's call_disposition. The first one reported wins.
pub const END_NODE: &str = "user_qualified"; // the End node's closing line played
pub const END_CALL_TOOL: &str = "end_call_tool"; // the LLM called end_call
pub const CALLER_SILENT: &str = "user_idle_max_duration_exceeded"; // caller silent twice
pub const MAX_DURATION: &str = "call_duration_exceeded"; // max_call_duration reached
pub const LLM_DOWN: &str = "llm_unavailable"; // the LLM failed several turns in a row
pub const USER_HANGUP: &str = "user_hangup"; // none of the above: the caller hung up

/// Dograh's realtime feedback event types, as its run page reads them.
const USER_TRANSCRIPTION: &str = "rtf-user-transcription";
const BOT_TEXT: &str = "rtf-bot-text";
const NODE_TRANSITION: &str = "rtf-node-transition";
const FUNCTION_START: &str = "rtf-function-call-start";
const FUNCTION_END: &str = "rtf-function-call-end";
// Turn timing that QA's per-call metrics read (qa/metrics.py compute_call_metrics).
const LATENCY: &str = "rtf-latency-measured";
const TTFB: &str = "rtf-ttfb-metric";

/// One entry of Dograh's realtime_feedback_events (InMemoryLogsBuffer's shape).
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct LogEvent {
    #[serde(rename = "type")]
    pub kind: String,
    pub payload: Map<String, Value>,
    pub timestamp: String,
    pub turn: u32,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub node_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub node_name: String,
}

/// Python's `isoformat()` on a UTC datetime, which is what Dograh stores: the
/// event's own timestamp with microseconds, a turn's with milliseconds.
fn event_time(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string()
}
fn turn_time(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%dT%H:%M:%S%.3f+00:00").to_string()
}
/// The wall-clock time a past `Instant` corresponds to.
fn wall(at: Instant) -> DateTime<Utc> {
    Utc::now() - chrono::Duration::from_std(at.elapsed()).unwrap_or_default()
}

/// Dograh's _safe_parse_timestamp/_event_sort_key: the payload's (turn)
/// timestamp, else the event's own, compared at the millisecond -- the turn
/// stamps' precision -- so events sharing a millisecond keep the order they
/// were logged in.
pub fn event_sort_time(e: &LogEvent) -> i64 {
    let ts = e.payload.get("timestamp").and_then(Value::as_str).filter(|t| !t.is_empty()).unwrap_or(&e.timestamp);
    DateTime::parse_from_rfc3339(ts).map(|t| t.timestamp_millis()).unwrap_or(0)
}

#[derive(Default)]
pub struct CallLog {
    events: Vec<LogEvent>,
    turn: u32,
    node: Option<(String, String)>, // (id, name)
    visited: Vec<String>,
    reason: Option<&'static str>,
    user_spoke: bool,
    pub agent_replies: u32,
    pub barge_ins: u32,
    pub pauses: u32,
    pub false_interruptions: u32,
    pub held_speech: u32,
    pub held_delivered: u32,
    pub errors: HashMap<String, u32>,
    pub reply_latencies_ms: Vec<u64>,
    pub llm_usage: HashMap<String, Usage>,
    pub tts_chars: u64,
    pub stt_seconds: f64,
}

/// Everything a call recorded, for Dograh's run and the summary log line.
#[derive(Default)]
pub struct Summary {
    pub events: Vec<LogEvent>,
    pub nodes_visited: Vec<String>,
    /// `None` until something ended the call: then it's a caller hangup.
    pub end_reason: Option<&'static str>,
    pub user_spoke: bool,
    pub user_turns: u32,
    pub agent_replies: u32,
    pub barge_ins: u32,
    pub pauses: u32,
    pub false_interruptions: u32,
    pub held_speech: u32,
    pub held_delivered: u32,
    pub errors: HashMap<String, u32>,
    pub reply_latencies_ms: Vec<u64>,
    pub llm_usage: HashMap<String, Usage>,
    pub tts_chars: u64,
    pub stt_seconds: f64,
    /// 16kHz mono PCM16 of the whole call (caller and agent mixed); empty when nothing was recorded.
    pub recording_pcm: Vec<u8>,
    pub recording_truncated: bool,
}

impl CallLog {
    fn add(&mut self, kind: &str, payload: Map<String, Value>) {
        let (node_id, node_name) = self.node.clone().unwrap_or_default();
        self.events.push(LogEvent { kind: kind.into(), payload, timestamp: event_time(Utc::now()), turn: self.turn, node_id, node_name });
    }

    fn obj(v: Value) -> Map<String, Value> {
        v.as_object().cloned().unwrap_or_default()
    }

    /// The conversation moved to node `(id, name)` (or started there).
    pub fn node_entered(&mut self, id: &str, name: &str, allow_interrupt: bool) {
        let prev = self.node.clone();
        self.add(
            NODE_TRANSITION,
            Self::obj(json!({
                "node_id": id, "node_name": name,
                "previous_node_id": prev.as_ref().map(|p| &p.0), "previous_node_name": prev.as_ref().map(|p| &p.1),
                "allow_interrupt": allow_interrupt,
            })),
        );
        self.node = Some((id.to_string(), name.to_string()));
        if !self.visited.iter().any(|v| v == name) {
            self.visited.push(name.to_string());
        }
    }

    /// A caller transcript became a turn. `started` is when the caller began
    /// saying it -- the time Dograh's transcript shows; the event's own
    /// timestamp stays when it was logged.
    pub fn user_said(&mut self, text: &str, started: Option<Instant>) {
        self.turn += 1;
        self.user_spoke |= !text.is_empty();
        let ts = turn_time(started.map(wall).unwrap_or_else(Utc::now));
        self.add(USER_TRANSCRIPTION, Self::obj(json!({"text": text, "final": true, "timestamp": ts})));
    }

    /// What the caller heard of one reply; `started` is when the reply began.
    pub fn agent_said(&mut self, text: &str, started: Option<Instant>) {
        let ts = turn_time(started.map(wall).unwrap_or_else(Utc::now));
        self.add(BOT_TEXT, Self::obj(json!({"text": text, "timestamp": ts})));
    }

    pub fn function_started(&mut self, name: &str, call_id: &str) {
        self.add(FUNCTION_START, Self::obj(json!({"function_name": name, "tool_call_id": call_id})));
    }

    pub fn function_ended(&mut self, name: &str, call_id: &str, result: &str) {
        let r = if result.is_empty() { Value::Null } else { json!(result) }; // Dograh: None for an empty result
        self.add(FUNCTION_END, Self::obj(json!({"function_name": name, "tool_call_id": call_id, "result": r})));
    }

    /// One reply's wait as the caller experienced it (Dograh's user-to-bot
    /// response latency), also kept for the summary's percentiles.
    pub fn reply_latency(&mut self, d: std::time::Duration) {
        self.reply_latencies_ms.push(d.as_millis() as u64);
        self.add(LATENCY, Self::obj(json!({"latency_seconds": d.as_secs_f64()})));
    }

    /// One turn's time to first byte for a service ("stt", "llm", "tts").
    pub fn ttfb(&mut self, service: &str, d: std::time::Duration) {
        self.add(TTFB, Self::obj(json!({"ttfb_seconds": d.as_secs_f64(), "service": service})));
    }

    /// Why the call is ending; only the first reason counts.
    pub fn ending(&mut self, reason: &'static str) {
        self.reason.get_or_insert(reason);
    }

    pub fn error(&mut self, stage: &str) {
        *self.errors.entry(stage.to_string()).or_default() += 1;
    }

    pub fn llm_used(&mut self, key: &str, u: Usage) {
        let t = self.llm_usage.entry(key.to_string()).or_default();
        t.prompt += u.prompt;
        t.completion += u.completion;
        t.total += u.total;
        t.cached += u.cached;
    }

    pub fn into_summary(self) -> Summary {
        Summary {
            events: sorted(self.events),
            nodes_visited: self.visited,
            end_reason: self.reason,
            user_spoke: self.user_spoke,
            user_turns: self.turn,
            agent_replies: self.agent_replies,
            barge_ins: self.barge_ins,
            pauses: self.pauses,
            false_interruptions: self.false_interruptions,
            held_speech: self.held_speech,
            held_delivered: self.held_delivered,
            errors: self.errors,
            reply_latencies_ms: self.reply_latencies_ms,
            llm_usage: self.llm_usage,
            tts_chars: self.tts_chars,
            stt_seconds: self.stt_seconds,
            ..Default::default()
        }
    }
}

/// Dograh's _sorted_events (what it stores, and what its transcript is built
/// from): a stable sort by `event_sort_time`, so a turn sits where it started
/// -- an interrupted reply before the caller who cut in.
fn sorted(mut events: Vec<LogEvent>) -> Vec<LogEvent> {
    events.sort_by_key(event_sort_time);
    events
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn nodes_visited_once_each_and_first_end_reason_wins() {
        let mut l = CallLog::default();
        l.node_entered("1", "Start", false);
        l.node_entered("2", "Main", true);
        l.node_entered("1", "Start", false);
        l.ending(END_CALL_TOOL);
        l.ending(MAX_DURATION);
        let s = l.into_summary();
        assert_eq!(s.nodes_visited, ["Start", "Main"]);
        assert_eq!(s.end_reason, Some(END_CALL_TOOL));
        assert_eq!(s.events[1].payload["previous_node_name"], "Start");
        assert_eq!(s.events[0].payload["previous_node_id"], Value::Null);
    }

    #[test]
    fn events_carry_turn_and_node_and_sort_by_when_the_turn_started() {
        let mut l = CallLog::default();
        l.node_entered("1", "Start", false);
        let started = Instant::now();
        std::thread::sleep(Duration::from_millis(30));
        l.user_said("नमस्ते", Some(Instant::now()));
        // The agent's reply began BEFORE the caller's utterance that cut it off, but was logged after.
        l.agent_said("welcome to", Some(started));
        let s = l.into_summary();
        let kinds: Vec<_> = s.events.iter().map(|e| (e.kind.as_str(), e.turn)).collect();
        assert_eq!(kinds, [(NODE_TRANSITION, 0), (BOT_TEXT, 1), (USER_TRANSCRIPTION, 1)]);
        assert_eq!(s.events[2].node_name, "Start");
        assert!(s.user_spoke && s.user_turns == 1);
        let j = serde_json::to_value(&s.events[1]).unwrap();
        assert!(j["timestamp"].as_str().unwrap().ends_with("+00:00") && j.get("node_id").is_some());
    }

    #[test]
    fn function_results_and_usage() {
        let mut l = CallLog::default();
        l.function_ended("f", "c1", "");
        l.llm_used("LLM|||m", Usage { prompt: 5, completion: 2, total: 7, cached: 1 });
        l.llm_used("LLM|||m", Usage { prompt: 1, completion: 1, total: 2, cached: 0 });
        assert_eq!(l.llm_usage["LLM|||m"], Usage { prompt: 6, completion: 3, total: 9, cached: 1 });
        assert_eq!(l.events[0].payload["result"], Value::Null);
    }
}

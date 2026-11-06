//! The call's workflow graph (a Dograh workflow): nodes carry their own system
//! prompt and tools; edges are offered to the LLM as parameterless functions.
//! Nodes live in a `Vec` and edges point by index, so the (cyclic) graph is a
//! plain immutable value shared by `Arc`.

use crate::ai::llm::{FunctionDef, Message};
use futures_util::future::BoxFuture;
use serde_json::Value;
use std::sync::Arc;

/// Runs a function tool with the LLM's JSON arguments; the returned JSON text
/// goes back to the LLM. An `Err` becomes a `{"status":"error"}` result.
pub type ToolRun = Arc<dyn Fn(Value) -> BoxFuture<'static, anyhow::Result<String>> + Send + Sync>;

#[derive(Clone)]
pub enum ToolKind {
    /// Result goes back to the LLM.
    Function(ToolRun),
    /// Hangs up once everything said so far has played; the LLM is not asked again.
    EndCall,
}

#[derive(Clone)]
pub struct Tool {
    pub def: FunctionDef,
    pub kind: ToolKind,
    /// Spoken before the tool acts (Dograh's customMessage). Empty = none.
    pub message: String,
}

#[derive(Clone)]
pub struct Edge {
    pub def: FunctionDef,
    /// Spoken before moving on (Dograh's transition_speech). Empty = none.
    pub speech: String,
    pub to: usize,
}

#[derive(Clone, Default)]
pub struct Node {
    pub name: String,
    /// Complete system prompt at this node (global prompt + own, variables filled).
    pub prompt: String,
    /// Start node only: spoken as-is before the caller says anything. Empty =
    /// the LLM generates the opening from `prompt`.
    pub greeting: String,
    /// Reaching it, the LLM says its closing line and the call hangs up.
    pub end: bool,
    /// May the caller cut in while the agent speaks here? (Dograh's allow_interrupt.)
    pub allow_interrupt: bool,
    pub tools: Vec<Tool>,
    pub edges: Vec<Edge>,
}

#[derive(Clone, Default)]
pub struct Workflow {
    pub nodes: Vec<Node>,
    pub start: usize,
}

impl Node {
    /// What the LLM is offered here: the node's tools, then one function per edge.
    pub fn tool_defs(&self) -> Vec<FunctionDef> {
        self.tools.iter().map(|t| t.def.clone()).chain(self.edges.iter().map(|e| e.def.clone())).collect()
    }
    pub fn edge(&self, name: &str) -> Option<&Edge> {
        self.edges.iter().find(|e| e.def.name == name)
    }
    pub fn tool(&self, name: &str) -> Option<&Tool> {
        self.tools.iter().find(|t| t.def.name == name)
    }
}

/// What the LLM is sent at `node`: its system prompt, then the conversation.
pub fn request_messages(node: &Node, history: &[Message]) -> Vec<Message> {
    if node.prompt.is_empty() {
        return history.to_vec();
    }
    let mut msgs = vec![Message::new("system", node.prompt.clone())];
    if history.is_empty() {
        // Opening turn: Gemini needs at least one non-system message, so the
        // prompt is repeated as the user message (what pipecat does too).
        msgs.push(Message::new("user", node.prompt.clone()));
    }
    msgs.extend_from_slice(history);
    msgs
}

/// For a node with no tools: tool calls/results rendered as plain text the way
/// Dograh does ("[Tool Response: name]"), since a provider may reject tool
/// messages when no tools are offered. Only the request changes, never history.
pub fn plain_history(msgs: &[Message]) -> Vec<Message> {
    use std::collections::HashMap;
    let names: HashMap<&str, &str> =
        msgs.iter().flat_map(|m| m.tool_calls.iter()).map(|c| (c.id.as_str(), c.function.name.as_str())).collect();
    let mut out: Vec<Message> = vec![];
    fn add(out: &mut Vec<Message>, role: &str, text: String) {
        if text.is_empty() {
            return;
        }
        match out.last_mut() {
            Some(last) if role == "assistant" && last.role == "assistant" => {
                last.content.push_str("\n\n");
                last.content.push_str(&text);
            }
            _ => out.push(Message::new(role, text)),
        }
    }
    for m in msgs {
        match m.role.as_str() {
            "tool" => {
                let name = names.get(m.tool_call_id.as_str()).copied().unwrap_or("unknown");
                if let Some(text) = tool_response_text(&m.content) {
                    add(&mut out, "assistant", format!("[Tool Response: {name}]\n{text}"));
                }
            }
            "assistant" => add(&mut out, "assistant", m.content.clone()), // its tool_calls are dropped
            role => out.push(Message::new(role, m.content.clone())),
        }
    }
    out
}

/// Dograh's _format_tool_response: a JSON object's "data" if present, else the
/// object without status/status_code; anything else as-is; cut at 2000 chars.
/// `None` for an edge transition's `{"status":"done"}`.
fn tool_response_text(raw: &str) -> Option<String> {
    let mut text = raw.to_string();
    if let Ok(Value::Object(mut obj)) = serde_json::from_str::<Value>(raw) {
        if obj.len() == 1 && obj.get("status") == Some(&Value::String("done".into())) {
            return None;
        }
        text = match obj.remove("data") {
            Some(Value::String(s)) => s,
            Some(d) => d.to_string(),
            None => {
                obj.remove("status");
                obj.remove("status_code");
                Value::Object(obj).to_string()
            }
        };
    }
    Some(text.chars().take(2000).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::llm::{FunctionCall, ToolCall};

    #[test]
    fn opening_turn_repeats_the_prompt_as_a_user_message() {
        let n = Node { prompt: "be nice".into(), ..Default::default() };
        let m = request_messages(&n, &[]);
        assert_eq!((m[0].role.as_str(), m[1].role.as_str()), ("system", "user"));
        let m = request_messages(&n, &[Message::new("user", "hi")]);
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn plain_history_renders_tool_results_and_drops_transitions() {
        let call = |id: &str, name: &str| ToolCall {
            id: id.into(),
            function: FunctionCall { name: name.into(), arguments: "{}".into() },
            ..Default::default()
        };
        let mut a = Message::new("assistant", "one sec");
        a.tool_calls = vec![call("1", "lookup"), call("2", "go_next")];
        let mut t1 = Message::new("tool", r#"{"status":"success","data":{"city":"Pune"}}"#);
        t1.tool_call_id = "1".into();
        let mut t2 = Message::new("tool", r#"{"status":"done"}"#);
        t2.tool_call_id = "2".into();
        let out = plain_history(&[Message::new("user", "hi"), a, t1, t2]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[1].role, "assistant");
        assert_eq!(out[1].content, "one sec\n\n[Tool Response: lookup]\n{\"city\":\"Pune\"}");
    }
}

//! Variable extraction, ported from Dograh's
//! api/services/workflow/pipecat_engine_variable_extractor.py and the engine's
//! _perform_variable_extraction_if_needed: when the conversation leaves a node
//! with extraction on, the call's own LLM reads the conversation so far and
//! returns the node's variables as JSON (in the background); when the call ends,
//! after those finish, the same is done for the node it ended at. The results go
//! to the call's record (`gathered_context`).

use super::workflow::{Extraction, tool_response_text};
use crate::ai::llm::{LlmClient, Message, Usage};
use regex::Regex;
use serde_json::{Map, Value};
use std::{collections::HashMap, sync::LazyLock, time::Duration};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Bounds one extraction, and how long the end of the call waits for those still
/// running -- Dograh waits 30s for them.
pub const TIMEOUT: Duration = Duration::from_secs(30);

/// Dograh's extraction system prompt, word for word.
const SYSTEM_PROMPT: &str = "You are an assistant tasked with extracting structured data from the conversation. \
Return ONLY a valid JSON object with the requested variables as top-level keys. Do not wrap the JSON in markdown.";

/// What one extraction returned: the variables, in the order the LLM wrote them
/// (Dograh's call tags depend on it), and the tokens it used.
#[derive(Debug, Clone, Default)]
pub struct Extracted {
    pub vars: Map<String, Value>,
    pub usage: Option<Usage>,
}

/// Dograh's two extraction messages for `x` over `history`.
pub fn messages(x: &Extraction, history: &[Message]) -> Vec<Message> {
    let mut system = SYSTEM_PROMPT.to_string();
    if !x.prompt.is_empty() {
        system.push_str("\n\n");
        system.push_str(&x.prompt);
    }
    let vars: Vec<String> = x
        .vars
        .iter()
        // As Dograh's Python 3.12 renders f"- {v.name} ({v.type}): {v.prompt}": the
        // type is an Enum member, printed "VariableType.string", and a missing
        // prompt prints "None".
        .map(|v| format!("- {} (VariableType.{}): {}", v.name, v.kind, v.prompt.as_deref().unwrap_or("None")))
        .collect();
    let user = format!("\n\nVariables to extract:\n{}\n\nConversation history:\n{}", vars.join("\n"), conversation_text(history));
    vec![Message::new("system", system), Message::new("user", user)]
}

/// Dograh's _build_conversation_history: "user: ..." and "assistant: ..." lines,
/// and each tool result as "[Tool Response: name]" plus its data -- except edge
/// transitions' {"status":"done"}.
fn conversation_text(history: &[Message]) -> String {
    let names: HashMap<&str, &str> =
        history.iter().flat_map(|m| m.tool_calls.iter()).map(|c| (c.id.as_str(), c.function.name.as_str())).collect();
    let mut lines = vec![];
    for m in history {
        match m.role.as_str() {
            "user" | "assistant" if !m.content.is_empty() => lines.push(format!("{}: {}", m.role, m.content)),
            "tool" => {
                if let Some(text) = tool_response_text(&m.content) {
                    lines.push(format!("[Tool Response: {}]\n{text}", names.get(m.tool_call_id.as_str()).copied().unwrap_or("unknown")));
                }
            }
            _ => {}
        }
    }
    lines.join("\n")
}

static CODE_BLOCK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"```(?:json)?\s*([\s\S]*?)\s*```").unwrap());

/// Dograh's parse_llm_json (api/services/gen_ai/json_parser.py): the reply as
/// JSON, else the JSON inside a ``` block, else the first {...} or [...] in it;
/// failing all that, `{"raw": reply}`. Never errors.
pub fn parse_llm_json(reply: &str) -> Value {
    let s = reply.trim();
    if s.is_empty() {
        return Value::Object(Map::new());
    }
    if let Some(v) = container(s) {
        return v;
    }
    if let Some(m) = CODE_BLOCK.captures(s)
        && let Some(v) = container(m[1].trim())
    {
        return v;
    }
    for open in ['{', '['] {
        if let Some(i) = s.find(open)
            && let Some(Ok(v)) = serde_json::Deserializer::from_str(&s[i..]).into_iter::<Value>().next()
        {
            return v;
        }
    }
    serde_json::json!({ "raw": reply })
}

/// `s` if it is exactly one JSON object or array.
fn container(s: &str) -> Option<Value> {
    (s.starts_with('{') || s.starts_with('[')).then(|| serde_json::from_str(s).ok()).flatten()
}

/// Runs `x` over `history` with the call's LLM. Only a JSON object counts, as in Dograh.
pub async fn extract(call_id: &str, node: &str, llm: &LlmClient, x: &Extraction, history: &[Message]) -> Option<Extracted> {
    let started = std::time::Instant::now();
    let mut reply = String::new();
    // Not the turn's context: an interruption or the call ending must not lose what the caller already answered.
    let cancel = CancellationToken::new();
    let out = match tokio::time::timeout(TIMEOUT, llm.stream(&cancel, &messages(x, history), &[], &mut |t| reply.push_str(t))).await {
        Ok(Ok(out)) => out,
        Ok(Err(e)) => {
            tracing::warn!(call_id, node, error = %e, "variable extraction failed");
            return None;
        }
        Err(_) => {
            tracing::warn!(call_id, node, "variable extraction timed out");
            return None;
        }
    };
    let Value::Object(vars) = parse_llm_json(&reply) else {
        tracing::warn!(call_id, node, reply = %reply.chars().take(200).collect::<String>(), "variable extraction returned no JSON object; skipped");
        return None;
    };
    tracing::info!(call_id, node, duration_ms = started.elapsed().as_millis() as u64, variables = %serde_json::Value::Object(vars.clone()), "variables extracted");
    Some(Extracted { vars, usage: out.usage })
}

/// An extraction running in the background, started as the conversation left a node.
pub type Pending = JoinHandle<Option<Extracted>>;

/// What the agent hands back for the end of the call (Dograh's
/// end_call_with_reason): the extractions still running, and the node the call
/// ended at, whose extraction runs over the whole conversation. It runs AFTER
/// the call is torn down, so a slow LLM never holds the call's port.
pub struct Finisher {
    pub call_id: String,
    pub llm: LlmClient,
    pub pending: Vec<Pending>,
    /// `(node name, its extraction, the whole conversation)`.
    pub last: Option<(String, Extraction, Vec<Message>)>,
}

impl Finisher {
    /// Waits (bounded) for the running extractions, then runs the last node's, and
    /// merges every result -- later ones winning -- into `summary`.
    pub async fn finish(self, summary: &mut super::calllog::Summary) {
        let mut results: Vec<Extracted> = vec![];
        let waited = tokio::time::timeout(TIMEOUT, async {
            for h in self.pending {
                if let Ok(Some(e)) = h.await {
                    results.push(e);
                }
            }
        })
        .await;
        if waited.is_err() {
            tracing::warn!(call_id = %self.call_id, "variable extraction still running at the end of the call; not waiting");
        } else if let Some((node, x, history)) = &self.last {
            results.extend(extract(&self.call_id, node, &self.llm, x, history).await);
        }
        for e in results {
            summary.merge_extracted(e.vars);
            if let Some(u) = e.usage {
                summary.add_llm_usage("LLM|||", &self.llm.model, u);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        agent::workflow::ExtractionVar,
        ai::llm::{FunctionCall, ToolCall},
    };
    use serde_json::json;

    #[test]
    fn the_prompt_matches_dograhs_rendering() {
        let x = Extraction {
            prompt: "Pull the facts.".into(),
            vars: vec![
                ExtractionVar { name: "party".into(), kind: "string".into(), prompt: Some("Which party?".into()) },
                ExtractionVar { name: "age".into(), kind: "number".into(), prompt: None },
            ],
        };
        let mut a = Message::new("assistant", "");
        a.tool_calls = vec![ToolCall {
            id: "c1".into(),
            function: FunctionCall { name: "lookup".into(), arguments: "{}".into() },
            ..Default::default()
        }];
        let mut t = Message::new("tool", r#"{"status":"success","data":{"city":"Pune"}}"#);
        t.tool_call_id = "c1".into();
        let mut done = Message::new("tool", r#"{"status":"done"}"#);
        done.tool_call_id = "c2".into();
        let history = [Message::new("user", "BJP"), a, t, done, Message::new("assistant", "Thanks.")];
        let m = messages(&x, &history);
        assert_eq!(m[0].content, format!("{SYSTEM_PROMPT}\n\nPull the facts."));
        assert_eq!(
            m[1].content,
            "\n\nVariables to extract:\n- party (VariableType.string): Which party?\n- age (VariableType.number): None\n\n\
             Conversation history:\nuser: BJP\n[Tool Response: lookup]\n{\"city\":\"Pune\"}\nassistant: Thanks."
        );
    }

    #[test]
    fn llm_json_is_found_wherever_the_model_put_it() {
        assert_eq!(parse_llm_json(r#"{"a": 1}"#), json!({"a": 1}));
        assert_eq!(parse_llm_json("Sure!\n```json\n{\"a\": 2}\n```\nDone"), json!({"a": 2}));
        assert_eq!(parse_llm_json("The answer is {\"a\": 3} ok"), json!({"a": 3}));
        assert_eq!(parse_llm_json("list: [1, 2]"), json!([1, 2]));
        assert_eq!(parse_llm_json("no json here"), json!({"raw": "no json here"}));
        assert_eq!(parse_llm_json("  "), json!({}));
    }

    #[test]
    fn keys_keep_the_order_the_llm_wrote_them() {
        let Value::Object(m) = parse_llm_json(r#"{"zebra": 1, "apple": 2, "mango": 3}"#) else { panic!() };
        assert_eq!(m.keys().collect::<Vec<_>>(), ["zebra", "apple", "mango"]);
    }
}

//! Dograh's `workflow_json` (its ReactFlow graph: api/services/workflow/dto.py)
//! into the agent's node graph, composing each node the way Dograh's engine
//! does (pipecat_engine_context_composer.py): system prompt = global node
//! prompt (unless the node opts out) + "\n\n" + node prompt, both with
//! variables filled in; functions = the node's tools + one per outgoing edge.
//!
//! Variables are filled in once, when the call starts (Dograh renders them on
//! entering each node -- for `{{current_time}}` the difference is the length
//! of the call).

use super::{
    template,
    tools::{self, ToolRow},
};
use crate::{
    agent::workflow::{Edge, Node, Tool, Workflow},
    ai::llm::FunctionDef,
};
use anyhow::{Result, anyhow, bail};
use chrono::{DateTime, Utc};
use regex::Regex;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::{collections::HashMap, sync::LazyLock};

#[derive(Deserialize, Default)]
pub struct WorkflowJson {
    #[serde(default)]
    nodes: Vec<JsonNode>,
    #[serde(default)]
    edges: Vec<JsonEdge>,
}

#[derive(Deserialize)]
struct JsonNode {
    id: String,
    #[serde(default, rename = "type")]
    kind: String,
    #[serde(default)]
    data: NodeData,
}

#[derive(Deserialize, Default)]
struct NodeData {
    #[serde(default)]
    name: String,
    #[serde(default)]
    prompt: String,
    #[serde(default)]
    is_start: bool,
    #[serde(default)]
    is_end: bool,
    /// Absent = false, Dograh's default (dto.py).
    #[serde(default)]
    allow_interrupt: bool,
    /// Dograh's default: true.
    #[serde(default)]
    add_global_prompt: Option<bool>,
    #[serde(default)]
    greeting: String,
    #[serde(default)]
    greeting_type: String,
    #[serde(default)]
    tool_uuids: Vec<String>,
}

#[derive(Deserialize)]
struct JsonEdge {
    source: String,
    target: String,
    #[serde(default)]
    data: EdgeData,
}

#[derive(Deserialize, Default)]
struct EdgeData {
    #[serde(default)]
    label: String,
    #[serde(default)]
    condition: String,
    #[serde(default)]
    transition_speech: String,
    #[serde(default)]
    transition_speech_type: String,
}

const GLOBAL_NODE: &str = "globalNode";

impl WorkflowJson {
    pub fn parse(json: &str) -> Result<Self> {
        serde_json::from_str(json).map_err(|e| anyhow!("parse workflow_json: {e}"))
    }

    /// The distinct tools attached to any node.
    pub fn tool_uuids(&self) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        self.nodes.iter().flat_map(|n| n.data.tool_uuids.iter()).filter(|u| seen.insert((*u).clone())).cloned().collect()
    }
}

static NON_EDGE_CHAR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[^a-z0-9]").unwrap());

/// The LLM function name for a workflow edge (Edge.get_function_name):
/// lowercase, anything but [a-z0-9] becomes "_" (no collapsing, unlike tool
/// names). "Move to Main Agenda" -> "move_to_main_agenda".
pub fn edge_function_name(label: &str) -> String {
    NON_EDGE_CHAR.replace_all(&label.to_lowercase(), "_").into_owned()
}

/// Problems that shouldn't stop a call (a tool that's missing, archived, of an
/// unsupported category, or misconfigured) come back as warnings.
pub fn build(wf: &WorkflowJson, vars: &Map<String, Value>, tool_rows: &[ToolRow], now: DateTime<Utc>) -> Result<(Workflow, Vec<String>)> {
    let render = |s: &str| template::render(s, vars, now);
    let mut warnings = vec![];

    let mut by_uuid: HashMap<&str, Tool> = HashMap::new();
    for r in tool_rows {
        match tools::build_tool(r) {
            Ok((t, w)) => {
                warnings.extend(w);
                by_uuid.insert(&r.uuid, t);
            }
            Err(e) => warnings.push(e.to_string()),
        }
    }

    let global_prompt = wf.nodes.iter().rfind(|n| n.kind == GLOBAL_NODE).map(|n| render(&n.data.prompt)).unwrap_or_default();

    let (mut nodes, mut index, mut start): (Vec<Node>, HashMap<&str, usize>, Option<usize>) = (vec![], HashMap::new(), None);
    for n in wf.nodes.iter().filter(|n| n.kind != GLOBAL_NODE) {
        let d = &n.data;
        let mut parts = vec![];
        if !global_prompt.is_empty() && d.add_global_prompt.unwrap_or(true) {
            parts.push(global_prompt.clone());
        }
        let p = render(&d.prompt);
        if !p.is_empty() {
            parts.push(p);
        }
        let mut node = Node {
            id: n.id.clone(),
            name: d.name.clone(),
            prompt: parts.join("\n\n"),
            end: d.is_end,
            allow_interrupt: d.allow_interrupt,
            ..Default::default()
        };
        for u in &d.tool_uuids {
            if let Some(t) = by_uuid.get(u.as_str()) {
                node.tools.push(t.clone());
            } else if !tool_rows.iter().any(|r| &r.uuid == u) {
                warnings.push(format!("node {:?}: tool {u} not found or not active", d.name));
            }
        }
        if d.is_start {
            if start.is_some() {
                bail!("workflow has more than one start node");
            }
            start = Some(nodes.len());
            if d.greeting_type == "audio" {
                warnings.push("audio greetings aren't supported yet; the LLM opens the call instead".into());
            } else if !d.greeting.is_empty() {
                node.greeting = render(&d.greeting);
            }
        }
        index.insert(&n.id, nodes.len());
        nodes.push(node);
    }
    let Some(start) = start else { bail!("workflow has no start node") };

    for e in &wf.edges {
        let (Some(&from), Some(&to)) = (index.get(e.source.as_str()), index.get(e.target.as_str())) else {
            bail!("edge {:?} connects unknown nodes {} -> {}", e.data.label, e.source, e.target);
        };
        let name = edge_function_name(&e.data.label);
        if nodes[from].tools.iter().any(|t| t.def.name == name) {
            warnings.push(format!(
                "node {:?}: edge {:?} and a tool share the function name {name:?}; the edge wins",
                nodes[from].name, e.data.label
            ));
        }
        let speech = if e.data.transition_speech_type == "audio" {
            warnings.push(format!("edge {:?}: audio transitions aren't supported yet; nothing is said", e.data.label));
            String::new()
        } else {
            render(&e.data.transition_speech)
        };
        nodes[from].edges.push(Edge { def: FunctionDef { name, description: e.data.condition.clone(), parameters: None }, speech, to });
    }
    Ok((Workflow { nodes, start }, warnings))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::workflow::ToolKind;
    use serde_json::json;

    /// The shape of the live "Survey & Feedback - Riya" workflow (prompts shortened).
    const RIYA: &str = r#"{"nodes": [
        {"id": "1", "type": "startCall", "data": {"name": "Start Call", "prompt": "Greet. Caller {{caller_number}}.",
            "is_start": true, "add_global_prompt": true, "greeting_type": "text", "allow_interrupt": false}},
        {"id": "2", "type": "agentNode", "data": {"name": "Main Agenda and Questions", "prompt": "Ask the questions.",
            "add_global_prompt": true, "tool_uuids": ["ec-1", "gone-2"]}},
        {"id": "0", "type": "globalNode", "data": {"name": "Global Node", "prompt": "You are Riya."}},
        {"id": "4", "type": "endCall", "data": {"name": "End Call", "prompt": "Say goodbye.", "add_global_prompt": false, "is_end": true, "allow_interrupt": true}}
    ], "edges": [
        {"source": "1", "target": "2", "data": {"label": "Move to Main Agenda", "condition": "When they engage."}},
        {"source": "1", "target": "4", "data": {"label": "End call", "condition": "Wrong number."}},
        {"source": "2", "target": "4", "data": {"label": "End call", "condition": "When done.", "transition_speech": "Thanks {{name | ji}}"}}
    ]}"#;

    #[test]
    fn builds_the_riya_workflow() {
        let wf = WorkflowJson::parse(RIYA).unwrap();
        assert_eq!(wf.tool_uuids(), ["ec-1", "gone-2"]);
        let rows = [ToolRow {
            uuid: "ec-1".into(),
            name: "Hang Up".into(),
            description: String::new(),
            category: "end_call".into(),
            definition: json!({}),
            credential: None,
        }];
        let vars = json!({"caller_number": "08448805135"});
        let (w, warnings) = build(&wf, vars.as_object().unwrap(), &rows, Utc::now()).unwrap();

        let start = &w.nodes[w.start];
        assert_eq!(start.name, "Start Call");
        assert_eq!(start.prompt, "You are Riya.\n\nGreet. Caller 08448805135.", "global prompt + node prompt, variables filled");
        assert!(start.greeting.is_empty(), "no greeting -> the LLM opens the call");
        assert_eq!(start.edges.len(), 2);
        assert_eq!(start.edges[0].def.name, "move_to_main_agenda");
        assert_eq!(start.edges[0].def.description, "When they engage.");
        assert_eq!(start.edges[1].def.name, "end_call");
        assert!(!start.allow_interrupt);

        let main = &w.nodes[start.edges[0].to];
        assert_eq!(main.name, "Main Agenda and Questions");
        assert!(!main.allow_interrupt, "absent -> false, Dograh's default");
        assert_eq!(main.tools.len(), 1);
        assert!(matches!(main.tools[0].kind, ToolKind::EndCall));
        assert_eq!(main.edges[0].speech, "Thanks ji");

        let end = &w.nodes[main.edges[0].to];
        assert!(end.end && end.allow_interrupt);
        assert_eq!(end.prompt, "Say goodbye.", "add_global_prompt=false leaves the global prompt out");
        assert_eq!(start.edges[1].to, main.edges[0].to, "both End call edges lead to the same node");
        assert_eq!(warnings, [r#"node "Main Agenda and Questions": tool gone-2 not found or not active"#]);
    }

    #[test]
    fn greeting_and_structural_errors() {
        let wf = WorkflowJson::parse(r#"{"nodes":[{"id":"1","type":"startCall","data":{"name":"S","is_start":true,"greeting":"Namaste {{name | ji}}"}}],"edges":[]}"#).unwrap();
        let (w, _) = build(&wf, &Map::new(), &[], Utc::now()).unwrap();
        assert_eq!(w.nodes[0].greeting, "Namaste ji");

        let no_start = WorkflowJson::parse(r#"{"nodes":[{"id":"1","type":"agentNode","data":{}}]}"#).unwrap();
        assert!(build(&no_start, &Map::new(), &[], Utc::now()).is_err());
        let bad_edge = WorkflowJson::parse(
            r#"{"nodes":[{"id":"1","data":{"is_start":true}}],"edges":[{"source":"1","target":"9","data":{"label":"x"}}]}"#,
        )
        .unwrap();
        assert!(build(&bad_edge, &Map::new(), &[], Utc::now()).is_err());
        let two = WorkflowJson::parse(r#"{"nodes":[{"id":"1","data":{"is_start":true}},{"id":"2","data":{"is_start":true}}]}"#).unwrap();
        assert!(build(&two, &Map::new(), &[], Utc::now()).is_err());
    }

    #[test]
    fn edge_names() {
        assert_eq!(edge_function_name("Move to Main Agenda"), "move_to_main_agenda");
        assert_eq!(edge_function_name("A--B"), "a__b");
    }
}

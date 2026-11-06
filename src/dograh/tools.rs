//! Dograh tools (the `tools` table) as agent tools: `end_call`, and the
//! `http_api` tool (ported from api/services/workflow/tools/custom_tool.py +
//! api/utils/credential_auth.py). `transfer_call` needs ARI call control and is
//! skipped with a warning for now.

use super::recordings::{ClipSet, recording_ref};
use crate::{
    agent::workflow::{Tool, ToolKind, ToolRun},
    ai::llm::{self, FunctionDef},
};
use anyhow::{Result, anyhow, bail};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::{collections::HashMap, sync::Arc, time::Duration};

pub const CATEGORY_END_CALL: &str = "end_call";
pub const CATEGORY_TRANSFER: &str = "transfer_call";
pub const CATEGORY_HTTP: &str = "http_api";

/// How long a transfer rings before giving up when the tool sets no timeout.
const DEFAULT_TRANSFER_TIMEOUT: Duration = Duration::from_secs(30);
/// Dograh's timeout_ms default.
const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(5);
/// ponytail: Dograh reads whole replies; 1 MiB is far more than an LLM turn can use.
const MAX_RESPONSE: usize = 1 << 20;

/// One row of Dograh's `tools` table.
#[derive(Debug, Clone)]
pub struct ToolRow {
    pub uuid: String,
    pub name: String,
    pub description: String,
    pub category: String,
    pub definition: Value,
    /// The active `external_credentials` row named by the config's credential_uuid.
    pub credential: Option<Credential>,
}

#[derive(Debug, Clone)]
pub struct Credential {
    pub kind: String,
    pub data: Value,
}

#[derive(Deserialize, Default)]
struct ToolConfig {
    #[serde(default, rename = "messageType")]
    message_type: String,
    #[serde(default, rename = "customMessage")]
    custom_message: String,
    #[serde(default, rename = "audioRecordingId", deserialize_with = "recording_ref")]
    audio_recording_id: i64,
    #[serde(default, rename = "customMessageType")]
    custom_message_type: String,
    #[serde(default, rename = "customMessageRecordingId", deserialize_with = "recording_ref")]
    custom_message_recording_id: i64,
    #[serde(default, rename = "endCallReason")]
    end_call_reason: bool,
    #[serde(default, rename = "endCallReasonDescription")]
    end_call_reason_description: String,
    // http_api. Its message is customMessage; unlike the other tools it doesn't use messageType.
    #[serde(default)]
    destination: String,
    /// transfer_call: seconds to ring; Dograh's default is 30.
    #[serde(default)]
    timeout: Option<f64>,
    #[serde(default)]
    method: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    credential_uuid: String,
    #[serde(default)]
    timeout_ms: Option<f64>,
    #[serde(default)]
    parameters: Vec<Param>,
}

#[derive(Deserialize, Default, Clone)]
struct Param {
    #[serde(default)]
    name: String,
    #[serde(default, rename = "type")]
    kind: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    required: Option<bool>, // Dograh's default: true
    #[serde(default)]
    item_schema: Vec<Param>,
}

/// The LLM function name for a Dograh tool (custom_tool.py): lowercase, anything
/// but [a-z0-9_] becomes "_", repeats collapsed, trimmed.
/// "Transfer Call to Support Team" -> "transfer_call_to_support_team".
pub fn function_name(tool_name: &str) -> String {
    let mut out = String::new();
    for c in tool_name.to_lowercase().chars() {
        let c = if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' { c } else { '_' };
        if !(c == '_' && out.ends_with('_')) {
            out.push(c);
        }
    }
    out.trim_matches('_').to_string()
}

/// Builds one agent tool. `Ok((tool, warning))`: a warning is a problem that
/// doesn't stop the tool from being used. `Err` skips the tool (logged by the caller).
/// The recording a tool's message plays, if it's an "audio" one (0 = none).
pub fn tool_clip_ref(r: &ToolRow) -> i64 {
    let Some(cfg) = r.definition.get("config").and_then(|c| serde_json::from_value::<ToolConfig>(c.clone()).ok()) else { return 0 };
    if r.category == CATEGORY_HTTP {
        if cfg.custom_message_type == "audio" { cfg.custom_message_recording_id } else { 0 }
    } else if cfg.message_type == "audio" {
        cfg.audio_recording_id
    } else {
        0
    }
}

pub fn build_tool(r: &ToolRow, clips: &ClipSet) -> Result<(Tool, Option<String>)> {
    // A missing or null config is an empty one.
    let cfg: ToolConfig = match r.definition.get("config").filter(|c| !c.is_null()) {
        None => ToolConfig::default(),
        Some(c) => serde_json::from_value(c.clone()).map_err(|e| anyhow!("tool {:?} ({}): bad definition: {e}", r.name, r.uuid))?,
    };
    let mut def = FunctionDef {
        name: function_name(&r.name),
        description: if r.description.is_empty() { format!("Execute {} tool", r.name) } else { r.description.clone() },
        parameters: None,
    };
    let (mut message, mut message_clip, mut warning) = (String::new(), None, None);
    match cfg.message_type.as_str() {
        "custom" => message = cfg.custom_message.clone(),
        "audio" => {
            message_clip = clips.by_pk.get(&cfg.audio_recording_id).cloned();
            if message_clip.is_none() && r.category != CATEGORY_HTTP {
                warning = Some(format!("tool {:?} ({}): message recording unavailable; nothing is said", r.name, r.uuid));
            }
        }
        _ => {}
    }

    match r.category.as_str() {
        CATEGORY_END_CALL => {
            if cfg.end_call_reason {
                let desc = if cfg.end_call_reason_description.is_empty() {
                    "The reason for ending the call (e.g., 'voicemail_detected', 'issue_resolved', 'customer_requested')".to_string()
                } else {
                    cfg.end_call_reason_description.clone()
                };
                def.parameters =
                    Some(json!({"type":"object","properties":{"reason":{"type":"string","description":desc}},"required":["reason"]}));
            }
            Ok((Tool { def, kind: ToolKind::EndCall, message, message_clip }, warning))
        }
        CATEGORY_HTTP => build_http_tool(r, cfg, def, clips),
        CATEGORY_TRANSFER => {
            if cfg.destination.trim().is_empty() {
                bail!("transfer tool {:?} ({}) has no destination", r.name, r.uuid);
            }
            let timeout = cfg.timeout.filter(|t| *t > 0.0).map(Duration::from_secs_f64).unwrap_or(DEFAULT_TRANSFER_TIMEOUT);
            Ok((Tool { def, kind: ToolKind::Transfer { destination: cfg.destination.clone(), timeout }, message, message_clip }, warning))
        }
        other => bail!("tool {:?} (category {other}): tool category not supported yet", r.name),
    }
}

fn build_http_tool(r: &ToolRow, cfg: ToolConfig, mut def: FunctionDef, clips: &ClipSet) -> Result<(Tool, Option<String>)> {
    if cfg.url.trim().is_empty() {
        bail!("http tool {:?} ({}) has no url", r.name, r.uuid);
    }
    let mut headers = cfg.headers.clone();
    let mut warning = None;
    // A credential that's missing, inactive or unreadable is a warning, not an
    // error: Dograh then sends the request without it.
    if !cfg.credential_uuid.is_empty() {
        match &r.credential {
            None => {
                warning = Some(format!(
                    "http tool {:?} ({}): credential {} not found or not active; calling without it",
                    r.name, r.uuid, cfg.credential_uuid
                ))
            }
            Some(c) => match auth_header(c) {
                Ok(Some((k, v))) => {
                    headers.insert(k, v);
                }
                Ok(None) => {}
                Err(e) => {
                    warning =
                        Some(format!("http tool {:?} ({}): credential {}: {e}; calling without it", r.name, r.uuid, cfg.credential_uuid))
                }
            },
        }
    }
    let http = HttpTool {
        method: if cfg.method.is_empty() { "POST".into() } else { cfg.method.to_uppercase() },
        url: cfg.url.clone(),
        headers,
        timeout: cfg.timeout_ms.filter(|t| *t > 0.0).map(|t| Duration::from_secs_f64(t / 1000.0)).unwrap_or(DEFAULT_HTTP_TIMEOUT),
    };
    def.parameters = Some(object_schema(&cfg.parameters, true));
    let run: ToolRun = Arc::new(move |args| {
        let t = http.clone();
        Box::pin(async move { Ok(t.run(args).await) })
    });
    // An http_api tool's message is customMessage, or with customMessageType
    // "audio" the recording customMessageRecordingId -- not messageType.
    let (mut message, mut message_clip) = (cfg.custom_message.clone(), None);
    if cfg.custom_message_type == "audio" {
        message.clear();
        message_clip = clips.by_pk.get(&cfg.custom_message_recording_id).cloned();
        if message_clip.is_none() {
            let msg = format!("http tool {:?} ({}): message recording unavailable; nothing is said", r.name, r.uuid);
            warning = Some(warning.map_or(msg.clone(), |w| format!("{w}; {msg}")));
        }
    }
    Ok((Tool { def, kind: ToolKind::Function(run), message, message_clip }, warning))
}

/// `{"type":"object","properties":...,"required":[...]}`. Only top-level
/// parameters may be arrays/objects: Dograh allows one level of item_schema.
fn object_schema(params: &[Param], top: bool) -> Value {
    let (mut props, mut required) = (Map::new(), vec![]);
    for p in params.iter().filter(|p| !p.name.is_empty()) {
        let schema = match (top, p.kind.as_str()) {
            (true, "array") => json!({"type":"array","description":p.description,"items":object_schema(&p.item_schema, false)}),
            (true, "object") => {
                let mut s = object_schema(&p.item_schema, false);
                s["description"] = json!(p.description);
                s
            }
            (_, t) => json!({"type": if matches!(t, "string"|"number"|"boolean") { t } else { "string" }, "description": p.description}),
        };
        props.insert(p.name.clone(), schema);
        if p.required.unwrap_or(true) {
            required.push(Value::String(p.name.clone()));
        }
    }
    json!({"type":"object","properties":props,"required":required})
}

/// Dograh's build_auth_header: the header a credential adds, `None` for type
/// "none" or an unknown type.
fn auth_header(c: &Credential) -> Result<Option<(String, String)>> {
    let d: HashMap<String, String> = if c.data.is_null() {
        HashMap::new()
    } else {
        serde_json::from_value(c.data.clone()).map_err(|e| anyhow!("bad credential_data: {e}"))?
    };
    // Defaults apply only when the key is absent, as with Python's dict.get.
    let get = |k: &str, def: &str| d.get(k).cloned().unwrap_or_else(|| def.to_string());
    Ok(match c.kind.as_str() {
        "bearer_token" => Some(("Authorization".into(), format!("Bearer {}", get("token", "")))),
        "api_key" => Some((get("header_name", "X-API-Key"), get("api_key", ""))),
        "basic_auth" => {
            Some(("Authorization".into(), format!("Basic {}", STANDARD.encode(format!("{}:{}", get("username", ""), get("password", ""))))))
        }
        "custom_header" => Some((get("header_name", "X-Custom"), get("header_value", ""))),
        _ => None,
    })
}

#[derive(Clone)]
struct HttpTool {
    method: String,
    url: String,
    headers: HashMap<String, String>,
    timeout: Duration,
}

fn http_error(msg: &str) -> String {
    json!({"status":"error","error":msg}).to_string()
}

fn query_value(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        other => other.to_string(), // true/false lowercase, numbers as written
    }
}

/// Python prints timeout_ms/1000: 5s -> "5.0".
fn py_seconds(d: Duration) -> String {
    let s = d.as_secs_f64().to_string();
    if s.contains('.') { s } else { format!("{s}.0") }
}

impl HttpTool {
    /// execute_http_tool: the LLM's arguments go as a JSON body for
    /// POST/PUT/PATCH and as query parameters for GET/DELETE. Like Dograh it
    /// never fails: any HTTP status is a "success" carrying the status code,
    /// and a failure is a `{"status":"error"}` result the LLM can react to.
    async fn run(&self, args: Value) -> String {
        let args = match args {
            Value::Null => Map::new(),
            Value::Object(o) => o,
            _ => return http_error("Tool execution failed: bad arguments: expected a JSON object"),
        };
        let method = match reqwest::Method::from_bytes(self.method.as_bytes()) {
            Ok(m) => m,
            Err(e) => return http_error(&format!("Tool execution failed: {e}")),
        };
        let mut req = llm::http().request(method.clone(), &self.url).timeout(self.timeout);
        match self.method.as_str() {
            "POST" | "PUT" | "PATCH" => req = req.json(&args),
            "GET" | "DELETE" if !args.is_empty() => {
                // Merged with the URL's own query, as httpx does; lists repeat the key.
                let mut pairs: Vec<(String, String)> = vec![];
                for (k, v) in &args {
                    match v {
                        Value::Array(items) => pairs.extend(items.iter().map(|i| (k.clone(), query_value(i)))),
                        other => pairs.push((k.clone(), query_value(other))),
                    }
                }
                req = req.query(&pairs);
            }
            _ => {}
        }
        for (k, v) in &self.headers {
            req = req.header(k, v);
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) if e.is_timeout() => return http_error(&format!("Request timed out after {} seconds", py_seconds(self.timeout))),
            Err(e) => return http_error(&format!("Request failed: {e}")),
        };
        let code = resp.status().as_u16();
        let body = match resp.bytes().await {
            Ok(b) => b,
            Err(e) if e.is_timeout() => return http_error(&format!("Request timed out after {} seconds", py_seconds(self.timeout))),
            Err(e) => return http_error(&format!("Request failed: {e}")),
        };
        let body = &body[..body.len().min(MAX_RESPONSE)];
        let data = serde_json::from_slice::<Value>(body).unwrap_or_else(|_| json!({"raw_response": String::from_utf8_lossy(body)}));
        json!({"status":"success","status_code":code,"data":data}).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn row(category: &str, name: &str, def: Value) -> ToolRow {
        ToolRow {
            uuid: "u".into(),
            name: name.into(),
            description: String::new(),
            category: category.into(),
            definition: def,
            credential: None,
        }
    }

    #[test]
    fn function_names_follow_dograh() {
        assert_eq!(function_name("Transfer Call to Support Team"), "transfer_call_to_support_team");
        assert_eq!(function_name("  Get--Order  Status! "), "get_order_status");
    }

    #[test]
    fn end_call_tool_with_a_required_reason() {
        let r = row("end_call", "End Call", json!({"config": {"messageType":"custom","customMessage":"Bye!","endCallReason":true}}));
        let (t, w) = build_tool(&r, &ClipSet::default()).unwrap();
        assert!(w.is_none() && matches!(t.kind, ToolKind::EndCall));
        assert_eq!((t.message.as_str(), t.def.name.as_str(), t.def.description.as_str()), ("Bye!", "end_call", "Execute End Call tool"));
        assert_eq!(t.def.parameters.unwrap()["required"], json!(["reason"]));
    }

    #[test]
    fn http_tool_schema_and_credentials() {
        let mut r = row(
            "http_api",
            "Lookup",
            json!({"config": {
            "url": "http://x/y", "method": "get", "credential_uuid": "c1", "customMessage": "One moment",
            "parameters": [
                {"name": "id", "type": "number", "description": "order"},
                {"name": "tags", "type": "array", "required": false, "item_schema": [{"name":"k","type":"weird"}]}]}}),
        );
        r.credential = Some(Credential { kind: "bearer_token".into(), data: json!({"token": "tok"}) });
        let (t, w) = build_tool(&r, &ClipSet::default()).unwrap();
        assert!(w.is_none());
        assert_eq!(t.message, "One moment");
        let p = t.def.parameters.unwrap();
        assert_eq!(p["properties"]["id"]["type"], "number");
        assert_eq!(p["properties"]["tags"]["items"]["properties"]["k"]["type"], "string");
        assert_eq!(p["required"], json!(["id"]));

        r.credential = None;
        let (_, w) = build_tool(&r, &ClipSet::default()).unwrap();
        assert!(w.unwrap().contains("not found or not active"));
        assert!(build_tool(&row("http_api", "X", json!({"config": {}})), &ClipSet::default()).is_err(), "no url");
        assert!(build_tool(&row("transfer_call", "T", json!({})), &ClipSet::default()).is_err(), "no destination");
        let (t, _) = build_tool(&row("transfer_call", "Transfer to Support", json!({"config": {"destination": "PJSIP/100@pbx", "timeout": 12.5, "messageType": "custom", "customMessage": "Please hold."}})), &ClipSet::default()).unwrap();
        assert_eq!(t.def.name, "transfer_to_support");
        assert_eq!(t.message, "Please hold.");
        assert!(
            matches!(&t.kind, ToolKind::Transfer { destination, timeout } if destination == "PJSIP/100@pbx" && *timeout == Duration::from_millis(12_500))
        );
        let (t, _) = build_tool(&row("transfer_call", "T", json!({"config": {"destination": "SIP/1"}})), &ClipSet::default()).unwrap();
        assert!(matches!(t.kind, ToolKind::Transfer { timeout, .. } if timeout == Duration::from_secs(30)));
    }

    #[test]
    fn auth_headers() {
        let h = |kind: &str, data: Value| auth_header(&Credential { kind: kind.into(), data }).unwrap();
        assert_eq!(h("bearer_token", json!({"token":"t"})), Some(("Authorization".into(), "Bearer t".into())));
        assert_eq!(h("api_key", json!({"api_key":"k"})), Some(("X-API-Key".into(), "k".into())));
        assert_eq!(h("basic_auth", json!({"username":"a","password":"b"})), Some(("Authorization".into(), "Basic YTpi".into())));
        assert_eq!(h("none", Value::Null), None);
    }

    /// One-shot HTTP server: records the request head+body, answers `reply`.
    async fn serve(reply: &'static str) -> (String, tokio::sync::oneshot::Receiver<String>) {
        let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", lis.local_addr().unwrap());
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut s, _) = lis.accept().await.unwrap();
            let mut buf = vec![0u8; 8192];
            let n = s.read(&mut buf).await.unwrap();
            let _ = tx.send(String::from_utf8_lossy(&buf[..n]).to_string());
            let resp = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}", reply.len());
            s.write_all(resp.as_bytes()).await.unwrap();
        });
        (url, rx)
    }

    #[tokio::test]
    async fn get_sends_query_params_and_wraps_the_reply() {
        let (url, req) = serve(r#"{"ok":true}"#).await;
        let t = HttpTool {
            method: "GET".into(),
            url: format!("{url}/o?x=1"),
            headers: [("X-A".into(), "b".into())].into(),
            timeout: Duration::from_secs(2),
        };
        let out: Value = serde_json::from_str(&t.run(json!({"id": 5, "on": true, "tags": ["a", "b"]})).await).unwrap();
        assert_eq!(out, json!({"status":"success","status_code":200,"data":{"ok":true}}));
        let head = req.await.unwrap();
        let line = head.lines().next().unwrap();
        assert!(line.starts_with("GET /o?"), "{line}");
        for part in ["x=1", "id=5", "on=true", "tags=a", "tags=b"] {
            assert!(line.contains(part), "{part} missing from {line}");
        }
        assert!(head.to_lowercase().contains("x-a: b"));
    }

    #[tokio::test]
    async fn post_sends_json_and_non_json_replies_are_wrapped() {
        let (url, req) = serve("plain text").await;
        let t = HttpTool { method: "POST".into(), url, headers: Default::default(), timeout: Duration::from_secs(2) };
        let out: Value = serde_json::from_str(&t.run(json!({"a": 1})).await).unwrap();
        assert_eq!(out["data"], json!({"raw_response": "plain text"}));
        assert!(req.await.unwrap().contains(r#"{"a":1}"#));

        let dead = HttpTool {
            method: "GET".into(),
            url: "http://127.0.0.1:1/".into(),
            headers: Default::default(),
            timeout: Duration::from_secs(1),
        };
        let out: Value = serde_json::from_str(&dead.run(Value::Null).await).unwrap();
        assert_eq!(out["status"], "error");
        assert_eq!(py_seconds(Duration::from_secs(5)), "5.0");
    }
}

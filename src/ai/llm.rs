//! Streaming chat completions from an OpenAI-compatible endpoint (SSE).
//! One process-wide `reqwest::Client` (HTTP/2, 200 idle conns per host):
//! never a client per request (invariant #5).

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{sync::OnceLock, time::Duration};
use tokio_util::sync::CancellationToken;

/// A reply with no bytes at all for this long is dead.
const STREAM_IDLE: Duration = Duration::from_secs(30);

pub fn http() -> &'static reqwest::Client {
    static C: OnceLock<reqwest::Client> = OnceLock::new();
    C.get_or_init(|| {
        reqwest::Client::builder()
            .pool_max_idle_per_host(200)
            .pool_idle_timeout(Duration::from_secs(90))
            .connect_timeout(Duration::from_secs(5))
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .expect("http client")
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct Message {
    pub role: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tool_call_id: String,
}

impl Message {
    pub fn new(role: &str, content: impl Into<String>) -> Self {
        Self { role: role.into(), content: content.into(), ..Default::default() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub function: FunctionCall,
    /// Kept verbatim and sent back unchanged: Gemini 3 puts the call's
    /// "thought signature" here and rejects the follow-up without it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra_content: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct FunctionCall {
    pub name: String,
    pub arguments: String,
}

/// A function offered to the model; `parameters` is a JSON Schema object.
#[derive(Debug, Clone, Serialize, Default)]
pub struct FunctionDef {
    pub name: String,
    pub description: String,
    pub parameters: Option<Value>,
}

/// One request's token counts (cached tokens are part of `prompt`, as in OpenAI's format).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub prompt: u64,
    pub completion: u64,
    pub total: u64,
    pub cached: u64,
}

/// A finished stream: the tool calls the model made and its token usage, when the provider reported it.
#[derive(Debug, Default)]
pub struct StreamOut {
    pub calls: Vec<ToolCall>,
    pub usage: Option<Usage>,
}

#[derive(Debug, thiserror::Error)]
pub enum LlmError {
    #[error("llm: non-2xx response: {code}: {body}")]
    Status { code: u16, body: String, retry_after: Option<Duration> },
    #[error("llm: request: {0}")]
    Request(String),
    #[error("llm: stream idle timeout: nothing received for {0:?}")]
    Idle(Duration),
    #[error("llm: {0}")]
    Other(String),
    #[error("llm: cancelled")]
    Cancelled,
}

impl LlmError {
    /// Failed before the provider sent any reply, in a way a retry can fix.
    fn retryable(&self) -> bool {
        match self {
            Self::Status { code, .. } => matches!(code, 429 | 502 | 503 | 504),
            Self::Request(_) => true,
            _ => false,
        }
    }
}

#[derive(Clone)]
pub struct LlmClient {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub retries: u32,
    pub retry_max_wait: Duration,
    /// The call this client works for, so each request's span and log line belong to it ("" = none).
    pub call_id: String,
}

impl LlmClient {
    pub fn new(base_url: &str, api_key: &str, model: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').into(),
            api_key: api_key.into(),
            model: model.into(),
            retries: 2,
            retry_max_wait: Duration::from_secs(2),
            call_id: String::new(),
        }
    }

    /// Streams the reply, calling `on_token` in order for each content delta.
    /// Returns the tool calls the model made (assembled from fragments). Text
    /// written before a tool call has already gone to `on_token`.
    pub async fn stream(
        &self,
        cancel: &CancellationToken,
        messages: &[Message],
        tools: &[FunctionDef],
        on_token: &mut (dyn FnMut(&str) + Send),
    ) -> Result<StreamOut, LlmError> {
        let mut res = self.stream_once(1, cancel, messages, tools, on_token).await;
        let mut attempt = 1;
        while let Err(e) = &res {
            if attempt > self.retries || !e.retryable() || cancel.is_cancelled() {
                break;
            }
            let wait = self.retry_wait(attempt, e);
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(wait) => {}
            }
            res = self.stream_once(attempt + 1, cancel, messages, tools, on_token).await;
            attempt += 1;
        }
        if attempt > 1 {
            crate::metrics::llm_retry(if res.is_ok() { "recovered" } else { "exhausted" });
        }
        res
    }

    /// 300ms x attempt plus jitter, or the provider's Retry-After; capped.
    fn retry_wait(&self, attempt: u32, e: &LlmError) -> Duration {
        let mut wait = Duration::from_millis(300 * u64::from(attempt) + rand::random_range(0..200));
        if let LlmError::Status { retry_after: Some(ra), .. } = e {
            wait = *ra;
        }
        wait.min(self.retry_max_wait)
    }

    /// One HTTP attempt, as a span of its call's trace and a `llm.request` log line (INFO when it took 3 s
    /// or more, WARN when it failed, DEBUG otherwise): the ids are what a provider's support asks for.
    async fn stream_once(
        &self,
        attempt: u32,
        cancel: &CancellationToken,
        messages: &[Message],
        tools: &[FunctionDef],
        on_token: &mut (dyn FnMut(&str) + Send),
    ) -> Result<StreamOut, LlmError> {
        let span = crate::telemetry::llm_span(&self.call_id);
        let (res, info, total) = self.measured(cancel, messages, tools, on_token).await;
        let err = res.as_ref().err().filter(|e| !matches!(e, LlmError::Cancelled)).map(ToString::to_string);
        if let Some(span) = span {
            span.end(&self.model, attempt, info.status, &info.request_id, &info.completion_id, err.as_deref());
        }
        let (call_id, model, ms) = (self.call_id.as_str(), self.model.as_str(), total.as_millis() as u64);
        let (status, rid, cid) = (info.status, info.request_id.as_str(), info.completion_id.as_str());
        match request_level(total, err.is_some()) {
            tracing::Level::WARN => {
                let error = err.as_deref().unwrap_or_default();
                tracing::warn!(
                    call_id,
                    event = "llm.request",
                    component = "llm",
                    attempt,
                    status,
                    request_id = rid,
                    completion_id = cid,
                    duration_ms = ms,
                    model,
                    error,
                    "llm request failed"
                );
            }
            tracing::Level::INFO => {
                tracing::info!(
                    call_id,
                    event = "llm.request",
                    component = "llm",
                    attempt,
                    status,
                    request_id = rid,
                    completion_id = cid,
                    duration_ms = ms,
                    model,
                    "slow llm request"
                );
            }
            _ => {
                tracing::debug!(
                    call_id,
                    event = "llm.request",
                    component = "llm",
                    attempt,
                    status,
                    request_id = rid,
                    completion_id = cid,
                    duration_ms = ms,
                    model,
                    "llm request"
                );
            }
        }
        res
    }

    /// The attempt, timed, with the ids that name it.
    async fn measured(
        &self,
        cancel: &CancellationToken,
        messages: &[Message],
        tools: &[FunctionDef],
        on_token: &mut (dyn FnMut(&str) + Send),
    ) -> (Result<StreamOut, LlmError>, ReqInfo, Duration) {
        let start = std::time::Instant::now();
        let mut info = ReqInfo::default();
        let res = self.stream_inner(cancel, messages, tools, on_token, &mut info).await;
        (res, info, start.elapsed())
    }

    async fn stream_inner(
        &self,
        cancel: &CancellationToken,
        messages: &[Message],
        tools: &[FunctionDef],
        on_token: &mut (dyn FnMut(&str) + Send),
        info: &mut ReqInfo,
    ) -> Result<StreamOut, LlmError> {
        let mut body = json!({
            "model": self.model,
            "messages": messages,
            "stream": true,
            "stream_options": {"include_usage": true},
        });
        if !tools.is_empty() {
            // Some providers reject a function without a parameters object.
            let sent: Vec<Value> = tools
                .iter()
                .map(|t| {
                    let params = t.parameters.clone().unwrap_or_else(|| json!({"type":"object","properties":{}}));
                    json!({"type":"function","function":{"name":t.name,"description":t.description,"parameters":params}})
                })
                .collect();
            body["tools"] = Value::Array(sent);
        }

        let req = http()
            .post(format!("{}/chat/completions", self.base_url))
            .bearer_auth(&self.api_key)
            .header("Accept", "text/event-stream")
            .json(&body)
            .timeout(Duration::from_secs(600)) // whole-stream ceiling; the idle timeout is the real bound
            .send();
        let resp = tokio::select! {
            _ = cancel.cancelled() => return Err(LlmError::Cancelled),
            r = tokio::time::timeout(Duration::from_secs(30), req) => match r {
                Err(_) => return Err(LlmError::Request("timed out waiting for response headers".into())),
                Ok(r) => r.map_err(|e| LlmError::Request(e.to_string()))?,
            },
        };

        let status = resp.status();
        info.status = status.as_u16();
        info.request_id = resp.headers().get("x-request-id").and_then(|v| v.to_str().ok()).unwrap_or_default().to_string();
        if !status.is_success() {
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok()?.parse::<u64>().ok())
                .filter(|s| *s > 0)
                .map(Duration::from_secs);
            let mut body = resp.text().await.unwrap_or_default();
            body.truncate(floor_boundary(&body, 4096));
            return Err(LlmError::Status { code: status.as_u16(), body, retry_after });
        }

        let mut stream = resp.bytes_stream();
        let (mut buf, mut calls, mut got, mut usage) = (Vec::<u8>::new(), ToolCallAssembler::default(), false, None::<Usage>);

        loop {
            let next = tokio::select! {
                _ = cancel.cancelled() => return Err(LlmError::Cancelled),
                n = tokio::time::timeout(STREAM_IDLE, stream.next()) => n.map_err(|_| LlmError::Idle(STREAM_IDLE))?,
            };
            let Some(chunk) = next else { break };
            buf.extend_from_slice(&chunk.map_err(|e| LlmError::Other(format!("reading stream: {e}")))?);

            while let Some(nl) = buf.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = buf.drain(..=nl).collect();
                let line = String::from_utf8_lossy(&line);
                // SSE: "data:" with an optional single space before the value.
                let Some(data) = line.trim_end().strip_prefix("data:") else { continue };
                let data = data.strip_prefix(' ').unwrap_or(data);
                if data == "[DONE]" {
                    return Ok(StreamOut { calls: calls.done(), usage });
                }
                // A malformed line is skipped: providers interleave keepalives.
                let Ok(c) = serde_json::from_str::<Chunk>(data) else { continue };
                if info.completion_id.is_empty() {
                    info.completion_id = c.id.clone().unwrap_or_default();
                }
                if let Some(err) = c.error.filter(|e| !e.is_null()) {
                    let mut s = err.to_string();
                    s.truncate(floor_boundary(&s, 500));
                    return Err(LlmError::Other(format!("provider error in stream: {s}")));
                }
                // Some providers repeat usage on several chunks; the last one is final.
                if let Some(u) = c.usage {
                    usage = Some(Usage {
                        prompt: u.prompt_tokens,
                        completion: u.completion_tokens,
                        total: u.total_tokens,
                        cached: u.prompt_tokens_details.map(|d| d.cached_tokens).unwrap_or(0),
                    });
                }
                for ch in c.choices {
                    if let Some(t) = ch.delta.content.filter(|t| !t.is_empty()) {
                        got = true;
                        on_token(&t);
                    }
                    for d in ch.delta.tool_calls {
                        got = true;
                        calls.add(d);
                    }
                }
            }
        }
        // EOF without [DONE]: fine if anything was delivered, a failure if not
        // (an empty reply must not pass for an answer).
        if !got {
            return Err(LlmError::Other("stream ended without any reply".into()));
        }
        Ok(StreamOut { calls: calls.done(), usage })
    }
}

/// Largest index <= max that is a char boundary of s.
fn floor_boundary(s: &str, max: usize) -> usize {
    let mut i = max.min(s.len());
    while !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

#[derive(Deserialize)]
struct ChunkUsage {
    #[serde(default)]
    prompt_tokens: u64,
    #[serde(default)]
    completion_tokens: u64,
    #[serde(default)]
    total_tokens: u64,
    #[serde(default)]
    prompt_tokens_details: Option<PromptDetails>,
}
#[derive(Deserialize)]
struct PromptDetails {
    #[serde(default)]
    cached_tokens: u64,
}

/// The level an attempt's log line is written at: WARN when it failed, INFO when it took 3 s or more, DEBUG otherwise.
fn request_level(total: Duration, failed: bool) -> tracing::Level {
    if failed {
        tracing::Level::WARN
    } else if total >= Duration::from_secs(3) {
        tracing::Level::INFO
    } else {
        tracing::Level::DEBUG
    }
}

/// What identifies one request: its HTTP status, the provider's request id header and the completion's id.
#[derive(Default)]
struct ReqInfo {
    status: u16,
    request_id: String,
    completion_id: String,
}

#[derive(Deserialize)]
struct Chunk {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    error: Option<Value>,
    #[serde(default)]
    usage: Option<ChunkUsage>,
    #[serde(default)]
    choices: Vec<ChunkChoice>,
}
#[derive(Deserialize)]
struct ChunkChoice {
    #[serde(default)]
    delta: Delta,
}
#[derive(Deserialize, Default)]
struct Delta {
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<ToolCallDelta>,
}
#[derive(Deserialize)]
struct ToolCallDelta {
    index: Option<i64>,
    #[serde(default)]
    id: String,
    #[serde(default, rename = "type")]
    kind: String,
    #[serde(default)]
    function: FnDelta,
    extra_content: Option<Value>,
}
#[derive(Deserialize, Default)]
struct FnDelta {
    #[serde(default)]
    name: String,
    #[serde(default)]
    arguments: String,
}

/// Joins streamed tool-call fragments back into whole calls. OpenAI streams a
/// call across many deltas matched by index; some compatible providers (Gemini)
/// send each call whole and may omit or reuse the index.
#[derive(Default)]
struct ToolCallAssembler {
    calls: Vec<ToolCall>,
}

impl ToolCallAssembler {
    fn add(&mut self, d: ToolCallDelta) {
        let i = self.slot(&d);
        let c = &mut self.calls[i];
        if !d.id.is_empty() {
            c.id = d.id;
        }
        if !d.kind.is_empty() {
            c.kind = d.kind;
        }
        if !d.function.name.is_empty() {
            c.function.name = d.function.name;
        }
        c.function.arguments.push_str(&d.function.arguments);
        if let Some(x) = d.extra_content.filter(|x| !x.is_null()) {
            c.extra_content = Some(x);
        }
    }

    /// By index when given -- unless that slot holds a different call id (a
    /// provider reusing index 0 per call) -- else by id, else the last call.
    fn slot(&mut self, d: &ToolCallDelta) -> usize {
        if let Some(i) = d.index.filter(|i| *i >= 0).map(|i| i as usize) {
            if i >= self.calls.len() {
                self.calls.resize_with(i + 1, ToolCall::default);
                return i;
            }
            if d.id.is_empty() || self.calls[i].id.is_empty() || self.calls[i].id == d.id {
                return i;
            }
        }
        if !d.id.is_empty() {
            if let Some(i) = self.calls.iter().position(|c| c.id == d.id) {
                return i;
            }
            self.calls.push(ToolCall::default());
            return self.calls.len() - 1;
        }
        if self.calls.is_empty() {
            self.calls.push(ToolCall::default());
        }
        self.calls.len() - 1
    }

    /// Drops fragments that never got a name; defaults type and empty args.
    fn done(self) -> Vec<ToolCall> {
        self.calls
            .into_iter()
            .filter(|c| !c.function.name.is_empty())
            .map(|mut c| {
                if c.kind.is_empty() {
                    c.kind = "function".into();
                }
                if c.function.arguments.trim().is_empty() {
                    c.function.arguments = "{}".into();
                }
                c
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn delta(index: Option<i64>, id: &str, name: &str, args: &str) -> ToolCallDelta {
        ToolCallDelta {
            index,
            id: id.into(),
            kind: String::new(),
            function: FnDelta { name: name.into(), arguments: args.into() },
            extra_content: None,
        }
    }

    #[test]
    fn assembles_fragments_and_whole_calls() {
        let mut a = ToolCallAssembler::default();
        a.add(delta(Some(0), "c1", "lookup", ""));
        a.add(delta(Some(0), "", "", "{\"q\":"));
        a.add(delta(Some(0), "", "", "1}"));
        // a provider that reuses index 0 for a second, whole call
        a.add(delta(Some(0), "c2", "end_call", ""));
        a.add(delta(Some(5), "", "", "x")); // nameless fragment is dropped
        let calls = a.done();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].function.arguments, "{\"q\":1}");
        assert_eq!(calls[0].kind, "function");
        assert_eq!(calls[1].function.name, "end_call");
        assert_eq!(calls[1].function.arguments, "{}");
    }

    /// Tiny one-shot HTTP/1.1 server answering with the given bodies in turn.
    async fn serve(responses: Vec<(u16, String)>) -> String {
        let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = lis.local_addr().unwrap();
        tokio::spawn(async move {
            for (code, body) in responses {
                let (mut s, _) = lis.accept().await.unwrap();
                let mut buf = vec![0u8; 16384];
                let _ = s.read(&mut buf).await;
                let head = format!(
                    "HTTP/1.1 {code} X\r\nContent-Type: text/event-stream\r\nX-Request-Id: req-{code}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                s.write_all(head.as_bytes()).await.unwrap();
                s.write_all(body.as_bytes()).await.unwrap();
            }
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn streams_tokens_tool_calls_and_retries_429() {
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}\n\n\
                   data: {\"choices\":[{\"delta\":{\"content\":\"lo\"}}]}\n\n\
                   : keepalive\n\ndata: not json\n\n\
                   data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"c1\",\"function\":{\"name\":\"end_call\",\"arguments\":\"\"}}]}}]}\n\n\
                   data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":3,\"total_tokens\":13,\"prompt_tokens_details\":{\"cached_tokens\":4}}}\n\n\
                   data: [DONE]\n\n";
        let url = serve(vec![(429, "slow down".into()), (200, sse.into())]).await;
        let mut c = LlmClient::new(&url, "k", "m");
        c.retry_max_wait = Duration::from_millis(50);
        let mut text = String::new();
        let out = c.stream(&CancellationToken::new(), &[Message::new("user", "hi")], &[], &mut |t| text.push_str(t)).await.unwrap();
        let calls = out.calls;
        assert_eq!(text, "Hello");
        assert_eq!(out.usage, Some(Usage { prompt: 10, completion: 3, total: 13, cached: 4 }));
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "end_call");
        let m = crate::metrics::render();
        let recovered =
            m.lines().find_map(|l| l.strip_prefix("vaani_llm_retries_total{result=\"recovered\"} ")?.trim().parse::<u64>().ok()).unwrap();
        assert!(recovered >= 1, "a request that needed a retry and then worked is counted as recovered");
    }

    #[tokio::test]
    async fn empty_stream_is_an_error_and_4xx_is_not_retried() {
        let url = serve(vec![(200, "data: {\"choices\":[]}\n\n".into())]).await;
        let c = LlmClient::new(&url, "k", "m");
        let e = c.stream(&CancellationToken::new(), &[], &[], &mut |_| {}).await.unwrap_err();
        assert!(matches!(e, LlmError::Other(_)), "{e}");

        let url = serve(vec![(400, "bad".into())]).await;
        let c = LlmClient::new(&url, "k", "m");
        let e = c.stream(&CancellationToken::new(), &[], &[], &mut |_| {}).await.unwrap_err();
        assert!(matches!(e, LlmError::Status { code: 400, .. }), "{e}");
    }

    /// Every HTTP attempt carries its ids: what a provider's support asks for when a request was slow or failed.
    #[tokio::test]
    async fn each_attempt_carries_its_request_ids() {
        let sse = "data: {\"id\":\"chatcmpl-7\",\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\ndata: [DONE]\n\n";
        let url = serve(vec![(429, "slow down".into()), (200, sse.into())]).await;
        let c = LlmClient::new(&url, "k", "m1");
        let msgs = [Message::new("user", "hi")];

        let (res, info, _) = c.measured(&CancellationToken::new(), &msgs, &[], &mut |_| {}).await;
        assert!(matches!(res, Err(LlmError::Status { code: 429, .. })));
        assert_eq!((info.status, info.request_id.as_str(), info.completion_id.as_str()), (429, "req-429", ""));

        let (res, info, _) = c.measured(&CancellationToken::new(), &msgs, &[], &mut |_| {}).await;
        assert!(res.is_ok());
        assert_eq!((info.status, info.request_id.as_str(), info.completion_id.as_str()), (200, "req-200", "chatcmpl-7"));
    }

    #[test]
    fn a_requests_log_level_says_what_to_look_at() {
        use tracing::Level;
        assert_eq!(request_level(Duration::from_millis(200), true), Level::WARN, "failed, however fast");
        assert_eq!(request_level(Duration::from_secs(3), false), Level::INFO, "slow: 3 s or more");
        assert_eq!(request_level(Duration::from_millis(2999), false), Level::DEBUG);
    }
}

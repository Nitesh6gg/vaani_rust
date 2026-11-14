//! OpenTelemetry export, as the Go server does it: set `OTEL_EXPORTER_OTLP_ENDPOINT` (e.g.
//! `http://<signoz-host>:4318`, or the `_LOGS_` / `_TRACES_` variants) and
//!
//! - every log line is also pushed over OTLP/HTTP, alongside (not instead of) the stderr output;
//!   a line that carries a `call_id` also carries that call's `trace_id` and `run_id`;
//! - each call is one trace: a `call` span, with a child `llm.request` span per LLM HTTP attempt
//!   (model, attempt, status, `llm.request_id`, `llm.completion_id`). The call's trace id is the
//!   `trace_id` on its log lines, so a log line finds the call's trace. The spans are `tracing` spans,
//!   exported by `tracing-opentelemetry`; the log lines are pushed by our own layer, because a call's lines are
//!   written from many tasks that are in no span -- what ties them to the call is the `call_id` field.
//!
//! Unset (the default): nothing is built, every function here is a cheap no-op. The service name is
//! `vaani-engine-rust` (`OTEL_SERVICE_NAME` overrides it) so both engines can report to one SigNoz
//! and be told apart.

use anyhow::{Context as _, Result};
use opentelemetry::{
    KeyValue,
    logs::{AnyValue, LogRecord as _, Logger as _, LoggerProvider as _, Severity},
    trace::{SpanId, Status, TraceContextExt as _, TraceFlags, TraceId, TracerProvider as _},
};
use opentelemetry_otlp::{LogExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    logs::{SdkLogger, SdkLoggerProvider},
    trace::{IdGenerator, SdkTracer, SdkTracerProvider},
};
use std::{
    cell::Cell,
    collections::HashMap,
    sync::{Mutex, RwLock},
    time::{Duration, Instant, SystemTime},
};
use tracing::{Event, Subscriber, field::Visit};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::{Layer, layer::Context as LayerContext, registry::LookupSpan};

const SERVICE_NAME: &str = "vaani-engine-rust";
/// How long a finished call's trace id is kept for the log lines written after its teardown.
const KEEP: Duration = Duration::from_secs(3600);
const MAX_CALLS: usize = 4096;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// What is known about a call being traced.
struct CallTrace {
    trace_id: TraceId,
    span_id: SpanId,
    run_id: Option<i64>,
    /// The `call` span, the parent of its LLM requests; `None` once ended or when only logs are exported.
    span: Option<tracing::Span>,
    at: Instant,
}

struct State {
    tracer: Option<SdkTracer>,
    calls: Mutex<HashMap<String, CallTrace>>,
}

static STATE: RwLock<Option<std::sync::Arc<State>>> = RwLock::new(None);

fn state() -> Option<std::sync::Arc<State>> {
    STATE.read().unwrap().clone()
}

thread_local! {
    /// The trace id for the root span about to be started on this thread (see `CallIds`).
    static NEXT_TRACE: Cell<Option<TraceId>> = const { Cell::new(None) };
}

/// Gives a call's root span the call's own trace id (the one its log lines carry), and every other
/// root span a random one.
#[derive(Debug)]
struct CallIds;

impl IdGenerator for CallIds {
    fn new_trace_id(&self) -> TraceId {
        NEXT_TRACE.with(Cell::take).unwrap_or_else(random_trace_id)
    }
    fn new_span_id(&self) -> SpanId {
        random_span_id()
    }
}

fn random_trace_id() -> TraceId {
    loop {
        let id = TraceId::from(rand::random::<u128>());
        if id != TraceId::INVALID {
            return id;
        }
    }
}

fn random_span_id() -> SpanId {
    loop {
        let id = SpanId::from(rand::random::<u64>());
        if id != SpanId::INVALID {
            return id;
        }
    }
}

/// Built by `start`; `shutdown` flushes what is batched.
pub struct Telemetry {
    logs: Option<SdkLoggerProvider>,
    traces: Option<SdkTracerProvider>,
    tracer: Option<SdkTracer>,
}

/// The tracing layer that pushes log lines over OTLP.
pub struct OtlpLogs {
    logger: SdkLogger,
}

fn endpoint_set(signal: &str) -> bool {
    let set = |k: &str| std::env::var(k).is_ok_and(|v| !v.is_empty());
    set(&format!("OTEL_EXPORTER_OTLP_{signal}_ENDPOINT")) || set("OTEL_EXPORTER_OTLP_ENDPOINT")
}

fn resource() -> Resource {
    let name = std::env::var("OTEL_SERVICE_NAME").ok().filter(|v| !v.is_empty()).unwrap_or_else(|| SERVICE_NAME.into());
    let env = std::env::var("DEPLOYMENT_ENVIRONMENT").ok().filter(|v| !v.is_empty()).unwrap_or_else(|| "development".into());
    Resource::builder()
        .with_service_name(name)
        .with_attributes([
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("deployment.environment", env),
            KeyValue::new("telemetry.sdk.language.engine", "rust"),
        ])
        .build()
}

/// Reads the OTLP environment: with an endpoint set, builds the exporters. Without one, returns nothing
/// and everything stays a no-op. A failure to build them is logged by the caller, never fatal.
pub fn start() -> Result<(Telemetry, Option<OtlpLogs>)> {
    build(endpoint_set("LOGS").then_some(None), endpoint_set("TRACES").then_some(None))
}

/// `logs` / `traces`: `None` = off; `Some(None)` = on, endpoint from the environment; `Some(Some(url))` = on, that URL.
fn build(logs: Option<Option<String>>, traces: Option<Option<String>>) -> Result<(Telemetry, Option<OtlpLogs>)> {
    let res = resource();
    let (mut tel, mut layer) = (Telemetry { logs: None, traces: None, tracer: None }, None);

    if let Some(url) = logs {
        let mut b = LogExporter::builder().with_http();
        if let Some(u) = url {
            b = b.with_endpoint(u);
        }
        let provider =
            SdkLoggerProvider::builder().with_batch_exporter(b.build().context("otlp log exporter")?).with_resource(res.clone()).build();
        layer = Some(OtlpLogs { logger: provider.logger("vaani") });
        tel.logs = Some(provider);
    }

    let mut tracer = None;
    if let Some(url) = traces {
        let mut b = SpanExporter::builder().with_http();
        if let Some(u) = url {
            b = b.with_endpoint(u);
        }
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(b.build().context("otlp span exporter")?)
            .with_resource(res)
            .with_id_generator(CallIds)
            .build();
        tracer = Some(provider.tracer("vaani"));
        tel.tracer = tracer.clone();
        tel.traces = Some(provider);
    }

    if tel.logs.is_some() || tel.traces.is_some() {
        *STATE.write().unwrap() = Some(std::sync::Arc::new(State { tracer, calls: Mutex::new(HashMap::new()) }));
    }
    Ok((tel, layer))
}

impl Telemetry {
    /// Nothing exported.
    pub fn off() -> Self {
        Self { logs: None, traces: None, tracer: None }
    }

    /// The layer that exports `tracing` spans as OpenTelemetry spans; `None` with traces off. It sees spans only, never
    /// events (so it doesn't switch every `debug!` on), and adds no attributes of its own beyond the span's fields.
    pub fn span_layer<S>(&self) -> Option<Box<dyn Layer<S> + Send + Sync>>
    where
        S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync,
    {
        use tracing_subscriber::filter::filter_fn;
        let tracer = self.tracer.clone()?;
        let layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_location(false)
            .with_threads(false)
            .with_level(false)
            .with_target(false)
            .with_tracked_inactivity(false);
        Some(layer.with_filter(filter_fn(|m| m.is_span())).boxed())
    }

    pub fn enabled(&self) -> bool {
        self.logs.is_some() || self.traces.is_some()
    }

    /// Flushes the batched logs and spans; call once, after the last line that should reach the collector.
    pub fn shutdown(self) {
        if let Some(t) = &self.traces {
            let _ = t.shutdown_with_timeout(SHUTDOWN_TIMEOUT);
        }
        if let Some(l) = &self.logs {
            let _ = l.shutdown_with_timeout(SHUTDOWN_TIMEOUT);
        }
        *STATE.write().unwrap() = None;
    }
}

// ------------------------------------------------------------------ calls ---

/// A call has been staged: it gets a trace id (so every line about it carries one) and, with traces on,
/// its `call` span.
pub fn start_call(call_id: &str, attrs: &[(&'static str, String)], run_id: Option<i64>) {
    let Some(st) = state() else { return };
    let mut trace_id = random_trace_id();
    let (mut span_id, mut span) = (SpanId::INVALID, None);
    if st.tracer.is_some() {
        // A root span, which takes the id in `NEXT_TRACE` when it is started. tracing-opentelemetry starts it on first
        // use, so the first `set_attribute` below does it, here, on this thread.
        NEXT_TRACE.with(|c| c.set(Some(trace_id)));
        let sp = tracing::info_span!(parent: None, "call", call_id = %call_id);
        for (k, v) in attrs {
            sp.set_attribute(*k, v.clone());
        }
        if let Some(r) = run_id {
            sp.set_attribute("run.id", r);
        }
        NEXT_TRACE.with(|c| c.set(None));
        let sc = sp.context().span().span_context().clone();
        if sc.is_valid() {
            (trace_id, span_id, span) = (sc.trace_id(), sc.span_id(), Some(sp));
        }
    }
    let mut calls = st.calls.lock().unwrap();
    if calls.len() >= MAX_CALLS {
        calls.retain(|_, c| c.at.elapsed() < KEEP);
    }
    calls.insert(call_id.to_string(), CallTrace { trace_id, span_id, run_id, span, at: Instant::now() });
}

/// The run Dograh recorded for the call, once known: later lines carry it, and the span gets it.
pub fn set_run(call_id: &str, run_id: i64) {
    let Some(st) = state() else { return };
    if let Some(c) = st.calls.lock().unwrap().get_mut(call_id) {
        c.run_id = Some(run_id);
        if let Some(sp) = &c.span {
            sp.set_attribute("run.id", run_id);
        }
    }
}

/// The call's agent has left it: ends the `call` span. Its trace id stays for the lines written after.
pub fn end_call(call_id: &str) {
    let Some(st) = state() else { return };
    let span = st.calls.lock().unwrap().get_mut(call_id).and_then(|c| c.span.take());
    drop(span); // the last handle closing is what ends the span
}

/// Everything about the call is logged: its trace id is no longer needed.
pub fn forget_call(call_id: &str) {
    let Some(st) = state() else { return };
    let call = st.calls.lock().unwrap().remove(call_id);
    drop(call);
}

/// The `call` span id of a call, for tests.
#[cfg(test)]
pub fn span_id_of(call_id: &str) -> Option<SpanId> {
    state()?.calls.lock().unwrap().get(call_id).map(|c| c.span_id)
}

/// The trace id of a call (hex), for tests and diagnostics.
#[cfg(test)]
pub fn trace_id_of(call_id: &str) -> Option<TraceId> {
    state()?.calls.lock().unwrap().get(call_id).map(|c| c.trace_id)
}

/// A request to the LLM, as a child span of its call's `call` span. `None` when traces are off.
pub struct LlmSpan(tracing::Span);

pub fn llm_span(call_id: &str) -> Option<LlmSpan> {
    let st = state()?;
    st.tracer.as_ref()?;
    let parent = st.calls.lock().unwrap().get(call_id).and_then(|c| c.span.clone());
    Some(LlmSpan(match parent {
        Some(p) => tracing::info_span!(parent: &p, "llm.request"),
        None => tracing::info_span!(parent: None, "llm.request"),
    }))
}

impl LlmSpan {
    /// Ends the span with what the request came to.
    pub fn end(self, model: &str, attempt: u32, status: u16, request_id: &str, completion_id: &str, error: Option<&str>) {
        let sp = &self.0;
        sp.set_attribute("llm.model", model.to_string());
        sp.set_attribute("llm.attempt", i64::from(attempt));
        sp.set_attribute("http.status_code", i64::from(status));
        sp.set_attribute("llm.request_id", request_id.to_string());
        sp.set_attribute("llm.completion_id", completion_id.to_string());
        if let Some(e) = error {
            sp.add_event("exception", vec![KeyValue::new("exception.message", e.to_string())]);
            sp.set_status(Status::error(e.to_string()));
        }
    }
}

// ------------------------------------------------------------------- logs ---

/// Collects an event's fields: `message` is the log body, the rest are attributes.
#[derive(Default)]
struct Fields {
    body: Option<String>,
    attrs: Vec<(String, AnyValue)>,
    call_id: Option<String>,
}

impl Fields {
    fn put(&mut self, name: &str, v: AnyValue) {
        if name == "call_id"
            && let AnyValue::String(s) = &v
        {
            self.call_id = Some(s.to_string());
        }
        self.attrs.push((name.to_string(), v));
    }
}

impl Visit for Fields {
    fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
        if f.name() == "message" {
            self.body = Some(v.to_string());
        } else {
            self.put(f.name(), AnyValue::from(v.to_string()));
        }
    }
    fn record_i64(&mut self, f: &tracing::field::Field, v: i64) {
        self.put(f.name(), AnyValue::from(v));
    }
    fn record_u64(&mut self, f: &tracing::field::Field, v: u64) {
        self.put(f.name(), AnyValue::from(i64::try_from(v).unwrap_or(i64::MAX)));
    }
    fn record_f64(&mut self, f: &tracing::field::Field, v: f64) {
        self.put(f.name(), AnyValue::from(v));
    }
    fn record_bool(&mut self, f: &tracing::field::Field, v: bool) {
        self.put(f.name(), AnyValue::from(v));
    }
    fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
        let s = format!("{v:?}");
        if f.name() == "message" {
            self.body = Some(s);
        } else {
            self.put(f.name(), AnyValue::from(s));
        }
    }
}

/// Targets whose events must never be exported: they are the exporter's own, and exporting them would feed on itself.
fn own_noise(target: &str) -> bool {
    ["opentelemetry", "reqwest", "hyper", "h2", "rustls", "tower", "tokio_tungstenite"].iter().any(|p| target.starts_with(p))
}

impl<S: Subscriber> Layer<S> for OtlpLogs {
    fn on_event(&self, event: &Event<'_>, _ctx: LayerContext<'_, S>) {
        let meta = event.metadata();
        if own_noise(meta.target()) {
            return;
        }
        let mut f = Fields::default();
        event.record(&mut f);

        let mut rec = self.logger.create_log_record();
        let now = SystemTime::now();
        rec.set_timestamp(now);
        rec.set_observed_timestamp(now);
        rec.set_target(meta.target().to_string());
        let (sev, text) = match *meta.level() {
            tracing::Level::TRACE => (Severity::Trace, "TRACE"),
            tracing::Level::DEBUG => (Severity::Debug, "DEBUG"),
            tracing::Level::INFO => (Severity::Info, "INFO"),
            tracing::Level::WARN => (Severity::Warn, "WARN"),
            tracing::Level::ERROR => (Severity::Error, "ERROR"),
        };
        rec.set_severity_number(sev);
        rec.set_severity_text(text);
        if let Some(b) = f.body.take() {
            rec.set_body(AnyValue::from(b));
        }
        // A line about a call carries the call's trace id (and run id), whatever task logged it.
        if let Some(id) = &f.call_id
            && let Some(st) = state()
            && let Some(c) = st.calls.lock().unwrap().get(id)
        {
            rec.set_trace_context(c.trace_id, c.span_id, Some(TraceFlags::SAMPLED));
            f.attrs.push(("trace_id".into(), AnyValue::from(c.trace_id.to_string())));
            if let Some(r) = c.run_id {
                f.attrs.push(("run_id".into(), AnyValue::from(r)));
            }
        }
        rec.add_attributes(f.attrs);
        self.logger.emit(rec);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tracing_subscriber::layer::SubscriberExt;

    type Seen = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

    static CALL_SPAN: Mutex<Option<SpanId>> = Mutex::new(None);
    /// The telemetry state is process-wide: the tests that build it take turns.
    static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// A collector: records every POST (path and protobuf body) and answers 200 with an empty response.
    async fn collector() -> (String, Seen) {
        let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", lis.local_addr().unwrap());
        let seen: Seen = Arc::default();
        let s = seen.clone();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = lis.accept().await.unwrap();
                let s = s.clone();
                tokio::spawn(async move {
                    let mut buf = vec![];
                    loop {
                        let mut chunk = [0u8; 8192];
                        let n = sock.read(&mut chunk).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else { continue };
                        let head = String::from_utf8_lossy(&buf[..end]).to_string();
                        let len = head
                            .to_lowercase()
                            .split("content-length:")
                            .nth(1)
                            .and_then(|v| v.split("\r\n").next()?.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        if buf.len() < end + 4 + len {
                            continue;
                        }
                        let path = head.lines().next().unwrap_or_default().split_whitespace().nth(1).unwrap_or_default().to_string();
                        s.lock().unwrap().push((path, buf[end + 4..end + 4 + len].to_vec()));
                        let _ = sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/x-protobuf\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await;
                        return;
                    }
                });
            }
        });
        (url, seen)
    }

    fn contains(hay: &[u8], needle: &[u8]) -> bool {
        hay.windows(needle.len()).any(|w| w == needle)
    }

    #[test]
    fn off_without_an_endpoint() {
        let (tel, layer) = build(None, None).unwrap();
        assert!(!tel.enabled() && layer.is_none());
        assert!(!own_noise("vaani::ari") && own_noise("opentelemetry_sdk::logs") && own_noise("hyper_util::client"));
    }

    /// A call's log lines and its `call` / `llm.request` spans reach the collector under the call's trace id.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_calls_logs_and_spans_share_its_trace_id() {
        let _turn = SERIAL.lock().await;
        let (url, seen) = collector().await;
        let (tel, layer) = build(Some(Some(format!("{url}/v1/logs"))), Some(Some(format!("{url}/v1/traces")))).unwrap();
        assert!(tel.enabled());

        let subscriber = tracing_subscriber::registry().with(layer).with(tel.span_layer());
        let trace_id = tracing::subscriber::with_default(subscriber, || {
            start_call("call-otel-1", &[("direction", "outbound".into())], None);
            let trace_id = trace_id_of("call-otel-1").expect("registered");
            *CALL_SPAN.lock().unwrap() = span_id_of("call-otel-1");
            tracing::info!(call_id = "call-otel-1", event = "call.answered", port = 20001, "call answered");
            set_run("call-otel-1", 6528);
            tracing::info!(call_id = "call-otel-1", event = "turn.user", "second line");
            tracing::warn!(event = "no.call", "a line about no call");
            tracing::info!(target: "hyper::proto", call_id = "call-otel-1", "exporter noise");
            llm_span("call-otel-1").expect("traces on").end("gpt-x", 2, 200, "req-abc-1", "chatcmpl-xyz-9", None);
            llm_span("call-otel-1").unwrap().end("gpt-x", 1, 429, "req-abc-0", "", Some("429 too many requests"));
            end_call("call-otel-1");
            forget_call("call-otel-1");
            trace_id
        });
        assert!(trace_id_of("call-otel-1").is_none(), "forgotten");

        tokio::task::spawn_blocking(move || tel.shutdown()).await.unwrap(); // flushes the batches
        let seen = seen.lock().unwrap().clone();
        let logs: Vec<Vec<u8>> = seen.iter().filter(|(p, _)| p == "/v1/logs").map(|(_, b)| b.clone()).collect();
        let traces: Vec<Vec<u8>> = seen.iter().filter(|(p, _)| p == "/v1/traces").map(|(_, b)| b.clone()).collect();
        assert!(
            !logs.is_empty() && !traces.is_empty(),
            "both signals were posted: {:?}",
            seen.iter().map(|(p, b)| (p, b.len())).collect::<Vec<_>>()
        );

        let tid = trace_id.to_bytes();
        let logs = logs.concat();
        assert!(contains(&logs, &tid), "log records carry the call's trace id");
        for want in ["call answered", "second line", "a line about no call", "call.answered", "vaani-engine-rust", "trace_id", "run_id"] {
            assert!(contains(&logs, want.as_bytes()), "{want} missing from the exported logs");
        }
        assert!(!contains(&logs, b"exporter noise"), "the exporter's own events are never exported");

        let traces = traces.concat();
        assert!(contains(&traces, &tid), "spans are in the call's trace");
        // The call span's id is its own and the two LLM requests' parent: three times in all.
        let sid = CALL_SPAN.lock().unwrap().expect("span id").to_bytes();
        assert_eq!(traces.windows(8).filter(|w| *w == sid).count(), 3, "llm.request spans are children of the call span");
        for noise in ["code.file.path", "thread.id", "busy_ns", "idle_ns"] {
            assert!(!contains(&traces, noise.as_bytes()), "{noise}: the span layer adds no attributes of its own");
        }
        for want in [
            "call",
            "llm.request",
            "llm.request_id",
            "req-abc-1",
            "chatcmpl-xyz-9",
            "gpt-x",
            "call_id",
            "call-otel-1",
            "429 too many requests",
            "vaani-engine-rust",
        ] {
            assert!(contains(&traces, want.as_bytes()), "{want} missing from the exported spans");
        }
    }

    /// The log layer's own level filter doesn't hide spans: LOG_LEVEL=error still exports the call's trace.
    #[tokio::test(flavor = "multi_thread")]
    async fn spans_are_exported_whatever_the_log_level() {
        let _turn = SERIAL.lock().await;
        use tracing_subscriber::{EnvFilter, Layer as _};
        let (url, seen) = collector().await;
        let (tel, layer) = build(Some(Some(format!("{url}/v1/logs"))), Some(Some(format!("{url}/v1/traces")))).unwrap();
        let subscriber = tracing_subscriber::registry().with(layer.map(|l| l.with_filter(EnvFilter::new("error")))).with(tel.span_layer());
        tracing::subscriber::with_default(subscriber, || {
            start_call("call-otel-2", &[], None);
            tracing::info!(call_id = "call-otel-2", "an info line the filter drops");
            llm_span("call-otel-2").unwrap().end("m", 1, 200, "r", "c", None);
            end_call("call-otel-2");
            forget_call("call-otel-2");
        });
        tokio::task::spawn_blocking(move || tel.shutdown()).await.unwrap();
        let seen = seen.lock().unwrap().clone();
        let traces: Vec<u8> = seen.iter().filter(|(p, _)| p == "/v1/traces").flat_map(|(_, b)| b.clone()).collect();
        let logs: Vec<u8> = seen.iter().filter(|(p, _)| p == "/v1/logs").flat_map(|(_, b)| b.clone()).collect();
        assert!(contains(&traces, b"call-otel-2") && contains(&traces, b"llm.request"));
        assert!(!contains(&logs, b"an info line"), "the log filter still applies to logs");
    }
}

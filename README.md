# vaani_rust

Real-time voice agent in Rust: **Asterisk ARI** (telephony) + **AudioSocket** media plane +
**Sarvam** STT/TTS + an **OpenAI-compatible LLM**, with **Earshot** VAD for barge-in. A port of
[`vaani`](https://github.com/nitesh6gg/vaani) (Go) aimed at more concurrent calls per box with
less memory. Agent configuration (workflow, prompts, tools, models, keys) is read per call from a
self-hosted **Dograh**'s Postgres, exactly as in the Go server — no model settings in `.env`.

## Status

Milestones 1-3 are done except QA analysis (below): a Rust call is a complete call, recorded and priced in Dograh like one of its own, with transfers, extraction and the STT gate. Ported from Go:

| Area | State |
|---|---|
| ARI events WebSocket (one per process), reconnect, REST ops, call lifecycle | done |
| AudioSocket media (single-write frames, 20ms coalescing pacer, port pool, dead-media watchdog) | done |
| Sarvam STT (VAD signals, lazy one-shot redial) and TTS (generation-tagged, FIFO attribution, keepalive, recorded clips) | done |
| Streaming LLM client (SSE, tool calls, token usage, retries on 429/5xx, idle timeout, shared HTTP/2 client) | done |
| Agent state machine: greeting / LLM opening, sentence chunker, tools + workflow edges, idle prompts, LLM-failure fallback | done |
| Barge-in: Earshot vote window, pause → confirm/resume, 5 cuts, backchannel filter, held speech | done |
| Dograh reads: workflow graph, `{{templates}}`, models/keys + overrides, `end_call` and `http_api` tools (with credentials), call limits | done |
| **Dograh call record**: `workflow_runs` created at call start and completed at the end (conversation events, nodes visited, disposition + organization mapping, usage/cost info, call tags) | done |
| **Recording + transcript** mixed to mono WAV and uploaded to Dograh's MinIO (hand-written SigV4, checked against the MinIO Python client) | done |
| **Recorded clips**: audio greeting, audio transitions, tool messages, and the `● recording_id` / `▸ text` reply mode (MinIO → ffmpeg → trimmed, cached in memory and on disk) | done |
| **`transfer_call`**: dial via ARI, hold audio while it rings, beep on answer, bridge swap, the agent leaves; failures go back to the LLM | done |
| **Outbound campaigns** (`docs/OUTBOUND.md`): the dialer over Dograh's queue, org rate/concurrency in Dograh's own Redis keys and Lua scripts, retries, circuit breaker, schedule, wallet, restart recovery, `OUTBOUND_TEST_CALL` | done |
| **Cost**: Dograh's pricing tables, `cost_info` breakdown, organization usage cycle and charge | done |
| **Variable extraction** (per node on leaving it, and at call end after teardown) feeding `gathered_context`, the disposition and call tags | done |
| **STT speech-only gate** (pre-roll, hangover, keepalive) and **early flush** | done |
| `/metrics` (Prometheus text) and `/healthz` | done |
| **Load generator** (`loadgen`: fake Asterisk + mock providers; `docs/LOADTEST.md`) | done |
| **Call setup under bursts**: an inbound caller rings until its workflow is loaded and its STT is open, then is answered (`ANSWER_WAIT_SECONDS`, else hung up as congestion); 16 setups at once against Asterisk, 32 prepares at once, CPU guard (`LOAD_SHED_CPU_PERCENT`), bounded post-call work | done |
| **Outbound HTTP identity and liveness**: every HTTP client and provider WebSocket sends `User-Agent: Rust-http-client/1.1` (gateways such as Bifrost name the app from it); the ARI REST client uses a 30 s TCP keepalive, and the ARI events WebSocket is pinged every 30 s and reconnected after 75 s of silence | done |
| **OpenTelemetry**: logs and traces over OTLP/HTTP (`OTEL_EXPORTER_OTLP_ENDPOINT`), as in the Go server | done |

Not ported yet (in rough priority order):

1. QA analysis nodes (their tokens aren't priced yet)
2. Debug WAV recording
3. TLS to Dograh's Postgres (`tokio-postgres-rustls`)

Needs on the server: **ffmpeg** on `PATH` (recorded clips) and Dograh's `MINIO_*` settings (see below).

Out of scope by decision: RTP transport, FreeSWITCH/mod_earshot, TEN VAD, RMS-energy VAD.

## How it differs from the Go design

* **One task owns all of a call's state.** The agent multiplexes the 20ms release tick, the STT's
  events and its helper tasks' events in one `select!`. The Go version's atomics and signal
  channels between its frame goroutine and event goroutine become plain method calls — no locks
  in the agent.
* **Frames are `[u8; 640]` by value**, so the hot path never allocates (invariant #3 without a pool).
* **TTS and LLM run off the agent task.** Opening the TTS socket (a TLS+WS handshake) happens in
  the turn task and is handed back by event, so it never stalls the 20ms tick. The Dograh read,
  STT handshake and Asterisk's TCP connect all overlap at call start.
* **TTS events are one ordered channel**, so a generation's audio always precedes its `Done`
  (the Go version needed a priority-peek to guarantee that).
* WebSocket buffers are trimmed to 16 KiB read / unbuffered write (tungstenite's 128 KiB defaults
  dominated per-call memory).

Invariants from the Go `CLAUDE.md` all hold; see `CLAUDE.md` here for where each one lives.

## Configuration

Environment variables (a `.env` in the working directory or a parent is loaded; real env wins).
Same names as the Go server.

| Variable | Default | |
|---|---|---|
| `ARI_URL` `ARI_USER` `ARI_PASS` `ARI_APP` | `http://localhost:8088/ari` `vaani` `vaani` `vaani` | |
| `MEDIA_IP` | `127.0.0.1` | address given to Asterisk; never auto-detected |
| `MEDIA_PORT_BASE` / `MEDIA_PORT_COUNT` | `20000` / `256` | one TCP port per call |
| `DOGRAH_DB_URL` | **required** | |
| `DOGRAH_WORKFLOW_ID` | `0` | workflow run by inbound calls |
| `DOGRAH_DB_MAX_CONNS` `DOGRAH_DB_WAIT_MS` | `40` `5000` | Postgres pool per engine process, shared by the campaign dial workers and the calls being set up; a caller waits at most the second for a free connection, then fails (`vaani_db_pool_timeouts_total`) |
| `BARGE_IN_ENABLED` | `1` | `0` = observe only |
| `BARGE_IN_MIN_SPEECH_MS` / `BARGE_IN_GUARD_MS` | `200` / `300` | |
| `BARGE_IN_POST_CUT_SILENCE_MS` | `1200` | |
| `BARGE_IN_ACK_FILTER` / `ACK_END_MARGIN_MS` | `1` / `300` | "जी"/"हाँ" resume instead of cut |
| `VAD_THRESHOLD` | `0.5` | Earshot speech probability |
| `MAX_CALL_DURATION_SECONDS` | `0` | backstop; 0 = off |
| `MEDIA_DEAD_TIMEOUT_SECONDS` | `30` | no inbound audio at all |
| `STT_FLUSH_AFTER_MS` | `400` | ask Sarvam to finalize once the VAD heard this much quiet after the caller (0 = off); replies ~0.2-0.3s sooner |
| `STT_SPEECH_ONLY` `STT_PREROLL_MS` `STT_HANGOVER_MS` `STT_GATE_RMS_FLOOR` | `0` `1000` `700` `400` | send the STT only the caller's speech (Sarvam bills audio sent; ~half the cost); hangover must exceed the flush wait |
| `LLM_RETRIES` `LLM_RETRY_MAX_WAIT_MS` `LLM_MAX_FAILED_TURNS` `LLM_FALLBACK_TEXT` | `2` `2000` `3` `` | |
| `SARVAM_STT_WS_URL` / `SARVAM_TTS_WS_URL` | Sarvam production | point at mocks for load runs |
| `MINIO_ENDPOINT` `MINIO_ACCESS_KEY` `MINIO_SECRET_KEY` `MINIO_BUCKET` `MINIO_SECURE` | `` `` `` `voice-audio` `false` | Dograh's MinIO (host:port). Without an endpoint, recordings/transcripts aren't uploaded and recorded clips can't play |
| `METRICS_ADDR` | `0.0.0.0:9091` | `/metrics`, `/healthz` |
| `CAMPAIGNS_ENABLED` `REDIS_URL` `OUTBOUND_MAX_CALLS` `OUTBOUND_DEFAULT_ORG_CONCURRENCY` `CAMPAIGN_POLL_SECONDS` `OUTBOUND_RING_TIMEOUT_SECONDS` `OUTBOUND_TEST_CALL` | `0` `` `20` `2` `5` `30` `` | outbound campaigns; see `docs/OUTBOUND.md` |
| `CAMPAIGN_DIAL_WORKERS` `OUTBOUND_MAX_SETUP` | `10` `30` | contacts of one campaign dialed at once; the dialer holds back while this many answered calls wait in setup (0 = no cap) |
| `LOAD_SHED_CPU_PERCENT` `LOAD_SHED_RESUME_PERCENT` | `90` `80` | CPU guard: while the whole machine's CPU (5 s average) is at/over the first, new calls are refused (inbound hung up as congestion before answering, campaigns don't dial); calls in progress are never dropped. 0 = off. Linux only |
| `ANSWER_WAIT_SECONDS` | `10` | An inbound caller is answered only once its workflow is loaded and its STT is open; it rings meanwhile. If that, plus its turn at Asterisk (16 setups at once), takes longer than this, the caller is hung up as congestion (SIP 503) instead of being answered late. 0 = no limit. Outbound calls are already answered and unaffected |
| `SETUP_CONCURRENCY` `PREPARE_CONCURRENCY` `SETUP_TIMEOUT_SECONDS` | `16` `32` `15` | call setups (answer, externalMedia, bridge) running against Asterisk at once; workflow loads + STT handshakes for unanswered callers at once; how long a staged call may wait for its media channel to enter Stasis before it is hung up, counted from the answer (from the start for a call we placed) |
| `POST_CALL_WORKERS` `SHUTDOWN_WAIT_SECONDS` | `32` `60` | ended calls writing their Dograh record (last extraction, run completion, recording and transcript uploads, cost) at once, the rest wait with their recordings in memory (`vaani_post_call_running` / `_waiting`); how long a SIGTERM shutdown waits for calls and records still being written (it returns as soon as they are done) |
| `REDIS_POOL_SIZE` | `4` | Redis connections of the campaign dialer (1 to 64) |
| `ORPHAN_SWEEP_SECONDS` | `30` | every N s, hang up channels of this ARI app that Vaani lost track of (older than 90 s). Needs this Vaani to be the only one on its ARI app name. 0 = off |
| `OTEL_EXPORTER_OTLP_ENDPOINT` (or `_LOGS_ENDPOINT` / `_TRACES_ENDPOINT`) `OTEL_SERVICE_NAME` `DEPLOYMENT_ENVIRONMENT` | unset (off) `vaani-engine-rust` `development` | OTLP/HTTP, e.g. `http://<signoz-host>:4318`: every log line is also pushed there (a line with a `call_id` gets that call's `trace_id` and `run_id`), and each call is a trace: a `call` span with an `llm.request` child per LLM attempt (model, attempt, status, `llm.request_id`, `llm.completion_id`). Unset: nothing is built. See "Observability" |
| `LOG_LEVEL` / `LOG_FORMAT` | `info` / text (`json`) | |

Asterisk side: route calls into `Stasis(vaani)`; the externalMedia channel uses
`encapsulation=audiosocket`, `transport=tcp`, `connection_type=client`, `format=slin16`
(see the Go repo's `docs/SETUP.md`).

## Run and test

```sh
cargo build --release && ./target/release/vaani

cargo test                       # unit + mock-provider end-to-end tests (no network)

# also run the tests that need a real Postgres (each uses its own schema) and Redis (campaign limiter and dialer):
DOGRAH_TEST_DB_URL=postgres://postgres@127.0.0.1:5432/postgres REDIS_TEST_URL=redis://127.0.0.1:6379/ cargo test

# rough capacity probe: N agents against shared mock providers (an upper bound,
# the mocks run in the same process)
BENCH_CALLS=500 cargo test --release bench_concurrent_agents -- --ignored --nocapture
```

The tests that prove the call path: `agent::e2e_tests` (a full turn, a confirmed barge-in with all
cuts, a backchannel resume, recorded greeting/transition/`●` replies) and `ari::manager::tests`
(StasisStart → answer → bind-before-externalMedia → bridge → Dograh workflow from Postgres → agent
audio over AudioSocket → teardown frees the port and ARI resources exactly once → the Dograh run is
completed with its conversation, usage, disposition, and the recording and transcript uploaded).
`dograh::store` tests load a workflow and its recordings (fake MinIO + real ffmpeg) from Postgres.

`/metrics` exports `process_cpu_seconds_total`, `process_resident_memory_bytes`, `process_threads` and `vaani_tokio_alive_tasks` under the Go engine's names; `docs/BENCHMARK.md` is the method for a fair Go-vs-Rust run on your box.

First numbers (4 vCPU sandbox, mock providers in-process): 500 concurrent agents ≈ 515 KB RSS per
call, ≈ 0.3% of a core per call, worst 20ms-tick lateness ≈ 1 ms. **Not a substitute for the
Go-vs-Rust SIPp run against real Asterisk** — do that next and record it in `docs/BENCHMARK.md`.

## Observability

With `OTEL_EXPORTER_OTLP_ENDPOINT` set, logs and traces go to the collector (SigNoz) as the Go server's do, under
`service.name = vaani-engine-rust` (the Go server reports `vaani-engine`), so the two engines can share one SigNoz and be
told apart. Metrics are pulled the other way: SigNoz scrapes `/metrics`.

- A log line about a call (it has a `call_id`) carries the call's `trace_id`, and its `run_id` once Dograh's run exists;
  the OTLP record also carries the trace context, so a line finds the call's trace and a trace finds its lines.
  The stderr output is unchanged (it does not carry `trace_id`).
- Each call is one trace: a `call` span from the answer to teardown (attributes `call_id`, `direction`, `run.id`), and a child
  `llm.request` span per LLM HTTP attempt (`llm.model`, `llm.attempt`, `http.status_code`, `llm.request_id`, `llm.completion_id`,
  an error status when it failed).
- The same ids are in the `llm.request` log line of every attempt (INFO when it took 3 s or more, WARN when it failed, DEBUG
  otherwise): what a provider's support asks for.
- Not in the Rust server (yet): the STT/TTS handshake request ids (`stt.connected` / `tts.connected` in Go).


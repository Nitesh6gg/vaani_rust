# vaani_rust

Real-time voice agent in Rust: **Asterisk ARI** (telephony) + **AudioSocket** media plane +
**Sarvam** STT/TTS + an **OpenAI-compatible LLM**, with **Earshot** VAD for barge-in. A port of
[`vaani`](https://github.com/nitesh6gg/vaani) (Go) aimed at more concurrent calls per box with
less memory. Agent configuration (workflow, prompts, tools, models, keys) is read per call from a
self-hosted **Dograh**'s Postgres, exactly as in the Go server — no model settings in `.env`.

## Status

Milestone 1 — the core call path — is done and tested. Ported from Go:

| Area | State |
|---|---|
| ARI events WebSocket (one per process), reconnect, REST ops, call lifecycle | done |
| AudioSocket media (single-write frames, 20ms coalescing pacer, port pool, dead-media watchdog) | done |
| Sarvam STT (VAD signals, lazy one-shot redial) and TTS (generation-tagged, FIFO attribution, keepalive) | done |
| Streaming LLM client (SSE, tool calls, retries on 429/5xx, idle timeout, shared HTTP/2 client) | done |
| Agent state machine: greeting / LLM opening, sentence chunker, tools + workflow edges, idle prompts, LLM-failure fallback | done |
| Barge-in: Earshot vote window, pause → confirm/resume, 5 cuts, backchannel filter, held speech | done |
| Dograh reads: workflow graph, `{{templates}}`, models/keys + overrides, `end_call` and `http_api` tools (with credentials), call limits | done |
| `/metrics` (Prometheus text) and `/healthz` | done |

Not ported yet (in rough priority order):

1. Writing each call's record back to Dograh (`workflow_runs`, call log, recording/transcript upload to MinIO), QA analysis nodes, cost
2. `transfer_call` tool (needs ARI dial + bridge swap), recorded clips (audio greetings / transition speech), variable extraction
3. Outbound campaigns (`Manager.Dial`, Redis limiter) — ARI only
4. STT speech-only gate / early flush, debug WAV recording, OpenTelemetry
5. TLS to Dograh's Postgres (`tokio-postgres-rustls`)

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
| `DOGRAH_DB_MAX_CONNS` | `50` | |
| `BARGE_IN_ENABLED` | `1` | `0` = observe only |
| `BARGE_IN_MIN_SPEECH_MS` / `BARGE_IN_GUARD_MS` | `200` / `300` | |
| `BARGE_IN_POST_CUT_SILENCE_MS` | `1200` | |
| `BARGE_IN_ACK_FILTER` / `ACK_END_MARGIN_MS` | `1` / `300` | "जी"/"हाँ" resume instead of cut |
| `VAD_THRESHOLD` | `0.5` | Earshot speech probability |
| `MAX_CALL_DURATION_SECONDS` | `0` | backstop; 0 = off |
| `MEDIA_DEAD_TIMEOUT_SECONDS` | `30` | no inbound audio at all |
| `LLM_RETRIES` `LLM_RETRY_MAX_WAIT_MS` `LLM_MAX_FAILED_TURNS` `LLM_FALLBACK_TEXT` | `2` `2000` `3` `` | |
| `SARVAM_STT_WS_URL` / `SARVAM_TTS_WS_URL` | Sarvam production | point at mocks for load runs |
| `METRICS_ADDR` | `0.0.0.0:9091` | `/metrics`, `/healthz` |
| `LOG_LEVEL` / `LOG_FORMAT` | `info` / text (`json`) | |

Asterisk side: route calls into `Stasis(vaani)`; the externalMedia channel uses
`encapsulation=audiosocket`, `transport=tcp`, `connection_type=client`, `format=slin16`
(see the Go repo's `docs/SETUP.md`).

## Run and test

```sh
cargo build --release && ./target/release/vaani

cargo test                       # unit + mock-provider end-to-end tests (no network)

# also run the tests that need a real Postgres (each uses its own schema):
DOGRAH_TEST_DB_URL=postgres://postgres@127.0.0.1:5432/postgres cargo test

# rough capacity probe: N agents against shared mock providers (an upper bound,
# the mocks run in the same process)
BENCH_CALLS=500 cargo test --release bench_concurrent_agents -- --ignored --nocapture
```

The tests that prove the call path: `agent::e2e_tests` (a full turn, a confirmed barge-in with all
cuts, a backchannel resume) and `ari::manager::tests` (StasisStart → answer → bind-before-
externalMedia → bridge → Dograh workflow from Postgres → agent audio over AudioSocket → teardown
frees the port and ARI resources exactly once).

First numbers (4 vCPU sandbox, mock providers in-process): 500 concurrent agents ≈ 515 KB RSS per
call, ≈ 0.3% of a core per call, worst 20ms-tick lateness ≈ 1 ms. **Not a substitute for the
Go-vs-Rust SIPp run against real Asterisk** — do that next and record it in `docs/BENCHMARK.md`.

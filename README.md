# vaani_rust

Real-time voice agent in Rust: **Asterisk ARI** (telephony) + **AudioSocket** media plane +
**Sarvam** STT/TTS + an **OpenAI-compatible LLM**, with **Earshot** VAD for barge-in. A port of
[`vaani`](https://github.com/nitesh6gg/vaani) (Go) aimed at more concurrent calls per box with
less memory. Agent configuration (workflow, prompts, tools, models, keys) is read per call from a
self-hosted **Dograh**'s Postgres, exactly as in the Go server — no model settings in `.env`.

## Status

Milestones 1 and 2 are done and tested: a Rust call is now a complete call, recorded in Dograh like one of its own. Ported from Go:

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
| **Cost**: Dograh's pricing tables, `cost_info` breakdown, organization usage cycle and charge | done |
| **Variable extraction** (per node on leaving it, and at call end after teardown) feeding `gathered_context`, the disposition and call tags | done |
| **STT speech-only gate** (pre-roll, hangover, keepalive) and **early flush** | done |
| `/metrics` (Prometheus text) and `/healthz` | done |

Not ported yet (in rough priority order):

1. `transfer_call` tool (needs ARI dial + bridge swap, hold music)
2. QA analysis nodes (their tokens aren't priced yet)
3. Outbound campaigns (`Manager.Dial`, Redis limiter) — ARI only
4. Debug WAV recording, OpenTelemetry
5. TLS to Dograh's Postgres (`tokio-postgres-rustls`)

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
| `DOGRAH_DB_MAX_CONNS` | `50` | |
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
cuts, a backchannel resume, recorded greeting/transition/`●` replies) and `ari::manager::tests`
(StasisStart → answer → bind-before-externalMedia → bridge → Dograh workflow from Postgres → agent
audio over AudioSocket → teardown frees the port and ARI resources exactly once → the Dograh run is
completed with its conversation, usage, disposition, and the recording and transcript uploaded).
`dograh::store` tests load a workflow and its recordings (fake MinIO + real ffmpeg) from Postgres.

First numbers (4 vCPU sandbox, mock providers in-process): 500 concurrent agents ≈ 515 KB RSS per
call, ≈ 0.3% of a core per call, worst 20ms-tick lateness ≈ 1 ms. **Not a substitute for the
Go-vs-Rust SIPp run against real Asterisk** — do that next and record it in `docs/BENCHMARK.md`.

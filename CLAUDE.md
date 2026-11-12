# Vaani (Rust) — Agent Guidance

Rust port of the Go voice agent in `nitesh6gg/vaani` (see its `CLAUDE.md` and `docs/`; behaviour
should match unless this repo says otherwise). ARI telephony + AudioSocket media + Sarvam STT/TTS +
OpenAI-compatible LLM + Earshot VAD, config from Dograh's Postgres. `README.md` has status and the
to-port list.

## Layout
- `src/ari/` — events WebSocket + REST (`mod.rs`), per-call lifecycle (`manager.rs`)
- `src/media/` — AudioSocket codec, per-call reader/writer tasks + pacer (`call.rs`), port pool
- `src/ai/` — `stt.rs`, `tts.rs`, `llm.rs`, `chunker.rs`
- `src/agent/` — `handler.rs` (the per-call state machine), `vad.rs` (Earshot), `workflow.rs`, `backchannel.rs`,
  `calllog.rs` (Dograh's call-history events + summary), `recorder.rs` (mono mix + WAV), `recordings.rs` (`●`/`▸` reply router)
- `src/dograh/` — Postgres reads: `store.rs`, `workflow.rs`, `services.rs`, `tools.rs`, `template.rs`;
  writes: `run.rs` (`workflow_runs`); MinIO: `storage.rs` (SigV4), `recordings.rs` (clips via ffmpeg)
- `src/campaign/` — outbound campaigns (`docs/OUTBOUND.md`): `runner.rs` the dialer, `limiter.rs` Redis counters, `testcall.rs`;
  `src/ari/outbound.rs` is `Manager::dial` (ARI only); `src/dograh/campaign.rs`/`outbound.rs` its tables
- `src/load.rs` — CPU guard (`LOAD_SHED_CPU_PERCENT`): refuse *new* calls while the machine is over the limit; `src/ari/sweep.rs` —
  hangs up channels of our Stasis app that nothing tracks (`ORPHAN_SWEEP_SECONDS`)
- `src/telemetry.rs` — OTLP logs and traces (`OTEL_EXPORTER_OTLP_ENDPOINT`): a `call` span per call (the call's trace id is the `trace_id`
  on its log lines), `llm.request` child spans; no-op when unset
- `src/bin/loadgen.rs` — load generator: fake Asterisk (ARI + AudioSocket callers) and mock STT/TTS/LLM (`docs/LOADTEST.md`)
- `src/callagent.rs` — builds a call's agent from Dograh; `src/testkit.rs` — mock Sarvam/LLM servers

## Invariants (from the Go repo; where they live here)
1. **20ms pacer**: `media::call::new_ticker()` — `tokio::time::interval` with `MissedTickBehavior::Skip`
   (coalesces after a stall). Never `sleep(20ms)` loops, never the default `Burst`.
2. **Media = ARI externalMedia over AudioSocket** (`transport=tcp`, `slin16`). RTP is out of scope.
3. **No allocation per frame**: frames are `Frame = [u8; 640]` moved by value.
4. **Two tasks per call** (reader, writer) joined to the agent by bounded channels (agent audio out: cap 5; caller audio in:
   cap 50, and the agent's tick takes EVERY frame waiting, `Agent::hear_late`, never one per tick); never a task per frame.
5. **One shared HTTP client** (`ai::llm::http()`, HTTP/2, 200 idle/host). No per-request clients.
6. **One ARI WebSocket per process** (`ari::run_events`).
7. **Barge-in = 5 cuts, only once CONFIRMED** (`Agent::confirm_interruption` → `cut_reply`): stop feeding
   the queue, cancel the turn (LLM), close the TTS connection, flush the queue; STT stays fed always. A
   detected interruption only *pauses* (`paused`); no transcript + 2s quiet resumes from the sentence start.
8. **slin16 end to end.** 9. **One media socket per call, bound BEFORE Asterisk is told the address**
   (`Manager::setup`; the manager test connects immediately after the externalMedia request returns).
10. n/a (RTP). 11. **AudioSocket frame = ONE write** (`audiosocket::write_frame`, 643-byte buffer).

## Conventions
- Dograh queries: `use super::store::Cached` wherever `db.query(...)` is called, so statements are prepared once per
  connection (tokio-postgres's own `query(&str)` is two round trips). The dialer's pre-dial read is one query, `Store::dial_check`.
- Errors: `anyhow` with `.context()`, never panic in the request path. Logging: `tracing`, `call_id` on
  every call line, an `event` field (`turn.user`, `call.summary`, …). INFO tells the call's story; detail is DEBUG.
- Edition 2024: `gen` is reserved — use `generation`.
- Tests: a behaviour that matters gets a test that fails if it breaks. Provider behaviour is tested against
  `testkit` mocks speaking the real protocol; Postgres tests are gated on `DOGRAH_TEST_DB_URL`.
- `cargo fmt` (140 cols), `cargo clippy --all-targets` clean before committing.

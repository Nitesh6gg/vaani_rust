# Runbook: running the Rust engine

Everything here is read from the code and the existing docs; "not built" marks what an operator might expect and does not
exist yet. Settings are in the `README.md` environment table, measured numbers in `BENCHMARK.md`.

## Start, health, stop

- **Start:** `./target/release/vaani` with the environment from `README.md`. It needs ffmpeg on `PATH`, `ca-certificates` (HTTPS is verified against the system trust store), Dograh's Postgres
  (`DOGRAH_DB_URL`), MinIO settings, Sarvam keys through Dograh, and Redis only if campaigns are enabled. One process connects one ARI
  WebSocket under `ARI_APP`; **no two engines may share an app name** (Asterisk sends a Stasis app's events only to the most
  recently connected client).
- **Health:** `GET :9091/healthz` is 200 only while the ARI WebSocket is connected (503 `ari disconnected` otherwise).
  `GET :9091/metrics` is Prometheus text. The address is `METRICS_ADDR`.
- **Stop (SIGTERM or Ctrl-C):** **every call in progress is hung up at once** (there is no drain mode: not built), then the
  process waits up to `SHUTDOWN_WAIT_SECONDS` (60) for ended calls to finish writing their Dograh records and exits as soon as
  they are done. Records still unwritten after that are lost (the recordings live in memory). Restart outside call hours or after
  taking the box out of the trunk's rotation.
- **Open files:** about 3 per call; run with `ulimit -n 65536` (or `LimitNOFILE=65536`).

## What it does when busy

An inbound caller rings while the engine loads its workflow and opens its STT, then waits for a setup turn
(`SETUP_CONCURRENCY`, 16), then is answered. The caller is refused as congestion (SIP 503) instead of being answered late when
that takes over `ANSWER_WAIT_SECONDS` (10), when the CPU guard is on (`LOAD_SHED_CPU_PERCENT`, 90; calls up are never dropped), or
when any step before the answer fails. A staged call whose media channel never enters Stasis is hung up after
`SETUP_TIMEOUT_SECONDS` (15). Calls in progress are not affected by any of these.

## Reading it

| Question | Look at |
|---|---|
| Is Asterisk connected? | `vaani_ari_connected`, `/healthz`, `vaani_ari_reconnects_total` (a reconnect hangs up every call: events in the gap were lost) |
| How many calls? | `vaani_calls_active`, `vaani_calls_setting_up` (answered, not yet bridged), `vaani_media_ports_in_use` |
| Are callers being refused? | `vaani_calls_rejected_total{reason="cpu"\|"answer_wait"\|"setup", direction}`, `vaani_overloaded`, `vaani_cpu_load_ratio`; log events `call.rejected`, `call.answer_wait_expired`, `load.overloaded` / `load.recovered` |
| Did setup fail or stall? | `vaani_calls_setup_timeout_total`; log events `call.setup_failed`, `call.setup_timeout`, `media.no_connection`, `media.port_busy` |
| Is audio late? | `vaani_pacer_drift_ms`, `vaani_media_queue_dropped_total`, `vaani_media_inbound_dropped_total`, `vaani_media_inbound_late_total`, `vaani_media_watchdog_timeouts_total` |
| Post-call work backing up? | `vaani_post_call_running`, `vaani_post_call_waiting`, `vaani_recording_bytes` (memory held by recordings) |
| Postgres pool? | `vaani_db_pool_in_use`, `_waiting`, `_timeouts_total` (waiting or timeouts mean the pool or a slow dependency) |
| How long do callers wait for a reply? | `vaani_caller_wait_ms` (a histogram: the caller's last speech to the first reply audio the engine produces, flush wait included; add about 100 ms for the playout queue, plus Asterisk and the phone network), `caller_wait_p50_ms` / `caller_wait_p95_ms` in `call.summary`, and one `turn.timing` line per reply: `vad_end_to_flush_ms`, `flush_to_transcript_ms`, `transcript_to_audio_ms`, `tts_cold` (true when the turn had to open its TTS socket). `reply_latency_*` starts at Sarvam's end-of-speech and leaves the flush wait out |
| Providers slow? | `vaani_llm_ttft_ms`, `vaani_tts_ttfa_ms`, `vaani_stt_endpoint_ms`, `vaani_llm_retries_total`, `vaani_llm_fallback_total` |
| Leaked channels? | `vaani_orphan_channels_hung_up_total` (the sweep, every `ORPHAN_SWEEP_SECONDS`), log events `channel.orphan`, `call.media_orphan` |
| Campaigns | `vaani_campaign_*`, `vaani_redis_errors_total`; log events `campaign.paused`, `campaign.redis_slow`, `campaign.recovered` |

Every call line carries `call_id` and `event`; with OTLP on, the call's `trace_id` is on its lines. `call.summary` is the
story of one call.

## Situations

- **Callers get busy/503 during a burst:** check `vaani_calls_rejected_total` by reason. `cpu` means the machine (Asterisk included)
  is over the limit: add capacity, do not raise the limit. `answer_wait` means the workflow load, the STT handshake or the
  setup turns take over 10 s: check `vaani_db_pool_waiting`, Sarvam STT latency and Asterisk's ARI response time before raising
  `ANSWER_WAIT_SECONDS`.
- **`/healthz` 503 / reconnect loop:** Asterisk's ARI is down or the credentials or `ARI_APP` are wrong. The engine retries with
  backoff up to 30 s and, on reconnect, hangs up the calls it had.
- **`vaani_media_ports_in_use` stays up with no calls:** a leak; `call.teardown` lines show whether teardown ran. Restart is the
  fix; report the lines.
- **`vaani_post_call_waiting` keeps growing:** MinIO or Postgres is slower than calls end. Recordings queue in memory
  (`vaani_recording_bytes`); raise `POST_CALL_WORKERS` only if the dependency can take it, and avoid a restart until it drains.
- **Dograh pool waiting/timeouts:** a connection is held across a slow call, or the pool (`DOGRAH_DB_MAX_CONNS`, 40) is too small
  for the burst; `DOGRAH_DB_WAIT_MS` bounds the wait.
- **A campaign is paused:** the circuit breaker tripped (failure rate), Redis was unreachable, or the org limit was hit:
  `campaign.paused` states which. Restart recovery requeues contacts that were never dialed and marks dialed ones processed
  (`campaign.recovered`).
- **The agent stops too easily (interruptions feel too sensitive):** read `agent.barge_in.detected` against
  `agent.barge_in.confirmed` and the `paused_ms` field (a long `paused_ms` is dead air). Raise `BARGE_IN_MIN_SPEECH_MS`
  (200 default; 500 asks for half a second of caller speech), `BARGE_IN_GUARD_MS` (300; how long after the agent starts
  talking it cannot be paused) and `VAD_THRESHOLD` (0.5; Earshot speech probability, 0.65 is stricter). In Dograh, turn
  `allow_interrupt` off on a node the caller must hear whole (greeting, goodbye). An acknowledgement ("जी", "ok") over a
  paused reply plays it on, and with no transcript it resumes after 1.2 s of caller silence.
- **`llm.fallback` / `llm.error` rising, `llm.request` warnings with status 429:** the LLM provider is rate-limiting. The
  engine retries and then speaks a canned line. Check the key and limit in Bifrost and at Sarvam; it is not an engine fault.
- **`campaign.redis_slow` logs:** a Redis step took over its threshold (the `ms` and `op` fields say how long and which
  step). On Nov 15 they came from the engine being at another site (18.5 ms through firewalls); with the engine on the
  data server's network there were none (`ASTERISK.md` section 8). If they return, check where the engine runs first,
  then capture Redis latency and the shared box's load (`ASTERISK.md` section 6).
- **Callers say the agent does not hear them:** in `call.summary`, compare `packets_in` with `packets_out` (both count the
  same connection; the agent sends every 20 ms). `packets_in` far below `packets_out` means caller audio did not reach the
  engine. With `inbound_dropped` at 0 the loss is before the engine: check that call's channel in `sip show channelstats`
  on Asterisk (`ASTERISK.md` section 6). Nov 17: 8.4% of calls under half (`CAMPAIGN_26_REPORT.md`).
  `call.summary` also says *how* the caller audio arrived: `caller_audio` is `none` (no frame ever: a silent caller, or audio
  that never came), `late_start` (the first frame after 2 s; see `first_caller_frame_ms`), `gaps` (a wait of 1 s or more inside
  the call; `max_caller_gap_ms`), `stopped` (nothing for the last 5 s; `caller_audio_end_gap_ms`) or `ok`. Count the calls by
  `caller_audio` to separate silent callers from broken audio before looking at Asterisk; for `none` calls, group by
  destination number prefix and trunk gateway.
- **Long silences after a pause:** a `false interruption; resuming the reply` line with `paused_ms` near 10000 is a pause on
  line noise that ran to the 10 s cap (`TECH_DEBT.md` #23).
- **Asterisk looks busy:** read per-thread CPU, not the `htop` process row (which sums all threads); the monitor script
  and what to read in it are in `ASTERISK.md` section 6.

## Known limits (see `TECH_DEBT.md`, `SYSTEM_DESIGN.md`)

- No drain mode; a stop drops live calls.
- Not safe to run several engines against the same campaigns yet: the startup recovery and the capacity counter assume one
  engine.
- Post-call records not written within the shutdown wait are lost.

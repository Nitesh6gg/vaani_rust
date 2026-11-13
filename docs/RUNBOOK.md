# Runbook: running the Rust engine

Everything here is read from the code and the existing docs; "not built" marks what an operator might expect and does not
exist yet. Settings are in the `README.md` environment table, measured numbers in `BENCHMARK.md`.

## Start, health, stop

- **Start:** `./target/release/vaani` with the environment from `README.md`. It needs ffmpeg on `PATH`, Dograh's Postgres
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

## Known limits (see `TECH_DEBT.md`, `SYSTEM_DESIGN.md`)

- No drain mode; a stop drops live calls.
- Not safe to run several engines against the same campaigns yet: the startup recovery and the capacity counter assume one
  engine.
- Post-call records not written within the shutdown wait are lost.

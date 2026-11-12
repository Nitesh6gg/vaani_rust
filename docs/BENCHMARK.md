# Benchmarking Vaani (Rust) against the Go server

This page is the method, so a Rust run is directly comparable to the Go server's `docs/BENCHMARK.md`
(in `nitesh6gg/vaani`), and the measured Rust results (at the end). **Never put an estimate in a table
of measured numbers**; add a row only from a real run. The Go-vs-Rust comparison is `GO_VS_RUST.md`.

## What `/metrics` exports

The same names, labels and histogram buckets the Go engine exports, so one set of queries and dashboards reads either:

| Metric | Meaning |
|---|---|
| `process_cpu_seconds_total`, `process_resident_memory_bytes` / `_peak_bytes`, `process_virtual_memory_bytes`, `process_threads`, `process_open_fds` | process CPU and memory |
| `process_network_receive_bytes_total` / `_transmit_bytes_total` | every interface of the process's network namespace, loopback included: read from `/proc/self/net/dev` as Go's collector does |
| `vaani_tokio_alive_tasks` | the Rust analogue of `go_goroutines` (there is no `go_memstats_*`: compare RSS) |
| `vaani_calls_active`, `vaani_calls_started_total{direction}`, `vaani_media_ports_in_use`, `vaani_calls_setting_up` | calls (started = bridged, as in Go) |
| `vaani_call_duration_seconds` (histogram) | bridged calls, staging to teardown: `_count` = answered calls, `_sum` = talk time |
| `vaani_llm_ttft_ms`, `vaani_tts_ttfa_ms`, `vaani_stt_endpoint_ms` (histograms, 50 ms..25.6 s) | latency |
| `vaani_pacer_drift_ms` (histogram) | how far each 20 ms writer tick is from the nominal interval (a stall is one big miss) |
| `vaani_agent_turns_total`, `vaani_agent_errors_total{stage}`, `vaani_agent_bargein_total`, `vaani_agent_interruption_pauses_total`, `vaani_agent_false_interruptions_total` | agent |
| `vaani_llm_retries_total{result}`, `vaani_llm_fallback_total{kind}`, `vaani_tool_calls_total{tool,outcome}`, `vaani_tool_call_duration_ms{tool}` | LLM and tools |
| `vaani_stt_audio_seconds_total` | caller audio sent to the STT (what Sarvam bills), added when a call ends |
| `vaani_audiosocket_frames_in_total` / `_frames_out_total` / `_bytes_in_total` / `_silence_sent_total` / `_send_errors_total` / `_malformed_total` | media |
| `vaani_media_queue_dropped_total`, `vaani_media_inbound_dropped_total`, `vaani_media_watchdog_timeouts_total` | dropped frames, dead media |
| `vaani_media_inbound_late_total` | caller frames that arrived more than one to a tick (call start-up, a clump from Asterisk) and were still heard |
| `vaani_recording_truncated_total`, `vaani_ari_reconnects_total`, `vaani_ari_event_queue_length` | |
| `vaani_recording_bytes` | call recordings held in memory right now (about 1.9 MB per call-minute) |
| `vaani_db_pool_in_use`, `vaani_db_pool_waiting`, `vaani_db_pool_max`, `vaani_db_pool_timeouts_total` | the Dograh Postgres pool: connections in use, callers waiting, its size, callers that gave up waiting. Waiting above 0 for more than a moment means the pool is too small or something holds connections |
| `vaani_cpu_load_ratio`, `vaani_overloaded`, `vaani_calls_rejected_total{reason,direction}`, `vaani_calls_setup_timeout_total`, `vaani_orphan_channels_hung_up_total` | CPU guard and call-setup safety nets |
| `vaani_campaign_*` | outbound campaigns (see `OUTBOUND.md`) |

Differences to keep in mind when comparing:
- `vaani_media_watchdog_timeouts_total` counts the event of a call's media ending after `MEDIA_DEAD_TIMEOUT_SECONDS`
  with no inbound audio (its hangup); the Go server also warns at 5 s, so its count is higher for the same calls.
- Go-only series with no Rust twin: `go_*` runtime metrics, the ARI library's queue and handler time, RTP and jitter-buffer
  counters (Rust is AudioSocket only), `vaani_audio_rms`.
- `vaani_agent_errors_total` has the same stages in both; `tts_speak` and `tts_cancel` are never raised by the Rust agent.

## The figures (same definitions as the Go report)

| Column | Formula |
|---|---|
| Answered calls | `increase(vaani_call_duration_seconds_count)` over the window |
| Talk time | `increase(vaani_call_duration_seconds_sum)` over the window |
| Peak talking | max of `vaani_calls_active` |
| Peak in flight | max of `vaani_campaign_calls_in_flight` |
| Peak CPU | max 1-minute `rate(process_cpu_seconds_total)`, in cores |
| **Core per talking call** | `increase(process_cpu_seconds_total)` over the WHOLE window ÷ talk time in seconds (counts all Vaani CPU in the window, so it is the honest per-call figure) |
| Peak RSS / threads / tasks | max of `process_resident_memory_bytes` / `process_threads` / `vaani_tokio_alive_tasks` |
| Reply latency | `histogram_quantile` over `vaani_llm_ttft_ms` and `vaani_tts_ttfa_ms`, or the `call.summary` log |
| Pacer drift p99 | `histogram_quantile(0.99, vaani_pacer_drift_ms)` |
| Network per talking call | `increase(process_network_*_bytes_total)` ÷ talk time |

Do not divide a one-minute CPU peak by the instantaneous peak call count (it understates the cost),
and query one window total, not summed hourly buckets. Idle first: record the idle baseline
(RSS, threads, CPU/s) with no calls.

## Logs and traces

With `OTEL_EXPORTER_OTLP_ENDPOINT` set on both engines, one SigNoz holds both: filter on `service.name` (`vaani-engine` for
Go, `vaani-engine-rust` for Rust; `OTEL_SERVICE_NAME` overrides). A call's trace (`call` span, `llm.request` children) and
its log lines share a `trace_id`, so a slow reply in one engine can be followed to the LLM request that caused it.

## A fair comparison

1. Same box, one engine at a time (stop the other), same Asterisk, same providers and LLM, same workflow, a different
   `ARI_APP` for each (the orphan sweep of one would hang up the other's calls). Run Rust as `cargo build --release`
   (a debug build uses several times the CPU), both at the same `LOG_LEVEL`, and with the same `CAMPAIGN_DIAL_WORKERS`,
   `OUTBOUND_MAX_SETUP`, `LOAD_SHED_CPU_PERCENT` and dial rate (10, then 15, then 20: ask the trunk for its calls-per-second limit).
   To compare the engines alone, point both at the mock Sarvam/LLM servers (`SARVAM_STT_WS_URL`, `SARVAM_TTS_WS_URL`).
   Also record the whole machine's CPU (`top`, node_exporter): the process counter leaves out Asterisk.
2. Same load: the same campaign (or SIPp scenario) with the same dial rate and concurrency limit.
   Turn on `STT_SPEECH_ONLY=1` on both, or on neither.
3. Record, per run: idle baseline, then peak talking, peak RSS, peak CPU, core per talking call,
   reply latency p50/p95 (`call.summary`), errors and dropped frames, and the 20 ms pacer
   (`packets_out` vs call length; Go: `vaani_pacer_drift_ms` p99).
4. Quick manual check on the server while calls are up:
   ```sh
   PID=$(pgrep -x vaani)
   ps -o pid,rss,vsz,nlwp,pcpu,etime -p $PID
   grep -E 'VmRSS|VmHWM|Threads' /proc/$PID/status
   curl -s localhost:9091/metrics | grep -E '^process_|calls_active|tokio'
   ```

## Results

Box: 192.168.36.19 (16 vCPU Xeon Gold 6330, 32 GB, VMware), Vaani alone on it (the Go engine stopped),
Asterisk, Postgres, Redis and Dograh on other servers. Source: SigNoz, `service.name = 'vaani-engine-rust'`,
metrics scraped every 10 s. Release build.

| Engine | Test | Answered | Talk time | Peak talking | Peak in flight | Peak CPU (cores) | **Core / talking call** | Peak RSS | Threads | Tasks |
|---|---|---|---|---|---|---|---|---|---|---|
| Rust | idle (Nov 10, 11:25–11:38 UTC) | – | – | 0 | 0 | 0.0005 | – | 22.8 MB | 21 | 13 |
| Rust | Nov 10 17:09 IST, campaign 22 retries, 266 dials in < 5 s | 118 | 3,039 s (51 min) | 55 (10-s max) | 265 | 0.42 (1-min avg) | **0.0144** | 229 MB | 25 | 556 |
| Rust (load generator, mocks) | Nov 11, 1,000 concurrent calls, engine alone, generator on a second machine (`LOADTEST.md`) | 7,101 | 425,860 s | 1,000 | – | 4.78 (1-min avg, 30% of the box) | **0.0048** | 2.71 GB | 17 | 7,576 |
| Rust | **Nov 11 17:39 IST, campaign 25 (`test_R_4K`), 4,987 dials, org limit 400 / 35 per s** | 2,354 | 34,807 s (580 min) | **149** (1-min max) | **400** (= org limit) | **1.01** (1-min avg, 6.3% of the box) | **0.0101** | 554 MB | 40 | 1,359 |
| Go (for comparison) | Nov 9 19:18 IST, campaign 22, 385 dials, after the event-loop fix | 173 | 4,792 s (80 min) | 72 | 227 | 1.78 (1-min avg) | **0.046** | 548 MB | ~23 | 1,107 goroutines |

### Per-run detail

**Nov 10, 2026 — campaign 22 retries (first Rust load test)**

- Window 11:38–11:46 UTC (17:08–17:16 IST). The dialer placed all 266 calls in one 5-second bucket
  (11:39:30–35 UTC, ~50 dials/s): they were the no-answer retries of campaign 22 (`retry_count` 1), so it was
  a burst that used the list up, **not a sustained test**.
- Outcomes: 118 answered and bridged (44%), 106 no-answer (cause 19), 41 rejected (cause 21), 1 failed (cause 1).
  **0** `outbound.timeout`, **0** `call.setup_failed`, **0** `media.dead`, **0** agent errors, **0** setup
  timeouts or orphans, **0** cause 34 (the Go engine's Nov 9 burst had 54 cause-34 rejects).
- Calls: average 25.5 s; talking peaked at 55 (10-s samples, so a shorter spike can be missed).
- CPU: 43.7 CPU-s over the window for 3,039 talk-s = **0.0144 core per talking call** (idle baseline
  included: 0.0005 core × 480 s = 0.24 s). Peak 0.42 cores in a 1-minute average (2.6% of the box).
- Memory: RSS 22.8 MB idle → 229 MB peak at 265 calls in flight (≈ 0.78 MB per call in flight), back to
  150 MB after the run (the allocator keeps freed pages; it stayed flat, so not a leak). 25 OS threads,
  556 tokio tasks at the peak, 208 open fds.
- Network (whole window, all interfaces): received 178.8 MB, sent 314.2 MB = 58.8 kB/s received and
  103 kB/s sent per talking call (audio to/from Asterisk, STT/TTS, recording upload, OTLP).
- Latency (from `turn.agent` logs, 221 agent turns): LLM first token p50 312 ms / p95 515 ms; TTS first
  audio p50 477 ms / p95 756 ms. Per-call reply latency (`call.summary`): median of the per-call p50 510 ms,
  p95 of the per-call p95 1,028 ms. Pacer drift p99 ≤ 0.5 ms (the histogram's lowest bucket).
- STT audio sent: 1,956 s for 3,008 s of calls (65%). Rust never logs `stt.gate_open`, so the logs do not say
  whether `STT_SPEECH_ONLY` was on; 65% points to **on**. Check the `.env`.
- Barge-in: 147 detected, 102 confirmed for 118 calls (0.86 confirmed per call; Go Nov 9 16:30 had 0.53).
  Earshot is more eager than the Go engine's TEN VAD here; worth checking that these are real interruptions.
- **What it does not show**: behaviour above 55 talking calls, a sustained dial rate, or a long run.

**Nov 11, 2026 — campaign 25 (`test_R_4K`, 4,000 contacts), first sustained real campaign on Rust**

- Org settings: `CONCURRENT_CALL_LIMIT` 400, `DIAL_RATE_LIMIT_PER_SECOND` 35. Dialed 12:09–12:15 UTC
  (17:39–17:45 IST), paused by hand from Dograh. 4,987 dials (4,000 first attempts + retries; plus 3 dials of a
  2-number test, campaign 24, at 12:07). The Go engine was stopped.
- **The dial rate was set by the concurrency cap, not by the 35/s limit**: 738–875 dials per minute (12–15/s),
  with calls in flight pinned at **400** for six minutes. Calls ring for up to 30 s, so 400 in flight allows
  about 13/s.
- Outcomes (logs): 2,354 answered and bridged (47%), 1,947 no-answer (cause 19), 659 rejected (cause 21), 14 failed
  cause 38, 6 failed cause 0, 1 cause 1, 8 busy. **0 cause 34** (the trunk's "no circuit"), **0** `outbound.timeout`,
  **0** `media.dead`, **0** agent errors, 0 media watchdog timeouts. Dispositions: 2,229 user hang-up (95%), 68
  qualified, 56 idle / max duration. Average call **14 s** (Dograh's dashboard: 12 s): most people hung up during
  the greeting; 1,206 caller turns in 2,353 calls.
- **CPU**: 353 CPU-s for 34,807 talk-s = **0.0101 core per talking call**; peak 1.01 cores (1-min) at 125–149
  talking. Short calls cost more per talk-second (the per-call setup is spread over 14 s, not 25–35 s).
- **Memory**: RSS 14 MB idle → 554 MB peak (400 in flight, 149 talking) → 435 MB after (kept, flat). 40 threads
  (17 in the load test: the extra are the blocking pool for Postgres/DNS/TLS), 1,359 tasks, 553 open files.
- **Network** per talking call: 48.6 kB/s received, 98.8 kB/s sent (1.69 GB in, 3.44 GB out).
- **Latency** (`turn.agent`, 1,405 turns with an LLM reply): LLM first token p50 284 ms / **p95 1,186 ms**; TTS first
  audio p50 416 ms / **p95 2,572 ms**. Per-call reply latency: median of the per-call p50 454 ms, p95 of the
  per-call p95 4.4 s. The p95s are 2–3× the 55-call run (515 ms / 756 ms): provider latency under load (Sarvam TTS
  above all), not engine CPU (6% of the box), but not proven.
- Pacer drift p99 ≤ 0.5 ms. 304 outbound frames dropped (agent audio queue full).
- **New: 16,155 caller frames dropped** (`vaani_media_inbound_dropped_total`; 0 in every load-test step): 323 s of
  caller audio, 0.9% of it, in **932 of 2,353 calls (40%)**, up to 192 frames (3.8 s) in one call. The counter
  means the call's agent task was behind reading the caller's audio. Likely while the call starts (Dograh workflow
  read, STT and TTS TLS handshakes), which the load test's local mocks make instant; not confirmed. Worth fixing:
  a dropped first word is a worse call.
- **New: Redis timeouts in the dialer**: 32 slot releases, 17 circuit-breaker writes and 17 rate/slot checks timed
  out (`campaign.release_failed`, `campaign.breaker_failed`, `campaign.limit_failed`). An unreleased slot holds one
  of the 400 until its stale timeout expires, so the effective cap was a little lower. Three bursts (17:41:10,
  17:44:15-20, 17:45:25-30 IST), each with a dip in the dial rate and in caller transcripts. The Go engine logged
  none, but go-redis waits 5 s and retries 3 times, which hides a short stall; see `CAMPAIGN_25_REPORT.md`.
- 4 setup failures: 3 `bind media port …: Address already in use` (a port handed out again while still in use) and
  1 externalMedia 500 from Asterisk.
- Asterisk (separate 4-core box, `htop` during the run): its main thread at 74–86% of one core, the box at
  19–26%. Close to its limit at ~150 talking, as with the Go engine (96% at 141 talking on Nov 9).
- STT audio sent: 20,672 s for 34,807 s of calls (59%).


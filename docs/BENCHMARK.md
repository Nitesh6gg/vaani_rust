# Benchmarking Vaani (Rust) against the Go server

No measured results yet: this page is the method, so a Rust run is directly comparable to the Go
server's `docs/BENCHMARK.md` (in `nitesh6gg/vaani`). **Never put an estimate in a table of measured
numbers**; add a row only from a real run.

## What `/metrics` exports

The same names the Go engine exports, so the same queries work on either:

| Metric | Meaning |
|---|---|
| `process_cpu_seconds_total` | CPU time used by the process (user + system) |
| `process_resident_memory_bytes` / `process_resident_memory_peak_bytes` | RSS now / its high-water mark |
| `process_virtual_memory_bytes`, `process_threads`, `process_open_fds` | |
| `vaani_tokio_alive_tasks` | the Rust analogue of `go_goroutines` |
| `vaani_calls_active`, `vaani_calls_started_total`, `vaani_media_ports_in_use` | calls |
| `vaani_campaign_*` | outbound campaigns (see `OUTBOUND.md`) |

## The figures (same definitions as the Go report)

| Column | Formula |
|---|---|
| Talk time | the sum of the calls' `duration_s` (`call.summary` log lines), or `increase(vaani_calls_started_total)` x average |
| Peak talking | max of `vaani_calls_active` |
| Peak CPU | max 1-minute `rate(process_cpu_seconds_total)`, in cores |
| **Core per talking call** | `increase(process_cpu_seconds_total)` over the WHOLE window ÷ talk time in seconds (counts all Vaani CPU in the window, so it is the honest per-call figure) |
| Peak RSS / threads / tasks | max of `process_resident_memory_bytes` / `process_threads` / `vaani_tokio_alive_tasks` |

Do not divide a one-minute CPU peak by the instantaneous peak call count (it understates the cost),
and query one window total, not summed hourly buckets. Idle first: record the idle baseline
(RSS, threads, CPU/s) with no calls.

## A fair comparison

1. Same box, one engine at a time (stop the other), same Asterisk, same providers and LLM, same workflow.
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

| Engine | Test | Peak talking | Peak CPU (cores) | **Core / talking call** | Peak RSS | Threads | Tasks |
|---|---|---|---|---|---|---|---|
| _none yet_ | | | | | | | |

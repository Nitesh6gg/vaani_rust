# Load-testing the engine without Asterisk, a trunk or providers

`loadgen` (`src/bin/loadgen.rs`) is a fake Asterisk and mock Sarvam STT/TTS and LLM in one process. It finds
the **engine's** ceiling: how many concurrent calls one Vaani process carries before audio is late or calls
fail. It says nothing about Asterisk, the SIP trunk or the real providers; those are tested with real campaigns
(`docs/BENCHMARK.md`).

## What it simulates

- **Inbound calls only.** Each caller enters Stasis; the engine answers, asks for an externalMedia channel and
  bridges; the generator then connects the AudioSocket, sends a 20 ms frame of silence every 20 ms and reads
  the agent's audio back.
- The mock STT "hears" the caller every `--turn-secs` (START_SPEECH, END_SPEECH, a transcript), which makes the
  engine call the mock LLM (first token after `--llm-ms`) and the mock TTS (2 s of audio per sentence).
- `--concurrent` calls are kept up for `--duration` seconds: when a call ends (after `--call-secs`, +-20%), its
  slot starts the next one.

`--burst N` sends the caller's audio N frames at a time (every N x 20 ms), as an Asterisk that delivers in
clumps does; the engine must still hear every frame (`vaani_media_inbound_dropped_total` stays 0 and
`vaani_media_inbound_late_total` counts the frames that arrived behind another).

Not simulated: outbound dialing, transfers, barge-in (the caller's audio is silence), TLS to the providers,
recording upload (leave `MINIO_ENDPOINT` empty), Dograh's real workflow (one node, no tools).

## Running it

```sh
cargo build --release                       # builds vaani and loadgen
./target/release/loadgen --concurrent 300 --call-secs 60 --duration 600 \
    --host <generator address> --db postgres://user:pass@host:5432/dbname
```

It creates a throwaway schema `vaani_loadgen` (a minimal Dograh: one workflow, id 42, whose LLM is the mock) in
that Postgres, prints the environment to start the engine with, and waits for the engine to connect. Use any
Postgres you may create a schema in; **do not** point it at Dograh's production database.

On the engine side, besides what it prints:

- `MEDIA_IP` = the engine's address as the generator reaches it; `MEDIA_PORT_COUNT` >= the concurrency.
- Open files: about 3 per call. `ulimit -n 65536` (or `LimitNOFILE=65536` in the systemd unit) on **both** processes.
- Run the generator on **another machine** for real numbers: on the same box it competes for CPU, and the CPU
  guard (`LOAD_SHED_CPU_PERCENT`) counts the whole machine. On one box, set `LOAD_SHED_CPU_PERCENT=0`.
- `LOG_LEVEL=warn` unless you are debugging: INFO logs a few lines per turn.

## Reading the result

Every 5 s, and in the summary:

| Field | Healthy | Meaning when not |
|---|---|---|
| `setup-failed` | 0 | the engine refused the call (CPU guard) or never bridged it within 20 s |
| `media-lost` | 0 | the engine closed the audio before the caller hung up |
| `engine-hangups` | 0 or a few | the engine hung the caller up (idle timeout, max duration, errors) |
| `agent audio` | 1.00 | frames received per frame sent: below 1.00 the engine's 20 ms writer is falling behind |
| `gaps(>60ms)` | 0, or a handful | audible holes in the agent's audio, as the caller would hear them |
| call setup p95 | tens of ms | Stasis to first audio, without Dograh's real latency |

Read the engine's `/metrics` alongside: `process_cpu_seconds_total` (core per talking call =
increase / `vaani_call_duration_seconds_sum` increase), `process_resident_memory_bytes`, `vaani_recording_bytes`,
`vaani_pacer_drift_ms`, `vaani_media_queue_dropped_total`, `vaani_media_inbound_dropped_total`, `process_open_fds`.

Ramp in steps (100, 300, 600, 1,000) and stop at the first step where `agent audio` drops below 1.00, gaps grow
with every report, or the engine's CPU passes 70% of the box.

## First run (Nov 10, 2026, development sandbox: NOT the production box)

4 vCPUs shared by the engine, the generator and Postgres; loopback; mocks. It shows that nothing in the engine
breaks structurally at 1,000 calls; the per-call figures are optimistic for production (no TLS, no real audio,
no recording upload, a one-node workflow) and must be re-measured on 192.168.36.19.

| Concurrent | Calls | Talk time | Setup failed / media lost | Agent audio | Gaps > 60 ms | Engine CPU | Core per call | Peak RSS | Open files | Tasks |
|---|---|---|---|---|---|---|---|---|---|---|
| 100 (15 s calls, 40 s) | 301 | 4,524 s | 0 / 0 | 1.00 | 0 | 16.7 CPU-s | 0.0037 | 94 MB | not read | not read |
| 1,000 (30 s calls, 60 s) | 2,104 | 63,012 s | 0 / 0 | 1.00 | 6 | 142.6 CPU-s | 0.0023 | 1.6 GB | 2,947 at 958 calls | 7,230 at 958 calls |

At 1,000: 3.1 million pacer ticks, 99.7% within 0.5 ms of 20 ms; 49 agent frames and 4,442 caller frames dropped
(0.14% of caller frames: the box was saturated by the generator and engine together); 8,974 LLM requests
(8.5 per call-minute, about twice a real call's 4.4); call setup p95 30 ms. One call was hung up by the engine
(a media write failed as the caller left).

## Production-box run (Nov 11, 2026): engine on 192.168.36.19, generator + Postgres on 192.168.36.20

The engine alone on the production box (16 vCPU Xeon Gold 6330, 32 GB, VMware), the generator and a throwaway
Postgres (docker) on a second machine on the same network. Mock providers, inbound calls, silence from the caller,
no TLS, no recording upload, `LOG_LEVEL=warn`, OTLP off, `ulimit -n 65536` on both. Engine figures from SigNoz
(`service.name = 'vaani-engine-rust'`, 10 s scrape); generator figures from its summary.

| Concurrent (call length, hold) | Calls | Talk time | Setup failed / lost | Agent audio | Gaps > 60 ms | Peak CPU (1-min) | **Core per call** | Peak RSS | Open files | Tasks |
|---|---|---|---|---|---|---|---|---|---|---|
| 100 (30 s, 180 s) | 639 | 19,133 s | 0 / 0 | 1.00 | 0 | ~0.7 cores | 0.0082 | 266 MB | 333 | 788 |
| 300 (60 s, 300 s) | 1,603 connected | 92,271 s | 186 / 0 (see below) | 1.00 | 0 | 2.06 cores (2.0 steady) | ~0.0067 (from 1-min averages) | not read | not read | not read |
| 600 (60 s, 300 s) | 3,118 | 186,987 s | 0 / 0 | 1.00 | 0 | 3.28 cores (20% of the box) | **0.0055** | 1.83 GB | 1,857 | 4,567 |
| **1,000** (60 s, 420 s) | 7,101 | 425,860 s | 0 / 0 | 1.00 | 171 (0.02 per call-minute) | **4.78 cores (30% of the box)** | **0.0048** | **2.71 GB** | 3,078 | 7,576 |

- **Step 2's 186 setup failures were an operator error**: the engine was stopped by mistake before the generator
  finished, so calls still starting could not set up. Every call that connected kept its audio (1.00, no gaps).
  The step was not rerun.
- **Core per call falls as load rises** (0.0082 → 0.0055 → 0.0048), as with the Go engine.
- **At 1,000 calls**: pacer drift p99 in the lowest bucket (≤ 0.5 ms), 2 outbound and 0 inbound frames dropped,
  ARI event queue 0, CPU guard never fired (machine load ratio ≤ 0.36), call setup p50 4 ms / p95 6 ms.
- **171 audible gaps over 60 ms at 1,000** (none at 600): 0.02 per call-minute. The engine's pacer shows nothing wrong,
  so the generator (one 4-core-class machine feeding 1,000 audio streams) may be the cause; not isolated.
- **Memory**: RSS rose during each step and levelled off at constant load (1,000 calls: 2.47, 2.59, 2.69, 2.71,
  2.71, 2.71 GB), so it is retained pages, not a leak that grows with time; roughly 2.0 MB per call in flight above
  the engine's starting RSS (the engine was not restarted between steps 3 and 4, so it started step 4 at 711 MB).
  The recordings held in RAM peaked at 1.05 GB (about 1 MB per call at 30 s in).
- **Network** per talking call: 59 kB/s received, 36 kB/s sent (about 475 Mbps in and 290 Mbps out at 1,000 calls).
  A real call sends more (TLS to providers, recording upload).
- **What it shows**: one Rust engine carries 1,000 concurrent calls on this box with room to spare. **What it does
  not show**: real per-call CPU (a real run cost 0.0144 core per call, about 3× this), Asterisk, the trunk and the
  providers. Scaling this figure to the real cost: 0.0144 core per call gives about 780 calls at 70% CPU.

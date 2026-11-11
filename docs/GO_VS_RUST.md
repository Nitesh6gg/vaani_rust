# Go vs Rust: resource use, scalability and readiness

Report date: Nov 10, 2026. Sources: SigNoz (`service.name = 'vaani-engine'` for Go, `'vaani-engine-rust'` for
Rust), the Go repo's `docs/BENCHMARK.md` and this repo's `docs/BENCHMARK.md`. Every number in the measured tables
comes from a real run; extrapolations are marked as such.

## Summary

- **Rust uses about 3× less CPU per call and about 2.4× less memory than Go** on the same box, campaign and
  load shape. Idle, it uses 4–6× less of both.
- **Latency is the same in both**: it is set by the providers (Sarvam, the LLM), not by the engine.
- **Go is the proven one**: it has run 3,545-dial campaigns with 141 calls talking at once; Rust has one run of
  118 calls (55 talking at once). Go also still has features Rust does not (below).
- **Neither engine is the limit today.** At 141 talking calls Go used 26% of the box. The ceiling we keep hitting
  is Asterisk (4 cores, one thread at 96%) and the SIP trunk (cause-34 rejects). Switching engines does not raise
  the number of calls the system can carry until Asterisk and the trunk are scaled.
- **Recommendation:** keep Go in production for now and make Rust the target. Switch after one sustained
  head-to-head test (≥150 talking calls, ≥10 minutes) where Rust stays error-free, and after QA analysis is ported.

## The comparison

The pair used (not a perfect match: Rust dialed only no-answer retries, on a different day, in one 5-second burst): both engines alone on the same box (192.168.36.19, 16 vCPU Xeon Gold 6330, 32 GB), both
dialing **campaign 22** as one burst with the parallel dialer, similar call length and load.

| | **Go** (Nov 9, 19:18 IST) | **Rust** (Nov 10, 17:09 IST) | Rust vs Go |
|---|---|---|---|
| Dials / answered | 385 / 173 | 266 / 118 | – |
| Talk time | 4,792 s (avg 27.7 s) | 3,039 s (avg 25.5 s) | similar calls |
| Peak talking / in flight | 72 / 227 | 55 / 265 | similar load |
| CPU for the run (whole window) | 219.7 CPU-s | 43.7 CPU-s | |
| **Core per talking call** | **0.046** | **0.0144** | **3.2× less** |
| Peak CPU (1-min avg) | 1.78 cores at 72 talking | 0.42 cores at 55 talking | |
| **Peak RSS** | **548 MB** (heap 271 MB) | **229 MB** | **2.4× less** |
| Memory per call in flight | ≈ 2.2 MB | ≈ 0.78 MB | 2.8× less |
| Concurrency units at peak | 1,107 goroutines, ~23 threads | 556 tokio tasks, 25 threads | |
| Network per talking call (rx / tx) | 55 / 97 kB/s | 59 / 103 kB/s | same (it is the audio) |
| Errors, dead media, setup failures | 0 / 0 / 0 | 0 / 0 / 0 | same |
| LLM first token p50 / p95 | (Nov 9 12:28: 195 / 367 ms) | 312 / 515 ms | provider-bound |
| TTS first audio p95 | (Nov 9 12:28: 576 ms) | 756 ms | provider-bound |
| Pacer drift p99 | never reached SigNoz | ≤ 0.5 ms | – |

Idle, no calls:

| | Go | Rust |
|---|---|---|
| CPU | 0.002–0.003 core | 0.0005 core |
| RSS | ~51 MB | 22.8 MB |
| Threads | 23 | 21 |

Go's cost per talking call falls as load rises (fixed work is shared by more calls): 0.046–0.048 in short bursts
like this one, 0.036–0.038 at 70–94 talking, 0.031 at 141 talking (Go `docs/BENCHMARK.md`). Compared with Go's
best figure at its largest load, Rust's 0.0144 at a small load is still **2.2× less**; at equal load the gap is
likely larger, but that is not measured yet.

## Why Rust uses less

Measured: the totals above. Not measured: how much each cause below contributes.

1. **No garbage collector.** Go's heap reached 271 MB of short-lived audio frames, JSON and WebSocket messages,
   and its GC scans and frees that continuously. Rust frees memory as each value goes out of scope.
2. **Less machinery per call.** In Rust one task owns all of a call's state; frames are fixed `[u8; 640]` values
   (no pool, no locks between a frame goroutine and an event goroutine); WebSocket buffers are trimmed to 16 KiB.
   Go runs several goroutines per call with channels and atomics between them (1,107 goroutines at 227 calls
   in flight).
3. **A different, lighter VAD (part of the gap is not the language).** Go runs **TEN VAD**, a neural model called
   through cgo on every frame; Rust runs **Earshot**, a small pure-Rust model. Part of the CPU gap is this choice.
   To separate the two, run the Go engine once with `VAD_MODE=energy`, or with the STT gate the same on both.
4. **The STT gate setting on Rust is unknown.** Rust never writes a `stt.gate_open` log line, so its absence
   proves nothing. Rust sent 65% of call time to Sarvam, which points to the gate being **on**. Read
   `STT_SPEECH_ONLY` in the Rust `.env` and make it the same on both engines before the next test.

## Which scales further

Per box, **Rust**. Extrapolated (NOT measured), linear, engine CPU only, at 70% of 16 cores (11.2 cores):

| | Core per talking call | Talking calls at 70% CPU | Basis |
|---|---|---|---|
| Go | 0.031 (measured at 141 talking) | ~360 | 2.6× beyond the largest test |
| Rust | 0.0144 (measured at 55 talking) | ~780 | 14× beyond its only test: a guess, not a forecast |

Memory limits neither for short calls: at ~2 MB (Go) or ~0.8 MB (Rust) per call in flight, 1,000 calls of ~25 s
fit in 2 GB of the 32 GB. **Long calls cost more**: the recording is held in RAM at about 1.9 MB per call-minute
(Rust test call: 2.48 MB for 77 s), so 1,000 five-minute calls need roughly 10 GB. Writing recordings to disk
would remove this.

Rust's advantage grows with load: no GC pauses that grow with the heap, and a fixed per-call footprint. But the
whole system's ceiling is set elsewhere today:

- **Asterisk** (separate 4-core box): one thread at 96% during a campaign, while total CPU showed 27%.
- **SIP trunk**: 54 cause-34 ("no circuit/channel available") rejects in a 385-dial burst on Nov 9.
- **Sarvam STT**: 113 STT errors at 120–140 talking calls on Nov 9 (Go); cause unconfirmed.

## Load test: 1,000 concurrent calls (Rust, Nov 11)

One Rust engine alone on the production box, with a load generator and mock providers on a second machine
(`LOADTEST.md`). All 7,101 calls set up and kept their audio; there were no failures, no lost media and no engine
hangups. At 1,000 calls: peak CPU 4.78 cores (30% of the 16-core box), **0.0048 core per call**, peak memory
2.71 GB (levelled off, not growing), 3,078 open files, pacer drift p99 ≤ 0.5 ms, 171 audible gaps over 60 ms
(0.02 per call-minute; the generator may be the cause). Cost per call fell with load: 0.0082 at 100, 0.0055 at 600,
0.0048 at 1,000.

This is the engine alone, with silence for audio, no TLS to providers and no recording upload: it is **not**
comparable with the Go figures above (real providers). Scaled to Rust's real-campaign cost (0.0144 core per call),
the box carries about 780 calls at 70% CPU; the mock-provider figure suggests more. Asterisk and the trunk remain
the system's limit. The same test has not been run on the Go engine (the Go repo has no load generator).

## Real campaign at ~150 talking calls (Rust Nov 11 vs Go Nov 9)

The closest real-traffic pair: both engines alone on the same box, dialing a large campaign with the parallel
dialer until paused, ~140–150 calls talking at once. Go's run was **before** its ARI event-loop fix, so its
failure counts are not comparable; its CPU and load are.

| | **Go** (Nov 9 16:59 IST, campaign 22) | **Rust** (Nov 11 17:39 IST, campaign 25) |
|---|---|---|
| Org limit / dial limit | 600 / 100 per s | 400 / 35 per s |
| Dials / answered | 3,545 / 1,204 (34%) | 4,987 / 2,354 (47%) |
| Peak in flight / talking | 602 / 141 | 400 / 149 |
| Average call | 34 s | 14 s |
| **Peak CPU (1-min)** | **4.23 cores** | **1.01 cores** (4.2× less at the same talking count) |
| **Core per talking call** | **0.031** | **0.0101** (3.1× less, despite shorter calls) |
| Peak memory | not pulled (696 MB at 94 talking on Nov 5) | 554 MB |
| Setup failures / dead media / no-result | 80 / 325 / 1,272 (event loss, fixed since) | 4 / 0 / 0 |
| Agent errors | 113 (all STT, at 120–140 talking) | 0 |
| LLM first token p95 | 512 ms | 1,186 ms |
| Trunk "no circuit" (cause 34) | not seen that run (54 on Nov 9 19:18) | 0 |
| Asterisk main thread | 96% of a core | 74–86% of a core |

What this adds to the comparison:
- **The 3× CPU gap holds on real traffic at ~150 calls**, not only in the 55-call burst.
- Rust's run was clean on the engine side (no lost events, dead media or agent errors), but it shows two problems
  Go's logs would not reveal the same way: **caller audio dropped in 40% of calls** (0.9% of frames) and **Redis
  timeouts in the dialer**. See `BENCHMARK.md`.
- Latency p95 was worse on Rust's run (LLM 1.2 s, TTS 2.6 s); at 6% CPU the engine is unlikely to be the cause, so
  provider latency under load is the first suspect.
- At ~150 talking, **Asterisk's main thread is the component closest to its limit** with either engine.

## Where Go is still ahead

| | Go | Rust |
|---|---|---|
| Load proven | 3,545 dials, 1,050 answered, 141 talking at once, 600 in flight | 266 dials, 118 answered, 55 talking |
| QA analysis nodes after each call | yes | **not ported** |
| Debug WAV recording, Postgres TLS | yes | not ported |
| FreeSWITCH, RTP transport, TEN VAD, energy VAD | yes | out of scope by decision |
| Barge-in tuning | TEN VAD, tuned on real calls | Earshot: 0.86 confirmed barge-ins per call vs Go's 0.53. Listen to some calls to check these were real interruptions |
| Provider request ids in logs (STT/TTS handshake) | yes | not yet |

Both: outbound campaigns with parallel dial workers, Dograh call record and recording upload, cost, transfers,
variable extraction, STT gate, orphan sweep and setup timeouts, OTLP logs and traces, the same metric names.

## Verdict

| Question | Answer |
|---|---|
| Which takes less resource? | **Rust**: ~3× less CPU per call, ~2.4× less memory, 4–6× less idle |
| Which is more scalable per box? | **Rust**, by roughly 2–3× before CPU is the limit (extrapolated) |
| Which is better to run in production today? | **Go**: proven at 2.5× Rust's tested load, has QA analysis, and its barge-in is tuned |
| Does switching raise system capacity now? | **No**: Asterisk and the trunk are the limit; fix those first |

## To make the decision final

1. **Sustained head-to-head**: same campaign size (≥3,000 fresh contacts), same dial rate (10/s), same
   `STT_SPEECH_ONLY`, 10+ minutes, one engine at a time, each on its own `ARI_APP`. Target ≥150 talking calls.
2. **Isolate the VAD**: one Go run with `VAD_MODE=energy`, to see how much of the 3× is TEN VAD.
3. **Barge-in quality**: listen to 10–20 Rust calls with confirmed barge-ins.
4. **Port QA analysis** to Rust.
5. **Scale Asterisk** (more cores, or a second box) and get the trunk's calls-per-second and channel limits.
   Without this, neither engine can be loaded past ~140 talking calls.

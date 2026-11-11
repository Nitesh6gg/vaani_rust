# Campaign 25 (`test_R_4K`): the first sustained real campaign on the Rust engine

Nov 11, 2026, 17:39-17:46 IST (12:09-12:16 UTC). Engine alone on 192.168.36.19 (16 vCPU, 32 GB), Go engine
stopped. Org settings: `CONCURRENT_CALL_LIMIT` 400, `DIAL_RATE_LIMIT_PER_SECOND` 35. 4,000 contacts, paused by hand.
Source: SigNoz (`service.name = 'vaani-engine-rust'`), the engine's logs and 10-s metrics. Short version and the
comparison with Go: `BENCHMARK.md`, `GO_VS_RUST.md`.

## Summary

The engine handled every answered call without losing a call, losing audio to dead media, or raising an agent error,
at 6% of the box's CPU. The weak points were the callers (78% of answered calls ended before the caller spoke), caller
audio dropped in 40% of calls, Redis timeouts in the dialer, and two short bursts of slow LLM replies.

## Minute by minute (IST)

| Minute | Dials | Answered | Not connected | Talking (max) | CPU (cores) |
|---|---|---|---|---|---|
| 17:39 | 407 | 31 | 4 | 1 | 0.01 |
| 17:40 | 738 | 460 | 398 | 140 | 0.87 |
| 17:41 | 818 | 421 | 367 | 149 | 0.88 |
| 17:42 | 849 | 354 | 466 | 123 | 0.76 |
| 17:43 | 811 | 375 | 482 | 125 | 1.01 |
| 17:44 | 875 | 398 | 445 | 125 | 0.78 |
| 17:45 | 489 | 306 | 416 | 115 | 0.84 |
| 17:46 | paused | 8 | 57 | 44 | 0.40 |

Dialing ran at 13-15 calls per second. The 35/s limit was never reached: calls in flight sat at exactly 400 for six
minutes, and with calls ringing up to 30 s, 400 in flight allows about 13 new calls a second. To dial faster, raise
`CONCURRENT_CALL_LIMIT` (and watch Asterisk's main thread).

## What happened to the 4,987 dials

```
4,987 dialed
 +- 2,358 answered (47%)
 |    +- 2,354 reached the agent (4 failed to start)
 |         +- 1,842 hung up before saying anything (78%)
 |         +-   511 said something
 |         +-    68 finished the survey ("qualified")
 +- 2,629 not answered
      +- 1,947 rang, no answer (cause 19)
      +-   659 rejected (cause 21)
      +-    21 failed (cause 38 x14, 0 x6, 1 x1)
      +-     8 busy
```

0 trunk "no circuit" (cause 34), 0 `outbound.timeout`, 0 `media.dead`, 0 agent errors, 0 media watchdog timeouts.

## Callers

| Call length | Calls |
|---|---|
| under 5 s | 633 |
| 5-10 s | 743 |
| 10-20 s | 496 |
| 20-60 s | 411 |
| over 60 s | 70 (longest 275 s) |

Median 8 s, average 14 s. Disposition: 2,229 caller hung up (95%), 68 qualified, 56 idle / max duration; 96 calls
were ended by the agent (`end_call`); 257 silent-caller prompts. Most people hear the greeting and hang up, so the
opening line is the first thing to change.

## Resources (engine process)

| Metric | Value |
|---|---|
| Peak CPU (1-min) | 1.01 cores (6% of 16) |
| CPU per talking call | 0.0101 core (353 CPU-s for 34,807 talk-s) |
| Memory | 14 MB idle, 554 MB peak, 435 MB after (flat) |
| Open files / threads / tasks | 553 / 40 / 1,359 |
| Network | 1.69 GB in, 3.44 GB out (49 / 99 kB/s per talking call) |
| 20 ms pacer drift p99 | <= 0.5 ms |
| ARI event queue | 0 throughout |
| CPU guard refusals | 0 |

Calls under 15 s cost more CPU per talk-second: each call's setup (workflow read, STT and TTS handshakes) is spread
over few seconds.

## Latency

| Step | p50 | p95 |
|---|---|---|
| STT, end of speech to final text | 93 ms | 299 ms |
| LLM first token | 284 ms | 1,186 ms |
| TTS first audio | 416 ms | 2,572 ms |
| Whole reply, per call (median of per-call p50 / p95 of per-call p95) | 454 ms | 4.4 s |

- The LLM's p95 spiked to 3.8 s (17:41), 4.1 s (17:43) and 2.9 s (17:44); it was 0.35-0.75 s otherwise. 101 LLM
  requests took 3 s or more. No retries, no fallback lines.
- TTS p95 was 1.0-2.1 s while the campaign was busy and 0.6-0.8 s after it slowed down: the provider under load is
  the first suspect (the engine was at 6% CPU), not proven.
- The Redis timeouts fell in the same minutes as two of the LLM bursts (17:41, 17:44): a short network or host
  hiccup on the engine's box is possible; not proven.

## Agent behaviour

1,465 agent turns. 811 barge-in pauses: 324 confirmed (reply cut), 350 false alarms (43%, resumed). 701 transcripts
ignored because the agent was speaking, 181 after a cut, 47 while it was thinking. 37 times the LLM asked for a
recording that is not available (`34bk399q`) and the agent spoke the text instead. STT audio sent: 20,672 s for
34,807 s of calls (59%).

## Problems found

1. **Caller audio dropped in 932 of 2,354 calls (40%)**: 16,155 frames (323 s, 0.9% of caller audio), up to 192
   frames (3.8 s) in one call. Short calls (< 10 s) lost about 7.6 frames each, longer ones about 5.8, so the loss does
   not grow with length: it most likely happens at the start of the call. The agent reads one caller frame per 20 ms
   tick from a 5-frame (100 ms) queue, so any delay or burst overflows it. The load test (even audio, instant fake
   providers) showed 0 drops; the real Asterisk (main thread at 86%) and real providers did not.
2. **Redis timeouts in the dialer**: 32 slot releases, 17 circuit-breaker writes and 17 rate or slot checks timed
   out. A slot that is not released keeps one of the 400 busy until its 20-minute stale timeout.
3. **4 calls failed to start**: 3 `bind media port ...: Address already in use`, 1 externalMedia 500 from Asterisk.
4. **Asterisk's main thread at 74-86% of one core** (box 19-26%), the component closest to its limit.

## Cost (from the `run.cost` records, 2,353 calls)

| | USD | Share |
|---|---|---|
| TTS | 2.78 | 56% |
| STT | 1.88 | 38% |
| LLM | 0.30 | 6% |
| **Total** | **4.96** | $0.0021 per answered call, $0.0085 per talk-minute |

TTS dominates because every answered call plays the greeting, even when the caller hangs up straight away.

## Scorecard

| Area | Metric | Value | Rating |
|---|---|---|---|
| Calls | answered / reached the agent | 47% / 99.8% | Good |
| Calls | lost by the engine (no result, dead media) | 0 | Good |
| Calls | trunk "no circuit" | 0 | Good |
| Calls | failed to start | 4 | Watch |
| Calls | dial speed | 13-15/s (held by the 400 cap) | Watch |
| Callers | hung up before speaking | 78% | **Fix** (greeting) |
| Callers | qualified | 2.9% of answered | Watch |
| Resources | peak CPU / per call | 6% / 0.0101 core | Good |
| Resources | memory | 554 MB peak, flat after | Good |
| Resources | open files, threads | 553, 40 | Good |
| Audio | pacer drift p99 | <= 0.5 ms | Good |
| Audio | agent frames dropped | 304 | Good |
| Audio | **caller frames dropped** | 16,155 in 40% of calls | **Fix** |
| Audio | dead media | 0 | Good |
| Speed | STT p95 | 299 ms | Good |
| Speed | LLM p95 | 1,186 ms (bursts to 4 s) | Watch |
| Speed | TTS p95 | 2,572 ms | Watch |
| Agent | errors, LLM retries, fallbacks | 0 | Good |
| Agent | barge-in false alarms | 43% of pauses | Watch |
| Agent | missing clip `34bk399q` | 37 | **Fix** (workflow) |
| Redis | slot-release / limit / breaker timeouts | 32 / 17 / 17 | **Fix** |
| Asterisk | main thread | 74-86% of a core | Watch |

## Next

1. Shorten or change the greeting (Dograh workflow); fix or remove the missing clip `34bk399q`.
2. Fix the caller-audio drops: take every waiting caller frame each tick, enlarge the queue (about 1 s), keep the
   recording aligned.
3. Redis: an explicit timeout, retry a failed slot release, count Redis errors; check `redis-cli --latency` during a
   campaign.
4. Port reuse: check the order in which a call frees its port and closes its listener.
5. To dial faster: raise `CONCURRENT_CALL_LIMIT`, watching Asterisk's main thread.

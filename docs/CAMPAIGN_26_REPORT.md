# Campaign 26: the first campaign with the engine next to Asterisk

Nov 17, 2026. Dialing ran **18:16-18:23 IST** (12:46-12:53 UTC); the last call ended 18:27:20 and the engine was stopped by
hand at 18:27:36 (Asterisk logged its ARI WebSocket closing then). Engine on **192.168.26.88** (32 cores), on the same
network as Asterisk (192.168.26.249, RTT ~0.5 ms) and the data server (192.168.26.130). Org 7, campaign 26 resumed.

Sources, all cross-checked: SigNoz metrics and logs (`service.name = 'vaani-engine-rust'`, instance `192.168.26.88:9092`),
SigNoz host metrics for `asterisk-dev-server-26.249` and `data-server-26.130`, and the Asterisk box itself over SSH
(`messages.log`, `cdr-csv/Master.csv`, `asterisk -rx`, `/proc/net/snmp`). The engine host has no host metrics in SigNoz.

The asked window was 17:16-18:40 IST; nothing was dialed and the engine logged nothing before 18:10 IST, so every figure
below is from 18:15-18:30 IST.

## Summary

- **The move worked for the link between Asterisk and the engine.** Asterisk logged **0** AudioSocket "Poll timed out"
  errors (Nov 15: 241 in 11 s), the dialer logged **0** Redis stalls (Nov 15: 103), and the engine dropped **1** caller
  frame of 1.74 M (Nov 15: 11,850). The engine's media code is the same as on Nov 15, so the change comes with the move.
- **The main media problem left is caller audio that never arrives.** In **121 of 1,435 calls (8.4%)** the engine received
  under half as many caller frames as it sent; in **35 (2.4%)** under a tenth, which is one-way audio: the agent talks, the
  caller is not heard. Asterisk's kernel dropped no UDP and Asterisk logged nothing for these calls, so the audio is
  missing before or inside Asterisk, not in the engine. Not yet pinned down (see "Caller audio missing").
- **No media problem explains the short calls.** 653 calls (45%) ended inside 10 s, nearly all by the caller hanging up
  before speaking; 584 of them had full audio both ways and the greeting started within ~0.2 s.
- **The agent can sit silent for 10 s** after a pause on noise: 52 pauses lasted 5-10 s, the cap.
- Not media: the LLM rate limit (2,460 HTTP 429 attempts, 320 fallback lines; being fixed separately) and the circuit
  breaker pausing the campaign twice on cause-38 failures from the trunk.

## Minute by minute

| IST | Dials | Answered | Not connected | Talking (max) | Engine CPU (cores) | Asterisk CPU busy (of 4) | Caller frames late |
|---|---|---|---|---|---|---|---|
| 18:16 | 420 | 10 | 9 | 0 | 0.01 | 0.19 | – |
| 18:17 | 0 (paused 18:16:59) | 123 | 278 | 69 | 0.64 | 0.43 | 4.9% |
| 18:18 | 198 (paused 18:18:31) | 60 | 117 | 34 | 0.37 | 0.29 | 3.1% |
| 18:19 | 280 | 1 | 22 | 28 | 0.28 | 0.21 | 3.9% |
| 18:20 | 1,046 | 479 | 523 | 161 | 1.21 | 0.86 | 7.1% |
| 18:21 | 516 | 209 | 239 | 185 | 1.60 | 0.80 | 10.0% |
| 18:22 | 905 | 492 | 560 | 176 | 1.95 | 1.03 | 12.3% |
| 18:23 | 0 | 61 | 182 | 153 | 1.37 | 0.62 | 11.5% |
| 18:24-27 | 0 | 0 | 0 | 45 → 2 | 0.50 → 0.02 | ~0.2 | 6.6% → 3.3% |

Dials and outcomes from the engine's logs; talking is the 1-minute max of `vaani_calls_active`; Asterisk busy is 4 minus
the idle rate of `system.cpu.time`. The busiest 10 s had 350 dials (35 per second). Calls in flight hit the **500** limit
in 18:20-18:22. Dialing stopped at 18:23 with no pause or completion logged; 896 busy/no-answer retries were scheduled
for later.

## What happened to the 3,365 dials

```
3,365 dialed
 +- 1,435 answered (43%)  = 1,435 call.summary lines = 1,435 ANSWERED CDRs on Asterisk
 |    +- 1,190 caller hung up, 152 qualified, 81 idle too long, 8 LLM unavailable, 4 max duration
 +- 1,930 not connected
      +- 1,509 no answer (cause 19)
      +-   378 rejected (cause 21)
      +-    37 failed, cause 38 ("network out of order"), 26 of them in 18:16-18:18
      +-     4 busy (cause 17), 2 failed cause 1
```

The two breaker pauses ("3 of 6 calls failed", "5 of 10") came from the cause-38 cluster at the start; dialing went on
after each. Cause 38 and 21 come back from the trunk (`server225`, 192.168.26.225), not from the engine.

Talk time: 36,921 s (engine) against 36,481 billable seconds on Asterisk's CDRs (1.2% apart: the engine counts from
staging). Average call 25.7 s. 0 calls refused by the engine, 0 setup timeouts, 0 orphans, 0 ARI reconnects, 0 database
pool timeouts, 0 Redis errors, post-call queue never waited. **The hand stop lost nothing:** every answered call has its
summary, and the stop came 16 s after the last call ended.

## Media

| | Nov 15 (engine at the other site) | **Nov 17 (engine on 26.88)** |
|---|---|---|
| Asterisk "Poll timed out" (AudioSocket) | 241 | **0** |
| `campaign.redis_slow` | 103 | **0** |
| Caller frames dropped (engine queue full) | 11,850 (1.0%) | **1** |
| Caller frames late (arrived in a clump, still heard) | 137,048 (12%) | 162,668 (9.4%) |
| Agent frames dropped (outbound queue full) | – | 0 |
| AudioSocket send errors / malformed frames | – | 0 / 0 |
| Pacer drift p99 | ≤ 0.5 ms | ≤ 0.5 ms (lowest bucket) |
| Dead media (`media.dead`, 30 s of no caller audio) | 0 | 2 |
| Asterisk UDP receive-buffer errors | 0 in the window | 0 (238,917 since boot, unchanged since Nov 15) |
| Network errors/drops on either server's NIC | – | 0 |

**Outbound (agent to caller) is clean.** Every call sent about 50 frames a second for its whole length
(`packets_out` / (duration × 50): p10 0.955, p50 1.000), no frame was dropped, the pacer was on time.

### Caller audio missing

Per call, `packets_in` against `packets_out` (both counted on the same connection, so the ratio is the share of the caller's
audio that arrived):

| Caller frames received / agent frames sent | Calls |
|---|---|
| under 10% (practically no caller audio) | 35 |
| 10-50% | 86 |
| 50-90% | 36 |
| 90-98% | 28 |
| 98% or more | 1,250 |

- Of the 121 calls under 50%, 34 still had a caller turn (the audio was missing for part of the call); median length 9 s.
- Examples: `vaani-out-884e5238-0e06` (30 s, **0** caller frames, ended by the 30 s dead-media timer),
  `vaani-out-cc4ce28b-73f0` (11 s, 3 frames), `vaani-out-94b33197-c069` (10 s, 2 frames).
- It is **not load**: the share of calls with gaps (in/out under 90%) was 20% at 18:17 with 69 talking and 8-14% at
  150-185 talking. It is not the engine (nothing dropped, ticks on time) and not the kernel on Asterisk (no UDP buffer errors).
- Asterisk writes a caller frame to the AudioSocket when the trunk's RTP gives it one (the trunk has `Jitterbuffer
  enabled: No`, `RTP Timeout: 0`), so missing frames most likely mean **RTP that never came from the trunk** (a gateway
  that suppresses silence, one-way audio on the far side), or Asterisk not passing it on. Which one needs per-channel RTP
  counts on Asterisk during a run (`sip show channelstats`, in `ASTERISK.md` section 6).

### Late caller frames

`vaani_media_inbound_late_total` counts caller frames that arrived two or more to a 20 ms tick; they are still fed to the
STT, VAD and recording, so they cost at most a few tens of milliseconds, not audio. The rate rose with load: 3-5% at 30-70
talking, 10-12% at 160-185. The engine's ticks were on time and the link is now 0.5 ms, so the clumps arrive that way:
from Asterisk forwarding the trunk's RTP without a jitter buffer, or from Asterisk's own scheduling as its CPU (mostly
kernel networking: system 0.56 + softirq 0.28 cores at the peak) rose. Harmless at this level; worth watching above 300 talking.

### Pauses that become dead air

When VAD hears something over the agent, the reply pauses. Of 304 pauses that resumed without a cut:

| Resume reason | Count | p50 | p90 | max |
|---|---|---|---|---|
| no transcript (noise) | 157 | 2.6 s | **10.0 s** | 10.0 s |
| backchannel ("जी", "ok") | 147 | 1.1 s | 3.3 s | 8.8 s |

52 "no transcript" pauses lasted 5 s or more, the longest stopped only by the 10 s cap (`FALSE_INTERRUPTION_MAX_PAUSE`). The resume waits for
1.2 s of quiet, and on a line with steady noise the quiet never comes, so the caller hears 10 s of silence. 1,023 pauses,
677 confirmed (66%; Nov 15 45%, before the new interruption settings).

## Servers

| | Peak (1-min avg) | Note |
|---|---|---|
| Engine (26.88, 32 cores) | 1.95 cores at 176-185 talking; 493 CPU-s for 36,921 talk-s = **0.0133 core per talking call** | Nov 15: 0.0104. RSS peak 964 MB (Nov 15: 674 MB) |
| Asterisk (26.249, 4 vCPU) | 1.03 cores busy (26%) at 18:22 | NIC 30,800 packets/s in, 17,400 out; no drops or errors |
| Data server (26.130, 8 cores) | 0.85 cores busy (11%) at 18:22 | |

No server was near a limit.

## Latency (histograms, 18:15-18:30)

| Step | p50 | p95 |
|---|---|---|
| STT, end of speech to final text | 86 ms | 171 ms |
| LLM first token (successful requests) | 288 ms | 905 ms |
| TTS first audio | 397 ms | 1,085 ms |
| Whole reply, p95 across calls of each call's p95 (`call.summary`) | | 2.1 s |

TTS p95 is about half of Nov 15 (1,951 ms).

## Other

- **LLM:** 2,460 request attempts answered HTTP 429 (Sarvam via Bifrost, key `sarvam_test_key`); 321 turns failed after
  retries and 320 fallback lines were spoken; 209 calls had at least one; 8 ended as `llm_unavailable`.
- **`tts.clip_missing` 87**, in 87 calls: `34bk399q` in 74 calls (the recording is missing in Dograh, as on Nov 11 and 15) and
  13 with IDs that look like the LLM misspelling a real one (`vsxxfi8z`, `vsxxfi8v`, `vsxxfi8k`, `hm7qal6n`, ...).
- STT audio sent: 23,914 s for 36,921 s of calls (65%).

## Not established

- Whether the missing caller audio is the trunk not sending RTP or Asterisk not forwarding it.
- What sits behind the trunk at 192.168.26.225, and whether it suppresses silence.
- Who resumed dialing after each breaker pause (the logs do not say).
- The engine host's own CPU, network and kernel counters: 26.88 is not in SigNoz host metrics.

## Next

1. Next run: record `sip show channelstats` on Asterisk every 5 s (`ASTERISK.md` section 6) and match the calls with
   little caller audio by time and channel.
2. Let a noise pause end sooner than 10 s (`TECH_DEBT.md` #23).
3. Upload `34bk399q` in Dograh or take it out of the prompt.
4. Add the metrics in `TECH_DEBT.md` #24 so the next report can read this from dashboards rather than from saved logs.
5. Add 26.88 to SigNoz host metrics.

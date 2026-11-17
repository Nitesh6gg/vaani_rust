# System design: 1,000+ concurrent talking calls (cells)

Status: **proposal, nothing here is built** except what section 4 marks as existing. Estimates are labelled; measured
figures come from `BENCHMARK.md` and `GO_VS_RUST.md`.

## 1. Requirements

**Functional**
- Inbound and outbound calls over a SIP trunk. Each call is a STT→LLM→TTS conversation, with barge-in and transfer.
- Agent config comes per call from Dograh's Postgres. Runs and recordings are written back to Dograh.

**Non-functional (assumed targets, to be confirmed)**
- 1,000 concurrent *talking* calls, with headroom, not 1,000 channels.
- Bursts of new calls, up to the trunk's 40 calls/s, must not degrade calls already up.
- One node failing must not drop more than that node's calls.
- Reply latency: keep the 454 ms median of campaign 25 and bring its p95 (4.4 s) under control.

**Constraints**
- Asterisk **20.19** (not 22) with ARI and AudioSocket; its SIP trunks run on the deprecated `chan_sip`, PJSIP has no
  endpoints (`ASTERISK.md`). Config stays in Dograh.
- SIP trunk of 40 cps and 1,000 channels *including ringing*.
- LLM quota of 1,000 req/min. Sarvam stream limits are unknown.

## 2. Measured facts the design rests on

| Fact | Value | Source |
|---|---|---|
| Engine CPU per call, mock providers | 0.0048 core | 1,000-call load test (`BENCHMARK.md`) |
| Engine CPU per call, real campaign | 0.0101 core | campaign 25 |
| Engine memory | 2.7 MB/call (includes ~1.9 MB per call-minute of recording) | load test |
| Asterisk (4 cores) | whole process 54-86% of one core (box 67% idle) at ~150 talking calls; busiest thread 2.6% at 652 channels. **Not** a single-thread ceiling (corrected Nov 15) | campaign 25, `ASTERISK.md` |
| Trunk | 40 cps, 1,000 channels | deployment |
| LLM | ~4,400 req/min needed at 1,000 calls vs 1,000 quota | earlier audit |

At 1,000 calls the engine needs about 10 cores for real traffic. What saturates first is not established: Asterisk was
assumed to, but at 150 talking it used about half a core of four. The limits actually seen are the trunk's (cause 34)
and the LLM's (429, 7% of turns on Nov 15), and Redis stalls of 1-2 s in the dialer.

## 3. High-level design: cells

```
                 SIP trunk (40 cps, 1,000 ch)
                          |
                  SIP dispatcher / SBC (Kamailio-style)
              503 -> try next cell; health-checked
        +-----------+-----------+-----------+
        |           |           |           |
     Cell 1      Cell 2   ...  Cell N         N = 7-9 (estimate)
   Asterisk(4c)  Asterisk      Asterisk
   ARI app "vaani-c1" ...      "vaani-cN"     unique app name per cell
        | ARI WS + AudioSocket (localhost/LAN)
   Vaani engine (2-4c)  -- one ARI WebSocket, per-call actors
        |            |             |
   Sarvam STT/TTS   LLM gateway   Dograh Postgres / MinIO / Redis
                   (Bifrost, rate-limited)
```

A **cell is one Asterisk plus one engine.** Asterisk delivers a Stasis app's events only to the most recently connected ARI client
(Dograh's Asterisk doc says the same), so each cell needs its own app name.

**Capacity (estimate, not measured):** ~150 calls per 4-core Asterisk gives about 7 cells for 1,000 calls, 9 with ~25% headroom.
An engine needs ~1.5 cores per cell, so Asterisk decides the cell count.

## 4. Burst path

Existing (`Manager::setup`, `ANSWER_WAIT_SECONDS`, CPU guard):
1. The engine prepares an inbound call (workflow load, STT open) while the caller rings.
2. It takes a setup turn (16 at once), then answers.
3. Past `ANSWER_WAIT_SECONDS` (10), over the CPU guard, or on a failure before the answer, the caller gets a 503.

Missing for this to work end to end:
- **Dispatcher failover** on 503 (the dispatcher itself).
- **A hard concurrent-call cap per cell** alongside the CPU guard.
- **Provider-slot tokens** so the LLM and Sarvam quotas are respected.

## 5. Shared state to fix before running several cells

- **Redis key `vaani_capacity:calls` is global.** Make it per-cell, or a global total on purpose.
- **Startup `recover_contacts` and `sweep(true)` are not node-scoped.** A restarting cell would requeue or hang up another cell's work. Scope them by cell id.
- **Campaign dialer:** one per cell, or one elected with a lease, to avoid double dialing.
- **The ARI events WebSocket has no TCP keepalive** (measured: the kernel timer is off). A dead Asterisk host can leave it hanging unnoticed. Add a ping and timeout.
- **The post-call backlog is lost beyond the 60 s shutdown wait.** Persist or drain it before a cell can be rotated safely.

## 6. Reliability and operations

- **Failover:** the dispatcher health-checks cells and stops sending to one that is draining or failing.
- **Drain mode:** stop accepting new calls, finish the calls up, flush post-call work, then restart. Not built.
- **Monitoring, per cell:** rejects by reason (`cpu`, `answer_wait`), the setup queue, the post-call backlog, pool wait, Asterisk per-thread CPU (not the `htop` process row). Alert on rejection rate, not only on errors.
- **Capacity alarm:** page when a cell sustains over 70% of its measured call count, before the CPU guard starts refusing calls.

## 7. Trade-offs

| Decision | Gain | Cost |
|---|---|---|
| Cells (one Asterisk + engine each) | A failure is contained; scales linearly; keeps each Asterisk (a deprecated single-socket `chan_sip` stack) small | A dispatcher to run; shared-state fixes; more boxes |
| One big Asterisk | Simple | One failure drops everything; its ceiling at 300+ talking is unmeasured (no single-thread limit has been shown) |
| Refuse early (503) over queueing | No dead air; trunk can fail over | Some calls are bounced in a burst |
| Per-cell engine over a shared engine pool | Fault isolation; local AudioSocket | Idle capacity per cell |

## 8. Assumptions and open questions

- The 150-calls-per-Asterisk figure is where the campaigns stopped, **not a measured limit**: no thread was saturated at ~150 talking (652 channels). Measure Asterisk at 300+ talking before sizing cells by it.
- The shared services (Redis, Postgres behind pgbouncer, MinIO, the Bifrost LLM gateway) run on one box (`master`). The
  Redis stalls of 1-2 s seen on Nov 15 came from the engine being at another site; with the engine on the same network
  (Nov 17) there were none, and Asterisk's AudioSocket timeouts went too (`ASTERISK.md` section 8). **A cell's engine
  must sit on the same network as its Asterisk and the data services.** Their limits, and `pgbouncer`'s pool settings, count as much as the engine's pools.
- Sarvam's stream limits and the LLM quota decide the real ceiling. Get both from the vendors.
- Does the trunk support 503 failover and spreading INVITEs across Asterisks? Confirm with the carrier.

## 9. To revisit as it grows

- A static cell count versus autoscaling on the capacity alarm.
- The dispatcher as a single point of failure.
- Whether the engine should leave the Asterisk host once calls per cell rise.

## Proposed next steps (none started)

1. The shared-state fixes in section 5.
2. A hard per-cell call cap and provider-slot limits.
3. A second load run on a different Asterisk size to test the 150-call figure.

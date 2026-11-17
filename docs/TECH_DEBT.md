# Tech debt

Audit of the Rust engine. Priority = (Impact + Risk) x (6 - Effort), each scored 1-5. Status is updated as items are fixed.

| # | Item | Type | I | R | E | Score | Status |
|---|---|---|---|---|---|---|---|
| 1 | **No CI, no deploy files, no dependency audit.** Nothing enforced fmt, clippy or the tests. | Infra | 4 | 4 | 2 | 32 | **Partly done**: `.github/workflows/ci.yml` runs fmt, clippy (`-D warnings`) and the tests against Postgres and Redis. Not yet run on GitHub. Still missing: a Dockerfile/deploy files and `cargo-deny`/`cargo-audit` |
| 2 | **Database and Redis tests skipped silently** when their URLs were unset, so a run could pass having tested nothing. | Test | 4 | 4 | 2 | 32 | **Done**: with `REQUIRE_TEST_SERVICES` set (CI does), a missing URL panics (`store::test_service_url`) |
| 3 | **ARI events WebSocket had no keepalive.** A dead Asterisk host looked the same as a quiet one. | Code | 3 | 4 | 2 | 28 | **Done**: ping every 30 s, taken for dead after 75-105 s of silence (checked on each ping tick; a ping that cannot be sent in 30 s counts too), then the normal reconnect (and teardown of calls) |
| 4 | **Answer-wait and prepare gates untested under a burst.** Unit tests prove ordering and the deadline; no load-generator run has used them. | Test | 3 | 4 | 2 | 28 | **Partly done**: two burst runs on the 4-core sandbox (below). The loadgen's mocks answer instantly, so they do not stress the gates the way a slow Sarvam handshake would; that needs a real-provider test or a `--stt-connect-ms` option in the generator |
| 5 | **Shared state isn't node-scoped**: `vaani_capacity:calls`, startup `recover_contacts` and `sweep(true)` assume one engine. | Architecture | 4 | 5 | 3 | 27 | Open: needs a design decision (see below and `SYSTEM_DESIGN.md` section 5) |
| 6 | **No operations runbook.** | Docs | 3 | 3 | 2 | 24 | **Done**: `RUNBOOK.md` |
| 7 | **Post-call backlog is lost past the shutdown wait** (records exist only in memory until written). | Architecture | 3 | 4 | 3 | 21 | Open |
| 8 | **Hard-coded limits** (16 setups, 32 prepares, 15 s setup timeout). | Code | 2 | 2 | 1 | 20 | **Done**: `SETUP_CONCURRENCY`, `PREPARE_CONCURRENCY`, `SETUP_TIMEOUT_SECONDS` |
| 9 | **Mutex poisoning can cascade**: ~73 `.lock().unwrap()` on std mutexes in `manager.rs`; one panic under a lock makes every later user of it panic. | Code | 3 | 3 | 3 | 18 | Open |
| 10 | **Stale leftovers.** | Code | 2 | 1 | 1 | 15 | **Done**: the `dead_code` allow on `LoadedWorkflow.org_id` is gone. The `#[ignore]`d `bench_concurrent_agents` (a capacity probe) and the `unused_mut` allow (test-only skew) are intentional |
| 11 | **Duplicate dependency versions** in the lockfile. | Dependency | 2 | 2 | 3 | 12 | **Partly done**: our `reqwest` is now 0.13 (features `rustls`, `query`), the same as OpenTelemetry's, so one copy, 2 fewer packages and a 1.2 MB smaller binary. It uses the system trust store through `rustls-platform-verifier` (0.12 bundled roots; the WebSocket clients already needed the system store) and our ring provider (installed at startup). What remains is upstream: `base64` 0.22 (tokio-postgres) vs 0.23 (hyper-util), `rand` 0.9 (opentelemetry_sdk) vs 0.10 (tokio-postgres), and `getrandom`, `phf`, `syn`, which no direct dependency choice can merge |
| 12 | **Go/Rust drift**: the Go repo lacks answer-when-ready, the User-Agent and other fixes. | Architecture | 3 | 3 | 4 | 12 | Open |
| 13 | **Large files**: `handler.rs` (~1.9k lines) and `manager.rs` (~1.8k, about half tests). | Code | 3 | 2 | 4 | 10 | Open |
| 14 | **The LLM is rate-limited under a campaign.** 71 of 1,020 LLM turns (7%) failed with HTTP 429 from Sarvam via Bifrost (key `sarvam_test_key`, `sarvam-105b-conversations`) and the agent spoke a canned line; 679 failed attempts including retries (Nov 15). | Capacity | 5 | 4 | 2 | 36 | Open: raise the Sarvam rate limit or use a production key / several keys in Bifrost; then consider backing off retries on 429 instead of retrying at once |
| 15 | **Redis stalls for 1-2 s in bursts** (`campaign.redis_slow`: slot release 1,813 ms, rate limiter 1,285, slot limiter 1,240 in the Nov 15 run, 103 logs) even with the pool of 4 and the 5 s timeout. Redis runs in Docker on `master`, shared with Postgres (via pgbouncer), MinIO and Bifrost. | Infra | 4 | 4 | 3 | 24 | **Cause found** (Nov 17): the link between the sites. With the engine moved to 192.168.26.88, next to Redis, campaign 26 logged **0** `campaign.redis_slow` (and Asterisk 0 AudioSocket poll timeouts, against 241 on Nov 15); Redis itself had been fast throughout (empty slowlog). Keep the engine on the data server's network (`ASTERISK.md` section 8) |
| 16 | **A confirmed barge-in costs ~145 ms more on the next reply**: the cut closes the TTS WebSocket and the next turn reconnects before asking the LLM (`turn_task`). Measured 133-163 ms across 18 cuts. | Code | 3 | 2 | 2 | 20 | Open: open the TTS connection concurrently with the LLM request (no extra connections, so no extra billing; the code notes TTS is billed on open, so no spare pre-opened connection) |
| 17 | **The recording `34bk399q` is missing**: the workflow asks for it, the agent speaks the text instead (47 times Nov 11, 36 on Nov 15, 74 calls on Nov 17). On Nov 17 the LLM also asked for 13 IDs that look like misspellings of real ones (`vsxxfi8z`, `vsxxfi8v`, `vsxxfi8k`, `hm7qal6n`). | Config | 2 | 2 | 1 | 20 | Open: upload it in Dograh or remove it from the prompt; check the prompt lists the recording IDs exactly |
| 18 | **Caller frames still dropped in a campaign**: 11,850 (1.0%) dropped and 137,048 (12%) heard late on Nov 15. | Code | 3 | 3 | 3 | 18 | **Dropped: gone** with the engine next to Asterisk (Nov 17: 1 of 1.74 M; same media code). Late: 162,668 (9.4%), still heard, rising with load (3-5% at 30-70 talking, 10-12% at 160-185); the engine's ticks are on time, so the clumps come from Asterisk (no jitter buffer on the trunk) or the trunk. Watch above 300 talking |
| 19 | **Asterisk SIP runs on the deprecated `chan_sip`** (Asterisk 20.19, UDP 5061); PJSIP has no endpoints. `chan_sip` is removed in Asterisk 21. | Infra | 3 | 3 | 5 | 6 | Open: migrate trunks, registrations, dialplan and ARI dial strings to PJSIP before any Asterisk upgrade |
| 20 | **Latency histograms were too coarse** (a 449 ms reply showed a p99 of 776 ms). | Code | 2 | 2 | 1 | 20 | **Done** (Nov 15, `0fea6d4`): TTFB buckets between 100 ms and 1.6 s are finer; the Go server still has the old ones |
| 21 | **Asterisk "main thread" claim was wrong** and sat in four docs. | Docs | 3 | 3 | 1 | 30 | **Done** (Nov 15): corrected in `BENCHMARK.md`, `CAMPAIGN_25_REPORT.md`, `GO_VS_RUST.md`, `SYSTEM_DESIGN.md`; findings in `ASTERISK.md` |
| 22 | **Caller audio missing in some calls** (Nov 17, campaign 26): in 121 of 1,435 calls (8.4%) the engine received under half as many caller frames as it sent, in 35 (2.4%) under a tenth (one-way audio; e.g. `vaani-out-884e5238-0e06`: 30 s, 0 caller frames). Not load-related, not the engine (0 dropped, pacer on time), not Asterisk's kernel (0 UDP buffer errors). | Telephony | 5 | 4 | 3 | 27 | Open: record `sip show channelstats` during a run (`ASTERISK.md` section 6) to see whether the trunk (`server225`, 192.168.26.225) stops sending RTP or Asterisk stops forwarding it; then the trunk's silence suppression / NAT, or Asterisk's settings |
| 23 | **A pause on line noise can hold the agent silent for 10 s**: the reply resumes after 1.2 s of quiet, but on a steadily noisy line the quiet never comes, so `FALSE_INTERRUPTION_MAX_PAUSE` (10 s) ends it. Nov 17: 157 "no transcript" resumes, p50 2.6 s, p90 10.0 s; 52 lasted 5 s or more. | Code | 3 | 2 | 1 | 25 | Open: a shorter cap (3-4 s), or resume when VAD has heard no new speech start for 1.2 s rather than any sound; needs approval |
| 24 | **Metrics missing for media diagnosis**: the per-call caller-audio share, gaps and pause lengths had to be worked out from 1,435 saved `call.summary` lines. | Observability | 3 | 3 | 2 | 24 | Open: (1) `inbound_late`, longest caller-audio gap and first-caller-frame delay in `call.summary`; (2) a counter of calls with caller frames under 50% of agent frames; (3) a histogram of pause length by outcome (resumed / confirmed); (4) a `cause` label on `vaani_campaign_outcomes_total`; (5) the engine host (26.88) in SigNoz host metrics; (6) Asterisk RTP per channel during runs |

## Found while doing phase A

Two tests were timing-dependent and failed about once in ten full runs: the campaign retry test slept a fixed 400 ms before
reading the run, and the whole-call test read the organization's usage cycle right after `cost_info`, which is written first.
Both now poll. Eight consecutive full runs passed afterwards.

## Burst runs (phase B, Nov 13, development sandbox, not production)

4 vCPUs shared by engine, generator and Postgres; loopback; mock providers; release build. Same box and mocks for all three.

| Run | Peak | Setup p50 / p95 | Agent audio | Gaps > 60 ms | Failed / refused | Ports / calls after |
|---|---|---|---|---|---|---|
| 300 calls, ramp 150/s | 300 | 2 / 5 ms | 1.00 | 0 | 0 / 0 | 0 / 0 |
| 1,000 calls **at once**, ramp 1,000/s | 1,000 | 574 / 1,166 ms | **0.66** (0.53-0.77 over the run) | 122,958 | 0 / 0 | 0 / 0 |
| 800 calls, ramp 20/s (calls ended before 1,000) | 800 | 3 / 17 ms | 1.00 | 0 | 0 / 0 | 0 / 0 |

What this shows: the gates admit a 1,000-call burst with no failure and no leak (every port and call freed afterwards,
`vaani_calls_rejected_total` all 0). It also shows a 1,000-call burst on this box degrades audio for as long as it is held
(0.66 frames per frame sent), where a gradual ramp of 800 does not. What it does **not** show: the cause. The generator
and the mocks share the four cores, and calls started together also take their speech turns together (the mock hears the
caller every ~6 s from the same instant), so synchronized turns and a saturated box are both plausible. The 800 and 1,000
runs also differ in size. To settle it: generator on another machine, 1,000 calls ramped and bursted.

## Remaining plan

- **Phase B**: #6 done, #4 partly; #5 waits for a design decision (below).
- **Phase C**: #7, #9, #11, then decide Go's future (#12) before splitting the large files (#13).
- Still to add to #1: a Dockerfile/deploy files and a dependency audit job.

## #5 design decision (not started)

`vaani_capacity:calls` can be scoped by a `CELL_ID` setting (an unset value keeps today's key). The harder part is the startup
recovery, which requeues every contact in `processing` and completes every `running` run nothing here knows about: with two
engines it would requeue (double-dial) and interrupt the other engine's live work. Options:
1. **Ownership in Redis** (per-cell sets of claimed contacts and live runs, written at claim and at completion): recovery
   touches only the restarting cell's entries. Correct, but a Redis loss leaves its leftovers unrecovered, and every claim and
   completion path gains a write.
2. **No automatic recovery when several cells run** (`CAMPAIGN_RECOVER_ON_START=0` on all but a designated one): simple and safe
   against double dialing, but leftovers of the other cells stay until someone requeues them by hand.
3. **One dialer for the fleet** (the campaign runner on one designated cell or lease-elected; the others only take calls): no
   shared-state change, at the cost of the dialer being a single point.

## Dependency review (Nov 14)

- `chrono` + `chrono-tz` replaced by `jiff` (Timestamp, bundled tz database): SigV4 dates, Dograh's isoformat timestamps, Asterisk creation times,
  the campaign schedule and the template time zones, all checked by the existing tests (plus one for the timestamp shapes).
- The call/LLM spans moved from hand-driven `SdkSpan`s to `tracing` spans exported by `tracing-opentelemetry`. The log bridge stays ours:
  `opentelemetry-appender-tracing` takes a line's trace context from the current span, but a call's lines are written from many tasks that
  are in no span, so the lines find their trace by `call_id`. This swap adds a dependency (`tracing-opentelemetry`) and removes about 40 lines.

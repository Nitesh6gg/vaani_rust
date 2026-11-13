# Tech debt

Audit of the Rust engine. Priority = (Impact + Risk) x (6 - Effort), each scored 1-5. Status is updated as items are fixed.

| # | Item | Type | I | R | E | Score | Status |
|---|---|---|---|---|---|---|---|
| 1 | **No CI, no deploy files, no dependency audit.** Nothing enforced fmt, clippy or the tests. | Infra | 4 | 4 | 2 | 32 | **Partly done**: `.github/workflows/ci.yml` runs fmt, clippy (`-D warnings`) and the tests against Postgres and Redis. Not yet run on GitHub. Still missing: a Dockerfile/deploy files and `cargo-deny`/`cargo-audit` |
| 2 | **Database and Redis tests skipped silently** when their URLs were unset, so a run could pass having tested nothing. | Test | 4 | 4 | 2 | 32 | **Done**: with `REQUIRE_TEST_SERVICES` set (CI does), a missing URL panics (`store::test_service_url`) |
| 3 | **ARI events WebSocket had no keepalive.** A dead Asterisk host looked the same as a quiet one. | Code | 3 | 4 | 2 | 28 | **Done**: ping every 30 s, taken for dead after 75 s of silence, then the normal reconnect (and teardown of calls) |
| 4 | **Answer-wait and prepare gates untested under a burst.** Unit tests prove ordering and the deadline; no load-generator run has used them. | Test | 3 | 4 | 2 | 28 | **Partly done**: two burst runs on the 4-core sandbox (below). The loadgen's mocks answer instantly, so they do not stress the gates the way a slow Sarvam handshake would; that needs a real-provider test or a `--stt-connect-ms` option in the generator |
| 5 | **Shared state isn't node-scoped**: `vaani_capacity:calls`, startup `recover_contacts` and `sweep(true)` assume one engine. | Architecture | 4 | 5 | 3 | 27 | Open: needs a design decision (see below and `SYSTEM_DESIGN.md` section 5) |
| 6 | **No operations runbook.** | Docs | 3 | 3 | 2 | 24 | **Done**: `RUNBOOK.md` |
| 7 | **Post-call backlog is lost past the shutdown wait** (records exist only in memory until written). | Architecture | 3 | 4 | 3 | 21 | Open |
| 8 | **Hard-coded limits** (16 setups, 32 prepares, 15 s setup timeout). | Code | 2 | 2 | 1 | 20 | **Done**: `SETUP_CONCURRENCY`, `PREPARE_CONCURRENCY`, `SETUP_TIMEOUT_SECONDS` |
| 9 | **Mutex poisoning can cascade**: ~73 `.lock().unwrap()` on std mutexes in `manager.rs`; one panic under a lock makes every later user of it panic. | Code | 3 | 3 | 3 | 18 | Open |
| 10 | **Stale leftovers.** | Code | 2 | 1 | 1 | 15 | **Done**: the `dead_code` allow on `LoadedWorkflow.org_id` is gone. The `#[ignore]`d `bench_concurrent_agents` (a capacity probe) and the `unused_mut` allow (test-only skew) are intentional |
| 11 | **Duplicate dependency versions** (reqwest 0.12 and 0.13, base64, rand, getrandom). | Dependency | 2 | 2 | 3 | 12 | Open |
| 12 | **Go/Rust drift**: the Go repo lacks answer-when-ready, the User-Agent and other fixes. | Architecture | 3 | 3 | 4 | 12 | Open |
| 13 | **Large files**: `handler.rs` (~1.9k lines) and `manager.rs` (~1.8k, about half tests). | Code | 3 | 2 | 4 | 10 | Open |

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

# Tech debt

Audit of the Rust engine. Priority = (Impact + Risk) x (6 - Effort), each scored 1-5. Status is updated as items are fixed.

| # | Item | Type | I | R | E | Score | Status |
|---|---|---|---|---|---|---|---|
| 1 | **No CI, no deploy files, no dependency audit.** Nothing enforced fmt, clippy or the tests. | Infra | 4 | 4 | 2 | 32 | **Partly done**: `.github/workflows/ci.yml` runs fmt, clippy (`-D warnings`) and the tests against Postgres and Redis. Not yet run on GitHub. Still missing: a Dockerfile/deploy files and `cargo-deny`/`cargo-audit` |
| 2 | **Database and Redis tests skipped silently** when their URLs were unset, so a run could pass having tested nothing. | Test | 4 | 4 | 2 | 32 | **Done**: with `REQUIRE_TEST_SERVICES` set (CI does), a missing URL panics (`store::test_service_url`) |
| 3 | **ARI events WebSocket had no keepalive.** A dead Asterisk host looked the same as a quiet one. | Code | 3 | 4 | 2 | 28 | **Done**: ping every 30 s, taken for dead after 75 s of silence, then the normal reconnect (and teardown of calls) |
| 4 | **Answer-wait and prepare gates untested under a burst.** Unit tests prove ordering and the deadline; no load-generator run has used them. | Test | 3 | 4 | 2 | 28 | Open |
| 5 | **Shared state isn't node-scoped**: `vaani_capacity:calls`, startup `recover_contacts` and `sweep(true)` assume one engine. | Architecture | 4 | 5 | 3 | 27 | Open (see `SYSTEM_DESIGN.md` section 5) |
| 6 | **No operations runbook.** | Docs | 3 | 3 | 2 | 24 | Open |
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

## Remaining plan

- **Phase B** (with the multi-cell work): #5, #4, #6.
- **Phase C**: #7, #9, #11, then decide Go's future (#12) before splitting the large files (#13).
- Still to add to #1: a Dockerfile/deploy files and a dependency audit job.

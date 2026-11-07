# Outbound campaigns

Vaani dials Dograh's campaigns itself (Asterisk ARI + AudioSocket only). Dograh stays the
control panel: campaigns are created, started, paused and resumed in its UI, and their contacts,
progress and reports stay in its database. This is the Rust port of the Go server's dialer; the
behaviour, keys and Dograh tables are the same, so the two engines can share an organization's limits.

## Who dials

An organization's **media engine** decides. A superadmin sets it per organization in Dograh
(Admin > Organizations > Engine, stored as the `MEDIA_ENGINE` organization configuration:
`pipecat`, the default, or `vaani`).

- `pipecat`: Dograh's own dispatcher dials, as always.
- `vaani`: Dograh's orchestrator skips the organization's campaigns; Vaani dials them. Dograh's
  sheet sync still runs, so rows land in `queued_runs` as usual.

## Turning it on

| Setting | Default | Meaning |
|---|---|---|
| `CAMPAIGNS_ENABLED` | 0 | Start the dialer. Needs `REDIS_URL` and `DOGRAH_DB_URL`. |
| `REDIS_URL` | | **Dograh's** Redis. The organizations' rate and concurrency counters live there, so limits hold across both engines. |
| `OUTBOUND_MAX_CALLS` | 20 | Campaign calls this Vaani may have up at once (own key `vaani_capacity:calls`). 0 = org limits only. |
| `OUTBOUND_DEFAULT_ORG_CONCURRENCY` | 2 | Limit of an organization with none set (Dograh's default). |
| `CAMPAIGN_POLL_SECONDS` | 5 | How often Dograh's database is scanned. A call ending wakes the scan at once. |
| `OUTBOUND_RING_TIMEOUT_SECONDS` | 30 | Ring time before a call counts as unanswered (5-300). |
| `OUTBOUND_TEST_CALL` | | `"<number>,<org id>,<workflow id>"`: dial once at startup, outside any campaign. |

The organization's Dograh telephony configuration must be provider `ari`; its `outbound_trunk`,
`sip_technology`, `strip_number_prefix`, `from_numbers` and `number_workflow_map` give the dial
string and caller ID (Vaani uses its own ARI connection from `.env`, never the config's ARI password):
`<tech>/<number minus prefix>@<trunk>`, caller ID = the number mapped to the workflow, else the first of `from_numbers`.
The Stasis app must be the one Vaani is connected as (`ARI_APP`): a dialed channel enters it on answer.

## One call

1. The dialer finds a `running` campaign of a `vaani` organization (each scan; one drain task per campaign).
2. Checks, in order: campaign still running, inside its schedule window
   (`orchestrator_metadata.schedule_config`; none = always), circuit breaker closed, wallet not empty.
3. Takes a rate token and a concurrency slot (Redis), **then** claims one contact (`queued_runs`,
   scheduled retries first, `FOR UPDATE SKIP LOCKED`). A contact is never held while waiting for a slot.
   Concurrency and dial rate are both Dograh's superadmin → Organizations → **Concurrency** dialog
   (org keys `CONCURRENT_CALL_LIMIT` and `DIAL_RATE_LIMIT_PER_SECOND`); the dial rate overrides the
   campaign's own `rate_limit_per_second`; the campaign's `max_concurrency` can only lower the limit.
   Both are read on every dial, so a change applies to a running campaign without pause/resume.
4. Creates the Dograh run (`WR-CAMPAIGN-<campaign>-<row>`, outbound, `initialized`) so even an
   unanswered call is recorded, then dials: `POST /channels` into the Stasis app with args
   `outbound,<run id>`. A dialed channel enters Stasis only when answered.
5. **Answered**: the usual call path (media socket bound before Asterisk is told its address,
   externalMedia, bridge) with direction `outbound`, the campaign's workflow and the contact's
   variables. The run goes `running`, and the agent completes it as for inbound.
6. **Not answered**: the hangup cause maps to a status as Dograh does: 17 busy, 19 no-answer,
   21 rejected, anything else failed. The run is completed as not connected (tags `not_connected`,
   `telephony_<status>`, disposition BUSY / NO_ANSWER / ..., through the organization's mapping).
   If Asterisk never reports, a watchdog (ring timeout + 5s) hangs the channel up and calls it no-answer.
7. Busy and no-answer are retried (new row `<source>_retry_<n>`, due after `retry_delay_seconds`)
   up to `max_retries`; when they are used up `failed_rows` goes up. Only failed and rejected
   count against the circuit breaker.

## Pausing, resuming, finishing

- **Pause / Resume** in Dograh's UI: checked before every call. Resume resets the breaker (Dograh does it, on the same Redis keys).
- **Vaani pauses a campaign** when the breaker trips (default: half of at least 5 calls failed within
  2 minutes), the wallet is empty, or the organization's telephony provider is not `ari`
  (the contact it had claimed goes back to the queue). Look for `campaign.paused` and its reason.
- **Completion**: when nothing is queued (a retry due later counts), none is being handled and no
  call is in flight. (Dograh completes as soon as nothing is queued, with calls still ringing.)

## After a restart

Once at startup (assuming no other Vaani dials the same campaigns): contacts left `processing` with
no Dograh run go back to the queue; ones with a run stay processed and are never redialed; runs of
calls the restart cut off are completed (`interrupted`). Every minute, runs that never reached the
agent are completed as not connected. Slots of crashed calls expire from Redis after 20 minutes
(Dograh's rule), so a campaign may dial a little under its limit meanwhile.

## Limits to know

- One Vaani instance per set of campaigns.
- No answering-machine detection (`retry_on_voicemail` has nothing to act on).
- Vaani does not debit Dograh's wallet; the wallet check only stops a campaign whose wallet is already empty.
- A call longer than 20 minutes stops counting toward its organization's limit (Dograh's stale-slot
  rule); consider `MAX_CALL_DURATION_SECONDS`.
- Dograh's `campaign_completed` Redis event is not published.

## Metrics

`vaani_campaign_dials_total`, `vaani_campaign_outcomes_total{status}` (connected, busy, no-answer,
rejected, failed), `vaani_campaign_retries_total`, `vaani_campaign_pauses_total{reason}` (breaker,
wallet, provider), `vaani_campaigns_completed_total`, `vaani_campaign_calls_in_flight`. Logs carry
`campaign_id`, `queued_run_id` and `run_id` with the events `campaign.dialed`, `campaign.finished`,
`campaign.not_connected`, `campaign.paused`, `campaign.completed`, `campaign.recovered`,
`campaign.swept`, `outbound.dialing`, `outbound.answered`, `outbound.unanswered`.

## Code

- `src/campaign/runner.rs` — the dialer; `limiter.rs` — Redis counters (Dograh's keys and Lua scripts); `testcall.rs` — `OUTBOUND_TEST_CALL`.
- `src/ari/outbound.rs` — `Manager::dial` and the outcomes.
- `src/dograh/campaign.rs`, `outbound.rs` — Dograh's tables: campaigns, contacts, runs, dial settings.
- Tests: `DOGRAH_TEST_DB_URL` (a Postgres the tests may create schemas in) and `REDIS_TEST_URL`
  (a Redis; the tests use random organization and campaign ids and flush nothing). They skip without them.

# The Asterisk box: what we found and how to diagnose it

Found Nov 15, 2026 by reading the box over SSH (read-only commands) while the Rust engine ran campaign 25 again;
Nov 17 (campaign 26, engine moved next to Asterisk) is section 8.
Earlier reports (`BENCHMARK.md`, `CAMPAIGN_25_REPORT.md`, `GO_VS_RUST.md`, `SYSTEM_DESIGN.md`) said Asterisk's
"main thread" was at 74-96% of a core. **That was wrong**; the correction is in section 3.

## 1. The host

| | |
|---|---|
| Machine | 4-vCPU VMware VM (Xeon Gold 5320, 2.2 GHz), 8 GB RAM, ~1 GB swap (about 270 MB used), Debian, kernel 6.1; up 306 days |
| Asterisk | **20.19.0**, built from source on the box (not 22; the docs said 22) |
| SIP stack | **`chan_sip`** (marked *deprecated*), bound to **UDP 5061**. PJSIP is loaded but has **no endpoints and no transports**. The trunks, the peers and the registrations to the voice-agent servers are all in `sip.conf` |
| Codecs on the trunk | ulaw, alaw (8 kHz); Vaani gets slin16 over AudioSocket, so Asterisk transcodes each way |
| Timing | `res_timing_timerfd` |
| Logging | `logger.conf` writes only notice, warning and error; verbose and debug are 0. Logging is not a cost |
| CDR | enabled (Adaptive ODBC and csv backends); the aggregator kept up (queue depth 15) |
| Other | `vaani_fs` (FreeSWITCH on this box's address, port 5080) shows UNREACHABLE and no FreeSWITCH process runs; one inbound DID was left pointing at it |

`chan_sip` was removed in Asterisk 21. Moving to 21 or 22 means moving every trunk, registration, dialplan and ARI dial
string to PJSIP first. That is planned work, not a fix for anything measured (section 3).

## 2. How a call reaches Vaani

- **Outbound (campaigns):** the engine originates through ARI with its own app (`vaani`); no DID is involved.
- **Inbound:** the dialplan in `extensions.conf` sends each DID to a label that ends in `Stasis(<app>)`. One DID goes to
  `Stasis(vaani)`; the others go to Dograh's per-organization apps (`org2` ... `org9`).
- **ARI apps connected:** `org2`, `org3`, `org7`, `org8`, `org9` (Dograh) and `vaani` (this engine).

## 3. The "saturated main thread" was the whole process

The old claim came from `htop`: thread 1522946, the process's first thread, at 74-96% of a core, with the box at 17-27%.

| Evidence (Nov 15) | |
|---|---|
| That thread's own CPU counter (`/proc/<pid>/task/<pid>/stat`) | 75 user + 5 system ticks = **0.8 CPU-seconds in 17 weeks**. It cannot have been at 96% |
| The box total at the time | 96% / 4 cores = 24%, equal to the "box 17-27%" reading: the row was the sum of all Asterisk threads, not one |
| Lifetime CPU by thread | `timer` 27,000 CPU-s, `SIP` 26,000, `chan_iax2` network thread 18,000: about nine-tenths of all the CPU Asterisk has used. `chan_sip`'s own monitor thread: 1,100 CPU-s |
| Thread 1522958 ("the second busiest") | the logger thread |

Measured live during the Nov 15 campaign, **652 active channels, about 150 calls talking** (10-second samples):

| | |
|---|---|
| Whole Asterisk process | **54% of one core**, spread over about 100-1,000 short-lived threads |
| Busiest single thread | **2.6%** |
| Box | user 11.1%, system 16.1%, softirq 5.3%, steal 0, **idle 67.3%**; load average peak 2.75 |
| UDP receive-buffer errors | 0 in the window (238,917 is the total since boot, from history) |
| UDP 5061 `Recv-Q` | 0 (the SIP socket is not dropping) |
| ARI queue for `vaani` | peak depth **158** of 500 (55,649 events) |
| Swap | 266 MB used, steady; 7.1 GB memory available |

So there is **no single-thread ceiling at this load**, and 150 talking calls per Asterisk is not an established limit.
What is still true: the box was one process at about half a core when we tested, nothing near full. The limits we
actually measured on the telephony side are the SIP trunk's (cause-34 rejects on Nov 9) and, now, the LLM's (section 5).

## 4. Fixed on Nov 15

1. **ARI `subscribeAll`.** Dograh's ARI manager opened every organization's connection with `subscribeAll=true`, so each
   app received every event of every call on the server. Counts proved it: `org2`, `org7`, `org8` and `org9` had each
   processed about 414,000 events, three queues had gone past the 500 high-water mark (635, 630, 611), and one call's
   `ChannelDestroyed` appeared for five apps. The flag was removed in Dograh (rebuilt). After the restart the
   `org*` queues stayed at **1 event each** while the campaign ran (checked at 15:28 and 15:45 IST); only `vaani`
   received events.
2. **Unused modules.** `chan_iax2` (its network thread was the third-busiest over the box's life, port 4569 open) and
   `chan_unistim` have no peers, devices or dialplan use. Both are now `noload` in `modules.conf`
   (backup `modules.conf.bak_15_11_26`); Asterisk restarted with no calls.

## 5. Findings that are not Asterisk's

Seen while watching the Asterisk side; details in `BENCHMARK.md` and `TECH_DEBT.md`.

- **The LLM is rate-limited.** Sarvam answers HTTP 429 "Rate limit exceeded" through Bifrost on the key named
  `sarvam_test_key`: 71 turns of 1,020 fell back to a canned line (7%).
- **Redis stalls for 1-2 s in bursts** (`campaign.redis_slow`). Redis runs in Docker on `master`, the same machine as
  Postgres (behind pgbouncer), MinIO and Bifrost. **Nov 17: gone once the engine moved** (section 8).

## 6. Commands for the next run

On the Asterisk box (as root; `claude` may only run `sudo asterisk -rx ...`). Snapshot, every 5 s, into `/var/log/astmon/`:

```bash
cat > /root/astmon.sh <<'EOF'
#!/bin/bash
INT=${1:-5}
P=$(pidof asterisk) || { echo "asterisk not running"; exit 1; }
mkdir -p /var/log/astmon
OUT=/var/log/astmon/mon_$(date +%Y%m%d_%H%M%S).log
snap() {   # tid, utime+stime ticks, name
  for t in /proc/$P/task/*; do
    { read -r l < $t/stat; } 2>/dev/null || continue
    set -- ${l##*) }; echo "$(basename $t) $(( ${12} + ${13} )) $(cat $t/comm)"
  done
}
{ echo "=== START $(date) ==="; netstat -su | grep -i -E "errors|buffer|packets received"; } >> $OUT
while true; do
  snap > /tmp/astmon.a; sleep $INT; snap > /tmp/astmon.b
  {
  echo "--- $(date +%H:%M:%S) ---"
  echo -n "channels: "; asterisk -rx "core show channels count" | head -1
  echo -n "box: "; top -bn2 -d1 | grep "^%Cpu" | tail -1
  echo "busiest threads (% of one core):"
  awk -v i=$INT 'NR==FNR{a[$1]=$2;next}{d=$2-a[$1]; if(d>0) printf "%.1f%% tid=%s %s\n", d/i, $1, $3}' /tmp/astmon.a /tmp/astmon.b | sort -rn | head -5
  echo "ARI/CDR queues (name processed inqueue maxdepth):"
  asterisk -rx "core show taskprocessors" | grep -E "ari:application|cdr:aggregator" | awk '{print "  "$1, $2, $3, $4}'
  echo -n "udp 5061 (Recv-Q): "; ss -uan 'sport = :5061' | tail -1
  echo -n "mem/swap: "; free -m | awk '/Mem:/{printf "avail=%sMB ",$7} /Swap:/{print "swap_used="$3"MB"}'
  echo "rtp per channel (recv/lost/jitter, both ways):"
  asterisk -rx "sip show channelstats" | grep -v "^Peer\|active SIP"
  } >> $OUT
done
EOF
chmod +x /root/astmon.sh && /root/astmon.sh 5      # inside tmux; Ctrl-C to stop
```

Per-thread CPU is read from `/proc/<pid>/task/*/stat` (fields 14 + 15, 100 ticks per second, so ticks per second is
percent of one core). A "No such file" message from a thread that exited between the list and the read is harmless.

What to read in the log:

- A thread above about 70% names the limit; compare with the box line (box low and one thread high = a single-thread limit).
- ARI queue "maxdepth" over 500 means events are backing up for an app.
- `Recv-Q` growing on UDP 5061, or `receive buffer errors` rising, means the SIP socket is dropping.
- `sip show channelstats`: a channel whose "Recv" count stops growing while the call is up gets no RTP from the trunk
  (match it to a call with little caller audio in `call.summary` by time); high "Lost" or "Jitter" there is the trunk's.
- To check a thread's name: `asterisk -rx "core show threads"` (the LWP is the tid).

On `master` (Redis in Docker, no `redis-cli` on the host):

```bash
docker exec voice-agent-redis-1 redis-cli --latency-history -i 1 | tee /tmp/redis_lat.txt   # whole campaign
while true; do date +%T; docker stats --no-stream --format "{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}"; sleep 5; done | tee /tmp/master_docker.txt
vmstat -t 5 | tee /tmp/master_vmstat.txt
# after the run:
R="docker exec voice-agent-redis-1 redis-cli"
$R slowlog get 20
$R info persistence | grep -E "rdb_bgsave_in_progress|rdb_last_bgsave_status|rdb_last_bgsave_time_sec|aof_enabled"
$R info stats | grep -E "latest_fork_usec|rejected_connections"
$R config get save
```

A latency spike matching a `campaign.redis_slow` log means Redis or the box stalled; `latest_fork_usec` high or a
background save in progress at the burst points at persistence; a busy `minio`/`postgres`/`bifrost` container or `wa` in
`vmstat` points at contention on the shared box.

## 7. Open

- Which Asterisk thread is busiest at 300+ talking calls (the run reached about 150): needs the monitor log through a bigger run.
- The trunk's written limits (calls per second, channels). `SYSTEM_DESIGN.md` assumes 40 cps and 1,000 channels.
- PJSIP migration (needed before Asterisk 21/22), the dead `vaani_fs` peer, and tightening the peer settings in `sip.conf`
  (shared secrets, `insecure=invite,port`, unknown access allowed).
- Calls with missing caller audio (Nov 17, section 8): trunk or Asterisk? Needs `sip show channelstats` during a run.
- The `CUT()` dialplan error and the refcount assertion in `messages.log`: which context and object (not looked at).

## 8. Nov 17: the engine on the same network (campaign 26)

The engine moved from 192.168.36.19 (the other site, 18.5 ms through the company firewalls) to 192.168.26.88 (0.5 ms).
Full report: `CAMPAIGN_26_REPORT.md`.

| | Nov 15 (engine at the other site) | Nov 17 (engine on 26.88) |
|---|---|---|
| `res_audiosocket` "Poll timed out while waiting for data" | 241 (243 channels failed, 15:46 IST) | **0** |
| `campaign.redis_slow` in the dialer | 103 | **0** |
| Caller frames dropped by the engine | 11,850 | **1** |

Same engine media code both days, so the AudioSocket timeouts (`res_audiosocket` hangs up when a frame's payload comes
more than 5 ms after its header) and the Redis stalls came from the link between the sites, not from Asterisk or Redis.
**Keep the engine on the same network as Asterisk.**

Read on the box on Nov 17:
- `messages.log` for Nov 17 has 23 lines: three trunk-registration retries, an AudioSocket connect refused at 16:26 (the
  old engine address, before `MEDIA_IP` was fixed) and ten pairs of "Web socket closed abruptly" / "WebSocket read error",
  one per engine restart or stop (the last at 18:27:36, the hand stop). Nothing during the campaign.
- **CDRs agree with the engine**: 1,435 ANSWERED CDRs with `Stasis(vaani,outbound,...)` on `SIP/server225`, 36,481
  billable seconds (engine: 1,435 calls, 36,921 s).
- UDP receive-buffer errors still 238,917 (the same as at 15:26 Nov 15): **none** in either campaign.
- Box CPU at the peak (1-min, 176-185 talking): 1.03 of 4 cores busy, of which system 0.56 and softirq 0.28 (kernel
  networking), user 0.44.

The trunk (`sip show peer server225`): 192.168.26.225:50660, ulaw/alaw, `DirectMedia: No`, symmetric RTP, qualify 1 ms.
`sip show settings`: **jitter buffer off**, RTP timeout off, RTP keepalive off. So Asterisk passes the trunk's RTP timing
straight to the AudioSocket, and a call whose trunk sends no RTP gives the engine no caller frames (8.4% of calls got under
half their caller audio, `CAMPAIGN_26_REPORT.md`).

Old errors still in `messages.log` (none from Nov 17): about 160,000 `func_cut.c` "Syntax: CUT(...)" errors (a dialplan
`CUT()` call missing an argument), 1,630 "Excessive refcount 100000 reached" assertions on one ao2 object, 4,224 "Too many
open files" (all Jul 13), thousands of Dograh `res_websocket_client` connection failures, 938 "HTTP version not supported".

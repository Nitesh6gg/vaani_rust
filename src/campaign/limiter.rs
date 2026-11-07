//! Shared counting for outbound campaigns. The Redis keys and Lua scripts are Dograh's
//! (api/services/campaign/rate_limiter.py and circuit_breaker.py), copied as they are,
//! so a slot taken here counts for Dograh and the other way round -- an organization's
//! concurrent-call limit holds across both engines, and a crashed call's slot expires
//! on its own.

use crate::dograh::campaign::BreakerConfig;
use anyhow::{Context, Result};
use redis::{Script, aio::ConnectionManager};
use std::{
    sync::LazyLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Dograh's stale_call_timeout: a slot older than this is dropped the next time anyone
/// acquires one. A call running longer than this stops counting toward its
/// organization's limit, as in Dograh.
const STALE_SLOT_AFTER: Duration = Duration::from_secs(20 * 60);

/// Vaani's own cap on outbound calls (OUTBOUND_MAX_CALLS). Deliberately not Dograh's
/// fleet_capacity:calls, which is sized for Dograh's workers, not for this process.
const CAPACITY_KEY: &str = "vaani_capacity:calls";

/// Dograh's acquire_token: a sliding one-second window.
static RATE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window_start = tonumber(ARGV[2])
local max_requests = tonumber(ARGV[3])

-- Remove timestamps older than window
redis.call('ZREMRANGEBYSCORE', key, 0, window_start)

-- Count requests in current window
local current_requests = redis.call('ZCARD', key)

if current_requests < max_requests then
    -- Add current timestamp
    redis.call('ZADD', key, now, now)
    redis.call('EXPIRE', key, 2)  -- Expire after 2 seconds
    return 1
else
    return 0
end
",
    )
});

/// Dograh's concurrent-slot script. Dograh runs the same text on the organization's key
/// and on its fleet key; here it runs on the organization's key and on Vaani's own
/// capacity key.
static SLOT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
local key = KEYS[1]
local now = tonumber(ARGV[1])
local max_concurrent = tonumber(ARGV[2])
local stale_cutoff = tonumber(ARGV[3])
local slot_id = ARGV[4]

-- Remove stale entries (older than 30 minutes)
redis.call('ZREMRANGEBYSCORE', key, 0, stale_cutoff)

-- Get current count
local current_count = redis.call('ZCARD', key)

if current_count < max_concurrent then
    -- Add new slot
    redis.call('ZADD', key, now, slot_id)
    redis.call('EXPIRE', key, 3600)  -- Expire after 1 hour
    return slot_id
else
    return nil
end
",
    )
});

/// Dograh's record_call_outcome.
static BREAKER: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
local fail_key = KEYS[1]
local succ_key = KEYS[2]
local now = tonumber(ARGV[1])
local window_start = tonumber(ARGV[2])
local is_failure = tonumber(ARGV[3])
local threshold = tonumber(ARGV[4])
local min_calls = tonumber(ARGV[5])
local ttl = tonumber(ARGV[6])

-- Trim both sets to the sliding window
redis.call('ZREMRANGEBYSCORE', fail_key, 0, window_start)
redis.call('ZREMRANGEBYSCORE', succ_key, 0, window_start)

-- Add the new outcome to the appropriate set
if is_failure == 1 then
    redis.call('ZADD', fail_key, now, now)
else
    redis.call('ZADD', succ_key, now, now)
end

-- Refresh TTL on both keys
redis.call('EXPIRE', fail_key, ttl)
redis.call('EXPIRE', succ_key, ttl)

-- Count via ZCARD (O(1))
local failures = redis.call('ZCARD', fail_key)
local successes = redis.call('ZCARD', succ_key)
local total = failures + successes

-- Check trip condition
if total >= min_calls and (failures / total) >= threshold then
    return {1, failures, successes, total}
end

return {0, failures, successes, total}
",
    )
});

/// Dograh's is_circuit_open (no outcome recorded).
static BREAKER_CHECK: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
local fail_key = KEYS[1]
local succ_key = KEYS[2]
local window_start = tonumber(ARGV[1])
local threshold = tonumber(ARGV[2])
local min_calls = tonumber(ARGV[3])

-- Trim both sets
redis.call('ZREMRANGEBYSCORE', fail_key, 0, window_start)
redis.call('ZREMRANGEBYSCORE', succ_key, 0, window_start)

-- Count via ZCARD
local failures = redis.call('ZCARD', fail_key)
local successes = redis.call('ZCARD', succ_key)
local total = failures + successes

if total >= min_calls and (failures / total) >= threshold then
    return {1, failures, successes, total}
end

return {0, failures, successes, total}
",
    )
});

fn rate_key(org: i32) -> String {
    format!("rate_limit:{org}")
}
fn concurrent_key(org: i32) -> String {
    format!("concurrent_calls:{org}")
}
fn breaker_keys(campaign: i32) -> (String, String) {
    (format!("cb_failures:{campaign}"), format!("cb_successes:{campaign}"))
}

/// A reserved place for one call; release it when the call ends or never started.
/// The same id releases both the organization's slot and Vaani's own cap.
#[derive(Debug, Clone)]
pub struct Slot {
    pub id: String,
    pub org: i32,
}

/// The circuit breaker's sliding window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BreakerStats {
    pub failures: i64,
    pub successes: i64,
}

impl BreakerStats {
    pub fn total(&self) -> i64 {
        self.failures + self.successes
    }
}

/// Counts outbound calls in Redis.
#[derive(Clone)]
pub struct Limiter {
    rdb: ConnectionManager,
    /// Vaani's own cap on calls at once; 0 = only the organizations' limits apply.
    max_calls: u32,
    #[cfg(test)]
    skew_ms: std::sync::Arc<std::sync::atomic::AtomicI64>,
}

impl Limiter {
    pub async fn connect(url: &str, max_calls: u32) -> Result<Self> {
        let client = redis::Client::open(url).context("campaign: redis url")?;
        let rdb = ConnectionManager::new(client).await.context("campaign: connect to redis")?;
        Ok(Self {
            rdb,
            max_calls,
            #[cfg(test)]
            skew_ms: Default::default(),
        })
    }

    /// Seconds since the epoch, as Redis scores.
    fn now(&self) -> f64 {
        #[allow(unused_mut)]
        let mut t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64();
        #[cfg(test)]
        {
            t += self.skew_ms.load(std::sync::atomic::Ordering::Relaxed) as f64 / 1000.0;
        }
        t
    }

    /// Takes one of the organization's `per_second` call starts in the current one-second
    /// window; `false` means wait (see `next_token_wait`). An error counts as a refusal, as in Dograh.
    pub async fn acquire_token(&self, org: i32, per_second: i32) -> Result<bool> {
        let now = self.now();
        let n: i64 = RATE
            .key(rate_key(org))
            .arg(now)
            .arg(now - 1.0)
            .arg(per_second)
            .invoke_async(&mut self.rdb.clone())
            .await
            .context("campaign: rate limit")?;
        Ok(n == 1)
    }

    /// How long until the organization's window has room again.
    pub async fn next_token_wait(&self, org: i32) -> Duration {
        let oldest: Result<Vec<(String, f64)>, _> =
            redis::cmd("ZRANGE").arg(rate_key(org)).arg(0).arg(0).arg("WITHSCORES").query_async(&mut self.rdb.clone()).await;
        match oldest {
            Ok(v) => v.first().map_or(Duration::ZERO, |(_, score)| Duration::from_secs_f64((score + 1.0 - self.now()).max(0.0))),
            Err(_) => Duration::from_secs(1),
        }
    }

    /// Reserves a concurrent-call slot for `org`, up to `limit` calls at once across both
    /// engines, and under Vaani's own cap. `None` when either is full.
    pub async fn acquire_slot(&self, org: i32, limit: i32) -> Result<Option<Slot>> {
        let now = self.now();
        let id = format!("{}_{:08x}", (now * 1000.0) as u64, rand::random::<u32>());
        let cutoff = now - STALE_SLOT_AFTER.as_secs_f64();

        // Own cap first: no point holding an organization slot without capacity.
        if self.max_calls > 0 && !self.take_slot(CAPACITY_KEY, now, self.max_calls as i32, cutoff, &id).await? {
            return Ok(None);
        }
        if !self.take_slot(&concurrent_key(org), now, limit, cutoff, &id).await? {
            self.drop_capacity(&id).await;
            return Ok(None);
        }
        Ok(Some(Slot { id, org }))
    }

    async fn take_slot(&self, key: &str, now: f64, limit: i32, cutoff: f64, id: &str) -> Result<bool> {
        let got: Option<String> = SLOT
            .key(key)
            .arg(now)
            .arg(limit)
            .arg(cutoff)
            .arg(id)
            .invoke_async(&mut self.rdb.clone())
            .await
            .with_context(|| format!("campaign: slot on {key}"))?;
        Ok(got.is_some())
    }

    async fn drop_capacity(&self, id: &str) {
        if self.max_calls > 0 {
            let _: Result<i64, _> = redis::cmd("ZREM").arg(CAPACITY_KEY).arg(id).query_async(&mut self.rdb.clone()).await;
        }
    }

    /// Gives the slot back (the organization's and Vaani's cap). Releasing twice, or a slot that
    /// already expired, is not an error.
    pub async fn release_slot(&self, s: &Slot) -> Result<()> {
        self.drop_capacity(&s.id).await;
        let _: i64 = redis::cmd("ZREM")
            .arg(concurrent_key(s.org))
            .arg(&s.id)
            .query_async(&mut self.rdb.clone())
            .await
            .with_context(|| format!("campaign: release slot {}", s.id))?;
        Ok(())
    }

    /// How many calls the organization has in flight, both engines together (stale slots included
    /// until the next acquire trims them).
    #[cfg(test)]
    pub async fn active_slots(&self, org: i32) -> Result<i64> {
        redis::cmd("ZCARD").arg(concurrent_key(org)).query_async(&mut self.rdb.clone()).await.context("campaign: active slots")
    }

    /// Adds a finished call to the campaign's window and says whether the breaker is now tripped.
    /// Like Dograh it fails open: a Redis error never trips it.
    pub async fn record_outcome(&self, campaign: i32, failure: bool, cfg: &BreakerConfig) -> Result<(bool, BreakerStats)> {
        if !cfg.enabled {
            return Ok((false, BreakerStats::default()));
        }
        let (fail_key, succ_key) = breaker_keys(campaign);
        let now = self.now();
        let res: Vec<i64> = BREAKER
            .key(fail_key)
            .key(succ_key)
            .arg(now)
            .arg(now - cfg.window.as_secs_f64())
            .arg(i32::from(failure))
            .arg(cfg.threshold)
            .arg(cfg.min_calls)
            .arg(cfg.window.as_secs() + 60)
            .invoke_async(&mut self.rdb.clone())
            .await
            .context("campaign: circuit breaker")?;
        breaker_result(&res)
    }

    /// Whether the campaign's breaker is tripped, without recording anything.
    pub async fn breaker_open(&self, campaign: i32, cfg: &BreakerConfig) -> Result<(bool, BreakerStats)> {
        if !cfg.enabled {
            return Ok((false, BreakerStats::default()));
        }
        let (fail_key, succ_key) = breaker_keys(campaign);
        let res: Vec<i64> = BREAKER_CHECK
            .key(fail_key)
            .key(succ_key)
            .arg(self.now() - cfg.window.as_secs_f64())
            .arg(cfg.threshold)
            .arg(cfg.min_calls)
            .invoke_async(&mut self.rdb.clone())
            .await
            .context("campaign: circuit breaker")?;
        breaker_result(&res)
    }

    /// Clears the campaign's window. Dograh does this itself on resume; here it's for tests.
    #[cfg(test)]
    pub async fn reset_breaker(&self, campaign: i32) -> Result<()> {
        let (f, s) = breaker_keys(campaign);
        let _: i64 = redis::cmd("DEL").arg(f).arg(s).query_async(&mut self.rdb.clone()).await.context("campaign: reset breaker")?;
        Ok(())
    }

    /// Moves the limiter's clock forward (tests of windows and stale slots).
    #[cfg(test)]
    pub fn advance(&self, d: Duration) {
        self.skew_ms.fetch_add(d.as_millis() as i64, std::sync::atomic::Ordering::Relaxed);
    }
}

fn breaker_result(res: &[i64]) -> Result<(bool, BreakerStats)> {
    match res {
        [tripped, failures, successes, _] => Ok((*tripped == 1, BreakerStats { failures: *failures, successes: *successes })),
        other => anyhow::bail!("campaign: circuit breaker: unexpected reply {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A limiter on the Redis named by `REDIS_TEST_URL` (these tests skip without it), and
    /// organization/campaign ids no other test run uses, so nothing needs flushing.
    async fn limiter(max_calls: u32) -> Option<(Limiter, i32)> {
        let url = std::env::var("REDIS_TEST_URL").ok()?;
        Some((Limiter::connect(&url, max_calls).await.unwrap(), (rand::random::<u32>() >> 2) as i32))
    }

    fn cfg(threshold: f64, min_calls: i64) -> BreakerConfig {
        BreakerConfig { enabled: true, threshold, window: Duration::from_secs(60), min_calls }
    }

    #[tokio::test]
    async fn the_rate_window_admits_n_starts_a_second() {
        let Some((l, org)) = limiter(0).await else { return eprintln!("skipped: REDIS_TEST_URL not set") };
        assert!(l.acquire_token(org, 2).await.unwrap() && l.acquire_token(org, 2).await.unwrap());
        assert!(!l.acquire_token(org, 2).await.unwrap(), "a third start in the same second waits");
        let wait = l.next_token_wait(org).await;
        assert!(wait > Duration::ZERO && wait <= Duration::from_secs(1), "{wait:?}");
        l.advance(Duration::from_millis(1100));
        assert!(l.acquire_token(org, 2).await.unwrap(), "the window moved on");
        assert_eq!(l.next_token_wait(org + 1).await, Duration::ZERO, "an untouched organization never waits");
    }

    #[tokio::test]
    async fn slots_are_limited_per_organization_and_by_vaanis_own_cap() {
        let Some((l, org)) = limiter(0).await else { return eprintln!("skipped: REDIS_TEST_URL not set") };
        let a = l.acquire_slot(org, 2).await.unwrap().expect("first");
        let b = l.acquire_slot(org, 2).await.unwrap().expect("second");
        assert!(l.acquire_slot(org, 2).await.unwrap().is_none(), "the organization's limit is 2");
        assert_eq!(l.active_slots(org).await.unwrap(), 2);
        assert!(l.acquire_slot(org + 1, 1).await.unwrap().is_some(), "another organization has its own");
        l.release_slot(&a).await.unwrap();
        l.release_slot(&a).await.unwrap(); // releasing twice is not an error
        assert_eq!(l.active_slots(org).await.unwrap(), 1);
        let c = l.acquire_slot(org, 2).await.unwrap().expect("a freed slot is reusable");
        assert_ne!(b.id, c.id);

        // A crashed call's slot expires on its own after 20 minutes.
        l.advance(Duration::from_secs(21 * 60));
        let d = l.acquire_slot(org, 2).await.unwrap();
        assert!(d.is_some(), "stale slots are dropped by the next acquire");
        assert_eq!(l.active_slots(org).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn vaanis_own_cap_holds_across_organizations_and_is_given_back() {
        let Some((l, org)) = limiter(2).await else { return eprintln!("skipped: REDIS_TEST_URL not set") };
        // The cap key is shared by every test using one: start from a clean slate.
        let _: i64 = redis::cmd("DEL").arg(CAPACITY_KEY).query_async(&mut l.rdb.clone()).await.unwrap();
        let a = l.acquire_slot(org, 10).await.unwrap().expect("1 of 2");
        let _b = l.acquire_slot(org + 1, 10).await.unwrap().expect("2 of 2");
        assert!(l.acquire_slot(org + 2, 10).await.unwrap().is_none(), "Vaani's cap of 2 is full");
        assert_eq!(l.active_slots(org + 2).await.unwrap(), 0, "a refused call holds no organization slot");
        l.release_slot(&a).await.unwrap();
        assert!(l.acquire_slot(org + 2, 10).await.unwrap().is_some(), "releasing gave the capacity back");
        // An organization that is full doesn't leak capacity either.
        let _: i64 = redis::cmd("DEL").arg(CAPACITY_KEY).query_async(&mut l.rdb.clone()).await.unwrap();
        let _x = l.acquire_slot(org + 3, 1).await.unwrap().unwrap();
        assert!(l.acquire_slot(org + 3, 1).await.unwrap().is_none());
        assert!(
            l.acquire_slot(org + 4, 1).await.unwrap().is_some() && l.acquire_slot(org + 5, 1).await.unwrap().is_none(),
            "2 slots in use, not 3"
        );
        let _: i64 = redis::cmd("DEL").arg(CAPACITY_KEY).query_async(&mut l.rdb.clone()).await.unwrap();
    }

    #[tokio::test]
    async fn the_breaker_trips_on_a_failure_rate_and_resets() {
        let Some((l, c)) = limiter(0).await else { return eprintln!("skipped: REDIS_TEST_URL not set") };
        let cfg = cfg(0.5, 4);
        for failure in [true, false, true] {
            let (tripped, _) = l.record_outcome(c, failure, &cfg).await.unwrap();
            assert!(!tripped, "under min_calls it never trips");
        }
        let (tripped, stats) = l.record_outcome(c, true, &cfg).await.unwrap();
        assert!(tripped, "3 of 4 failed");
        assert_eq!((stats.failures, stats.successes, stats.total()), (3, 1, 4));
        assert!(l.breaker_open(c, &cfg).await.unwrap().0, "checking records nothing and stays tripped");
        l.reset_breaker(c).await.unwrap();
        assert_eq!(l.breaker_open(c, &cfg).await.unwrap(), (false, BreakerStats::default()));

        // Outcomes older than the window don't count.
        l.record_outcome(c, true, &cfg).await.unwrap();
        l.advance(Duration::from_secs(61));
        assert_eq!(l.breaker_open(c, &cfg).await.unwrap().1.total(), 0);
        let off = BreakerConfig { enabled: false, ..cfg };
        assert_eq!(l.record_outcome(c, true, &off).await.unwrap(), (false, BreakerStats::default()), "a disabled breaker records nothing");
    }
}

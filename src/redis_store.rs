/// Redis-backed sliding window rate limiter.
///
/// Uses a sorted set per key:
///   - Score  = request timestamp (µs)
///   - Member = unique request ID (timestamp + counter)
///
/// All state mutations run inside a single Lua script for atomicity
/// without requiring MULTI/EXEC transactions.
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::atomic::{AtomicU64, Ordering};
use async_trait::async_trait;
use deadpool_redis::Pool;
use redis::Script;
use tracing::debug;

use crate::error::{Result, RateLimiterError};
use crate::limiter::{Decision, RateLimiter};

// Lua script: atomic sliding-window check + record.
//
// KEYS[1]  = rate limit key
// ARGV[1]  = now_us        (current time, microseconds)
// ARGV[2]  = window_us     (window size, microseconds)
// ARGV[3]  = limit         (max requests)
// ARGV[4]  = member        (unique ID for this request)
// ARGV[5]  = ttl_ms        (key TTL in milliseconds)
//
// Returns: { count, oldest_score_or_0 }
//   count         = number of requests in window AFTER adding this one (if allowed)
//   oldest_score  = score of the oldest entry (0 when allowed with headroom)
const SLIDING_WINDOW_LUA: &str = r#"
local key       = KEYS[1]
local now       = tonumber(ARGV[1])
local window    = tonumber(ARGV[2])
local limit     = tonumber(ARGV[3])
local member    = ARGV[4]
local ttl_ms    = tonumber(ARGV[5])
local cutoff    = now - window

-- Remove expired entries
redis.call('ZREMRANGEBYSCORE', key, '-inf', cutoff)

-- Count current window
local count = redis.call('ZCARD', key)

if count < limit then
    -- Allow: record this request
    redis.call('ZADD', key, now, member)
    redis.call('PEXPIRE', key, ttl_ms)
    return {count + 1, 0}
else
    -- Denied: return oldest entry score so caller can compute retry-after
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local oldest_score = 0
    if #oldest > 0 then
        oldest_score = tonumber(oldest[2])
    end
    return {count, oldest_score}
end
"#;

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_member(now_us: u64) -> String {
    let seq = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{now_us}-{seq}")
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_micros() as u64
}

/// A pool-backed, fully async Redis rate limiter.
pub struct RedisRateLimiter {
    pool: Pool,
    script: Script,
}

impl RedisRateLimiter {
    /// Connect to Redis at `url` (e.g. `"redis://127.0.0.1/"`) with `pool_size` connections.
    pub fn new(url: &str, pool_size: usize) -> Result<Self> {
        let pool = deadpool_redis::Config::from_url(url)
            .builder()
            .map_err(|e| redis::RedisError::from((redis::ErrorKind::IoError, "pool build failed", e.to_string())))?
            .max_size(pool_size)
            .build()
            .map_err(|e| redis::RedisError::from((redis::ErrorKind::IoError, "pool build failed", e.to_string())))?;

        Ok(Self {
            pool,
            script: Script::new(SLIDING_WINDOW_LUA),
        })
    }

    /// Build from an already-created `deadpool_redis::Pool`.
    pub fn from_pool(pool: Pool) -> Self {
        Self {
            pool,
            script: Script::new(SLIDING_WINDOW_LUA),
        }
    }
}

#[async_trait]
impl RateLimiter for RedisRateLimiter {
    async fn check(&self, key: &str, limit: u64, window_ms: u64) -> Result<Decision> {
        let mut conn = self.pool.get().await?;
        let now = now_us();
        let window_us = window_ms * 1_000;
        let member = unique_member(now);
        // TTL = window + small grace so Redis doesn't evict too early
        let ttl_ms = window_ms + 1_000;

        let result: Vec<u64> = self
            .script
            .key(key)
            .arg(now)
            .arg(window_us)
            .arg(limit)
            .arg(&member)
            .arg(ttl_ms)
            .invoke_async(&mut *conn)
            .await?;

        let count = result[0];
        // oldest_us == 0  → request was recorded (allowed)
        // oldest_us > 0   → limit hit, request NOT recorded (oldest entry score returned)
        let oldest_us = result[1];

        debug!(key, count, limit, oldest_us, "rate limit check");

        if oldest_us == 0 {
            Ok(Decision::allowed(count, limit, 0))
        } else {
            let expires_at_us = oldest_us + window_us;
            let retry_after_ms = expires_at_us.saturating_sub(now) / 1_000;
            Err(RateLimiterError::Limited { retry_after_ms })
        }
    }

    async fn reset(&self, key: &str) -> Result<()> {
        let mut conn = self.pool.get().await?;
        redis::cmd("DEL")
            .arg(key)
            .query_async::<()>(&mut *conn)
            .await?;
        Ok(())
    }
}

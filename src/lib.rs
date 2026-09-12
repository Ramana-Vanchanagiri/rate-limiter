pub mod error;
pub mod limiter;
pub mod local_cache;
pub mod middleware;
pub mod redis_store;

pub use error::{RateLimiterError, Result};
pub use limiter::{Decision, RateLimiter};
pub use local_cache::LocalCachedLimiter;
pub use redis_store::RedisRateLimiter;

/// Convenience builder.
pub struct Builder {
    redis_url: String,
    pool_size: usize,
    batch_size: Option<u64>,
}

impl Builder {
    pub fn new(redis_url: impl Into<String>) -> Self {
        Self {
            redis_url: redis_url.into(),
            pool_size: 32,
            batch_size: None,
        }
    }

    /// Number of Redis connections in the pool.
    pub fn pool_size(mut self, n: usize) -> Self {
        self.pool_size = n;
        self
    }

    /// Enable the local-cache layer. `batch_size` tokens are reserved from
    /// Redis per cache fill. Set to 0 to disable the local cache.
    pub fn with_local_cache(mut self, batch_size: u64) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Build a Redis-backed limiter (no local cache).
    pub fn build_redis(self) -> Result<RedisRateLimiter> {
        RedisRateLimiter::new(&self.redis_url, self.pool_size)
    }

    /// Build a Redis-backed limiter wrapped with a local cache.
    pub fn build_cached(self) -> Result<LocalCachedLimiter<RedisRateLimiter>> {
        let batch = self.batch_size.unwrap_or(100);
        let redis = RedisRateLimiter::new(&self.redis_url, self.pool_size)?;
        Ok(LocalCachedLimiter::new(redis, batch))
    }
}

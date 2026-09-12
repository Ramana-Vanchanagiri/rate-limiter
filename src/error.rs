use thiserror::Error;

#[derive(Debug, Error)]
pub enum RateLimiterError {
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Pool error: {0}")]
    Pool(#[from] deadpool_redis::PoolError),

    #[error("Rate limit exceeded: retry after {retry_after_ms}ms")]
    Limited { retry_after_ms: u64 },
}

pub type Result<T> = std::result::Result<T, RateLimiterError>;

use async_trait::async_trait;
use crate::error::Result;

/// Decision returned by the rate limiter.
#[derive(Debug, Clone)]
pub struct Decision {
    /// Whether the request is allowed.
    pub allowed: bool,
    /// Current count within the window.
    pub count: u64,
    /// Maximum requests allowed per window.
    pub limit: u64,
    /// Remaining requests before hitting the limit.
    pub remaining: u64,
    /// Milliseconds until the oldest request leaves the window.
    /// Zero when `allowed` is true and there's headroom.
    pub retry_after_ms: u64,
}

impl Decision {
    pub fn allowed(count: u64, limit: u64, retry_after_ms: u64) -> Self {
        Self {
            allowed: true,
            count,
            limit,
            remaining: limit.saturating_sub(count),
            retry_after_ms,
        }
    }

    pub fn denied(count: u64, limit: u64, retry_after_ms: u64) -> Self {
        Self {
            allowed: false,
            count,
            limit,
            remaining: 0,
            retry_after_ms,
        }
    }
}

/// Core trait — implement this to plug in any backend.
#[async_trait]
pub trait RateLimiter: Send + Sync {
    /// Check and record a request for `key`.
    /// `limit`  — max requests in the window.
    /// `window_ms` — sliding window size in milliseconds.
    async fn check(&self, key: &str, limit: u64, window_ms: u64) -> Result<Decision>;

    /// Reset all counters for `key` (e.g. after a successful captcha).
    async fn reset(&self, key: &str) -> Result<()>;
}

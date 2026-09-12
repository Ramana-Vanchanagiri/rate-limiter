/// Axum middleware that enforces rate limits per IP (or any custom key extractor).
///
/// Adds standard rate-limit headers to every response:
///   X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset,
///   Retry-After (on 429 responses).
#[cfg(feature = "axum")]
pub mod axum_middleware {
    use std::sync::Arc;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use axum::{
        body::Body,
        extract::ConnectInfo,
        http::{HeaderName, HeaderValue, Request, Response, StatusCode},
    };
    use tower::{Layer, Service};
    use std::net::SocketAddr;

    use crate::error::RateLimiterError;
    use crate::limiter::RateLimiter;

    pub type KeyExtractor = Arc<dyn Fn(&Request<Body>) -> String + Send + Sync>;

    /// Configuration for the rate limit middleware.
    #[derive(Clone)]
    pub struct RateLimitConfig {
        pub limit: u64,
        pub window_ms: u64,
        pub key_extractor: KeyExtractor,
    }

    impl RateLimitConfig {
        /// Rate limit by client IP address.
        pub fn by_ip(limit: u64, window_ms: u64) -> Self {
            Self {
                limit,
                window_ms,
                key_extractor: Arc::new(|req| {
                    // Try X-Forwarded-For first (behind a proxy)
                    if let Some(xff) = req.headers().get("x-forwarded-for") {
                        if let Ok(v) = xff.to_str() {
                            return format!("rl:{}", v.split(',').next().unwrap_or("unknown").trim());
                        }
                    }
                    // Fall back to connection IP
                    req.extensions()
                        .get::<ConnectInfo<SocketAddr>>()
                        .map(|ci| format!("rl:{}", ci.0.ip()))
                        .unwrap_or_else(|| "rl:unknown".to_string())
                }),
            }
        }

        /// Rate limit by a custom header value (e.g. `Authorization` or `X-API-Key`).
        pub fn by_header(limit: u64, window_ms: u64, header: &'static str) -> Self {
            Self {
                limit,
                window_ms,
                key_extractor: Arc::new(move |req| {
                    req.headers()
                        .get(header)
                        .and_then(|v| v.to_str().ok())
                        .map(|v| format!("rl:{header}:{v}"))
                        .unwrap_or_else(|| "rl:anonymous".to_string())
                }),
            }
        }
    }

    /// Tower layer that injects rate limiting into an Axum router.
    pub struct RateLimitLayer<L> {
        limiter: Arc<L>,
        config: RateLimitConfig,
    }

    impl<L> Clone for RateLimitLayer<L> {
        fn clone(&self) -> Self {
            Self {
                limiter: Arc::clone(&self.limiter),
                config: self.config.clone(),
            }
        }
    }

    impl<L: RateLimiter + 'static> RateLimitLayer<L> {
        pub fn new(limiter: L, config: RateLimitConfig) -> Self {
            Self {
                limiter: Arc::new(limiter),
                config,
            }
        }
    }

    impl<L, S> Layer<S> for RateLimitLayer<L>
    where
        L: RateLimiter + 'static,
    {
        type Service = RateLimitService<L, S>;

        fn layer(&self, inner: S) -> Self::Service {
            RateLimitService {
                inner,
                limiter: Arc::clone(&self.limiter),
                config: self.config.clone(),
            }
        }
    }

    pub struct RateLimitService<L, S> {
        inner: S,
        limiter: Arc<L>,
        config: RateLimitConfig,
    }

    impl<L, S: Clone> Clone for RateLimitService<L, S> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
                limiter: Arc::clone(&self.limiter),
                config: self.config.clone(),
            }
        }
    }

    impl<L, S> Service<Request<Body>> for RateLimitService<L, S>
    where
        L: RateLimiter + 'static,
        S: Service<Request<Body>, Response = Response<Body>, Error = std::convert::Infallible>
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
    {
        type Response = Response<Body>;
        type Error = std::convert::Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.inner.poll_ready(cx)
        }

        fn call(&mut self, req: Request<Body>) -> Self::Future {
            let key = (self.config.key_extractor)(&req);
            let limit = self.config.limit;
            let window_ms = self.config.window_ms;
            let limiter = Arc::clone(&self.limiter);
            let mut inner = self.inner.clone();

            Box::pin(async move {
                match limiter.check(&key, limit, window_ms).await {
                    Ok(decision) => {
                        let mut resp = inner.call(req).await?;
                        let headers = resp.headers_mut();
                        set_headers(headers, decision.count, decision.limit, decision.remaining, window_ms);
                        Ok(resp)
                    }
                    Err(RateLimiterError::Limited { retry_after_ms }) => {
                        let mut resp = Response::builder()
                            .status(StatusCode::TOO_MANY_REQUESTS)
                            .header("content-type", "application/json")
                            .body(Body::from(format!(
                                r#"{{"error":"rate_limit_exceeded","retry_after_ms":{retry_after_ms}}}"#
                            )))
                            .unwrap();
                        let headers = resp.headers_mut();
                        set_headers(headers, limit, limit, 0, window_ms);
                        headers.insert(
                            HeaderName::from_static("retry-after"),
                            HeaderValue::from_str(&(retry_after_ms / 1000).to_string()).unwrap(),
                        );
                        Ok(resp)
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "rate limiter backend error");
                        // Fail open: allow the request if the backend is down
                        inner.call(req).await
                    }
                }
            })
        }
    }

    fn set_headers(
        headers: &mut axum::http::HeaderMap,
        count: u64,
        limit: u64,
        remaining: u64,
        window_ms: u64,
    ) {
        let reset_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + window_ms / 1000;

        let _ = headers.insert("x-ratelimit-limit", HeaderValue::from_str(&limit.to_string()).unwrap());
        let _ = headers.insert("x-ratelimit-remaining", HeaderValue::from_str(&remaining.to_string()).unwrap());
        let _ = headers.insert("x-ratelimit-reset", HeaderValue::from_str(&reset_epoch.to_string()).unwrap());
        let _ = headers.insert("x-ratelimit-used", HeaderValue::from_str(&count.to_string()).unwrap());
    }
}

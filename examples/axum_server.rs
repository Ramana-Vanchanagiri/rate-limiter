//! Example: Axum HTTP server with per-IP rate limiting.
//!
//! Run with:
//!   REDIS_URL=redis://127.0.0.1/ cargo run --example axum_server --features axum
//!
//! Test:
//!   for i in $(seq 1 15); do curl -s -o /dev/null -w "%{http_code}\n" http://localhost:3000/; done

use std::net::SocketAddr;
use axum::{routing::get, Router};
use rate_limiter::{Builder, middleware::axum_middleware::{RateLimitConfig, RateLimitLayer}};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rate_limiter=debug,info".parse().unwrap()),
        )
        .init();

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());

    // Direct Redis limiter — strict per-request enforcement.
    // For ultra-high throughput (>100k req/s), use build_cached() with a
    // batch_size that is a small fraction of the limit (e.g. limit=10000, batch=50).
    let limiter = Builder::new(&redis_url)
        .pool_size(32)
        .build_redis()
        .expect("failed to connect to Redis");

    // 10 requests per 60 seconds per IP.
    let rate_limit = RateLimitLayer::new(
        limiter,
        RateLimitConfig::by_ip(10, 60_000),
    );

    let app = Router::new()
        .route("/", get(handler))
        .layer(rate_limit);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    println!("Listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
        .await
        .unwrap();
}

async fn handler() -> &'static str {
    "Hello, world!"
}

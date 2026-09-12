# rate-limiter

A high-throughput, distributed sliding window rate limiter written in Rust, backed by Redis.

## Features

- **Sliding window algorithm** — no burst spikes at window boundaries
- **Atomic enforcement** — single Lua script per request, no race conditions
- **Distributed** — shared state across all instances via Redis sorted sets
- **Local cache layer** — optional in-process token batching for ultra-high throughput
- **Axum middleware** — plug-and-play Tower layer with standard rate-limit headers
- **Fail open** — if Redis is unreachable, requests pass through (configurable)

## How It Works

Each key maps to a Redis sorted set where:
- **Score** = request timestamp (microseconds)
- **Member** = unique request ID

On every request, a single Lua script atomically:
1. Removes entries older than the window
2. Counts remaining entries
3. Allows (adds entry) or denies (returns oldest entry for `Retry-After` calculation)

```
Request → Lua script (atomic) → Allow / 429
              ↓
         Redis sorted set
         [ts-1, ts-2, ts-3, ...]
```

## Architecture

```
┌─────────────────────────────────────────┐
│              Your Axum App              │
├─────────────────────────────────────────┤
│         RateLimitLayer (Tower)          │  ← adds X-RateLimit-* headers
├─────────────────────────────────────────┤
│     LocalCachedLimiter  (optional)      │  ← serves from in-process tokens
├─────────────────────────────────────────┤
│         RedisRateLimiter                │  ← deadpool connection pool
├─────────────────────────────────────────┤
│    Redis  (sorted set + Lua script)     │  ← atomic sliding window
└─────────────────────────────────────────┘
```

## Quickstart

### Prerequisites

- Rust 1.75+
- Redis 6+

### Add to your project

```toml
[dependencies]
rate-limiter = { path = ".", features = ["axum"] }
```

### Axum middleware

```rust
use rate_limiter::{Builder, middleware::axum_middleware::{RateLimitConfig, RateLimitLayer}};

let limiter = Builder::new("redis://127.0.0.1/")
    .pool_size(32)
    .build_redis()?;

// 100 requests per minute, per IP
let layer = RateLimitLayer::new(
    limiter,
    RateLimitConfig::by_ip(100, 60_000),
);

let app = Router::new()
    .route("/", get(handler))
    .layer(layer);
```

### Rate limit by API key header

```rust
RateLimitConfig::by_header(1000, 60_000, "x-api-key")
```

### Use the limiter directly

```rust
use rate_limiter::{Builder, RateLimiter};

let limiter = Builder::new("redis://127.0.0.1/")
    .pool_size(32)
    .build_redis()?;

match limiter.check("user:42", 100, 60_000).await {
    Ok(decision) => println!("allowed, {} remaining", decision.remaining),
    Err(e) => println!("rate limited: {e}"),
}
```

## Configuration

| Builder method | Default | Description |
|---|---|---|
| `pool_size(n)` | 32 | Redis connection pool size |
| `with_local_cache(batch)` | off | Enable local token batching |
| `build_redis()` | — | Strict per-request Redis enforcement |
| `build_cached()` | — | Redis + local cache (higher throughput) |

### Local cache

For very high traffic (>100k req/s), the local cache pre-reserves a batch of tokens from Redis and serves subsequent requests from memory:

```rust
let limiter = Builder::new("redis://127.0.0.1/")
    .pool_size(64)
    .with_local_cache(50)   // reserve 50 tokens per batch
    .build_cached()?;
```

**Trade-off:** a node can over-allow by at most `batch_size` requests before Redis reflects the true count. Keep `batch_size` to ~0.5% of your limit (e.g. limit=10000, batch=50).

## Response Headers

Every response includes:

| Header | Description |
|---|---|
| `X-RateLimit-Limit` | Max requests in the window |
| `X-RateLimit-Remaining` | Requests left |
| `X-RateLimit-Used` | Requests consumed |
| `X-RateLimit-Reset` | Unix timestamp when window resets |
| `Retry-After` | Seconds to wait (429 responses only) |

## Running the Example

```bash
# Start Redis
redis-server

# Run the Axum server (10 req/60s per IP)
REDIS_URL=redis://127.0.0.1/ cargo run --example axum_server --features axum

# Test it
for i in $(seq 1 15); do
  curl -s -o /dev/null -w "req $i: %{http_code}\n" http://localhost:3000/
done
```

Expected output:
```
req 1:  200
...
req 10: 200
req 11: 429   ← rate limited
req 12: 429
...
```

## Benchmarks

```bash
# Requires Redis on localhost
cargo bench --bench throughput
```

| Mode | Throughput (single core) |
|---|---|
| Redis direct | ~80,000 req/s |
| Local cache (batch=100) | ~2,000,000 req/s |

## License

MIT

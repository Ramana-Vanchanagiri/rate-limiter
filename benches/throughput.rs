//! Benchmark: raw check() throughput against a local Redis.
//!
//! Run: cargo bench --bench throughput
//!
//! Requires Redis running at redis://127.0.0.1/

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rate_limiter::{Builder, RateLimiter};
use tokio::runtime::Runtime;

fn bench_redis_direct(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let limiter = Builder::new("redis://127.0.0.1/")
        .pool_size(64)
        .build_redis()
        .expect("Redis not running — start Redis before benchmarking");

    // Warm up
    rt.block_on(async { limiter.reset("bench:direct").await.unwrap() });

    let mut group = c.benchmark_group("redis_direct");
    group.throughput(Throughput::Elements(1));

    group.bench_function("check_allow", |b| {
        b.to_async(&rt).iter(|| async {
            limiter.check("bench:direct", 1_000_000, 60_000).await.unwrap()
        });
    });

    group.finish();
}

fn bench_local_cache(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let limiter = Builder::new("redis://127.0.0.1/")
        .pool_size(64)
        .with_local_cache(500)
        .build_cached()
        .expect("Redis not running");

    rt.block_on(async { limiter.reset("bench:cached").await.unwrap() });

    let mut group = c.benchmark_group("local_cache");
    group.throughput(Throughput::Elements(1));

    group.bench_function("check_allow", |b| {
        b.to_async(&rt).iter(|| async {
            limiter.check("bench:cached", 1_000_000, 60_000).await.unwrap()
        });
    });

    // Concurrent throughput
    for concurrency in [4u64, 16, 64] {
        group.bench_with_input(
            BenchmarkId::new("concurrent", concurrency),
            &concurrency,
            |b, &n| {
                b.to_async(&rt).iter(|| async move {
                    let limiter = Builder::new("redis://127.0.0.1/")
                        .pool_size(64)
                        .with_local_cache(500)
                        .build_cached()
                        .unwrap();
                    let handles: Vec<_> = (0..n)
                        .map(|_| {
                            let l = std::sync::Arc::new(
                                Builder::new("redis://127.0.0.1/")
                                    .pool_size(64)
                                    .with_local_cache(500)
                                    .build_cached()
                                    .unwrap(),
                            );
                            tokio::spawn(async move {
                                l.check("bench:concurrent", 1_000_000, 60_000).await.unwrap()
                            })
                        })
                        .collect();
                    for h in handles {
                        h.await.unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_redis_direct, bench_local_cache);
criterion_main!(benches);

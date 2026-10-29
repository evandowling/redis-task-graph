//! A long-running mixed workload that exports metrics over OTLP, for watching the
//! redis-task-graph dashboard fill in.
//!
//! ```text
//! powershell -ExecutionPolicy Bypass -File monitoring/start-openobserve.ps1
//! # set the two OTEL_EXPORTER_OTLP_* variables it prints, then:
//! cargo run --release --example monitored --features otel -- [minutes, default 10]
//! ```
//!
//! The workload mixes I/O-bound tasks, CPU work in `spawn_blocking`, a flaky processor that
//! retries and dead-letters, three-step chains, periodic bursts, and every 90 s a burst of tasks
//! that block the runtime, so pushback shows up as a dip in the concurrency limit.

use std::time::Duration;

use opentelemetry_otlp::MetricExporter;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use redis_task_graph::{Registry, TaskQueue, TaskType, Worker};

const PARSE: TaskType<String, Vec<u32>> = TaskType::new("parse");
const ENRICH: TaskType<Vec<u32>, u64> = TaskType::new("enrich");
const STORE: TaskType<u64, ()> = TaskType::new("store");

/// Spins the CPU for about `ms` milliseconds.
fn burn(ms: u64) {
    let until = std::time::Instant::now() + Duration::from_millis(ms);
    let mut x = 1u64;
    while std::time::Instant::now() < until {
        x = std::hint::black_box(x.wrapping_mul(6364136223846793005).wrapping_add(1));
    }
}

async fn io(ms_lo: u64, ms_hi: u64) {
    tokio::time::sleep(Duration::from_millis(rand::random_range(ms_lo..=ms_hi))).await;
}

fn registry() -> Registry {
    let mut reg = Registry::new();
    reg.register("fetch", |_: u64| async {
        io(10, 60).await;
        Ok(())
    })
    .unwrap()
    .register("resize", |_: u64| async {
        tokio::task::spawn_blocking(|| burn(5)).await?;
        Ok(())
    })
    .unwrap()
    .register("flaky", |_: u64| async {
        io(5, 20).await;
        if rand::random_bool(0.3) { Err("upstream timed out".into()) } else { Ok(()) }
    })
    .unwrap()
    // Deliberately blocks a runtime thread: the case pushback exists for.
    .register("hog", |_: u64| async {
        burn(250);
        Ok(())
    })
    .unwrap()
    .register(PARSE, |s: String| async move {
        io(2, 10).await;
        Ok(s.bytes().map(u32::from).collect())
    })
    .unwrap()
    .register(ENRICH, |v: Vec<u32>| async move {
        io(10, 40).await;
        Ok(v.iter().map(|&n| n as u64).sum())
    })
    .unwrap()
    .register(STORE, |_: u64| async {
        io(5, 15).await;
        Ok(())
    })
    .unwrap();
    reg
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Retries are expected in this workload; only show errors.
    tracing_subscriber::fmt().with_env_filter("error").init();
    let minutes: u64 = std::env::args().nth(1).and_then(|a| a.parse().ok()).unwrap_or(10);

    // Endpoint and auth headers come from OTEL_EXPORTER_OTLP_ENDPOINT / _HEADERS.
    let exporter = MetricExporter::builder().with_http().build()?;
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter).with_interval(Duration::from_secs(5)).build())
        .with_resource(Resource::builder().with_service_name("redis-task-graph-demo").build())
        .build();
    opentelemetry::global::set_meter_provider(provider.clone());

    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".into());
    let queue = TaskQueue::connect(&url, "demo").await?;
    let worker = Worker::builder(queue.clone(), registry())
        .concurrency(128)
        .max_attempts(3)
        .backoff(Duration::from_millis(200), Duration::from_secs(5))
        .spawn()
        .await?;

    let end = tokio::time::Instant::now() + Duration::from_secs(minutes * 60);
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let mut n = 0u64;
    println!("running for {minutes} min; Ctrl-C to stop early");
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = tokio::signal::ctrl_c() => break,
        }
        if tokio::time::Instant::now() >= end {
            break;
        }
        n += 1;
        let secs = n / 10;
        // Steady load: ~150 fetch, 30 resize, 20 flaky tasks and 5 chains per second.
        for _ in 0..15 {
            queue.submit("fetch", &n).await?;
        }
        for _ in 0..3 {
            queue.submit("resize", &n).await?;
        }
        for _ in 0..2 {
            queue.submit("flaky", &n).await?;
        }
        if n.is_multiple_of(2) {
            queue.chain(PARSE, &format!("job-{n}")).then(ENRICH).then(STORE).submit().await?;
        }
        // A burst of 3000 fetches every minute, and runtime-blocking tasks every 90 s.
        if n % 600 == 300 {
            for _ in 0..3000 {
                queue.submit("fetch", &n).await?;
            }
        }
        if n % 900 == 450 {
            for _ in 0..40 {
                queue.submit("hog", &n).await?;
            }
        }
        if n.is_multiple_of(50) {
            let s = worker.stats();
            println!(
                "{secs:>4}s  limit {:>3}  in flight {:>3}  lag {:>6.1} ms",
                s.concurrency_limit,
                s.in_flight,
                s.scheduling_lag.as_secs_f64() * 1e3
            );
        }
    }

    println!("shutting down");
    worker.shutdown().await;
    provider.shutdown()?;
    Ok(())
}

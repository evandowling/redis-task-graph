//! Checks the metrics recorded with the `otel` feature, using an in-memory exporter. Runs in its
//! own test binary so it can install the process-wide meter provider before anything records.
//! Needs Redis, like `integration.rs`.
#![cfg(feature = "otel")]

use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use redis_task_graph::{Registry, TaskQueue, Worker};

fn has(attrs: &mut dyn Iterator<Item = &KeyValue>, wanted: &[(&str, &str)]) -> bool {
    let attrs: Vec<&KeyValue> = attrs.collect();
    wanted.iter().all(|(k, v)| {
        attrs
            .iter()
            .any(|kv| kv.key.as_str() == *k && kv.value.as_str() == *v)
    })
}

/// Sum of a u64 counter's points matching `attrs`, or the count of an f64 histogram's points.
fn value(export: &ResourceMetrics, name: &str, attrs: &[(&str, &str)]) -> Option<u64> {
    let metric = export
        .scope_metrics()
        .flat_map(|s| s.metrics())
        .find(|m| m.name() == name)?;
    Some(match metric.data() {
        AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
            .data_points()
            .filter(|p| has(&mut p.attributes(), attrs))
            .map(|p| p.value())
            .sum(),
        AggregatedMetrics::U64(MetricData::Gauge(gauge)) => gauge
            .data_points()
            .filter(|p| has(&mut p.attributes(), attrs))
            .map(|p| p.value())
            .max()
            .unwrap_or(0),
        AggregatedMetrics::F64(MetricData::Histogram(h)) => h
            .data_points()
            .filter(|p| has(&mut p.attributes(), attrs))
            .map(|p| p.count())
            .sum(),
        AggregatedMetrics::F64(MetricData::Gauge(gauge)) => gauge.data_points().count() as u64,
        _ => return None,
    })
}

/// Deletes a test's namespace when dropped, also when the test panics. A blocking connection,
/// since a task spawned here may never run once the test's runtime shuts down.
struct Cleanup {
    url: String,
    ns: String,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        let Ok(mut conn) = redis::Client::open(self.url.as_str())
            .and_then(|c| c.get_connection_with_timeout(Duration::from_secs(2)))
        else {
            return;
        };
        let keys: Vec<String> = match redis::Commands::scan_match(&mut conn, format!("{{{}}}:*", self.ns)) {
            Ok(keys) => keys.filter_map(Result::ok).collect(),
            Err(_) => return,
        };
        for chunk in keys.chunks(1000) {
            let _: redis::RedisResult<()> = redis::Commands::del(&mut conn, chunk);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn records_task_lifecycle_metrics() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    opentelemetry::global::set_meter_provider(provider.clone());

    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".into());
    let ns = format!("rtg-otel-{}", uuid::Uuid::new_v4());
    let Ok(Ok(queue)) = tokio::time::timeout(Duration::from_secs(2), TaskQueue::connect(&url, &ns)).await else {
        eprintln!("skipping: Redis not reachable at {url}");
        return;
    };
    let _cleanup = Cleanup { url, ns };

    let mut reg = Registry::new();
    reg.register("ok", |_: u32| async { Ok(()) }).unwrap();
    reg.register("bad", |_: u32| async { Err("nope".into()) }).unwrap();
    let worker = Worker::builder(queue.clone(), reg)
        .max_attempts(2)
        .backoff(Duration::from_millis(10), Duration::from_millis(10))
        .promote_interval(Duration::from_millis(20))
        .block(Duration::from_millis(100))
        .spawn()
        .await
        .unwrap();
    queue.submit("ok", &1u32).await.unwrap();
    queue.submit("bad", &2u32).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while queue.dead_letters(1).await.unwrap().is_empty() {
        assert!(tokio::time::Instant::now() < deadline, "bad task was not dead-lettered");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Read the gauges while the worker is still running.
    provider.force_flush().unwrap();
    let exports = exporter.get_finished_metrics().unwrap();
    let last = exports.last().expect("an export");
    worker.shutdown().await;

    assert_eq!(value(last, "rtg.tasks.submitted", &[("processor_type", "ok")]), Some(1));
    assert_eq!(value(last, "rtg.tasks.submitted", &[("processor_type", "bad")]), Some(1));
    assert_eq!(value(last, "rtg.tasks.claimed", &[("processor_type", "bad"), ("source", "fetch")]), Some(2));
    assert_eq!(value(last, "rtg.tasks.finished", &[("processor_type", "ok"), ("outcome", "success")]), Some(1));
    assert_eq!(value(last, "rtg.tasks.finished", &[("processor_type", "bad"), ("outcome", "retry")]), Some(1));
    assert_eq!(value(last, "rtg.tasks.finished", &[("processor_type", "bad"), ("outcome", "dead")]), Some(1));
    assert_eq!(value(last, "rtg.task.run_time", &[("processor_type", "bad")]), Some(2));
    assert_eq!(value(last, "rtg.task.queue_time", &[("processor_type", "ok")]), Some(1));
    assert_eq!(value(last, "rtg.task.finalize_time", &[("processor_type", "ok")]), Some(1));
    assert!(value(last, "rtg.worker.concurrency_limit", &[]).unwrap() >= 1);
    assert!(value(last, "rtg.worker.scheduling_lag", &[]).unwrap() >= 1, "a lag sample per live worker");
    let ok_stream = queue.stream_key("ok");
    assert_eq!(value(last, "rtg.queue.depth", &[("stream", ok_stream.as_str())]).map(|_| ()), Some(()));
    provider.shutdown().unwrap();
}

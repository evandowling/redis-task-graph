# Monitoring with OpenObserve

`redis-task-graph` can export live metrics over OpenTelemetry (OTLP). This folder runs
[OpenObserve](https://openobserve.ai), an open-source observability backend written in Rust, as a
native Windows binary, and sets it up with a ready-made dashboard.

## Quick start

```powershell
# 1. Start OpenObserve. The first run downloads v1.1.2 (a 150 MB zip; 510 MB unpacked) into monitoring/openobserve
#    and creates local credentials in monitoring/.env. It then imports the dashboard and prints
#    the two OTEL_* variables to set.
powershell -ExecutionPolicy Bypass -File monitoring/start-openobserve.ps1

# 2. Set the printed variables in your shell:
$env:OTEL_EXPORTER_OTLP_ENDPOINT = 'http://127.0.0.1:5080/api/default'
$env:OTEL_EXPORTER_OTLP_HEADERS  = 'Authorization=Basic%20...'

# 3. Run the demo workload for 10 minutes (or any app built with the `otel` feature):
cargo run --release --example monitored --features otel -- 10

# 4. Open http://127.0.0.1:5080, sign in with the credentials in monitoring/.env,
#    and open Dashboards > redis-task-graph.

# Stop the server (data is kept in monitoring/data):
powershell -ExecutionPolicy Bypass -File monitoring/stop-openobserve.ps1
```

To stream a load test into the dashboard, set the same variables and run
`cargo bench --bench load --features otel -- --otel <scenario>`.

Use `127.0.0.1` rather than `localhost`. OpenObserve listens only on IPv4, and on Windows a
refused IPv6 (`::1`) connection costs about 2 seconds before the client falls back to IPv4.

## Instrumenting your own application

1. Enable the feature: `redis-task-graph = { version = "...", features = ["otel"] }`.
2. Install an OpenTelemetry meter provider at startup. The library records through the global
   meter and never installs an exporter itself. Without a provider, recording is a no-op.

```rust
let exporter = opentelemetry_otlp::MetricExporter::builder().with_http().build()?;
let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
    .with_reader(
        opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
            .with_interval(std::time::Duration::from_secs(5))
            .build(),
    )
    .build();
opentelemetry::global::set_meter_provider(provider.clone());
// ... run workers ...
provider.shutdown()?;
```

Any OTLP-compatible backend works the same way, for example the OpenTelemetry Collector,
Grafana, SigNoz or GreptimeDB.

## Metrics

OpenObserve turns the dots in metric names into underscores (e.g. `rtg_tasks_finished`) and
splits histograms into `_bucket`, `_count` and `_sum` series for PromQL.

The per-task path is kept off the OpenTelemetry SDK. Recording every task through the SDK's
shared aggregation state raised latency near capacity by several times in the load tests.
Instead:
- **Counters** are per-processor-type atomics that the SDK reads at each export, so counts
  are exact.
- **Duration histograms** record a uniform random sample of up to 500 tasks per second per
  processor type. Percentiles stay unbiased and every task is recorded at lower rates, but a
  histogram's `_count` is the number of samples. Use `rtg.tasks.finished` for task counts.

| Metric | Kind | Attributes | Meaning |
|---|---|---|---|
| `rtg.tasks.submitted` | counter | `processor_type` | Tasks enqueued by producers |
| `rtg.chains.submitted` | counter | `processor_type` (first step) | Chains enqueued |
| `rtg.tasks.claimed` | counter | `processor_type`, `source` | Claimed from the stream (`fetch`) or taken over from an expired lease (`reclaim`) |
| `rtg.tasks.finished` | counter | `processor_type`, `outcome` | Executions that ended as `success`, `retry`, `dead` or `lease_lost` |
| `rtg.chain.steps` | counter | `processor_type`, `event` | Chain steps `advanced`, chains `completed` or `failed` |
| `rtg.task.queue_time` | histogram (s), sampled | `processor_type` | Submission to processor start (includes retry backoff) |
| `rtg.task.run_time` | histogram (s), sampled | `processor_type` | Processor execution time |
| `rtg.task.finalize_time` | histogram (s), sampled | `processor_type` | Time to record the outcome in Redis |
| `rtg.leases.lost` | counter | `stream` | Leases taken over by another worker while still held |
| `rtg.worker.concurrency_limit` | gauge | `worker` | Current pushback limit |
| `rtg.worker.in_flight` | gauge | `worker` | Tasks holding a concurrency permit |
| `rtg.worker.scheduling_lag` | gauge (s) | `worker` | How long ready work waits for a runtime thread |
| `rtg.queue.depth` / `rtg.queue.pending` | gauge | `stream` | Entries in the stream / claimed but unacknowledged (sampled every 5 s) |
| `rtg.queue.delayed` | gauge | `namespace` | Tasks waiting for a retry |

## Dashboard

[`dashboards/redis-task-graph.json`](dashboards/redis-task-graph.json) has 10 PromQL panels:
- throughput by outcome and by processor type
- queue-time p50/p99 and run-time p99 per processor type
- concurrency limit vs. in flight
- scheduling lag
- queue depth, pending and delayed entries
- retries, dead letters and lost leases
- chain steps
- submissions

`start-openobserve.ps1` imports every JSON file in `dashboards/` whose title doesn't exist yet.
To update the dashboard, delete it in the UI and run the script again.

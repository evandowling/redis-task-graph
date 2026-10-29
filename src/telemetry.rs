//! OpenTelemetry metrics, enabled by the `otel` cargo feature.
//!
//! The library only records through the global meter provider. Applications choose where
//! metrics go by installing a provider (e.g. an OTLP exporter). Without the feature, everything
//! here compiles to nothing.
//!
//! The per-task path is kept off the OpenTelemetry SDK: counters are plain atomics that
//! observable counters read at export time, and the duration histograms record a random sample of
//! at most [`HISTOGRAM_BUDGET`] tasks per second per processor type. Recording every task through
//! the SDK's shared aggregation state measurably raised latency near capacity.
//!
//! | Metric | Kind | Attributes |
//! |---|---|---|
//! | `rtg.tasks.submitted` | counter | `processor_type` |
//! | `rtg.chains.submitted` | counter | `processor_type` (first step) |
//! | `rtg.tasks.claimed` | counter | `processor_type`, `source` (`fetch` / `reclaim`) |
//! | `rtg.tasks.finished` | counter | `processor_type`, `outcome` (`success` / `retry` / `dead` / `lease_lost`) |
//! | `rtg.chain.steps` | counter | `processor_type`, `event` (`advanced` / `completed` / `failed`) |
//! | `rtg.task.queue_time` | histogram (s), sampled | `processor_type` |
//! | `rtg.task.run_time` | histogram (s), sampled | `processor_type` |
//! | `rtg.task.finalize_time` | histogram (s), sampled | `processor_type` |
//! | `rtg.leases.lost` | counter | `stream` |
//! | `rtg.worker.concurrency_limit`, `rtg.worker.in_flight` | gauge | `worker` |
//! | `rtg.worker.scheduling_lag` | gauge (s) | `worker` |
//! | `rtg.queue.depth`, `rtg.queue.pending` | gauge | `stream` |
//! | `rtg.queue.delayed` | gauge | `namespace` |

#[derive(Debug, Clone, Copy)]
pub(crate) enum ClaimSource {
    Fetch,
    Reclaim,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Finished {
    Success,
    Retry,
    Dead,
    LeaseLost,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ChainEvent {
    Advanced,
    Completed,
    Failed,
}

/// Maximum duration samples recorded per second per processor type.
#[cfg_attr(not(feature = "otel"), allow(dead_code))]
pub(crate) const HISTOGRAM_BUDGET: u64 = 500;

pub(crate) use imp::*;

#[cfg(feature = "otel")]
mod imp {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, LazyLock, Mutex, RwLock, Weak};
    use std::time::{Duration, Instant};

    use opentelemetry::KeyValue;
    use opentelemetry::metrics::{Counter, Histogram, Meter, ObservableCounter, ObservableGauge};

    use super::{ChainEvent, ClaimSource, Finished, HISTOGRAM_BUDGET};
    use crate::monitor::Monitored;

    /// Queue samples older than this are no longer reported (their worker has stopped).
    const STALE_AFTER: Duration = Duration::from_secs(30);

    const SOURCES: [(ClaimSource, &str); 2] = [(ClaimSource::Fetch, "fetch"), (ClaimSource::Reclaim, "reclaim")];
    const OUTCOMES: [(Finished, &str); 4] = [
        (Finished::Success, "success"),
        (Finished::Retry, "retry"),
        (Finished::Dead, "dead"),
        (Finished::LeaseLost, "lease_lost"),
    ];
    const EVENTS: [(ChainEvent, &str); 3] = [
        (ChainEvent::Advanced, "advanced"),
        (ChainEvent::Completed, "completed"),
        (ChainEvent::Failed, "failed"),
    ];

    /// Decides which tasks' durations to record, keeping recordings near [`HISTOGRAM_BUDGET`]
    /// per second by sampling uniformly at random.
    struct Sampler {
        window_start_ms: AtomicU64,
        window_count: AtomicU64,
        /// Probability of keeping a sample, in parts per million.
        keep_ppm: AtomicU64,
    }

    static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

    impl Sampler {
        fn new() -> Self {
            Self {
                window_start_ms: AtomicU64::new(EPOCH.elapsed().as_millis() as u64),
                window_count: AtomicU64::new(0),
                keep_ppm: AtomicU64::new(1_000_000),
            }
        }

        fn keep(&self) -> bool {
            self.keep_at(EPOCH.elapsed().as_millis() as u64)
        }

        /// [`Sampler::keep`] at a given time, in milliseconds since `EPOCH`.
        fn keep_at(&self, now: u64) -> bool {
            let count = self.window_count.fetch_add(1, Ordering::Relaxed) + 1;
            let start = self.window_start_ms.load(Ordering::Relaxed);
            let elapsed = now.saturating_sub(start);
            if elapsed >= 1000
                && self
                    .window_start_ms
                    .compare_exchange(start, now, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            {
                let rate = count * 1000 / elapsed;
                let ppm = if rate <= HISTOGRAM_BUDGET { 1_000_000 } else { HISTOGRAM_BUDGET * 1_000_000 / rate };
                self.keep_ppm.store(ppm, Ordering::Relaxed);
                self.window_count.store(0, Ordering::Relaxed);
            }
            let ppm = self.keep_ppm.load(Ordering::Relaxed);
            ppm >= 1_000_000 || rand::random_range(0..1_000_000) < ppm
        }
    }

    /// Counters and the duration sampler for one processor type.
    pub(crate) struct TypeMetrics {
        attrs: [KeyValue; 1],
        submitted: AtomicU64,
        chains_submitted: AtomicU64,
        claimed: [AtomicU64; 2],
        finished: [AtomicU64; 4],
        chain_steps: [AtomicU64; 3],
        sampler: Sampler,
    }

    /// Cheap, cloneable handle to a processor type's metrics; resolve once and reuse.
    #[derive(Clone)]
    pub(crate) struct TypeHandle(Arc<TypeMetrics>);

    static TYPES: LazyLock<RwLock<HashMap<String, Arc<TypeMetrics>>>> = LazyLock::new(Default::default);

    pub(crate) fn handle(processor_type: &str) -> TypeHandle {
        LazyLock::force(&INSTRUMENTS);
        if let Some(m) = TYPES.read().unwrap().get(processor_type) {
            return TypeHandle(m.clone());
        }
        let mut types = TYPES.write().unwrap();
        let m = types.entry(processor_type.to_owned()).or_insert_with(|| {
            Arc::new(TypeMetrics {
                attrs: [KeyValue::new("processor_type", processor_type.to_owned())],
                submitted: AtomicU64::new(0),
                chains_submitted: AtomicU64::new(0),
                claimed: Default::default(),
                finished: Default::default(),
                chain_steps: Default::default(),
                sampler: Sampler::new(),
            })
        });
        TypeHandle(m.clone())
    }

    impl TypeHandle {
        pub(crate) fn claimed(&self, source: ClaimSource) {
            self.0.claimed[source as usize].fetch_add(1, Ordering::Relaxed);
        }

        pub(crate) fn finished(&self, outcome: Finished) {
            self.0.finished[outcome as usize].fetch_add(1, Ordering::Relaxed);
        }

        pub(crate) fn chain_step(&self, event: ChainEvent) {
            self.0.chain_steps[event as usize].fetch_add(1, Ordering::Relaxed);
        }

        /// Records one task's durations, if it is sampled.
        pub(crate) fn times(&self, queue: Duration, run: Duration, finalize: Duration) {
            if self.0.sampler.keep() {
                let i = &*INSTRUMENTS;
                i.queue_time.record(queue.as_secs_f64(), &self.0.attrs);
                i.run_time.record(run.as_secs_f64(), &self.0.attrs);
                i.finalize_time.record(finalize.as_secs_f64(), &self.0.attrs);
            }
        }
    }

    pub(crate) fn task_submitted(processor_type: &str) {
        handle(processor_type).0.submitted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn chain_submitted(first_type: &str) {
        handle(first_type).0.chains_submitted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn lease_lost(stream: &str) {
        INSTRUMENTS.leases_lost.add(1, &[KeyValue::new("stream", stream.to_owned())]);
    }

    struct Instruments {
        queue_time: Histogram<f64>,
        run_time: Histogram<f64>,
        finalize_time: Histogram<f64>,
        leases_lost: Counter<u64>,
        _counters: Vec<ObservableCounter<u64>>,
        _gauges: Vec<ObservableGauge<u64>>,
        _lag: ObservableGauge<f64>,
    }

    /// A worker's consumer name and its shared state, dropped when the worker stops.
    type WorkerEntry = (String, Weak<Monitored>);

    /// Live workers, read by the worker gauges' callbacks.
    static WORKERS: LazyLock<Mutex<Vec<WorkerEntry>>> = LazyLock::new(Default::default);

    #[derive(Clone, Copy)]
    struct QueueSample {
        depth: u64,
        pending: u64,
        at: Instant,
    }

    /// Latest sampled queue sizes by stream, and delayed-retry counts by namespace.
    static QUEUES: LazyLock<Mutex<HashMap<String, QueueSample>>> = LazyLock::new(Default::default);
    static DELAYED: LazyLock<Mutex<HashMap<String, (u64, Instant)>>> = LazyLock::new(Default::default);

    /// An observable counter reporting `read(type)` for each processor type.
    fn type_counter(meter: &Meter, name: &'static str, description: &'static str, read: fn(&TypeMetrics) -> u64) -> ObservableCounter<u64> {
        meter
            .u64_observable_counter(name)
            .with_description(description)
            .with_callback(move |observer| {
                for m in TYPES.read().unwrap().values() {
                    observer.observe(read(m), &m.attrs);
                }
            })
            .build()
    }

    /// An observable counter reporting one atomic per `(label, value)` for each processor type.
    fn labelled_counter<const N: usize, T: Copy + Send + Sync + 'static>(
        meter: &Meter,
        name: &'static str,
        description: &'static str,
        label: &'static str,
        values: [(T, &'static str); N],
        read: fn(&TypeMetrics) -> &[AtomicU64; N],
    ) -> ObservableCounter<u64> {
        meter
            .u64_observable_counter(name)
            .with_description(description)
            .with_callback(move |observer| {
                for m in TYPES.read().unwrap().values() {
                    for (i, (_, value)) in values.iter().enumerate() {
                        let attrs = [m.attrs[0].clone(), KeyValue::new(label, *value)];
                        observer.observe(read(m)[i].load(Ordering::Relaxed), &attrs);
                    }
                }
            })
            .build()
    }

    fn worker_gauge(meter: &Meter, name: &'static str, description: &'static str, read: fn(&Monitored) -> u64) -> ObservableGauge<u64> {
        meter
            .u64_observable_gauge(name)
            .with_description(description)
            .with_callback(move |observer| {
                WORKERS.lock().unwrap().retain(|(worker, m)| match m.upgrade() {
                    Some(m) => {
                        observer.observe(read(&m), &[KeyValue::new("worker", worker.clone())]);
                        true
                    }
                    None => false,
                });
            })
            .build()
    }

    fn queue_gauge(meter: &Meter, name: &'static str, description: &'static str, read: fn(&QueueSample) -> u64) -> ObservableGauge<u64> {
        meter
            .u64_observable_gauge(name)
            .with_description(description)
            .with_callback(move |observer| {
                for (stream, sample) in QUEUES.lock().unwrap().iter() {
                    if sample.at.elapsed() < STALE_AFTER {
                        observer.observe(read(sample), &[KeyValue::new("stream", stream.clone())]);
                    }
                }
            })
            .build()
    }

    static INSTRUMENTS: LazyLock<Instruments> = LazyLock::new(|| {
        let meter = opentelemetry::global::meter("redis-task-graph");
        let seconds = |name: &'static str, description: &'static str| {
            meter
                .f64_histogram(name)
                .with_unit("s")
                .with_description(description)
                .with_boundaries(vec![
                    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
                ])
                .build()
        };
        Instruments {
            queue_time: seconds("rtg.task.queue_time", "Time from submission to the processor starting (sampled)"),
            run_time: seconds("rtg.task.run_time", "Processor run time (sampled)"),
            finalize_time: seconds("rtg.task.finalize_time", "Time to record a task's outcome in Redis (sampled)"),
            leases_lost: meter
                .u64_counter("rtg.leases.lost")
                .with_description("Leases lost to another worker while held")
                .build(),
            _counters: vec![
                type_counter(&meter, "rtg.tasks.submitted", "Tasks submitted", |m| m.submitted.load(Ordering::Relaxed)),
                type_counter(&meter, "rtg.chains.submitted", "Chains submitted", |m| {
                    m.chains_submitted.load(Ordering::Relaxed)
                }),
                labelled_counter(&meter, "rtg.tasks.claimed", "Tasks claimed by workers", "source", SOURCES, |m| &m.claimed),
                labelled_counter(&meter, "rtg.tasks.finished", "Task executions by outcome", "outcome", OUTCOMES, |m| {
                    &m.finished
                }),
                labelled_counter(&meter, "rtg.chain.steps", "Chain step transitions", "event", EVENTS, |m| &m.chain_steps),
            ],
            _gauges: vec![
                worker_gauge(&meter, "rtg.worker.concurrency_limit", "Current adaptive concurrency limit", |m| {
                    m.stats().concurrency_limit as u64
                }),
                worker_gauge(&meter, "rtg.worker.in_flight", "Tasks holding a concurrency permit", |m| {
                    m.stats().in_flight as u64
                }),
                queue_gauge(&meter, "rtg.queue.depth", "Entries in the stream", |s| s.depth),
                queue_gauge(&meter, "rtg.queue.pending", "Claimed entries not yet acknowledged", |s| s.pending),
                meter
                    .u64_observable_gauge("rtg.queue.delayed")
                    .with_description("Tasks waiting for a retry")
                    .with_callback(|observer| {
                        for (ns, (n, at)) in DELAYED.lock().unwrap().iter() {
                            if at.elapsed() < STALE_AFTER {
                                observer.observe(*n, &[KeyValue::new("namespace", ns.clone())]);
                            }
                        }
                    })
                    .build(),
            ],
            _lag: meter
                .f64_observable_gauge("rtg.worker.scheduling_lag")
                .with_unit("s")
                .with_description("How long ready work waits for a runtime thread")
                .with_callback(|observer| {
                    for (worker, m) in WORKERS.lock().unwrap().iter() {
                        if let Some(m) = m.upgrade() {
                            let lag = m.stats().scheduling_lag.as_secs_f64();
                            observer.observe(lag, &[KeyValue::new("worker", worker.clone())]);
                        }
                    }
                })
                .build(),
        }
    });

    /// Reports a worker's limit, in-flight count and lag until it is dropped.
    pub(crate) fn register_worker(worker: &str, monitored: Weak<Monitored>) {
        LazyLock::force(&INSTRUMENTS);
        WORKERS.lock().unwrap().push((worker.to_owned(), monitored));
    }

    pub(crate) fn record_queue(stream: &str, depth: u64, pending: u64) {
        let sample = QueueSample { depth, pending, at: Instant::now() };
        QUEUES.lock().unwrap().insert(stream.to_owned(), sample);
    }

    pub(crate) fn record_delayed(namespace: &str, delayed: u64) {
        DELAYED.lock().unwrap().insert(namespace.to_owned(), (delayed, Instant::now()));
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn sampler_keeps_everything_under_budget_and_thins_above_it() {
            let s = Sampler::new();
            s.window_start_ms.store(0, Ordering::Relaxed);
            assert!((0..100).all(|_| s.keep_at(10)), "the first window keeps everything");
            // A full one-second window at 10x the budget.
            s.window_count.store(HISTOGRAM_BUDGET * 10 - 1, Ordering::Relaxed);
            s.keep_at(1000);
            let ppm = s.keep_ppm.load(Ordering::Relaxed);
            assert!((90_000..=110_000).contains(&ppm), "keeps ~10%: {ppm} ppm");
            let kept = (0..100_000).filter(|_| s.keep_at(1500)).count();
            assert!((8_000..=12_000).contains(&kept), "kept {kept} of 100000");
        }

        #[test]
        fn handles_for_the_same_type_share_counters() {
            let a = handle("telemetry-test-type");
            let b = handle("telemetry-test-type");
            a.finished(Finished::Success);
            b.finished(Finished::Success);
            assert_eq!(a.0.finished[Finished::Success as usize].load(Ordering::Relaxed), 2);
        }
    }
}

#[cfg(not(feature = "otel"))]
#[allow(unused_variables)]
mod imp {
    use std::sync::Weak;
    use std::time::Duration;

    use super::{ChainEvent, ClaimSource, Finished};
    use crate::monitor::Monitored;

    #[derive(Clone)]
    pub(crate) struct TypeHandle;

    pub(crate) fn handle(processor_type: &str) -> TypeHandle {
        TypeHandle
    }

    impl TypeHandle {
        pub(crate) fn claimed(&self, source: ClaimSource) {}
        pub(crate) fn finished(&self, outcome: Finished) {}
        pub(crate) fn chain_step(&self, event: ChainEvent) {}
        pub(crate) fn times(&self, queue: Duration, run: Duration, finalize: Duration) {}
    }

    pub(crate) fn task_submitted(processor_type: &str) {}
    pub(crate) fn chain_submitted(first_type: &str) {}
    pub(crate) fn lease_lost(stream: &str) {}
    pub(crate) fn register_worker(worker: &str, monitored: Weak<Monitored>) {}
}

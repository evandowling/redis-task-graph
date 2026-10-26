//! The benchmark scenarios. Each returns a [`Figure`] holding its measurements.

use std::time::{Duration, Instant};

use crate::figure::{Figure, Format, Scale};
use crate::harness::*;

pub struct Opts {
    /// Multiplier on job counts and repetitions; < 1 for a fast smoke run.
    pub scale: f64,
}

impl Opts {
    fn jobs(&self, n: f64, min: usize, max: usize) -> usize {
        ((n * self.scale) as usize).clamp(((min as f64 * self.scale) as usize).max(8), max)
    }
}

pub type Scenario = fn(&Opts) -> Figure;
type ShapeFn = fn(u32) -> Shape;
/// Number of tasks that can make progress at once, given (concurrency, runtime threads).
type Parallelism = fn(usize, usize) -> usize;

pub const ALL: &[(&str, Scenario)] = &[
    ("consume_throughput", consume_throughput),
    ("submit_throughput", submit_throughput),
    ("latency_under_load", latency_under_load),
    ("chain_latency", chain_latency),
    ("chain_latency_io", chain_latency_io),
    ("chain_throughput", chain_throughput),
    ("chain_throughput_io", chain_throughput_io),
    ("io_bound", io_bound),
    ("cpu_bound", cpu_bound),
    ("lease_starvation", lease_starvation),
    ("payload_size", payload_size),
    ("worker_scaling", worker_scaling),
];

const POW2: fn(u32, u32) -> Vec<usize> = |lo, hi| (lo..=hi).map(|k| 1usize << k).collect();

fn log(msg: impl AsRef<str>) {
    eprintln!("  {}", msg.as_ref());
}

/// No-op tasks drained by 1, 2 and 4 workers across per-worker concurrency limits.
pub fn consume_throughput(o: &Opts) -> Figure {
    let mut fig = Figure::new(
        "consume_throughput",
        "No-op task throughput vs. concurrency",
        "Concurrency limit per worker",
        "Tasks completed / s",
    );
    fig.subtitle = "Preloaded queue of no-op tasks drained by N workers (4 runtime threads each)".into();
    fig.x_scale = Scale::Log2;
    for workers in [1usize, 2, 4] {
        let name = if workers == 1 { "1 worker".to_string() } else { format!("{workers} workers") };
        for c in POW2(0, 8) {
            let jobs = o.jobs((c * workers * 1500) as f64, 6000, 30_000);
            let spec = ClusterSpec { workers, threads: 4, ..ClusterSpec::new(c, Work::Noop) };
            let r = drain_median(spec, Shape::Task, jobs, 0, 3);
            log(format!("{name} c={c}: {:.0} tasks/s ({jobs} jobs)", r.rate));
            fig.push(&name, c as f64, r.rate);
        }
    }
    fig
}

/// Enqueue rate for plain tasks and for 4-step chains with N concurrent producers.
pub fn submit_throughput(o: &Opts) -> Figure {
    let mut fig = Figure::new(
        "submit_throughput",
        "Submission throughput vs. concurrent producers",
        "Concurrent producers",
        "Submissions / s",
    );
    fig.subtitle = "A chain submit writes its status hash and first step in one MULTI".into();
    fig.x_scale = Scale::Log2;
    let total = o.jobs(20_000.0, 2000, 20_000);
    for (name, shape) in [("task", Shape::Task), ("4-step chain", Shape::Chain(4))] {
        for p in POW2(0, 8) {
            let ns = fresh_ns();
            let rt = driver();
            let elapsed = rt.block_on(async {
                let q = connect(&ns).await;
                let per = total.div_ceil(p);
                let start = Instant::now();
                let handles: Vec<_> = (0..p)
                    .map(|w| {
                        let q = q.clone();
                        tokio::spawn(async move {
                            for i in 0..per {
                                enqueue(&q, shape, Job::new((w * per + i) as u64, shape.len(), 0)).await;
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.await.unwrap();
                }
                start.elapsed()
            });
            let n = total.div_ceil(p) * p;
            let rate = n as f64 / elapsed.as_secs_f64();
            log(format!("{name} producers={p}: {rate:.0}/s"));
            fig.push(name, p as f64, rate);
            rt.block_on(cleanup(&ns));
        }
    }
    fig
}

/// Open-loop arrivals at fractions of measured capacity; latency from submit to completion.
pub fn latency_under_load(o: &Opts) -> Figure {
    let mut fig = Figure::new(
        "latency_under_load",
        "End-to-end latency vs. offered load",
        "Offered load (tasks / s)",
        "Submit-to-completion latency (ms)",
    );
    fig.y_scale = Scale::Log10;
    let spec = ClusterSpec::new(64, Work::Noop);
    let capacity = drain(spec, Shape::Task, o.jobs(30_000.0, 3000, 30_000), 0).rate;
    log(format!("drain capacity: {capacity:.0} tasks/s"));
    fig.subtitle = format!(
        "One worker, concurrency 64; drain capacity measured at {:.0} tasks/s",
        capacity
    );
    fig.note("drain_capacity_tasks_per_s", format!("{capacity:.0}"));
    let secs = 4.0 * o.scale.max(0.25);
    for frac in [0.1, 0.25, 0.5, 0.7, 0.85, 0.95, 1.1] {
        let rate = capacity * frac;
        let ns = fresh_ns();
        let rec = Recorder::new();
        let cluster = Cluster::start(spec, &ns, &rec);
        let rt = driver();
        let total = (rate * secs) as u64;
        rt.block_on(async {
            let q = connect(&ns).await;
            let start = Instant::now();
            let mut sent = 0u64;
            while sent < total {
                let due = ((start.elapsed().as_secs_f64() * rate) as u64).min(total);
                while sent < due {
                    let (q, id) = (q.clone(), sent);
                    tokio::spawn(async move {
                        q.submit(STEP, &Job::new(id, 1, 0)).await.unwrap();
                    });
                    sent += 1;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            wait_done(&rec, total as usize, Duration::from_secs(60)).await;
        });
        cluster.stop();
        rt.block_on(cleanup(&ns));
        let lat = rec.take_latencies_ms();
        let (p50, p90, p99) = (percentile(&lat, 0.5), percentile(&lat, 0.9), percentile(&lat, 0.99));
        log(format!("{:.0}% ({rate:.0}/s): p50 {p50:.2} p90 {p90:.2} p99 {p99:.2} ms", frac * 100.0));
        fig.push("p50", rate, p50);
        fig.push("p90", rate, p90);
        fig.push("p99", rate, p99);
    }
    fig
}

/// Runs jobs one at a time on an otherwise idle worker and records the median submit-to-finish
/// latency for each shape and length.
fn latency_sweep(fig: &mut Figure, o: &Opts, work: Work, shapes: &[(&str, ShapeFn)], lens: &[usize]) {
    let reps = ((40.0 * o.scale) as usize).max(5);
    let spec = ClusterSpec::new(64, work);
    for &(name, make) in shapes {
        for &len in lens {
            let len = len as u32;
            let shape = make(len);
            let ns = fresh_ns();
            let rec = Recorder::new();
            let cluster = Cluster::start(spec, &ns, &rec);
            let rt = driver();
            let mut samples = Vec::new();
            rt.block_on(async {
                let q = connect(&ns).await;
                let mut done = 0usize;
                for rep in 0..reps {
                    if shape == Shape::Task {
                        // `len` independent tasks submitted together; latency is the slowest one.
                        let t0 = now_us();
                        for i in 0..len {
                            let mut job = Job::new((rep as u64) << 16 | i as u64, 1, 0);
                            job.t0_us = t0;
                            enqueue(&q, Shape::Task, job).await;
                        }
                        done += len as usize;
                    } else {
                        enqueue(&q, shape, Job::new(rep as u64, len, 0)).await;
                        done += 1;
                    }
                    wait_done(&rec, done, Duration::from_secs(30)).await;
                    let lat = rec.take_latencies_ms();
                    samples.push(lat.last().copied().unwrap_or(f64::NAN));
                }
            });
            cluster.stop();
            rt.block_on(cleanup(&ns));
            samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let (p50, p90) = (percentile(&samples, 0.5), percentile(&samples, 0.9));
            log(format!("{name} len={len}: p50 {p50:.2} ms, p90 {p90:.2} ms"));
            fig.push(name, len as f64, p50);
            fig.note(format!("{name} len={len} p90_ms"), format!("{p90:.2}"));
        }
    }
}

/// Unloaded end-to-end latency of one chain of no-op steps vs. its length, against alternatives.
pub fn chain_latency(o: &Opts) -> Figure {
    let mut fig = Figure::new(
        "chain_latency",
        "Unloaded latency vs. chain length (no-op steps)",
        "Steps",
        "Median submit-to-finish latency (ms)",
    );
    fig.subtitle = "One job in flight at a time on an idle worker; median of repeated runs".into();
    fig.x_scale = Scale::Log2;
    let shapes: [(&str, ShapeFn); 3] = [
        ("chain", Shape::Chain),
        ("DIY chain (step submits next)", Shape::Diy),
        ("independent tasks (parallel)", |_| Shape::Task),
    ];
    latency_sweep(&mut fig, o, Work::Noop, &shapes, &POW2(0, 5));
    fig
}

/// Unloaded latency when every step waits 5 ms on I/O: chains serialize the waits.
pub fn chain_latency_io(o: &Opts) -> Figure {
    let wait = Duration::from_millis(5);
    let mut fig = Figure::new(
        "chain_latency_io",
        "Unloaded latency vs. chain length (5 ms I/O wait per step)",
        "Steps",
        "Median submit-to-finish latency (ms)",
    );
    fig.subtitle = "A chain pays every step's wait in sequence; independent tasks wait in parallel".into();
    fig.x_scale = Scale::Log2;
    fig.y_scale = Scale::Log10;
    let shapes: [(&str, ShapeFn); 2] = [
        ("chain", Shape::Chain),
        ("independent tasks (parallel)", |_| Shape::Task),
    ];
    let lens = POW2(0, 5);
    latency_sweep(&mut fig, o, Work::AsyncWait(wait), &shapes, &lens);
    let ideal = fig.series("steps × 5 ms");
    ideal.reference = true;
    for &l in &lens {
        ideal.points.push((l as f64, l as f64 * 5.0));
    }
    fig
}

/// Step throughput under load for chains of length L vs. the same number of plain tasks.
pub fn chain_throughput(o: &Opts) -> Figure {
    let mut fig = Figure::new(
        "chain_throughput",
        "Step throughput under load vs. chain length (no-op steps)",
        "Steps per job",
        "Steps completed / s",
    );
    fig.subtitle = "Same total number of steps at every point; one worker, concurrency 64; median of 3".into();
    fig.x_scale = Scale::Log2;
    let total_steps = o.jobs(24_000.0, 2400, 24_000);
    let spec = ClusterSpec::new(64, Work::Noop);
    let shapes: [(&str, ShapeFn); 3] = [
        ("independent tasks", |_| Shape::Task),
        ("chain", Shape::Chain),
        ("DIY chain (step submits next)", Shape::Diy),
    ];
    for (name, make) in shapes {
        for len in POW2(0, 5) {
            let shape = make(len as u32);
            let jobs = if shape == Shape::Task { total_steps } else { total_steps / len };
            let r = drain_median(spec, shape, jobs, 0, 3);
            log(format!("{name} len={len}: {:.0} steps/s", r.rate));
            fig.push(name, len as f64, r.rate);
        }
    }
    fig
}

/// Step throughput when every step waits 10 ms on I/O: with enough concurrency, chaining costs
/// no throughput because waits overlap across jobs.
pub fn chain_throughput_io(o: &Opts) -> Figure {
    let wait = Duration::from_millis(10);
    let measured = measured_io_wait(wait).as_secs_f64();
    let conc = 256usize;
    let mut fig = Figure::new(
        "chain_throughput_io",
        "Step throughput under load vs. chain length (10 ms I/O wait per step)",
        "Steps per job",
        "Steps completed / s",
    );
    fig.subtitle = format!("One worker, concurrency {conc}; same total number of steps at every point");
    fig.x_scale = Scale::Log2;
    let total_steps = o.jobs(conc as f64 / measured * 2.5, 2560, 30_000);
    let spec = ClusterSpec::new(conc, Work::AsyncWait(wait));
    let lens = POW2(0, 5);
    for (name, chained) in [("independent tasks", false), ("chain", true)] {
        for &len in &lens {
            let (shape, jobs) = if chained {
                (Shape::Chain(len as u32), total_steps / len)
            } else {
                (Shape::Task, total_steps)
            };
            let r = drain(spec, shape, jobs, 0);
            log(format!("{name} len={len}: {:.0} steps/s", r.rate));
            fig.push(name, len as f64, r.rate);
        }
    }
    let ideal = fig.series(&format!("ideal: {conc} ÷ wait"));
    ideal.reference = true;
    for &l in &lens {
        ideal.points.push((l as f64, conc as f64 / measured));
    }
    fig
}

/// I/O-wait-bound tasks: well-behaved async waits vs. blocking the runtime.
pub fn io_bound(o: &Opts) -> Figure {
    let d = Duration::from_millis(20);
    let actual = measured_io_wait(d);
    let mut fig = Figure::new(
        "io_bound",
        "I/O-bound tasks (20 ms wait) vs. concurrency",
        "Concurrency limit",
        "Tasks completed / s",
    );
    fig.subtitle = "One worker, 8 runtime threads; each task waits 20 ms on simulated I/O".into();
    fig.x_scale = Scale::Log2;
    fig.y_scale = Scale::Log10;
    fig.note("measured_io_wait_20ms", format!("{:.2} ms", actual.as_secs_f64() * 1e3));
    let wait = actual.as_secs_f64();
    let threads = 8usize;
    let variants: [(&str, Work, Parallelism); 3] = [
        ("async wait", Work::AsyncWait(d), |c, _| c),
        ("spawn_blocking", Work::SpawnBlockingSleep(d), |c, _| c.min(512)),
        ("blocking on runtime", Work::BlockingSleep(d), |c, t| c.min(t)),
    ];
    let concs = POW2(0, 10).into_iter().step_by(2).collect::<Vec<_>>();
    for (name, work, parallel) in variants {
        for &c in &concs {
            let ideal = parallel(c, threads) as f64 / wait;
            let jobs = o.jobs(ideal * 2.5, 40, 30_000);
            let spec = ClusterSpec { threads, ..ClusterSpec::new(c, work) };
            let r = drain(spec, Shape::Task, jobs, 0);
            log(format!("{name} c={c}: {:.0} tasks/s (ideal {ideal:.0})", r.rate));
            fig.push(name, c as f64, r.rate);
        }
    }
    let ideal = fig.series("ideal: concurrency ÷ wait");
    ideal.reference = true;
    for &c in &concs {
        ideal.points.push((c as f64, c as f64 / wait));
    }
    fig
}

/// CPU-bound tasks: compute on the runtime thread vs. in `spawn_blocking`.
pub fn cpu_bound(o: &Opts) -> Figure {
    let target = Duration::from_millis(5);
    let iters = calibrate_burn(target);
    let hw = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
    let mut fig = Figure::new(
        "cpu_bound",
        "CPU-bound tasks (5 ms compute) vs. concurrency",
        "Concurrency limit",
        "Tasks completed / s",
    );
    fig.subtitle = format!("One worker, {hw} runtime threads, {hw} hardware threads");
    fig.x_scale = Scale::Log2;
    fig.note("burn_iterations_for_5ms", iters.to_string());
    fig.note("hardware_threads", hw.to_string());
    let concs = POW2(0, 6);
    for (name, work) in [("compute on runtime", Work::CpuInline(iters)), ("spawn_blocking", Work::CpuSpawnBlocking(iters))] {
        for &c in &concs {
            let ideal = c.min(hw) as f64 / target.as_secs_f64();
            let jobs = o.jobs(ideal * 2.5, 40, 8000);
            let spec = ClusterSpec { threads: hw, ..ClusterSpec::new(c, work) };
            let r = drain(spec, Shape::Task, jobs, 0);
            log(format!("{name} c={c}: {:.0} tasks/s (ideal {ideal:.0})", r.rate));
            fig.push(name, c as f64, r.rate);
        }
    }
    let ideal = fig.series(&format!("ideal: min(concurrency, {hw}) ÷ 5 ms"));
    ideal.reference = true;
    for &c in &concs {
        ideal.points.push((c as f64, c.min(hw) as f64 / target.as_secs_f64()));
    }
    fig
}

/// Long CPU-bound tasks with a short lease: blocking the runtime starves heartbeats, leases
/// expire, and other workers reclaim and re-run tasks that are still running.
pub fn lease_starvation(o: &Opts) -> Figure {
    let task = Duration::from_millis(600);
    let iters = calibrate_burn(task);
    let mut fig = Figure::new(
        "lease_starvation",
        "Duplicate executions when CPU work starves heartbeats",
        "Concurrency limit per worker (4 runtime threads each)",
        "Duplicate executions (% of tasks)",
    );
    fig.subtitle = "2 workers, 600 ms CPU tasks, 300 ms lease (heartbeat every 100 ms)".into();
    fig.x_scale = Scale::Log2;
    fig.y_format = Format::Percent;
    let jobs = o.jobs(32.0, 16, 32);
    for (name, work) in [("compute on runtime", Work::CpuInline(iters)), ("spawn_blocking", Work::CpuSpawnBlocking(iters))] {
        for c in POW2(0, 3) {
            let spec = ClusterSpec {
                workers: 2,
                threads: 4,
                lease: Duration::from_millis(300),
                ..ClusterSpec::new(c, work)
            };
            let r = drain(spec, Shape::Task, jobs, 0);
            log(format!(
                "{name} c={c}: {} executions for {} tasks ({:.0}% duplicates)",
                r.executions,
                r.steps,
                r.duplicate_pct()
            ));
            fig.push(name, c as f64, r.duplicate_pct());
            fig.note(format!("{name} c={c} executions/tasks"), format!("{}/{}", r.executions, r.steps));
        }
    }
    fig
}

/// Throughput as the context grows; chains carry the payload through every hop.
pub fn payload_size(o: &Opts) -> Figure {
    let mut fig = Figure::new(
        "payload_size",
        "Throughput vs. context size",
        "Context size",
        "Steps completed / s",
    );
    fig.subtitle = "No-op steps, concurrency 16; a chain passes the full payload to every step through Redis".into();
    fig.x_scale = Scale::Log2;
    fig.y_scale = Scale::Log10;
    fig.x_format = Format::Bytes;
    let spec = ClusterSpec::new(16, Work::Noop);
    for (name, shape) in [("task", Shape::Task), ("4-step chain", Shape::Chain(4))] {
        for size in [64usize, 1 << 10, 8 << 10, 64 << 10, 256 << 10, 512 << 10] {
            // At least 256 steps (16 waves at concurrency 16) and at most ~128 MB in Redis.
            let steps = o.jobs((128usize << 20) as f64 / size as f64, 256, 20_000).max(256);
            let jobs = steps / shape.len() as usize;
            let r = drain_median(spec, shape, jobs, size, 3);
            let mb_s = r.rate * size as f64 / (1 << 20) as f64;
            log(format!("{name} {size} B: {:.0} steps/s, {mb_s:.1} MB/s", r.rate));
            fig.push(name, size as f64, r.rate);
            fig.note(format!("{name} {size}B MB/s"), format!("{mb_s:.1}"));
        }
    }
    fig
}

/// Adding workers: Redis-bound no-op tasks vs. I/O-bound tasks.
pub fn worker_scaling(o: &Opts) -> Figure {
    let mut fig = Figure::new(
        "worker_scaling",
        "Throughput vs. number of workers",
        "Workers (2 runtime threads, concurrency 32 each)",
        "Tasks completed / s",
    );
    fig.subtitle = "No-op tasks are bound by Redis round trips; 10 ms async waits by concurrency".into();
    fig.x_scale = Scale::Log2;
    let d = Duration::from_millis(10);
    let wait = measured_io_wait(d).as_secs_f64();
    let workers = POW2(0, 3);
    for (name, work) in [("no-op", Work::Noop), ("10 ms async wait", Work::AsyncWait(d))] {
        for &w in &workers {
            let ideal = if matches!(work, Work::Noop) { 30_000.0 } else { (w * 32) as f64 / wait };
            let jobs = o.jobs(ideal * 2.5, 200, 30_000);
            let spec = ClusterSpec { workers: w, threads: 2, ..ClusterSpec::new(32, work) };
            let r = drain_median(spec, Shape::Task, jobs, 0, 3);
            log(format!("{name} workers={w}: {:.0} tasks/s", r.rate));
            fig.push(name, w as f64, r.rate);
        }
    }
    let ideal = fig.series("ideal: workers × 32 ÷ wait");
    ideal.reference = true;
    for &w in &workers {
        ideal.points.push((w as f64, (w * 32) as f64 / wait));
    }
    fig
}

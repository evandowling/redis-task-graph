//! Integration tests against a live Redis (7.x). Set `REDIS_URL` to point elsewhere than
//! `redis://127.0.0.1/`; tests are skipped when Redis is unreachable.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use redis::AsyncCommands;
use redis_task_graph::{DeadLetter, Registry, TaskEnvelope, TaskQueue, Worker};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Ctx {
    n: u32,
    label: String,
}

fn ctx(n: u32) -> Ctx {
    Ctx { n, label: format!("task-{n}") }
}

/// The dead-lettered tasks, failing the test if any entry couldn't be decoded.
async fn dead_tasks(queue: &TaskQueue) -> Vec<TaskEnvelope> {
    let letters = queue.dead_letters(10).await.unwrap();
    letters.into_iter().map(|l| l.into_task().expect("a decodable task")).collect()
}

struct TestQueue {
    queue: TaskQueue,
    conn: redis::aio::MultiplexedConnection,
}

impl TestQueue {
    async fn xlen(&mut self, processor_type: &str) -> usize {
        self.conn.xlen(self.queue.stream_key(processor_type)).await.unwrap()
    }
}

impl Drop for TestQueue {
    fn drop(&mut self) {
        // Best-effort cleanup of this test's namespace.
        let mut conn = self.conn.clone();
        // Current keys carry the namespace in a hash tag; 0.1 keys (from migration tests) don't.
        let ns = self.queue.namespace().to_owned();
        tokio::spawn(async move {
            let mut keys: Vec<String> = conn.keys(format!("{{{ns}}}:*")).await.unwrap_or_default();
            keys.extend(conn.keys::<_, Vec<String>>(format!("{ns}:*")).await.unwrap_or_default());
            if !keys.is_empty() {
                let _: redis::RedisResult<()> = conn.del(keys).await;
            }
        });
    }
}

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".into())
}

async fn setup() -> Option<TestQueue> {
    let url = redis_url();
    let client = redis::Client::open(url.as_str()).ok()?;
    let conn = match tokio::time::timeout(
        Duration::from_secs(2),
        client.get_multiplexed_async_connection(),
    )
    .await
    {
        Ok(Ok(conn)) => conn,
        _ => {
            eprintln!("skipping: Redis not reachable at {url}");
            return None;
        }
    };
    let ns = format!("rtg-test-{}", uuid::Uuid::new_v4());
    let queue = TaskQueue::from_client(client, ns).await.ok()?;
    Some(TestQueue { queue, conn })
}

/// Polls `cond` until it is true or `timeout` elapses.
async fn eventually<F, Fut>(timeout: Duration, mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if cond().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    false
}

fn fast(worker: Worker) -> Worker {
    worker
        .block(Duration::from_millis(100))
        .backoff(Duration::from_millis(10), Duration::from_millis(50))
        .promote_interval(Duration::from_millis(25))
}

#[tokio::test]
async fn processes_task_with_its_context() {
    let Some(mut t) = setup().await else { return };
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut reg = Registry::new();
    reg.register("echo", move |c: Ctx| {
        let tx = tx.clone();
        async move {
            tx.send(c).unwrap();
            Ok(())
        }
    })
    .unwrap();
    let worker = fast(Worker::builder(t.queue.clone(), reg)).spawn().await.unwrap();

    t.queue.submit("echo", &ctx(7)).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap().unwrap();
    assert_eq!(got, ctx(7));

    worker.shutdown().await;
    assert_eq!(t.xlen("echo").await, 0, "acked entries are removed from the stream");
}

#[tokio::test]
async fn tasks_submitted_before_worker_starts_are_processed() {
    let Some(t) = setup().await else { return };
    t.queue.submit("early", &ctx(1)).await.unwrap();

    let seen = Arc::new(AtomicUsize::new(0));
    let mut reg = Registry::new();
    let s = seen.clone();
    reg.register("early", move |_: Ctx| {
        let s = s.clone();
        async move {
            s.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .unwrap();
    let worker = fast(Worker::builder(t.queue.clone(), reg)).spawn().await.unwrap();
    assert!(eventually(Duration::from_secs(5), || async { seen.load(Ordering::SeqCst) == 1 }).await);
    worker.shutdown().await;
}

/// Registers a processor that records each task's `n` and tracks peak concurrency.
fn counting_registry(
    counts: Arc<Mutex<HashMap<u32, usize>>>,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
) -> Registry {
    let mut reg = Registry::new();
    reg.register("count", move |c: Ctx| {
        let (counts, active, peak) = (counts.clone(), active.clone(), peak.clone());
        async move {
            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(10)).await;
            *counts.lock().unwrap().entry(c.n).or_default() += 1;
            active.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .unwrap();
    reg
}

#[tokio::test]
async fn processes_each_task_once_in_parallel_across_workers() {
    let Some(mut t) = setup().await else { return };
    const N: u32 = 300;
    let counts = Arc::new(Mutex::new(HashMap::new()));
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let mut workers = Vec::new();
    for _ in 0..3 {
        let reg = counting_registry(counts.clone(), active.clone(), peak.clone());
        workers.push(
            fast(Worker::builder(t.queue.clone(), reg))
                .concurrency(8)
                .spawn()
                .await
                .unwrap(),
        );
    }
    for n in 0..N {
        t.queue.submit("count", &ctx(n)).await.unwrap();
    }

    assert!(
        eventually(Duration::from_secs(15), || async {
            counts.lock().unwrap().len() == N as usize
        })
        .await,
        "all tasks processed"
    );
    for w in workers {
        w.shutdown().await;
    }
    {
        let counts = counts.lock().unwrap();
        assert!(counts.values().all(|&c| c == 1), "no task ran twice: {counts:?}");
    }
    assert!(peak.load(Ordering::SeqCst) > 1, "tasks ran concurrently");
    assert_eq!(t.xlen("count").await, 0);
}

#[tokio::test]
async fn failing_task_is_retried_then_dead_lettered() {
    let Some(t) = setup().await else { return };
    let calls = Arc::new(AtomicUsize::new(0));
    let mut reg = Registry::new();
    let c = calls.clone();
    reg.register("fail", move |_: Ctx| {
        let c = c.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            Err("always fails".into())
        }
    })
    .unwrap();
    let worker = fast(Worker::builder(t.queue.clone(), reg))
        .max_attempts(3)
        .spawn()
        .await
        .unwrap();

    let id = t.queue.submit("fail", &ctx(1)).await.unwrap();
    assert!(
        eventually(Duration::from_secs(5), || async {
            !t.queue.dead_letters(10).await.unwrap().is_empty()
        })
        .await
    );
    worker.shutdown().await;

    let dead = dead_tasks(&t.queue).await;
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].id, id);
    assert_eq!(dead[0].attempt, 3);
    assert_eq!(dead[0].last_error.as_deref(), Some("always fails"));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn task_succeeds_after_transient_failures() {
    let Some(t) = setup().await else { return };
    let calls = Arc::new(AtomicUsize::new(0));
    let mut reg = Registry::new();
    let c = calls.clone();
    reg.register("flaky", move |_: Ctx| {
        let c = c.clone();
        async move {
            if c.fetch_add(1, Ordering::SeqCst) < 2 {
                Err("not yet".into())
            } else {
                Ok(())
            }
        }
    })
    .unwrap();
    let worker = fast(Worker::builder(t.queue.clone(), reg))
        .max_attempts(5)
        .spawn()
        .await
        .unwrap();

    t.queue.submit("flaky", &ctx(1)).await.unwrap();
    assert!(eventually(Duration::from_secs(5), || async { calls.load(Ordering::SeqCst) == 3 }).await);
    tokio::time::sleep(Duration::from_millis(200)).await;
    worker.shutdown().await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert!(t.queue.dead_letters(10).await.unwrap().is_empty());
}

#[tokio::test]
async fn panic_counts_as_failure_and_worker_survives() {
    let Some(t) = setup().await else { return };
    let ok = Arc::new(AtomicUsize::new(0));
    let mut reg = Registry::new();
    reg.register("boom", |_: Ctx| async { panic!("kaboom") }).unwrap();
    let o = ok.clone();
    reg.register("fine", move |_: Ctx| {
        let o = o.clone();
        async move {
            o.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .unwrap();
    let worker = fast(Worker::builder(t.queue.clone(), reg))
        .max_attempts(1)
        .spawn()
        .await
        .unwrap();

    t.queue.submit("boom", &ctx(1)).await.unwrap();
    assert!(
        eventually(Duration::from_secs(5), || async {
            !t.queue.dead_letters(10).await.unwrap().is_empty()
        })
        .await
    );
    let dead = dead_tasks(&t.queue).await;
    assert!(dead[0].last_error.as_deref().unwrap().contains("kaboom"));

    t.queue.submit("fine", &ctx(2)).await.unwrap();
    assert!(eventually(Duration::from_secs(5), || async { ok.load(Ordering::SeqCst) == 1 }).await);
    worker.shutdown().await;
}

#[tokio::test]
async fn undecodable_context_is_dead_lettered_without_retry() {
    let Some(t) = setup().await else { return };
    let calls = Arc::new(AtomicUsize::new(0));
    let mut reg = Registry::new();
    let c = calls.clone();
    reg.register("typed", move |_: Ctx| {
        let c = c.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .unwrap();
    let worker = fast(Worker::builder(t.queue.clone(), reg))
        .max_attempts(5)
        .spawn()
        .await
        .unwrap();

    t.queue.submit("typed", &serde_json::json!({ "wrong": true })).await.unwrap();
    assert!(
        eventually(Duration::from_secs(5), || async {
            !t.queue.dead_letters(10).await.unwrap().is_empty()
        })
        .await
    );
    worker.shutdown().await;
    let dead = dead_tasks(&t.queue).await;
    assert_eq!(dead[0].attempt, 0);
    assert!(dead[0].last_error.as_deref().unwrap().contains("invalid context"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn undecodable_entries_are_listed_alongside_dead_tasks() {
    let Some(mut t) = setup().await else { return };
    let mut reg = Registry::new();
    reg.register("t", |_: Ctx| async { Err::<(), _>("nope".into()) }).unwrap();
    let worker = fast(Worker::builder(t.queue.clone(), reg)).max_attempts(1).spawn().await.unwrap();

    let id = t.queue.submit("t", &ctx(1)).await.unwrap();
    let stream = t.queue.stream_key("t");
    let _: String = t.conn.xadd(&stream, "*", &[("task", "not json")]).await.unwrap();
    let both = eventually(Duration::from_secs(5), || async {
        t.queue.dead_letters(10).await.unwrap().len() == 2
    })
    .await;
    worker.shutdown().await;
    assert!(both, "one undecodable entry must not hide the other dead letters");

    let letters = t.queue.dead_letters(10).await.unwrap();
    assert!(letters.iter().any(|l| l.task().is_some_and(|task| task.id == id)));
    let undecodable = letters.iter().find_map(|l| match l {
        DeadLetter::Undecodable { payload, error } => Some((payload.as_str(), error.as_str())),
        _ => None,
    });
    let (payload, error) = undecodable.expect("the bad entry is listed");
    assert_eq!(payload, "not json");
    assert!(error.contains("invalid task payload"), "{error}");
}

#[tokio::test]
async fn expired_lease_is_reclaimed_by_another_worker() {
    let Some(mut t) = setup().await else { return };
    let stream = t.queue.stream_key("orphan");
    let group = t.queue.group_name();
    let _: () = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&stream)
        .arg(&group)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(&mut t.conn)
        .await
        .unwrap();
    t.queue.submit("orphan", &ctx(1)).await.unwrap();

    // A "crashed" consumer claims the task and never acks it.
    let claimed: redis::Value = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(&group)
        .arg("ghost")
        .arg("COUNT")
        .arg(1)
        .arg("STREAMS")
        .arg(&stream)
        .arg(">")
        .query_async(&mut t.conn)
        .await
        .unwrap();
    assert_ne!(claimed, redis::Value::Nil);

    let seen = Arc::new(AtomicUsize::new(0));
    let mut reg = Registry::new();
    let s = seen.clone();
    reg.register("orphan", move |_: Ctx| {
        let s = s.clone();
        async move {
            s.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .unwrap();
    let worker = fast(Worker::builder(t.queue.clone(), reg))
        .lease(Duration::from_millis(300))
        .spawn()
        .await
        .unwrap();

    assert!(eventually(Duration::from_secs(5), || async { seen.load(Ordering::SeqCst) == 1 }).await);
    worker.shutdown().await;
    assert_eq!(t.xlen("orphan").await, 0);
}

#[tokio::test]
async fn heartbeat_keeps_long_task_from_being_reclaimed() {
    let Some(t) = setup().await else { return };
    let runs = Arc::new(AtomicUsize::new(0));
    let reg = |runs: Arc<AtomicUsize>| {
        let mut reg = Registry::new();
        reg.register("slow", move |_: Ctx| {
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(1200)).await;
                Ok(())
            }
        })
        .unwrap();
        reg
    };
    // Two workers with a lease far shorter than the task's runtime.
    let w1 = fast(Worker::builder(t.queue.clone(), reg(runs.clone())))
        .lease(Duration::from_millis(300))
        .spawn()
        .await
        .unwrap();
    let w2 = fast(Worker::builder(t.queue.clone(), reg(runs.clone())))
        .lease(Duration::from_millis(300))
        .spawn()
        .await
        .unwrap();

    t.queue.submit("slow", &ctx(1)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(2000)).await;
    w1.shutdown().await;
    w2.shutdown().await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn tasks_for_unregistered_types_are_left_alone() {
    let Some(mut t) = setup().await else { return };
    let mut reg = Registry::new();
    reg.register("known", |_: Ctx| async { Ok(()) }).unwrap();
    let worker = fast(Worker::builder(t.queue.clone(), reg)).spawn().await.unwrap();

    t.queue.submit("unknown", &ctx(1)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    worker.shutdown().await;
    assert_eq!(t.xlen("unknown").await, 1);
}

#[tokio::test]
async fn shutdown_waits_for_in_flight_tasks() {
    let Some(mut t) = setup().await else { return };
    let started = Arc::new(Notify::new());
    let finished = Arc::new(AtomicBool::new(false));
    let mut reg = Registry::new();
    let (s, f) = (started.clone(), finished.clone());
    reg.register("slow", move |_: Ctx| {
        let (s, f) = (s.clone(), f.clone());
        async move {
            s.notify_one();
            tokio::time::sleep(Duration::from_millis(500)).await;
            f.store(true, Ordering::SeqCst);
            Ok(())
        }
    })
    .unwrap();
    let worker = fast(Worker::builder(t.queue.clone(), reg)).spawn().await.unwrap();

    t.queue.submit("slow", &ctx(1)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), started.notified()).await.unwrap();
    worker.shutdown().await;
    assert!(finished.load(Ordering::SeqCst));
    assert_eq!(t.xlen("slow").await, 0);
}

mod chains {
    use super::*;
    use redis_task_graph::{ChainStatus, TaskType};

    const PARSE: TaskType<String, Vec<u32>> = TaskType::new("parse");
    const SUM: TaskType<Vec<u32>, u32> = TaskType::new("sum");
    const FORMAT: TaskType<u32, String> = TaskType::new("format");

    fn register_parse(reg: &mut Registry) {
        reg.register(PARSE, |s| async move {
            s.split(',')
                .map(|n| n.trim().parse::<u32>().map_err(Into::into))
                .collect()
        })
        .unwrap();
    }

    fn register_sum(reg: &mut Registry) {
        reg.register(SUM, |ns| async move { Ok(ns.iter().sum()) }).unwrap();
    }

    fn register_format(reg: &mut Registry) {
        reg.register(FORMAT, |n| async move { Ok(format!("total={n}")) })
            .unwrap();
    }

    const WAIT: Duration = Duration::from_secs(10);

    #[tokio::test]
    async fn steps_run_in_order_across_workers() {
        let Some(t) = setup().await else { return };
        // Each step's processor lives on a different worker, so every hand-off crosses workers.
        let mut workers = Vec::new();
        for register in [register_parse, register_sum, register_format] {
            let mut reg = Registry::new();
            register(&mut reg);
            workers.push(fast(Worker::builder(t.queue.clone(), reg)).spawn().await.unwrap());
        }

        let chain = t
            .queue
            .chain(PARSE, &"1, 2, 3".to_string())
            .then(SUM)
            .then(FORMAT)
            .submit()
            .await
            .unwrap();
        let status = t.queue.wait_for_chain(&chain, WAIT).await.unwrap();
        assert_eq!(status, Some(ChainStatus::Completed("total=6".to_string())));
        for w in workers {
            w.shutdown().await;
        }
    }

    #[tokio::test]
    async fn single_step_chain_records_output() {
        let Some(t) = setup().await else { return };
        let mut reg = Registry::new();
        register_parse(&mut reg);
        let worker = fast(Worker::builder(t.queue.clone(), reg)).spawn().await.unwrap();

        let chain = t.queue.chain(PARSE, &"4,5".to_string()).submit().await.unwrap();
        let status = t.queue.wait_for_chain(&chain, WAIT).await.unwrap();
        assert_eq!(status, Some(ChainStatus::Completed(vec![4, 5])));
        worker.shutdown().await;
    }

    #[tokio::test]
    async fn failed_step_fails_the_chain_and_stops_it() {
        let Some(t) = setup().await else { return };
        let formatted = Arc::new(AtomicUsize::new(0));
        let mut reg = Registry::new();
        register_parse(&mut reg);
        register_sum(&mut reg);
        let f = formatted.clone();
        reg.register(FORMAT, move |n| {
            let f = f.clone();
            async move {
                f.fetch_add(1, Ordering::SeqCst);
                Ok(n.to_string())
            }
        })
        .unwrap();
        let worker = fast(Worker::builder(t.queue.clone(), reg))
            .max_attempts(2)
            .spawn()
            .await
            .unwrap();

        // "x" fails to parse on every attempt, so step 0 is dead-lettered.
        let chain = t
            .queue
            .chain(PARSE, &"1,x".to_string())
            .then(SUM)
            .then(FORMAT)
            .submit()
            .await
            .unwrap();
        let status = t.queue.wait_for_chain(&chain, WAIT).await.unwrap();
        worker.shutdown().await;

        let Some(ChainStatus::Failed { step, error }) = status else {
            panic!("expected failure, got {status:?}");
        };
        assert_eq!(step, 0);
        assert!(error.contains("invalid digit"), "{error}");
        assert_eq!(formatted.load(Ordering::SeqCst), 0, "later steps never run");

        let dead = dead_tasks(&t.queue).await;
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].attempt, 2);
        assert_eq!(dead[0].chain.as_ref().unwrap().chain_id, chain.uuid());
    }

    #[tokio::test]
    async fn retried_step_keeps_its_place_in_the_chain() {
        let Some(t) = setup().await else { return };
        let calls = Arc::new(AtomicUsize::new(0));
        let mut reg = Registry::new();
        register_parse(&mut reg);
        register_format(&mut reg);
        let c = calls.clone();
        reg.register(SUM, move |ns| {
            let c = c.clone();
            async move {
                if c.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err("transient".into());
                }
                Ok(ns.iter().sum())
            }
        })
        .unwrap();
        let worker = fast(Worker::builder(t.queue.clone(), reg)).spawn().await.unwrap();

        let chain = t
            .queue
            .chain(PARSE, &"10,20".to_string())
            .then(SUM)
            .then(FORMAT)
            .submit()
            .await
            .unwrap();
        let status = t.queue.wait_for_chain(&chain, WAIT).await.unwrap();
        worker.shutdown().await;
        assert_eq!(status, Some(ChainStatus::Completed("total=30".to_string())));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn status_reports_progress_and_result_expires() {
        let Some(mut t) = setup().await else { return };
        let release = Arc::new(Notify::new());
        let mut reg = Registry::new();
        let r = release.clone();
        reg.register(PARSE, move |s| {
            let r = r.clone();
            async move {
                r.notified().await;
                Ok(vec![s.len() as u32])
            }
        })
        .unwrap();
        register_sum(&mut reg);
        let worker = fast(Worker::builder(t.queue.clone(), reg)).spawn().await.unwrap();

        let chain = t
            .queue
            .chain(PARSE, &"abc".to_string())
            .then(SUM)
            .result_ttl(Duration::from_secs(30))
            .submit()
            .await
            .unwrap();
        assert_eq!(
            t.queue.chain_status(&chain).await.unwrap(),
            Some(ChainStatus::Running { step: 0, total: 2 })
        );

        release.notify_one();
        let status = t.queue.wait_for_chain(&chain, WAIT).await.unwrap();
        worker.shutdown().await;
        assert_eq!(status, Some(ChainStatus::Completed(3)));

        let key = format!("{{{}}}:chain:{}", t.queue.namespace(), chain.uuid());
        let ttl: i64 = t.conn.ttl(&key).await.unwrap();
        assert!((1..=30).contains(&ttl), "ttl = {ttl}");
    }

    #[tokio::test]
    async fn untyped_steps_can_be_chained_and_output_read_as_json() {
        let Some(t) = setup().await else { return };
        let mut reg = Registry::new();
        register_parse(&mut reg);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let s = seen.clone();
        reg.register("record", move |ns: Vec<u32>| {
            let s = s.clone();
            async move {
                s.lock().unwrap().extend(ns);
                Ok(())
            }
        })
        .unwrap();
        let worker = fast(Worker::builder(t.queue.clone(), reg)).spawn().await.unwrap();

        let chain = t
            .queue
            .chain(PARSE, &"7,8".to_string())
            .then("record")
            .submit()
            .await
            .unwrap()
            .cast::<serde_json::Value>();
        let status = t.queue.wait_for_chain(&chain, WAIT).await.unwrap();
        worker.shutdown().await;
        assert_eq!(status, Some(ChainStatus::Completed(serde_json::Value::Null)));
        assert_eq!(*seen.lock().unwrap(), vec![7, 8]);
    }
}

mod pushback {
    use super::*;

    /// Registers a processor that blocks its runtime thread for `block`, counting executions.
    fn blocking_registry(block: Duration, runs: Arc<AtomicUsize>) -> Registry {
        let mut reg = Registry::new();
        reg.register("block", move |_: Ctx| {
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(block);
                Ok(())
            }
        })
        .unwrap();
        reg
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocked_runtime_lowers_the_limit_and_recovery_raises_it() {
        let Some(t) = setup().await else { return };
        let runs = Arc::new(AtomicUsize::new(0));
        let mut reg = blocking_registry(Duration::from_millis(200), runs.clone());
        reg.register("noop", |_: Ctx| async { Ok(()) }).unwrap();
        let worker = fast(Worker::builder(t.queue.clone(), reg))
            .concurrency(8)
            .lease(Duration::from_millis(300))
            .spawn()
            .await
            .unwrap();
        // Slow start begins at the runtime's thread count.
        assert_eq!(worker.stats().concurrency_limit, 2);

        // A flood of cheap tasks keeps the worker saturated without lag, so the limit grows.
        let flood = |from: u32| {
            let q = t.queue.clone();
            async move {
                for n in from..from + 3000 {
                    q.submit("noop", &ctx(n)).await.unwrap();
                }
            }
        };
        flood(0).await;
        assert!(
            eventually(Duration::from_secs(10), || async { worker.stats().concurrency_limit == 8 }).await,
            "slow start reached the maximum: {:?}",
            worker.stats()
        );

        // Processors that block the runtime push the limit back down.
        for n in 0..16 {
            t.queue.submit("block", &ctx(n)).await.unwrap();
        }
        assert!(
            eventually(Duration::from_secs(5), || async { worker.stats().concurrency_limit < 8 }).await,
            "pushback lowered the limit: {:?}",
            worker.stats()
        );
        assert!(eventually(Duration::from_secs(10), || async { runs.load(Ordering::SeqCst) == 16 }).await);

        // Once the runtime keeps up again, the limit recovers.
        flood(10_000).await;
        assert!(
            eventually(Duration::from_secs(10), || async { worker.stats().concurrency_limit == 8 }).await,
            "limit recovered: {:?}",
            worker.stats()
        );
        worker.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn leases_survive_processors_that_block_every_runtime_thread() {
        let Some(mut t) = setup().await else { return };
        let runs = Arc::new(AtomicUsize::new(0));
        for n in 0..3 {
            t.queue.submit("block", &ctx(n)).await.unwrap();
        }

        // The worker gets its own single-thread runtime, which each task blocks for three leases.
        let worker_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let ns = t.queue.namespace().to_owned();
        let reg = blocking_registry(Duration::from_millis(900), runs.clone());
        let worker = worker_rt
            .spawn(async move {
                let queue = TaskQueue::connect(&redis_url(), ns).await.unwrap();
                fast(Worker::builder(queue, reg))
                    .concurrency(1)
                    .lease(Duration::from_millis(300))
                    .adaptive_concurrency(false)
                    .spawn()
                    .await
                    .unwrap()
            })
            .await
            .unwrap();

        // Meanwhile a thief tries to take over any entry whose lease has lapsed.
        let (stream, group) = (t.queue.stream_key("block"), t.queue.group_name());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while runs.load(Ordering::SeqCst) < 3 || t.xlen("block").await > 0 {
            assert!(tokio::time::Instant::now() < deadline, "tasks did not finish");
            let stolen: redis::streams::StreamAutoClaimReply = redis::cmd("XAUTOCLAIM")
                .arg(&stream)
                .arg(&group)
                .arg("thief")
                .arg(300)
                .arg("0-0")
                .arg("COUNT")
                .arg(10)
                .query_async(&mut t.conn)
                .await
                .unwrap();
            assert!(stolen.claimed.is_empty(), "a lease lapsed while its task was running");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        worker.shutdown().await;
        worker_rt.shutdown_background();
        assert_eq!(runs.load(Ordering::SeqCst), 3, "no task ran twice");
    }
}

mod leases {
    use super::*;
    use redis::streams::StreamReadReply;
    use redis_task_graph::{CancelReason, Error, TaskControl};

    /// Creates the consumer group for `processor_type`'s stream, as a worker would.
    async fn create_group(t: &mut TestQueue, processor_type: &str) -> String {
        let stream = t.queue.stream_key(processor_type);
        let _: () = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&stream)
            .arg(t.queue.group_name())
            .arg("0")
            .arg("MKSTREAM")
            .query_async(&mut t.conn)
            .await
            .unwrap();
        stream
    }

    /// Claims up to `count` new entries as `consumer` and never acks them, like a crashed worker.
    async fn claim_as(t: &mut TestQueue, stream: &str, consumer: &str, count: usize) -> Vec<String> {
        let reply: StreamReadReply = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(t.queue.group_name())
            .arg(consumer)
            .arg("COUNT")
            .arg(count)
            .arg("STREAMS")
            .arg(stream)
            .arg(">")
            .query_async(&mut t.conn)
            .await
            .unwrap();
        reply.keys.into_iter().flat_map(|k| k.ids).map(|e| e.id).collect()
    }

    #[tokio::test]
    async fn heartbeating_task_with_a_short_lease_runs_once() {
        let Some(t) = setup().await else { return };
        let runs = Arc::new(AtomicUsize::new(0));
        let reg = |runs: Arc<AtomicUsize>| {
            let mut reg = Registry::new();
            reg.register_with_control("long", move |_: Ctx, hb: TaskControl| {
                let runs = runs.clone();
                async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    for _ in 0..30 {
                        hb.beat();
                        tokio::time::sleep(Duration::from_millis(40)).await;
                    }
                    Ok(())
                }
            })
            .unwrap()
            .set_lease("long", Duration::from_millis(200))
            .unwrap();
            reg
        };
        // Two workers, so a lease that expired would be picked up by the other one.
        let w1 = fast(Worker::builder(t.queue.clone(), reg(runs.clone()))).spawn().await.unwrap();
        let w2 = fast(Worker::builder(t.queue.clone(), reg(runs.clone()))).spawn().await.unwrap();

        t.queue.submit("long", &ctx(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1800)).await;
        w1.shutdown().await;
        w2.shutdown().await;
        assert_eq!(runs.load(Ordering::SeqCst), 1, "a beating task keeps its 200 ms lease for 1.2 s");
    }

    /// Sets a flag when dropped, to show that a future was aborted.
    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn shutdown_cancels_processors_and_releases_their_tasks() {
        let Some(t) = setup().await else { return };
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let mut reg = Registry::new();
        reg.register_with_control("waits", move |_: Ctx, control: TaskControl| {
            let started_tx = started_tx.clone();
            async move {
                started_tx.send(()).unwrap();
                let reason = control.cancelled().await;
                Err::<(), _>(format!("stopped: {reason:?}").into())
            }
        })
        .unwrap();
        // One attempt only: if the release used it up, the task would be dead-lettered.
        let worker = fast(Worker::builder(t.queue.clone(), reg)).max_attempts(1).spawn().await.unwrap();
        t.queue.submit("waits", &ctx(1)).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), started_rx.recv()).await.unwrap();

        // The processor would wait forever; shutdown returns because it asks it to stop.
        tokio::time::timeout(Duration::from_secs(5), worker.shutdown()).await.expect("shutdown finished");
        assert!(t.queue.dead_letters(10).await.unwrap().is_empty(), "a released task isn't a failure");

        // Another worker picks the released task up and runs it.
        let ran = Arc::new(AtomicUsize::new(0));
        let mut reg = Registry::new();
        let r = ran.clone();
        reg.register("waits", move |_: Ctx| {
            let r = r.clone();
            async move {
                r.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();
        let worker = fast(Worker::builder(t.queue.clone(), reg)).max_attempts(1).spawn().await.unwrap();
        assert!(eventually(Duration::from_secs(5), || async { ran.load(Ordering::SeqCst) == 1 }).await);
        worker.shutdown().await;
        assert!(t.queue.dead_letters(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn processor_is_told_when_its_lease_is_lost() {
        let Some(mut t) = setup().await else { return };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut reg = Registry::new();
        reg.register_with_control("stolen", move |_: Ctx, control: TaskControl| {
            let tx = tx.clone();
            async move {
                tx.send(None).unwrap();
                let reason = tokio::time::timeout(Duration::from_secs(5), control.cancelled()).await;
                tx.send(reason.ok()).unwrap();
                Ok(())
            }
        })
        .unwrap()
        .set_lease("stolen", Duration::from_millis(300))
        .unwrap();
        let worker = fast(Worker::builder(t.queue.clone(), reg)).spawn().await.unwrap();
        t.queue.submit("stolen", &ctx(1)).await.unwrap();
        let wait = Duration::from_secs(5);
        assert_eq!(tokio::time::timeout(wait, rx.recv()).await.unwrap(), Some(None), "started");

        // Another consumer takes the entry over while the processor runs.
        let stream = t.queue.stream_key("stolen");
        let _: redis::Value = redis::cmd("XAUTOCLAIM")
            .arg(&stream)
            .arg(t.queue.group_name())
            .arg("thief")
            .arg(0)
            .arg("0-0")
            .query_async(&mut t.conn)
            .await
            .unwrap();
        let reason = tokio::time::timeout(wait, rx.recv()).await.unwrap();
        worker.shutdown().await;
        assert_eq!(reason, Some(Some(CancelReason::LeaseLost)));
    }

    #[tokio::test]
    async fn processor_that_stops_beating_is_aborted_and_retried() {
        let Some(mut t) = setup().await else { return };
        let calls = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicBool::new(false));
        let mut reg = Registry::new();
        let (c, a) = (calls.clone(), aborted.clone());
        reg.register_with_control("hangs", move |_: Ctx, hb: TaskControl| {
            let (c, a) = (c.clone(), a.clone());
            async move {
                if c.fetch_add(1, Ordering::SeqCst) == 0 {
                    // First attempt: beat once, then hang forever.
                    let _flag = DropFlag(a);
                    hb.beat();
                    std::future::pending::<()>().await;
                }
                Ok(())
            }
        })
        .unwrap()
        .set_lease("hangs", Duration::from_millis(300))
        .unwrap();
        let started = tokio::time::Instant::now();
        let worker = fast(Worker::builder(t.queue.clone(), reg)).spawn().await.unwrap();

        t.queue.submit("hangs", &ctx(1)).await.unwrap();
        assert!(eventually(Duration::from_secs(5), || async { calls.load(Ordering::SeqCst) == 2 }).await);
        assert!(aborted.load(Ordering::SeqCst), "the hung processor's future was dropped");
        // Detection takes one lease plus up to a third of a lease, far below the worker's 30 s.
        assert!(started.elapsed() < Duration::from_secs(3), "took {:?}", started.elapsed());
        worker.shutdown().await;
        assert_eq!(t.xlen("hangs").await, 0);
        assert!(t.queue.dead_letters(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn per_type_lease_sets_how_soon_expired_tasks_are_reclaimed() {
        let Some(mut t) = setup().await else { return };
        let stream = create_group(&mut t, "orphan").await;
        t.queue.submit("orphan", &ctx(1)).await.unwrap();
        assert_eq!(claim_as(&mut t, &stream, "ghost", 1).await.len(), 1);

        let seen = Arc::new(AtomicUsize::new(0));
        let mut reg = Registry::new();
        let s = seen.clone();
        reg.register("orphan", move |_: Ctx| {
            let s = s.clone();
            async move {
                s.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap()
        .set_lease("orphan", Duration::from_millis(200))
        .unwrap();
        // The worker's own lease is the 30 s default; only the type's lease is short.
        let worker = fast(Worker::builder(t.queue.clone(), reg)).spawn().await.unwrap();

        assert!(eventually(Duration::from_secs(3), || async { seen.load(Ordering::SeqCst) == 1 }).await);
        worker.shutdown().await;
    }

    #[tokio::test]
    async fn task_that_keeps_crashing_workers_is_dead_lettered_without_running() {
        let Some(mut t) = setup().await else { return };
        let stream = create_group(&mut t, "poison").await;
        let id = t.queue.submit("poison", &ctx(7)).await.unwrap();
        let entry = claim_as(&mut t, &stream, "crashed-1", 1).await.remove(0);
        // Two more workers took the entry over and also died: three deliveries, no outcome.
        for consumer in ["crashed-2", "crashed-3"] {
            let _: redis::Value = redis::cmd("XCLAIM")
                .arg(&stream)
                .arg(t.queue.group_name())
                .arg(consumer)
                .arg(0)
                .arg(&entry)
                .query_async(&mut t.conn)
                .await
                .unwrap();
        }

        let runs = Arc::new(AtomicUsize::new(0));
        let mut reg = Registry::new();
        let r = runs.clone();
        reg.register("poison", move |_: Ctx| {
            let r = r.clone();
            async move {
                r.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap()
        .set_lease("poison", Duration::from_millis(200))
        .unwrap();
        let worker = fast(Worker::builder(t.queue.clone(), reg)).max_attempts(3).spawn().await.unwrap();

        let dead = eventually(Duration::from_secs(3), || async {
            !t.queue.dead_letters(10).await.unwrap().is_empty()
        })
        .await;
        worker.shutdown().await;
        assert!(dead, "the task should be dead-lettered");
        assert_eq!(runs.load(Ordering::SeqCst), 0, "a crash-looping task must not run again");
        let letters = dead_tasks(&t.queue).await;
        assert_eq!(letters[0].id, id);
        assert!(letters[0].last_error.as_deref().unwrap().contains("delivered 4 times"));
        assert_eq!(t.xlen("poison").await, 0);
    }

    #[tokio::test]
    async fn expired_task_behind_many_live_ones_is_reclaimed() {
        let Some(mut t) = setup().await else { return };
        let stream = create_group(&mut t, "busy").await;
        // 200 tasks held by a healthy consumer, more than one XAUTOCLAIM call examines (10 x the
        // batch size of 16), then one task held by a consumer that crashed.
        for n in 0..200 {
            t.queue.submit("busy", &ctx(n)).await.unwrap();
        }
        let healthy = claim_as(&mut t, &stream, "healthy", 200).await;
        assert_eq!(healthy.len(), 200);
        t.queue.submit("busy", &ctx(999)).await.unwrap();
        assert_eq!(claim_as(&mut t, &stream, "crashed", 1).await.len(), 1);

        // Keep the healthy consumer's leases fresh, as its own heartbeats would.
        let refresher = tokio::spawn({
            let (mut conn, stream, group) = (t.conn.clone(), stream.clone(), t.queue.group_name());
            async move {
                loop {
                    let _: redis::RedisResult<redis::Value> = redis::cmd("XCLAIM")
                        .arg(&stream)
                        .arg(&group)
                        .arg("healthy")
                        .arg(0)
                        .arg(&healthy)
                        .arg("JUSTID")
                        .query_async(&mut conn)
                        .await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        });

        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut reg = Registry::new();
        let s = seen.clone();
        reg.register("busy", move |c: Ctx| {
            let s = s.clone();
            async move {
                s.lock().unwrap().push(c.n);
                Ok(())
            }
        })
        .unwrap()
        .set_lease("busy", Duration::from_millis(300))
        .unwrap();
        let worker = fast(Worker::builder(t.queue.clone(), reg)).spawn().await.unwrap();

        let reclaimed =
            eventually(Duration::from_secs(4), || async { seen.lock().unwrap().contains(&999) }).await;
        refresher.abort();
        worker.shutdown().await;
        assert!(reclaimed, "the crashed consumer's task was never reclaimed");
        assert_eq!(*seen.lock().unwrap(), vec![999], "live tasks must not be reclaimed");
    }

    #[tokio::test]
    async fn more_long_tasks_than_one_renewal_batch_keep_their_leases() {
        let Some(mut t) = setup().await else { return };
        let runs = Arc::new(AtomicUsize::new(0));
        let reg = |runs: Arc<AtomicUsize>| {
            let mut reg = Registry::new();
            reg.register("slow", move |_: Ctx| {
                let runs = runs.clone();
                async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(1200)).await;
                    Ok(())
                }
            })
            .unwrap()
            .set_lease("slow", Duration::from_millis(300))
            .unwrap();
            reg
        };
        // 300 tasks held at once by one worker need three renewal batches every 100 ms. The
        // second worker would reclaim any lease that lapsed.
        let queue = t.queue.clone();
        let worker = |runs| fast(Worker::builder(queue.clone(), reg(runs))).concurrency(300).adaptive_concurrency(false);
        let w1 = worker(runs.clone()).spawn().await.unwrap();
        for n in 0..300 {
            t.queue.submit("slow", &ctx(n)).await.unwrap();
        }
        assert!(eventually(Duration::from_secs(2), || async { runs.load(Ordering::SeqCst) == 300 }).await);
        let w2 = worker(runs.clone()).spawn().await.unwrap();
        // Shutdown waits for in-flight tasks, giving w2 a second to reclaim any lapsed lease.
        w1.shutdown().await;
        w2.shutdown().await;
        assert_eq!(t.xlen("slow").await, 0);
        assert_eq!(runs.load(Ordering::SeqCst), 300, "no lease lapsed, so no task ran twice");
    }

    #[tokio::test]
    async fn leases_below_the_minimum_are_rejected() {
        let Some(t) = setup().await else { return };
        let mut reg = Registry::new();
        reg.register("t", |_: Ctx| async { Ok(()) }).unwrap();
        let short = fast(Worker::builder(t.queue.clone(), reg.clone())).lease(Duration::from_millis(10));
        assert!(matches!(short.spawn().await, Err(Error::InvalidConfig(_))));

        reg.set_lease("t", Duration::from_millis(50)).unwrap();
        let short_type = fast(Worker::builder(t.queue.clone(), reg));
        assert!(matches!(short_type.spawn().await, Err(Error::InvalidConfig(_))));
    }
}

/// Data written in the 0.1 key layout is drained by 0.2 workers.
mod migration {
    use super::*;
    use redis_task_graph::{ChainId, ChainStatus, TaskType};

    const OLD: TaskType<Ctx, u32> = TaskType::new("old");

    fn envelope(n: u32, chain: Option<uuid::Uuid>) -> String {
        let mut env = serde_json::json!({
            "id": uuid::Uuid::new_v4(),
            "processor_type": "old",
            "context": ctx(n),
            "attempt": 0,
            "enqueued_at": 0,
        });
        if let Some(chain_id) = chain {
            env["chain"] = serde_json::json!({
                "chain_id": chain_id, "step": 0, "remaining": [], "result_ttl_secs": 60,
            });
        }
        env.to_string()
    }

    #[tokio::test]
    async fn tasks_retries_and_chains_from_0_1_are_drained() {
        let Some(mut t) = setup().await else { return };
        let ns = t.queue.namespace().to_owned();
        let (old_stream, old_delayed) = (format!("{ns}:stream:old"), format!("{ns}:delayed"));
        // As 0.1 left them: a stream with its group and a queued task that is the last step of
        // a chain, and a due retry in the shared retry set.
        let _: () = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&old_stream)
            .arg(t.queue.group_name())
            .arg("0")
            .arg("MKSTREAM")
            .query_async(&mut t.conn)
            .await
            .unwrap();
        let chain = uuid::Uuid::new_v4();
        let _: String = t.conn.xadd(&old_stream, "*", &[("task", envelope(1, Some(chain)))]).await.unwrap();
        let _: () = t.conn.zadd(&old_delayed, envelope(2, None), 0).await.unwrap();

        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut reg = Registry::new();
        let s = seen.clone();
        reg.register(OLD, move |c: Ctx| {
            let s = s.clone();
            async move {
                s.lock().unwrap().push(c.n);
                Ok(c.n * 10)
            }
        })
        .unwrap();
        let worker = fast(Worker::builder(t.queue.clone(), reg)).spawn().await.unwrap();
        let both = eventually(Duration::from_secs(5), || async { seen.lock().unwrap().len() == 2 }).await;
        worker.shutdown().await;
        assert!(both, "ran {:?}", seen.lock().unwrap());

        let status = t.queue.chain_status(&ChainId::<u32>::from_uuid(chain)).await.unwrap();
        assert_eq!(status, Some(ChainStatus::Completed(10)), "the 0.1 chain's status is still readable");
        let left: usize = t.conn.xlen(&old_stream).await.unwrap();
        let retries: usize = t.conn.zcard(&old_delayed).await.unwrap();
        assert_eq!((left, retries), (0, 0));
        assert!(t.queue.dead_letters(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn namespaces_with_braces_are_rejected() {
        let client = redis::Client::open(redis_url()).unwrap();
        let err = TaskQueue::from_client(client, "a{b}").await.unwrap_err();
        assert!(matches!(err, redis_task_graph::Error::InvalidNamespace(_)), "{err}");
    }
}

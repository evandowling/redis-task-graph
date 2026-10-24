//! Integration tests against a live Redis (7.x). Set `REDIS_URL` to point elsewhere than
//! `redis://127.0.0.1/`; tests are skipped when Redis is unreachable.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use redis::AsyncCommands;
use redis_task_graph::{Registry, TaskQueue, Worker};
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
        let pattern = format!("{}:*", self.queue.namespace());
        tokio::spawn(async move {
            let keys: Vec<String> = conn.keys(pattern).await.unwrap_or_default();
            if !keys.is_empty() {
                let _: redis::RedisResult<()> = conn.del(keys).await;
            }
        });
    }
}

async fn setup() -> Option<TestQueue> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".into());
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

    let dead = t.queue.dead_letters(10).await.unwrap();
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
    let dead = t.queue.dead_letters(10).await.unwrap();
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
    let dead = t.queue.dead_letters(10).await.unwrap();
    assert_eq!(dead[0].attempt, 0);
    assert!(dead[0].last_error.as_deref().unwrap().contains("invalid context"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
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

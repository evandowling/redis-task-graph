//! Two printer tasks, "ping" and "pong", rally until one of them loses.
//!
//! Each printer prints its context, then flips a coin: on heads it prints "I lost" and the game
//! ends; on tails it submits a task to the other printer with its own name as the context.
//!
//! Requires Redis: `REDIS_URL=redis://127.0.0.1/ cargo run --example ping_pong`

use std::sync::Arc;

use redis_task_graph::{Registry, TaskQueue, Worker};
use tokio::sync::Notify;

/// Registers a printer named `name` that returns volleys to `opponent`.
fn register_printer(
    registry: &mut Registry,
    queue: &TaskQueue,
    game_over: &Arc<Notify>,
    name: &'static str,
    opponent: &'static str,
) -> redis_task_graph::Result<()> {
    let (queue, game_over) = (queue.clone(), game_over.clone());
    registry.register(name, move |context: String| {
        let (queue, game_over) = (queue.clone(), game_over.clone());
        async move {
            println!("[{name}] {context}");
            if rand::random_bool(0.5) {
                println!("[{name}] I lost");
                game_over.notify_one();
            } else {
                queue.submit(opponent, &name).await?;
            }
            Ok(())
        }
    })?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".into());
    let queue = TaskQueue::connect(&url, "ping-pong").await?;
    let game_over = Arc::new(Notify::new());

    let mut registry = Registry::new();
    register_printer(&mut registry, &queue, &game_over, "ping", "pong")?;
    register_printer(&mut registry, &queue, &game_over, "pong", "ping")?;

    let worker = Worker::builder(queue.clone(), registry).spawn().await?;

    // Serve the first ball: pong receives a volley from ping.
    queue.submit("pong", &"ping").await?;

    game_over.notified().await;
    worker.shutdown().await;
    Ok(())
}

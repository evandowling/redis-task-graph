//! Submits a few tasks and processes them with a background worker.
//!
//! Requires Redis: `REDIS_URL=redis://127.0.0.1/ cargo run --example basic`

use std::time::Duration;

use redis_task_graph::{Registry, TaskQueue, Worker};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct SendEmail {
    to: String,
    subject: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ResizeImage {
    key: String,
    width: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".into());
    let queue = TaskQueue::connect(&url, "example").await?;

    let mut registry = Registry::new();
    registry
        .register("send_email", |ctx: SendEmail| async move {
            println!("emailing {} about {:?}", ctx.to, ctx.subject);
            Ok(())
        })?
        .register("resize_image", |ctx: ResizeImage| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            println!("resized {} to {}px", ctx.key, ctx.width);
            Ok(())
        })?;

    let worker = Worker::builder(queue.clone(), registry)
        .concurrency(8)
        .spawn()
        .await?;

    for i in 0..3 {
        queue
            .submit(
                "send_email",
                &SendEmail {
                    to: format!("user{i}@example.com"),
                    subject: "hello".into(),
                },
            )
            .await?;
        queue
            .submit(
                "resize_image",
                &ResizeImage {
                    key: format!("img-{i}.png"),
                    width: 256,
                },
            )
            .await?;
    }

    tokio::time::sleep(Duration::from_secs(2)).await;
    worker.shutdown().await;
    Ok(())
}

mod delaypool;
mod prioritytaskpool;
mod taskpool;

use std::{any::Any, panic::AssertUnwindSafe};

use futures::FutureExt;

pub use delaypool::DelayPool;
pub use prioritytaskpool::PriorityTaskPool;
pub use taskpool::{BoxTask, TaskPool};

async fn run_task(pool: &str, task: BoxTask) {
    if let Err(panic) = AssertUnwindSafe(task).catch_unwind().await {
        tracing::error!(
            pool,
            panic = panic_message(panic.as_ref()),
            "panic in pool worker"
        );
    }
}

fn panic_message(panic: &(dyn Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

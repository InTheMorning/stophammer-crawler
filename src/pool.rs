use std::future::Future;
use std::sync::Arc;

use tokio::sync::Semaphore;

/// Run tasks with bounded concurrency. Returns when all complete.
pub async fn run_pool<F, Fut>(tasks: Vec<F>, concurrency: usize)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    let sem = Arc::new(Semaphore::new(concurrency));
    let mut handles = Vec::with_capacity(tasks.len());

    for task in tasks {
        let permit = Arc::clone(&sem).acquire_owned().await.expect("semaphore closed");
        handles.push(tokio::spawn(async move {
            task().await;
            drop(permit);
        }));
    }

    for h in handles {
        let _: Result<(), _> = h.await;
    }
}

//! Lossless admission control for blocking WebSocket work.
//!
//! Tokio's blocking pool has a deliberately high global thread limit. WebSocket
//! fan-out can otherwise turn a burst of independent jobs into thousands of OS
//! threads. These lanes cap only concurrently executing blocking jobs; callers
//! remain queued until a permit is available. Subscription builds stop waiting
//! when their connection closes.

use std::{
    env,
    error::Error,
    fmt,
    num::NonZeroUsize,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

const COMMAND_CONCURRENCY_ENV: &str = "MYKO_WS_COMMAND_BLOCKING_CONCURRENCY";
const SUBSCRIPTION_CONCURRENCY_ENV: &str = "MYKO_WS_SUBSCRIPTION_BLOCKING_CONCURRENCY";
const CLEANUP_CONCURRENCY_ENV: &str = "MYKO_WS_CLEANUP_BLOCKING_CONCURRENCY";

pub static WS_COMMAND_BLOCKING: LazyLock<Arc<BlockingLane>> = LazyLock::new(|| {
    let parallelism = available_parallelism();
    BlockingLane::from_env("websocket-command", COMMAND_CONCURRENCY_ENV, parallelism)
});

pub static WS_SUBSCRIPTION_BLOCKING: LazyLock<Arc<BlockingLane>> = LazyLock::new(|| {
    let parallelism = available_parallelism();
    BlockingLane::from_env(
        "websocket-subscription",
        SUBSCRIPTION_CONCURRENCY_ENV,
        parallelism.saturating_div(2).max(4),
    )
});

pub static WS_CLEANUP_BLOCKING: LazyLock<Arc<BlockingLane>> = LazyLock::new(|| {
    let parallelism = available_parallelism();
    BlockingLane::from_env(
        "websocket-cleanup",
        CLEANUP_CONCURRENCY_ENV,
        parallelism.saturating_div(8).clamp(2, 8),
    )
});

#[derive(Debug)]
pub enum BlockingWorkError {
    LaneClosed { lane: &'static str },
    Join(tokio::task::JoinError),
}

impl fmt::Display for BlockingWorkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LaneClosed { lane } => write!(formatter, "blocking lane {lane} is closed"),
            Self::Join(error) => write!(formatter, "blocking job failed: {error}"),
        }
    }
}

impl Error for BlockingWorkError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::LaneClosed { .. } => None,
            Self::Join(error) => Some(error),
        }
    }
}

pub struct BlockingLane {
    name: &'static str,
    concurrency: usize,
    semaphore: Arc<Semaphore>,
    queued: AtomicUsize,
    active: AtomicUsize,
}

impl BlockingLane {
    fn from_env(name: &'static str, env_var: &'static str, default: usize) -> Arc<Self> {
        let concurrency = env::var(env_var)
            .ok()
            .and_then(|value| value.parse::<NonZeroUsize>().ok())
            .map_or(default, NonZeroUsize::get);
        tracing::info!(
            lane = name,
            concurrency,
            env_var,
            "configured blocking work lane"
        );
        Arc::new(Self::new(name, concurrency))
    }

    fn new(name: &'static str, concurrency: usize) -> Self {
        let concurrency = concurrency.max(1);
        Self {
            name,
            concurrency,
            semaphore: Arc::new(Semaphore::new(concurrency)),
            queued: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
        }
    }

    #[allow(
        clippy::significant_drop_tightening,
        reason = "the semaphore permit must remain held for the entire blocking job"
    )]
    pub async fn run<F, R>(
        self: &Arc<Self>,
        job_kind: &'static str,
        job: F,
    ) -> Result<R, BlockingWorkError>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let queued = self
            .queued
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if self.semaphore.available_permits() == 0 && (queued == 1 || queued.is_power_of_two()) {
            tracing::warn!(
                lane = self.name,
                job_kind,
                queued,
                active = self.active.load(Ordering::Relaxed),
                concurrency = self.concurrency,
                "blocking work is queued; jobs are being retained"
            );
        }

        let queued_guard = QueuedJobGuard(self.clone());
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| BlockingWorkError::LaneClosed { lane: self.name })?;
        drop(queued_guard);

        self.active.fetch_add(1, Ordering::Relaxed);
        let guard = ActiveJobGuard {
            lane: self.clone(),
            _permit: permit,
        };
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            job()
        })
        .await
        .map_err(BlockingWorkError::Join)
    }

    pub fn spawn_for_receiver_until<T, F, R>(
        self: &Arc<Self>,
        job_kind: &'static str,
        sender: mpsc::UnboundedSender<T>,
        cancel: CancellationToken,
        job: F,
    ) where
        T: Send + 'static,
        F: FnOnce(mpsc::UnboundedSender<T>) -> R + Send + 'static,
        R: Send + 'static,
    {
        let lane = self.clone();
        std::mem::drop(tokio::spawn(async move {
            let completion_sender = sender.clone();
            let cancel_for_job = cancel.clone();
            tokio::select! {
                biased;
                () = cancel.cancelled() => {}
                () = sender.closed() => {}
                result = lane.run(job_kind, move || {
                    // NOTE(ts): The blocking pool may queue this after lane admission.
                    if completion_sender.is_closed() || cancel_for_job.is_cancelled() {
                        return None;
                    }
                    Some(job(completion_sender))
                }) => {
                    if let Err(error) = result {
                        tracing::error!(lane = lane.name, job_kind, %error, "blocking work failed");
                    }
                }
            }
        }));
    }

    pub fn spawn<F, R>(self: &Arc<Self>, job_kind: &'static str, job: F)
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let lane = self.clone();
        std::mem::drop(tokio::spawn(async move {
            if let Err(error) = lane.run(job_kind, job).await {
                tracing::error!(lane = lane.name, job_kind, %error, "blocking work failed");
            }
        }));
    }
}

struct QueuedJobGuard(Arc<BlockingLane>);

impl Drop for QueuedJobGuard {
    fn drop(&mut self) {
        self.0.queued.fetch_sub(1, Ordering::Relaxed);
    }
}

struct ActiveJobGuard {
    lane: Arc<BlockingLane>,
    _permit: OwnedSemaphorePermit,
}

impl Drop for ActiveJobGuard {
    fn drop(&mut self) {
        self.lane.active.fetch_sub(1, Ordering::Relaxed);
    }
}

fn available_parallelism() -> usize {
    std::thread::available_parallelism().map_or(4, NonZeroUsize::get)
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    reason = "test synchronization failures must fail the test"
)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use super::BlockingLane;

    async fn wait_for_count(counter: &AtomicUsize, expected: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while counter.load(Ordering::SeqCst) != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("lane count did not settle");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_build_releases_queued_capture_without_disconnect() {
        let lane = Arc::new(BlockingLane::new("test", 1));
        let _permit = lane
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("test permit");
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (capture, released) = tokio::sync::oneshot::channel::<()>();
        let cancel = tokio_util::sync::CancellationToken::new();
        lane.spawn_for_receiver_until("cancelled", sender, cancel.clone(), move |_| {
            let _capture = capture;
        });
        wait_for_count(&lane.queued, 1).await;
        cancel.cancel();
        assert!(
            tokio::time::timeout(Duration::from_secs(2), released)
                .await
                .is_ok()
        );
        wait_for_count(&lane.queued, 0).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn already_disconnected_receiver_never_runs_factory() {
        let lane = Arc::new(BlockingLane::new("test", 1));
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<()>();
        drop(receiver);
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel::<()>();
        let executed = Arc::new(AtomicUsize::new(0));
        let job_executed = executed.clone();
        lane.spawn_for_receiver_until(
            "closed",
            sender,
            tokio_util::sync::CancellationToken::new(),
            move |_| {
                let _completion = dropped_tx;
                job_executed.fetch_add(1, Ordering::SeqCst);
            },
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(5), dropped_rx)
                .await
                .expect("test operation must succeed")
                .is_err()
        );
        assert_eq!(executed.load(Ordering::SeqCst), 0);
        assert_eq!(lane.queued.load(Ordering::SeqCst), 0);
        assert_eq!(lane.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disconnected_receiver_cancels_backlog_without_running_factories() {
        let lane = Arc::new(BlockingLane::new("test", 1));
        let permit = lane
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("test operation must succeed");
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<()>();
        let executed = Arc::new(AtomicUsize::new(0));
        for _ in 0..128 {
            let executed = executed.clone();
            lane.spawn_for_receiver_until(
                "abandoned",
                sender.clone(),
                tokio_util::sync::CancellationToken::new(),
                move |_| {
                    executed.fetch_add(1, Ordering::SeqCst);
                },
            );
        }
        let (live_sender, mut live_receiver) = tokio::sync::mpsc::unbounded_channel();
        lane.spawn_for_receiver_until(
            "live",
            live_sender,
            tokio_util::sync::CancellationToken::new(),
            |sender| {
                sender.send(42).expect("test operation must succeed");
            },
        );
        wait_for_count(&lane.queued, 129).await;
        drop(receiver);
        wait_for_count(&lane.queued, 1).await;
        assert_eq!(executed.load(Ordering::SeqCst), 0);
        assert_eq!(lane.active.load(Ordering::SeqCst), 0);
        drop(permit);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), live_receiver.recv())
                .await
                .expect("test operation must succeed"),
            Some(42)
        );
        wait_for_count(&lane.active, 0).await;
        assert_eq!(lane.queued.load(Ordering::SeqCst), 0);
        assert_eq!(executed.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disconnect_does_not_release_running_job_permit_early() {
        let lane = Arc::new(BlockingLane::new("test", 1));
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        lane.spawn_for_receiver_until(
            "running",
            sender,
            tokio_util::sync::CancellationToken::new(),
            move |_| {
                started_tx.send(()).expect("test operation must succeed");
                release_rx.recv().expect("test operation must succeed");
            },
        );
        tokio::time::timeout(Duration::from_secs(5), started_rx)
            .await
            .expect("test operation must succeed")
            .expect("test operation must succeed");
        drop(receiver);
        let (next_sender, mut next_receiver) = tokio::sync::mpsc::unbounded_channel();
        lane.spawn_for_receiver_until(
            "next",
            next_sender,
            tokio_util::sync::CancellationToken::new(),
            |sender| {
                sender.send(()).expect("test operation must succeed");
            },
        );
        wait_for_count(&lane.queued, 1).await;
        assert_eq!(lane.active.load(Ordering::SeqCst), 1);
        assert_eq!(lane.semaphore.available_permits(), 0);
        release_tx.send(()).expect("test operation must succeed");
        tokio::time::timeout(Duration::from_secs(5), next_receiver.recv())
            .await
            .expect("test operation must succeed")
            .expect("test operation must succeed");
        wait_for_count(&lane.active, 0).await;
        assert_eq!(lane.queued.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_lane_caps_concurrency_without_dropping_work() {
        const JOB_COUNT: usize = 12;
        const CONCURRENCY: usize = 2;

        let lane = Arc::new(BlockingLane::new("test", CONCURRENCY));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::with_capacity(JOB_COUNT);

        for _ in 0..JOB_COUNT {
            let lane = lane.clone();
            let active = active.clone();
            let max_active = max_active.clone();
            let completed = completed.clone();
            tasks.push(tokio::spawn(async move {
                lane.run("test-job", move || {
                    let now_active = active.fetch_add(1, Ordering::SeqCst).saturating_add(1);
                    max_active.fetch_max(now_active, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(10));
                    active.fetch_sub(1, Ordering::SeqCst);
                    completed.fetch_add(1, Ordering::SeqCst);
                })
                .await
                .is_ok()
            }));
        }

        for task in tasks {
            assert!(matches!(task.await, Ok(true)));
        }

        assert_eq!(completed.load(Ordering::SeqCst), JOB_COUNT);
        assert!(max_active.load(Ordering::SeqCst) <= CONCURRENCY);
        assert_eq!(lane.queued.load(Ordering::SeqCst), 0);
        assert_eq!(lane.active.load(Ordering::SeqCst), 0);
    }
}

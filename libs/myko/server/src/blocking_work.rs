//! Lossless admission control for blocking WebSocket work.
//!
//! Tokio's blocking pool has a deliberately high global thread limit. WebSocket
//! fan-out can otherwise turn a burst of independent jobs into thousands of OS
//! threads. These lanes cap only concurrently executing blocking jobs; callers
//! remain queued until a permit is available.

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

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

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

        let permit = self.semaphore.clone().acquire_owned().await.map_err(|_| {
            self.queued.fetch_sub(1, Ordering::Relaxed);
            BlockingWorkError::LaneClosed { lane: self.name }
        })?;
        self.queued.fetch_sub(1, Ordering::Relaxed);

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
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use super::BlockingLane;

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

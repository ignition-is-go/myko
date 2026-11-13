use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{SubscriptionReady, ViewError, WS_SUBSCRIPTION_BLOCKING};

pub(super) struct SubscriptionBuild {
    cancel: CancellationToken,
}

pub(super) struct SubscriptionBuilt {
    pub tx: Arc<str>,
    pub owner: Arc<SubscriptionBuild>,
    pub result: Option<Result<SubscriptionReady, ViewError>>,
}

struct BuildCompletion {
    sender: mpsc::UnboundedSender<SubscriptionBuilt>,
    built: Option<SubscriptionBuilt>,
}

impl Drop for BuildCompletion {
    fn drop(&mut self) {
        if let Some(built) = self.built.take()
            && !built.owner.cancel.is_cancelled()
        {
            let _ = self.sender.send(built);
        }
    }
}

#[derive(Default)]
pub(super) struct SubscriptionBuilds {
    pending: Mutex<HashMap<Arc<str>, Arc<SubscriptionBuild>>>,
}

impl SubscriptionBuilds {
    pub fn begin(&self, tx: Arc<str>) -> Arc<SubscriptionBuild> {
        let owner = Arc::new(SubscriptionBuild {
            cancel: CancellationToken::new(),
        });
        let predecessor = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(tx, owner.clone());
        if let Some(predecessor) = predecessor {
            predecessor.cancel.cancel();
        }
        owner
    }

    pub fn cancel(&self, tx: &str) {
        let owner = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(tx);
        if let Some(owner) = owner {
            owner.cancel.cancel();
        }
    }

    pub fn finish(&self, built: &SubscriptionBuilt) -> bool {
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !pending
            .get(&built.tx)
            .is_some_and(|owner| Arc::ptr_eq(owner, &built.owner))
        {
            return false;
        }
        pending.remove(&built.tx);
        true
    }

    pub fn spawn(
        &self,
        job_kind: &'static str,
        tx: Arc<str>,
        sender: mpsc::UnboundedSender<SubscriptionBuilt>,
        job: impl FnOnce() -> Option<Result<SubscriptionReady, ViewError>> + Send + 'static,
    ) {
        let owner = self.begin(tx.clone());
        let cancel = owner.cancel.clone();
        let completion = BuildCompletion {
            sender: sender.clone(),
            built: Some(SubscriptionBuilt {
                tx,
                owner,
                result: None,
            }),
        };
        WS_SUBSCRIPTION_BLOCKING.spawn_for_receiver_until(job_kind, sender, cancel, move |_| {
            let mut completion = completion;
            let result = job();
            if let Some(built) = completion.built.as_mut() {
                built.result = result;
            }
        });
    }
}

impl Drop for SubscriptionBuilds {
    fn drop(&mut self) {
        for (_, owner) in self
            .pending
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain()
        {
            owner.cancel.cancel();
        }
    }
}

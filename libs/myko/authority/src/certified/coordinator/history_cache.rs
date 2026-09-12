use std::sync::Arc;

use myko_federation::Node;
use tokio::sync::Mutex;

use super::{AuthorityAnchor, AuthorityHistory};

#[derive(Debug)]
pub(in crate::certified) struct AuthorityHistoryCache {
    node: Node,
    anchor: AuthorityAnchor,
    snapshot: Mutex<Option<Arc<AuthorityHistory>>>,
}

impl AuthorityHistoryCache {
    pub(in crate::certified) fn new(node: Node, anchor: AuthorityAnchor) -> Self {
        Self {
            node,
            anchor,
            snapshot: Mutex::new(None),
        }
    }

    pub(in crate::certified) async fn history_for_exact_snapshot(
        &self,
    ) -> Result<Arc<AuthorityHistory>, String> {
        let mut cached = self.snapshot.lock().await;
        let events = self
            .node
            .events_after(None)
            .map_err(|error| error.to_string())?;
        // New evidence can invalidate a chain without advancing its retained head.
        if let Some(history) = cached.as_ref()
            && history.history() == events
        {
            return Ok(Arc::clone(history));
        }
        let anchor = self.anchor.clone();
        let previous = cached.as_ref().map(Arc::clone);
        let started = std::time::Instant::now();
        let event_count = events.len();
        let executor = tokio::runtime::Handle::try_current()
            .map_err(|error| format!("authority history verification requires Tokio: {error}"))?;
        // A cancelled replay must not keep the node's journal open in a blocking task.
        let history = Arc::new(
            executor
                .spawn_blocking(move || match previous {
                    Some(previous) => previous.refresh(events),
                    None => AuthorityHistory::from_events(events, anchor),
                })
                .await
                .map_err(|error| {
                    format!("authority history verification task failed: {error}")
                })??,
        );
        *cached = Some(Arc::clone(&history));
        drop(cached);
        tracing::debug!(
            event_count,
            elapsed_ms = started.elapsed().as_millis(),
            "authority history refreshed"
        );
        Ok(history)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future as _,
        task::{Context, Poll, Waker},
        time::Duration,
    };

    use ed25519_dalek::SigningKey;
    use myko_federation::control_quorum::{
        ControlBallot, ControlEpochId, ControlHead, ControlQuorumVerifier, ControlSlot,
        ControllerId,
    };

    use super::*;
    use crate::{AuthorityRealmKey, authority_realm_scope};

    #[tokio::test]
    async fn exact_snapshot_reuse_does_not_hide_new_evidence_at_the_same_head()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let node = myko_redb::RedbJournal::open_node(directory.path().join("history.redb"))?;
        let key = SigningKey::from_bytes(&[31; 32]);
        let controller = ControllerId(key.verifying_key().to_bytes());
        let realm = AuthorityRealmKey::new("history-cache");
        let epoch = ControlEpochId([32; 32]);
        let genesis = ControlHead([33; 32]);
        let anchor = AuthorityAnchor::new(realm.clone(), epoch, genesis, vec![controller])?;
        let cache = Arc::new(AuthorityHistoryCache::new(node.clone(), anchor));
        let first = cache.history_for_exact_snapshot().await?;
        let repeated = cache.history_for_exact_snapshot().await?;
        if !Arc::ptr_eq(&first, &repeated) {
            return Err("unchanged history was not reused".into());
        }
        let verifier = ControlQuorumVerifier::new(
            ControlSlot {
                realm: authority_realm_scope(&realm),
                epoch,
                predecessor: genesis,
            },
            [controller],
        )?;
        node.vote_control(
            &verifier.prepare_request(ControlBallot {
                counter: 1,
                proposer: controller,
            })?,
            &key,
        )?;
        let changed = cache.history_for_exact_snapshot().await?;
        if Arc::ptr_eq(&first, &changed)
            || first.retained_head()? != changed.retained_head()?
            || changed.history() != node.events_after(None)?
        {
            return Err("new evidence at the same head did not refresh cached history".into());
        }
        if !Arc::ptr_eq(&changed, &cache.history_for_exact_snapshot().await?) {
            return Err("refreshed history was not reused".into());
        }
        Ok(())
    }

    #[tokio::test]
    async fn contended_history_replay_yields_the_async_executor() -> Result<(), String> {
        let key = SigningKey::from_bytes(&[41; 32]);
        let anchor = AuthorityAnchor::new(
            AuthorityRealmKey::new("history-cache-responsiveness"),
            ControlEpochId([42; 32]),
            ControlHead([43; 32]),
            vec![ControllerId(key.verifying_key().to_bytes())],
        )?;
        let cache = Arc::new(AuthorityHistoryCache::new(Node::in_memory(), anchor));
        let held = Arc::clone(&cache);
        let (ready, acquired) = flume::bounded(1);
        let (release, released) = flume::bounded(1);
        let blocker = std::thread::spawn(move || -> Result<(), String> {
            let guard = held.snapshot.blocking_lock();
            ready.send(()).map_err(|error| error.to_string())?;
            // Release even if a regression blocks the test's only executor thread.
            let _ = released.recv_timeout(Duration::from_secs(2));
            drop(guard);
            Ok(())
        });
        acquired
            .recv_async()
            .await
            .map_err(|error| error.to_string())?;
        let mut replay = std::pin::pin!(cache.history_for_exact_snapshot());
        let first_poll =
            std::future::poll_fn(|context| Poll::Ready(replay.as_mut().poll(context))).await;
        let _ = release.send(());
        blocker
            .join()
            .map_err(|_| "cache lock holder panicked".to_owned())??;
        match first_poll {
            Poll::Pending => {
                replay.await?;
                Ok(())
            }
            Poll::Ready(_) => Err(
                "history replay blocked the executor until the lock holder timed out".to_owned(),
            ),
        }
    }

    #[test]
    fn replay_without_an_executor_returns_an_error() -> Result<(), String> {
        let key = SigningKey::from_bytes(&[51; 32]);
        let anchor = AuthorityAnchor::new(
            AuthorityRealmKey::new("history-cache-no-executor"),
            ControlEpochId([52; 32]),
            ControlHead([53; 32]),
            vec![ControllerId(key.verifying_key().to_bytes())],
        )?;
        let cache = AuthorityHistoryCache::new(Node::in_memory(), anchor);
        let mut replay = std::pin::pin!(cache.history_for_exact_snapshot());
        let mut context = Context::from_waker(Waker::noop());
        match replay.as_mut().poll(&mut context) {
            Poll::Ready(Err(error)) if error.contains("requires Tokio") => Ok(()),
            _ => Err("history replay did not report its missing executor".to_owned()),
        }
    }
}

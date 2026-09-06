use std::sync::{Arc, Mutex};

use myko_federation::Node;

use super::{AuthorityAnchor, AuthorityHistory};

#[derive(Debug)]
pub(super) struct AuthorityHistoryCache {
    node: Node,
    anchor: AuthorityAnchor,
    snapshot: Mutex<Option<Arc<AuthorityHistory>>>,
}

impl AuthorityHistoryCache {
    pub(super) const fn new(node: Node, anchor: AuthorityAnchor) -> Self {
        Self {
            node,
            anchor,
            snapshot: Mutex::new(None),
        }
    }

    pub(super) fn history_for_exact_snapshot(&self) -> Result<Arc<AuthorityHistory>, String> {
        let mut cached = self.snapshot.lock().map_err(|error| error.to_string())?;
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
        let history = Arc::new(AuthorityHistory::from_events(events, self.anchor.clone())?);
        *cached = Some(Arc::clone(&history));
        drop(cached);
        Ok(history)
    }
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;
    use myko_federation::control_quorum::{
        ControlBallot, ControlEpochId, ControlHead, ControlQuorumVerifier, ControlSlot,
        ControllerId,
    };

    use super::*;
    use crate::{AuthorityRealmKey, authority_realm_scope};

    #[test]
    fn exact_snapshot_reuse_does_not_hide_new_evidence_at_the_same_head()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let node = myko_redb::RedbJournal::open_node(directory.path().join("history.redb"))?;
        let key = SigningKey::from_bytes(&[31; 32]);
        let controller = ControllerId(key.verifying_key().to_bytes());
        let realm = AuthorityRealmKey::new("history-cache");
        let epoch = ControlEpochId([32; 32]);
        let genesis = ControlHead([33; 32]);
        let anchor = AuthorityAnchor::new(realm.clone(), epoch, genesis, vec![controller])?;
        let cache = AuthorityHistoryCache::new(node.clone(), anchor);
        let first = cache.history_for_exact_snapshot()?;
        let repeated = cache.history_for_exact_snapshot()?;
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
        let changed = cache.history_for_exact_snapshot()?;
        if Arc::ptr_eq(&first, &changed)
            || first.retained_head()? != changed.retained_head()?
            || changed.history() != node.events_after(None)?
        {
            return Err("new evidence at the same head did not refresh cached history".into());
        }
        if !Arc::ptr_eq(&changed, &cache.history_for_exact_snapshot()?) {
            return Err("refreshed history was not reused".into());
        }
        Ok(())
    }
}

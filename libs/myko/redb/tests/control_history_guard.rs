use std::error::Error;

use ed25519_dalek::SigningKey;
use myko_federation::{NodeError, ScopeId, control_quorum::*};
use myko_redb::RedbJournal;

#[test]
fn validated_history_is_checked_before_vote_and_proposal_release() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("guarded.redb");
    let node = RedbJournal::open_node(&path)?;
    let key = SigningKey::from_bytes(&[42; 32]);
    let controller = ControllerId(key.verifying_key().to_bytes());
    let verifier = ControlQuorumVerifier::new(
        ControlSlot {
            realm: ScopeId::new("guarded"),
            epoch: ControlEpochId([1; 32]),
            predecessor: ControlHead([2; 32]),
        },
        [controller],
    )?;
    let ballot = ControlBallot {
        counter: 1,
        proposer: controller,
    };
    let initial = node.events_after(None)?;
    let request = verifier.prepare_request(ballot)?.bind_history(&initial);
    let promise = node.vote_control(&request, &key)?;
    let after_prepare = node.events_after(None)?;
    if node.vote_control(&request, &key)
        != Err(NodeError::ControlVote(ControlQuorumError::HistoryChanged))
        || node.events_after(None)? != after_prepare
    {
        return Err("stale snapshot released a vote or modified the journal".into());
    }
    let refreshed = verifier
        .prepare_request(ballot)?
        .bind_history(&after_prepare);
    if node.vote_control(&refreshed, &key)? != promise || node.events_after(None)? != after_prepare
    {
        return Err("revalidated prepare retry did not recover without append".into());
    }
    let prepared = verifier.verify_prepare(ballot, &[promise])?;
    let value = ControlValue(b"chosen".to_vec());
    let stale = prepared.proposal_request(&value)?.bind_history(&initial);
    if node.propose_control(&stale, &key)
        != Err(NodeError::ControlVote(ControlQuorumError::HistoryChanged))
        || node.events_after(None)? != after_prepare
    {
        return Err("stale snapshot released a proposal or modified the journal".into());
    }
    let request = prepared
        .proposal_request(&value)?
        .bind_history(&after_prepare);
    let proposal = node.propose_control(&request, &key)?;
    let after_propose = node.events_after(None)?;
    let stale_accept = verifier
        .accept_request(&proposal)?
        .bind_history(&after_prepare);
    if node.vote_control(&stale_accept, &key)
        != Err(NodeError::ControlVote(ControlQuorumError::HistoryChanged))
        || node.events_after(None)? != after_propose
    {
        return Err("stale snapshot released an acceptance".into());
    }
    drop(node);
    let node = RedbJournal::open_node(&path)?;
    let refreshed = node.events_after(None)?;
    let request = prepared.proposal_request(&value)?.bind_history(&refreshed);
    if node.propose_control(&request, &key)? != proposal || node.events_after(None)? != refreshed {
        return Err("reopened revalidated proposal retry did not recover original evidence".into());
    }
    node.vote_control(
        &verifier.accept_request(&proposal)?.bind_history(&refreshed),
        &key,
    )?;
    Ok(())
}

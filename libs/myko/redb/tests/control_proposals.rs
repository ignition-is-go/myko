use std::error::Error;

use ed25519_dalek::{Signer as _, SigningKey};
use myko_federation::{
    EventId, EventJournal as _, FrameworkControlEvent, LogPosition, NodeError, NodeEvent, NodeId,
    ScopeId, control_quorum::*,
};
use myko_redb::RedbJournal;

#[test]
fn same_ballot_proposal_reuse_is_rejected() -> Result<(), Box<dyn Error>> {
    let [a_key, b_key, c_key] = [1, 2, 3].map(|seed| SigningKey::from_bytes(&[seed; 32]));
    let controllers =
        [&a_key, &b_key, &c_key].map(|key| ControllerId(key.verifying_key().to_bytes()));
    let verifier = ControlQuorumVerifier::new(
        ControlSlot {
            realm: ScopeId::new("authority:proposals"),
            epoch: ControlEpochId([4; 32]),
            predecessor: ControlHead([5; 32]),
        },
        controllers,
    )?;
    let directory = tempfile::tempdir()?;
    let path_a = directory.path().join("a.redb");
    let (a, journal) = RedbJournal::open_node_with_journal(&path_a)?;
    let (b, _) = RedbJournal::open_node_with_journal(directory.path().join("b.redb"))?;
    let ballot = ControlBallot {
        counter: 1,
        proposer: ControllerId(a_key.verifying_key().to_bytes()),
    };
    let prepare = verifier.prepare_request(ballot)?;
    let prepared = verifier.verify_prepare(
        ballot,
        &[
            a.vote_control(&prepare, &a_key)?,
            b.vote_control(&prepare, &b_key)?,
        ],
    )?;
    let request = prepared.proposal_request(&ControlValue(b"first".to_vec()))?;
    let proposal = a.propose_control(&request, &a_key)?;
    let first = verifier.accept_request(&proposal)?;
    a.vote_control(&first, &a_key)?;
    let before = journal.replay()?;
    let conflicting = prepared.proposal_request(&ControlValue(b"conflicting".to_vec()))?;
    if a.propose_control(&conflicting, &a_key)
        != Err(NodeError::ControlVote(
            ControlQuorumError::ConflictingProposals,
        ))
        || journal.replay()? != before
    {
        return Err("one proposer ballot issued different values to separate acceptors".into());
    }
    drop(a);
    drop(journal);
    let (a, journal) = RedbJournal::open_node_with_journal(&path_a)?;
    if a.propose_control(&request, &a_key)? != proposal
        || journal.replay()? != before
        || a.propose_control(&conflicting, &a_key)
            != Err(NodeError::ControlVote(
                ControlQuorumError::ConflictingProposals,
            ))
    {
        return Err("reopened proposer did not retain its exact ballot binding".into());
    }
    Ok(())
}

#[test]
fn conflicting_replay_cannot_hide_behind_a_matching_proposal() -> Result<(), Box<dyn Error>> {
    let key = SigningKey::from_bytes(&[1; 32]);
    let controller = ControllerId(key.verifying_key().to_bytes());
    let verifier = ControlQuorumVerifier::new(
        ControlSlot {
            realm: ScopeId::new("authority:conflicting-proposals"),
            epoch: ControlEpochId([4; 32]),
            predecessor: ControlHead([5; 32]),
        },
        [controller],
    )?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("proposer.redb");
    let (node, journal) = RedbJournal::open_node_with_journal(&path)?;
    let ballot = ControlBallot {
        counter: 1,
        proposer: controller,
    };
    let promise = node.vote_control(&verifier.prepare_request(ballot)?, &key)?;
    let prepared = verifier.verify_prepare(ballot, &[promise])?;
    let request = prepared.proposal_request(&ControlValue(b"original".to_vec()))?;
    let original = node.propose_control(&request, &key)?;
    let mut conflicting = original;
    conflicting.message.value = ControlValue(b"conflicting".to_vec());
    conflicting.signature = key.sign(&conflicting.message.signing_bytes()?).to_bytes();
    let mut event = journal
        .replay()?
        .into_iter()
        .find(|event| {
            matches!(
                event.event,
                NodeEvent::FrameworkControl(FrameworkControlEvent::ControlProposal(_))
            )
        })
        .ok_or("missing original proposal")?;
    event.origin = EventId::new(NodeId::new(), LogPosition::FIRST);
    event.event = NodeEvent::FrameworkControl(FrameworkControlEvent::ControlProposal(conflicting));
    node.ingest(event)?;
    drop(node);
    drop(journal);
    let (reopened, journal) = RedbJournal::open_node_with_journal(&path)?;
    let before = journal.replay()?;
    if reopened.propose_control(&request, &key)
        != Err(NodeError::ControlVote(
            ControlQuorumError::ConflictingProposals,
        ))
        || journal.replay()? != before
    {
        return Err("matching proposal hid a later conflicting retained proposal".into());
    }
    Ok(())
}

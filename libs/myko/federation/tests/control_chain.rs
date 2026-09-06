use std::error::Error;

use chrono::Utc;
use ed25519_dalek::{Signer as _, SigningKey};
use myko_federation::{
    CertifiedControlChain, CommandId, ControlAnchor, ControlTransition, EventEnvelope, EventId,
    FrameworkControlEvent, LogPosition, NodeEvent, NodeId, ScopeId,
    control_quorum::{
        ControlBallot, ControlEpochId, ControlHead, ControlProposal, ControlQuorumVerifier,
        ControlSlot, ControlValue, ControlVote, ControlVoteKind, ControllerId,
        SignedControlProposal, SignedControlVote,
    },
};

type TestResult = Result<(), Box<dyn Error>>;

fn keys(seed: u8) -> [SigningKey; 3] {
    [seed, seed.saturating_add(1), seed.saturating_add(2)]
        .map(|byte| SigningKey::from_bytes(&[byte; 32]))
}

fn id(key: &SigningKey) -> ControllerId {
    ControllerId(key.verifying_key().to_bytes())
}

fn controllers(keys: &[SigningKey; 3]) -> Vec<ControllerId> {
    let mut controllers = keys.iter().map(id).collect::<Vec<_>>();
    controllers.sort_unstable();
    controllers
}

fn ballot(counter: u64, key: &SigningKey) -> ControlBallot {
    ControlBallot {
        counter,
        proposer: id(key),
    }
}

fn anchor(keys: &[SigningKey; 3]) -> Result<ControlAnchor, String> {
    ControlAnchor::new(
        ScopeId::new("authority:main"),
        ControlEpochId([4; 32]),
        ControlHead([5; 32]),
        controllers(keys),
    )
}

fn context_verifier(
    anchor: &ControlAnchor,
    keys: &[SigningKey; 3],
) -> Result<ControlQuorumVerifier, Box<dyn Error>> {
    Ok(ControlQuorumVerifier::new(
        ControlSlot {
            realm: anchor.realm().clone(),
            epoch: ControlEpochId([4; 32]),
            predecessor: anchor.genesis(),
        },
        controllers(keys),
    )?)
}

fn promise(
    slot: ControlSlot,
    key: &SigningKey,
    ballot: ControlBallot,
) -> Result<SignedControlVote, serde_json::Error> {
    sign_vote(
        slot,
        key,
        ballot,
        ControlVoteKind::Promise { accepted: None },
    )
}

fn accept(
    slot: ControlSlot,
    key: &SigningKey,
    ballot: ControlBallot,
    value: ControlValue,
) -> Result<SignedControlVote, serde_json::Error> {
    sign_vote(slot, key, ballot, ControlVoteKind::Accept { value })
}

fn sign_vote(
    slot: ControlSlot,
    key: &SigningKey,
    ballot: ControlBallot,
    vote: ControlVoteKind,
) -> Result<SignedControlVote, serde_json::Error> {
    let message = ControlVote {
        slot,
        ballot,
        controller: id(key),
        vote,
    };
    let signature = key.sign(&message.signing_bytes()?).to_bytes();
    Ok(SignedControlVote { message, signature })
}

fn sign_proposal(
    slot: ControlSlot,
    key: &SigningKey,
    ballot: ControlBallot,
    value: ControlValue,
    prepare_votes: Vec<SignedControlVote>,
) -> Result<SignedControlProposal, serde_json::Error> {
    let message = ControlProposal {
        slot,
        ballot,
        value,
        prepare_votes,
    };
    let signature = key.sign(&message.signing_bytes()?).to_bytes();
    Ok(SignedControlProposal { message, signature })
}

fn choose(
    anchor: &ControlAnchor,
    keys: &[SigningKey; 3],
    transition: &ControlTransition,
) -> Result<(ControlHead, Vec<EventEnvelope>), Box<dyn Error>> {
    let verifier = context_verifier(anchor, keys)?;
    let slot = ControlSlot {
        realm: anchor.realm().clone(),
        epoch: ControlEpochId([4; 32]),
        predecessor: anchor.genesis(),
    };
    choose_in_slot(&verifier, slot, keys, transition)
}

fn choose_in_slot(
    verifier: &ControlQuorumVerifier,
    slot: ControlSlot,
    keys: &[SigningKey; 3],
    transition: &ControlTransition,
) -> Result<(ControlHead, Vec<EventEnvelope>), Box<dyn Error>> {
    let value = transition.control_value()?;
    choose_value_in_slot(verifier, slot, keys, &value)
}

fn choose_value_in_slot(
    verifier: &ControlQuorumVerifier,
    slot: ControlSlot,
    keys: &[SigningKey; 3],
    value: &ControlValue,
) -> Result<(ControlHead, Vec<EventEnvelope>), Box<dyn Error>> {
    let ballot = ballot(1, &keys[0]);
    let promises = vec![
        promise(slot.clone(), &keys[0], ballot)?,
        promise(slot.clone(), &keys[1], ballot)?,
    ];
    let proposal = sign_proposal(slot.clone(), &keys[0], ballot, value.clone(), promises)?;
    verifier.accept_request(&proposal)?;
    let first_accept = accept(slot.clone(), &keys[0], ballot, value.clone())?;
    let second_accept = accept(slot, &keys[1], ballot, value.clone())?;
    let accepts = vec![first_accept.clone(), second_accept.clone()];
    let prepared = verifier.verify_prepare(ballot, &proposal.message.prepare_votes)?;
    let head = prepared.verify_chosen(value, &accepts)?.head()?;
    Ok((
        head,
        records(vec![
            NodeEvent::FrameworkControl(FrameworkControlEvent::ControlProposal(proposal)),
            NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(first_accept)),
            NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(second_accept)),
        ]),
    ))
}

fn records(events: Vec<NodeEvent>) -> Vec<EventEnvelope> {
    let node_id = NodeId::new();
    events
        .into_iter()
        .enumerate()
        .map(|(index, event)| {
            let position =
                LogPosition::new(u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1));
            EventEnvelope {
                position,
                origin: EventId::new(node_id, position),
                recorded_at: Utc::now(),
                event,
            }
        })
        .collect()
}

fn expect_error_contains<T>(result: Result<T, String>, expected: &str) -> TestResult {
    match result {
        Ok(_) => Err(format!("expected error containing {expected:?}").into()),
        Err(error) if error.contains(expected) => Ok(()),
        Err(error) => Err(format!("expected {expected:?}, got {error:?}").into()),
    }
}

fn malformed_signed_proposal(
    proposal: &SignedControlProposal,
    key: &SigningKey,
) -> Result<SignedControlProposal, serde_json::Error> {
    sign_proposal(
        proposal.message.slot.clone(),
        key,
        proposal.message.ballot,
        proposal.message.value.clone(),
        Vec::new(),
    )
}

#[test]
fn retained_transition_replays_from_static_anchor() -> TestResult {
    let keys = keys(1);
    let anchor = anchor(&keys)?;
    let transition = ControlTransition::retain(
        CommandId::new(),
        ControlValue(b"authority-records".to_vec()),
    );
    let (head, events) = choose(&anchor, &keys, &transition)?;
    let chain = CertifiedControlChain::replay(&events, anchor.clone())?;
    if chain.context_at(anchor.genesis())?.slot().predecessor != anchor.genesis()
        || chain.transitions_to(head)? != vec![&transition]
        || chain.retained_head()? != head
    {
        return Err("certified retain transition did not replay from the anchor".into());
    }
    Ok(())
}

#[test]
fn recovered_operation_evidence_verifies_under_its_original_electorate() -> TestResult {
    let old = keys(1);
    let new = keys(10);
    let anchor = anchor(&old)?;
    let transition = ControlTransition::rotate(
        CommandId::new(),
        controllers(&new),
        ControlValue(b"original".to_vec()),
    )?;
    let (head, mut events) = choose(&anchor, &old, &transition)?;
    let chain = CertifiedControlChain::replay(&events, anchor.clone())?;
    let context = chain.context_at(head)?;
    let successor =
        ControlTransition::retain(CommandId::new(), ControlValue(b"successor".to_vec()));
    let (latest, additional) = choose_in_slot(
        &context.verifier()?,
        context.slot().clone(),
        &new,
        &successor,
    )?;
    events.extend(additional);
    let chain = CertifiedControlChain::replay(&events, anchor.clone())?;
    let proof = chain
        .operation_evidence_at(latest, transition.operation())?
        .ok_or("missing historical proof")?;
    let proposal = proof.proposal();
    proposal.verify_signature()?;
    let verifier = chain
        .context_at(proposal.message.slot.predecessor)?
        .verifier()?;
    let prepared =
        verifier.verify_prepare(proposal.message.ballot, &proposal.message.prepare_votes)?;
    let chosen = prepared.verify_chosen(&proposal.message.value, proof.accepts())?;
    if proof.head() != head
        || chosen.head()? != head
        || proposal.message.value != transition.control_value()?
    {
        return Err("recovered evidence does not prove the original operation".into());
    }
    if chain
        .operation_evidence_at(anchor.genesis(), transition.operation())?
        .is_some()
        || chain
            .operation_evidence_at(latest, CommandId::new())?
            .is_some()
        || chain
            .operation_evidence_at(ControlHead([88; 32]), transition.operation())
            .is_ok()
    {
        return Err("historical proof lookup crossed its requested history boundary".into());
    }
    Ok(())
}

#[test]
fn losing_rotation_and_old_epoch_decision_are_rejected() -> TestResult {
    let old = keys(10);
    let new = keys(20);
    let anchor = anchor(&old)?;
    let rotation = ControlTransition::rotate(
        CommandId::new(),
        controllers(&new),
        ControlValue(b"rotate".to_vec()),
    )?;
    let (rotation_head, mut events) = choose(&anchor, &old, &rotation)?;
    let chain = CertifiedControlChain::replay(&events, anchor.clone())?;
    let successor = chain.context_at(rotation_head)?;
    if successor.slot().predecessor != rotation_head {
        return Err("rotation did not create the successor context".into());
    }
    let successor_transition = ControlTransition::retain(
        CommandId::new(),
        ControlValue(b"new epoch decision".to_vec()),
    );
    let successor_verifier = successor.verifier()?;
    let (successor_head, successor_events) = choose_in_slot(
        &successor_verifier,
        successor.slot().clone(),
        &new,
        &successor_transition,
    )?;
    let mut complete_events = events.clone();
    complete_events.extend(successor_events);
    let complete = CertifiedControlChain::replay(&complete_events, anchor.clone())?;
    if complete.transitions_to(successor_head)? != vec![&rotation, &successor_transition] {
        return Err("disjoint successor controllers did not extend the rotated chain".into());
    }

    let stale = ControlTransition::retain(CommandId::new(), ControlValue(b"stale".to_vec()));
    let (_, stale_events) = choose(&anchor, &old, &stale)?;
    events.extend(stale_events);
    let losing = CertifiedControlChain::replay(&events, anchor.clone())?;
    expect_error_contains(
        losing.transitions_to(rotation_head),
        "distinct chosen successors",
    )?;

    let stale_epoch = ControlSlot {
        realm: anchor.realm().clone(),
        epoch: ControlEpochId([4; 32]),
        predecessor: rotation_head,
    };
    let stale_ballot = ballot(1, &old[0]);
    let value = ControlTransition::retain(CommandId::new(), ControlValue(b"old epoch".to_vec()))
        .control_value()?;
    let proposal = sign_proposal(
        stale_epoch.clone(),
        &old[0],
        stale_ballot,
        value.clone(),
        vec![
            promise(stale_epoch.clone(), &old[0], stale_ballot)?,
            promise(stale_epoch.clone(), &old[1], stale_ballot)?,
        ],
    )?;
    let mut stale_epoch_events = complete_events.iter().take(3).cloned().collect::<Vec<_>>();
    if stale_epoch_events.len() != 3 {
        return Err("fixture did not retain the rotation records".into());
    }
    stale_epoch_events.extend(records(vec![
        NodeEvent::FrameworkControl(FrameworkControlEvent::ControlProposal(proposal)),
        NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(accept(
            stale_epoch.clone(),
            &old[0],
            stale_ballot,
            value.clone(),
        )?)),
        NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(accept(
            stale_epoch,
            &old[1],
            stale_ballot,
            value,
        )?)),
    ]));
    if CertifiedControlChain::replay(&stale_epoch_events, anchor)?
        .context_at(rotation_head)?
        .slot()
        .epoch
        == ControlEpochId([4; 32])
    {
        return Err("successor context reused the old epoch".into());
    }
    Ok(())
}

#[test]
fn reused_operation_invalidates_only_the_later_head() -> TestResult {
    let keys = keys(30);
    let anchor = anchor(&keys)?;
    let operation = CommandId::new();
    let first = ControlTransition::retain(operation, ControlValue(b"first".to_vec()));
    let (first_head, mut events) = choose(&anchor, &keys, &first)?;
    let context = CertifiedControlChain::replay(&events, anchor.clone())?.context_at(first_head)?;
    let verifier = context.verifier()?;
    let reused = ControlTransition::retain(operation, ControlValue(b"reused".to_vec()));
    let (reused_head, reused_events) =
        choose_in_slot(&verifier, context.slot().clone(), &keys, &reused)?;
    events.extend(reused_events);

    let chain = CertifiedControlChain::replay(&events, anchor)?;
    if chain.transitions_to(first_head)? != vec![&first] {
        return Err("operation reuse invalidated the valid predecessor".into());
    }
    expect_error_contains(
        chain.transitions_to(reused_head),
        "control operation was reused",
    )?;
    expect_error_contains(chain.retained_head(), "control operation was reused")?;
    Ok(())
}

#[test]
fn chosen_malformed_payload_is_remembered_by_head() -> TestResult {
    let keys = keys(35);
    let anchor = anchor(&keys)?;
    let verifier = context_verifier(&anchor, &keys)?;
    let slot = ControlSlot {
        realm: anchor.realm().clone(),
        epoch: ControlEpochId([4; 32]),
        predecessor: anchor.genesis(),
    };
    let value = ControlValue(b"not a transition".to_vec());
    let (head, events) = choose_value_in_slot(&verifier, slot, &keys, &value)?;

    let chain = CertifiedControlChain::replay(&events, anchor.clone())?;
    if chain.context_at(anchor.genesis()).is_err() {
        return Err("malformed chosen payload poisoned the predecessor context".into());
    }
    expect_error_contains(
        chain.context_at(head),
        "chosen control transition value is malformed",
    )?;
    expect_error_contains(
        chain.retained_head(),
        "chosen control transition value is malformed",
    )?;
    Ok(())
}

#[test]
fn valid_duplicate_proposal_dominates_malformed_duplicate() -> TestResult {
    let keys = keys(40);
    let anchor = anchor(&keys)?;
    let transition = ControlTransition::retain(CommandId::new(), ControlValue(b"ok".to_vec()));
    let (head, events) = choose(&anchor, &keys, &transition)?;
    let valid = match events.first().map(|event| &event.event) {
        Some(NodeEvent::FrameworkControl(FrameworkControlEvent::ControlProposal(proposal))) => {
            proposal.clone()
        }
        _ => return Err("fixture did not start with a proposal".into()),
    };
    let malformed = malformed_signed_proposal(&valid, &keys[0])?;
    for ordered_events in [
        records(vec![NodeEvent::FrameworkControl(
            FrameworkControlEvent::ControlProposal(malformed.clone()),
        )])
        .into_iter()
        .chain(events.clone())
        .collect::<Vec<_>>(),
        events
            .into_iter()
            .chain(records(vec![NodeEvent::FrameworkControl(
                FrameworkControlEvent::ControlProposal(malformed),
            )]))
            .collect::<Vec<_>>(),
    ] {
        let chain = CertifiedControlChain::replay(&ordered_events, anchor.clone())?;
        if chain.transitions_to(head)? != vec![&transition] {
            return Err("malformed duplicate proposal hid the valid certified head".into());
        }
    }
    Ok(())
}

#[test]
fn malformed_chosen_sibling_invalidates_competing_successor() -> TestResult {
    let keys = keys(50);
    let anchor = anchor(&keys)?;
    let verifier = context_verifier(&anchor, &keys)?;
    let slot = ControlSlot {
        realm: anchor.realm().clone(),
        epoch: ControlEpochId([4; 32]),
        predecessor: anchor.genesis(),
    };
    let valid = ControlTransition::retain(CommandId::new(), ControlValue(b"valid".to_vec()));
    let (valid_head, mut events) = choose_in_slot(&verifier, slot.clone(), &keys, &valid)?;
    let malformed = ControlValue(b"not a transition".to_vec());
    let (malformed_head, malformed_events) =
        choose_value_in_slot(&verifier, slot, &keys, &malformed)?;
    events.extend(malformed_events);

    let chain = CertifiedControlChain::replay(&events, anchor)?;
    expect_error_contains(
        chain.transitions_to(valid_head),
        "distinct chosen successors",
    )?;
    expect_error_contains(
        chain.context_at(malformed_head),
        "distinct chosen successors",
    )?;
    expect_error_contains(chain.retained_head(), "distinct chosen successors")?;
    Ok(())
}

#[test]
fn deep_chain_replays_from_reversed_delivery_order() -> TestResult {
    let old = keys(70);
    let new = keys(80);
    let anchor = anchor(&old)?;
    let mut cursor = anchor.genesis();
    let mut events = Vec::new();
    let mut expected = Vec::new();

    for index in 0_u8..64 {
        let slot = ControlSlot {
            realm: anchor.realm().clone(),
            epoch: ControlEpochId([4; 32]),
            predecessor: cursor,
        };
        let verifier = ControlQuorumVerifier::new(slot.clone(), controllers(&old))?;
        let transition = ControlTransition::retain(
            CommandId::new(),
            ControlValue(vec![b'r', b'e', b't', b'a', b'i', b'n', index]),
        );
        let (head, chosen_events) = choose_in_slot(&verifier, slot, &old, &transition)?;
        expected.push(transition);
        events.extend(chosen_events);
        cursor = head;
    }

    let rotation_chain = CertifiedControlChain::replay(&events, anchor.clone())?;
    let rotation_context = rotation_chain.context_at(cursor)?;
    let rotation_verifier = rotation_context.verifier()?;
    let rotation = ControlTransition::rotate(
        CommandId::new(),
        controllers(&new),
        ControlValue(b"rotate after retained prefix".to_vec()),
    )?;
    let (rotation_head, rotation_events) = choose_in_slot(
        &rotation_verifier,
        rotation_context.slot().clone(),
        &old,
        &rotation,
    )?;
    expected.push(rotation);
    events.extend(rotation_events);

    let successor_chain = CertifiedControlChain::replay(&events, anchor.clone())?;
    let successor_context = successor_chain.context_at(rotation_head)?;
    let successor_verifier = successor_context.verifier()?;
    let successor = ControlTransition::retain(
        CommandId::new(),
        ControlValue(b"successor after rotation".to_vec()),
    );
    let (successor_head, successor_events) = choose_in_slot(
        &successor_verifier,
        successor_context.slot().clone(),
        &new,
        &successor,
    )?;
    expected.push(successor);
    events.extend(successor_events);
    events.reverse();

    let reversed = CertifiedControlChain::replay(&events, anchor)?;
    let expected_refs = expected.iter().collect::<Vec<_>>();
    if reversed.transitions_to(successor_head)? != expected_refs
        || reversed.retained_head()? != successor_head
    {
        return Err("reversed delivery did not replay the full certified chain".into());
    }
    let rotation_context = reversed.context_at(rotation_head)?;
    if rotation_context.slot().epoch == ControlEpochId([4; 32]) {
        return Err("rotation context kept the predecessor epoch".into());
    }
    let final_context = reversed.context_at(successor_head)?;
    if final_context.slot().predecessor != successor_head {
        return Err("successor context did not advance to the final head".into());
    }
    final_context.verifier()?;
    Ok(())
}

#[test]
fn conflicting_immutable_origin_fails_closed() -> TestResult {
    let keys = keys(60);
    let anchor = anchor(&keys)?;
    let (_, mut events) = choose(
        &anchor,
        &keys,
        &ControlTransition::retain(CommandId::new(), ControlValue(b"one".to_vec())),
    )?;
    let Some(first) = events.first() else {
        return Err("fixture did not create a first control record".into());
    };
    let Some(second) = events.get(1) else {
        return Err("fixture did not create a second control record".into());
    };
    let mut conflict = first.clone();
    conflict.event = second.event.clone();
    events.push(conflict);
    expect_error_contains(
        CertifiedControlChain::replay(&events, anchor),
        "different content",
    )?;
    Ok(())
}

#[test]
fn controller_and_transition_boundaries_reject_unusable_or_noncanonical_values() -> TestResult {
    let weak = ControllerId([0; 32]);
    if ControlAnchor::new(
        ScopeId::new("weak"),
        ControlEpochId([1; 32]),
        ControlHead([2; 32]),
        vec![weak],
    )
    .is_ok()
        || ControlTransition::rotate(CommandId::new(), vec![weak], ControlValue(vec![])).is_ok()
    {
        return Err("weak signing key established a controller electorate".into());
    }
    let mut reversed = controllers(&keys(90));
    reversed.reverse();
    let unsorted = ControlTransition::Rotate {
        operation: CommandId::new(),
        successor: reversed,
        payload: ControlValue(vec![]),
    };
    if unsorted.control_value().is_ok() {
        return Err("direct enum construction bypassed canonical controller order".into());
    }
    let transition = ControlTransition::retain(CommandId::new(), ControlValue(vec![]));
    let mut noncanonical = transition.control_value()?;
    noncanonical.0.push(b' ');
    if ControlTransition::from_control_value(&noncanonical).is_ok() {
        return Err("noncanonical JSON created another encoding for the same transition".into());
    }
    Ok(())
}

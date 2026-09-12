use std::error::Error;

use ed25519_dalek::{Signer as _, SigningKey};
use myko_federation::{
    CertifiedControlChain, CommandId, ControlAnchor, ControlTransition, ExecutionAssignment,
    ExecutionAssignmentController, ExecutionAssignmentObservation, ExecutionAssignmentsAtHead,
    Node, NodeId, ScopeId, ServiceId,
    control_quorum::{
        ControlBallot, ControlEpochId, ControlHead, ControlProposal, ControlSlot, ControlValue,
        ControllerId, SignedControlProposal, SignedControlVote,
    },
};
use myko_redb::RedbJournal;

type TestResult = Result<(), Box<dyn Error>>;

fn anchor(keys: &[SigningKey]) -> Result<ControlAnchor, String> {
    ControlAnchor::new(
        ScopeId::new("execution:mesh"),
        ControlEpochId([41; 32]),
        ControlHead([42; 32]),
        keys.iter()
            .map(|key| ControllerId(key.verifying_key().to_bytes()))
            .collect(),
    )
}

fn ballot(key: &SigningKey, counter: u64) -> ControlBallot {
    ControlBallot {
        counter,
        proposer: ControllerId(key.verifying_key().to_bytes()),
    }
}

fn value(
    anchor: &ControlAnchor,
    operation: CommandId,
    executor: NodeId,
) -> Result<ControlValue, String> {
    ExecutionAssignment::new(
        operation,
        anchor.realm().clone(),
        ScopeId::new("work"),
        ServiceId::new("records"),
        [executor],
    )
    .transition()?
    .control_value()
}

fn assignments(node: &Node, anchor: &ControlAnchor) -> Result<ExecutionAssignmentsAtHead, String> {
    let history = node.events_after(None).map_err(|error| error.to_string())?;
    let chain = CertifiedControlChain::replay(&history, anchor.clone())?;
    ExecutionAssignmentsAtHead::replay(&chain, chain.retained_head()?)
}

fn choose_single(
    controller: &ExecutionAssignmentController,
    head: ControlHead,
    key: &SigningKey,
    value: &ControlValue,
) -> Result<(SignedControlProposal, SignedControlVote), String> {
    let ballot = ballot(key, 1);
    let promise = controller.prepare(head, ballot, key)?;
    let proposal = controller.propose(head, ballot, &[promise], value, key)?;
    let accepted = controller.accept(head, &proposal, key)?;
    Ok((proposal, accepted))
}

#[test]
fn assignment_voting_survives_reopen_without_application_modules() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("controller.redb");
    let key = SigningKey::from_bytes(&[51; 32]);
    let anchor = anchor(std::slice::from_ref(&key))?;
    let executor = NodeId::new();
    let operation = CommandId::new();
    let value = value(&anchor, operation, executor)?;
    let node = RedbJournal::open_node(&path)?;
    let controller = ExecutionAssignmentController::new(node.clone(), anchor.clone());
    let (proposal, accepted) = choose_single(&controller, anchor.genesis(), &key, &value)?;
    let assigned = assignments(&node, &anchor)?;
    if assigned.exact(&ScopeId::new("work"), &ServiceId::new("records")) != Some(&[executor].into())
    {
        return Err("durably chosen assignment was not projected".into());
    }
    let history = node.events_after(None)?;
    drop(controller);
    drop(node);

    let reopened = RedbJournal::open_node(&path)?;
    let controller = ExecutionAssignmentController::new(reopened.clone(), anchor.clone());
    if assignments(&reopened, &anchor)? != assigned
        || controller.accept(anchor.genesis(), &proposal, &key)? != accepted
        || controller.propose(
            anchor.genesis(),
            ballot(&key, 1),
            &proposal.message.prepare_votes,
            &value,
            &key,
        )? != proposal
        || reopened.events_after(None)? != history
    {
        return Err("reopen did not recover the original assignment evidence idempotently".into());
    }
    let head = assigned.head();
    let promise = controller.prepare(head, ballot(&key, 1), &key)?;
    let before = reopened.events_after(None)?;
    if controller
        .propose(head, ballot(&key, 1), &[promise], &value, &key)
        .is_ok()
        || reopened.events_after(None)? != before
    {
        return Err("a chosen operation was proposed again after its successor head".into());
    }
    Ok(())
}

#[test]
fn assignment_requires_majority_and_recovers_an_accepted_value() -> TestResult {
    let directory = tempfile::tempdir()?;
    let keys = [61, 62, 63].map(|seed| SigningKey::from_bytes(&[seed; 32]));
    let anchor = anchor(&keys)?;
    let [first_key, second_key, _] = &keys;
    let first = RedbJournal::open_node(directory.path().join("first.redb"))?;
    let second = RedbJournal::open_node(directory.path().join("second.redb"))?;
    let a = ExecutionAssignmentController::new(first.clone(), anchor.clone());
    let b = ExecutionAssignmentController::new(second.clone(), anchor.clone());
    let initial = ballot(first_key, 1);
    let head = anchor.genesis();
    let value = value(&anchor, CommandId::new(), NodeId::new())?;
    let first_promise = a.prepare(head, initial, first_key)?;
    let before = first.events_after(None)?;
    if a.propose(
        head,
        initial,
        std::slice::from_ref(&first_promise),
        &value,
        first_key,
    )
    .is_ok()
        || first.events_after(None)? != before
    {
        return Err("a minority manufactured an assignment proposal".into());
    }
    let second_promise = b.prepare(head, initial, second_key)?;
    let proposal = a.propose(
        head,
        initial,
        &[first_promise, second_promise],
        &value,
        first_key,
    )?;
    a.accept(head, &proposal, first_key)?;
    if assignments(&first, &anchor)?.head() != head {
        return Err("one acceptance was treated as a chosen assignment".into());
    }

    let recovery = ballot(second_key, 2);
    let promises = [
        a.prepare(head, recovery, first_key)?,
        b.prepare(head, recovery, second_key)?,
    ];
    let different = self::value(&anchor, CommandId::new(), NodeId::new())?;
    let before = second.events_after(None)?;
    if b.propose(head, recovery, &promises, &different, second_key)
        .is_ok()
        || second.events_after(None)? != before
    {
        return Err("a new proposer replaced the previously accepted assignment".into());
    }
    let recovered = b.propose(head, recovery, &promises, &value, second_key)?;
    a.accept(head, &recovered, first_key)?;
    b.accept(head, &recovered, second_key)?;
    for event in first.events_after(None)? {
        second.ingest(event)?;
    }
    let assigned = assignments(&second, &anchor)?;
    if assigned.head() != recovered.message.slot.head_for(&value)? {
        return Err("majority recovery did not choose the original complete assignment".into());
    }
    Ok(())
}

#[test]
fn execution_controller_rejects_foreign_payloads_before_persistence() -> TestResult {
    let directory = tempfile::tempdir()?;
    let node = RedbJournal::open_node(directory.path().join("controller.redb"))?;
    let key = SigningKey::from_bytes(&[71; 32]);
    let anchor = anchor(std::slice::from_ref(&key))?;
    let controller = ExecutionAssignmentController::new(node.clone(), anchor.clone());
    let ballot = ballot(&key, 1);
    let promise = controller.prepare(anchor.genesis(), ballot, &key)?;
    let foreign = ExecutionAssignment::new(
        CommandId::new(),
        ScopeId::new("another-realm"),
        ScopeId::new("work"),
        ServiceId::new("records"),
        [NodeId::new()],
    )
    .transition()?
    .control_value()?;
    let unrelated =
        ControlTransition::retain(CommandId::new(), ControlValue(b"another-domain".to_vec()))
            .control_value()?;
    let before = node.events_after(None)?;
    for value in [foreign, unrelated, ControlValue(vec![0])] {
        if controller
            .propose(
                anchor.genesis(),
                ballot,
                std::slice::from_ref(&promise),
                &value,
                &key,
            )
            .is_ok()
            || node.events_after(None)? != before
        {
            return Err("invalid assignment reached durable proposal recording".into());
        }
        let message = ControlProposal {
            slot: ControlSlot {
                realm: anchor.realm().clone(),
                epoch: ControlEpochId([41; 32]),
                predecessor: anchor.genesis(),
            },
            ballot,
            value,
            prepare_votes: vec![promise.clone()],
        };
        let signature = key.sign(&message.signing_bytes()?).to_bytes();
        if controller
            .accept(
                anchor.genesis(),
                &SignedControlProposal { message, signature },
                &key,
            )
            .is_ok()
            || node.events_after(None)? != before
        {
            return Err("valid signature authorized an invalid assignment payload".into());
        }
    }
    Ok(())
}

#[test]
fn volatile_execution_controller_cannot_issue_a_promise() -> TestResult {
    let key = SigningKey::from_bytes(&[81; 32]);
    let anchor = anchor(std::slice::from_ref(&key))?;
    let node = Node::in_memory();
    let controller = ExecutionAssignmentController::new(node.clone(), anchor.clone());
    if controller
        .prepare(anchor.genesis(), ballot(&key, 1), &key)
        .is_ok()
        || !node.events_after(None)?.is_empty()
    {
        return Err("volatile node issued execution-control evidence".into());
    }
    Ok(())
}

#[test]
fn observation_controller_validates_payload_and_exact_predecessor_before_signing() -> TestResult {
    let directory = tempfile::tempdir()?;
    let node = RedbJournal::open_node(directory.path().join("observation.redb"))?;
    let key = SigningKey::from_bytes(&[82; 32]);
    let anchor = anchor(std::slice::from_ref(&key))?;
    let controller = ExecutionAssignmentController::new(node.clone(), anchor.clone());
    let operation = CommandId::new();
    let valid =
        ExecutionAssignmentObservation::new(operation, anchor.realm().clone(), anchor.genesis())
            .transition()?;
    let mut padded = valid.payload().clone();
    padded.0.push(b' ');
    let invalid = [
        ExecutionAssignmentObservation::new(operation, ScopeId::new("foreign"), anchor.genesis())
            .transition()?,
        ExecutionAssignmentObservation::new(
            operation,
            anchor.realm().clone(),
            ControlHead([99; 32]),
        )
        .transition()?,
        ControlTransition::retain(CommandId::new(), valid.payload().clone()),
        ControlTransition::rotate(
            operation,
            vec![ballot(&key, 1).proposer],
            valid.payload().clone(),
        )?,
        ControlTransition::retain(operation, padded),
        ControlTransition::retain(
            operation,
            ControlValue(b"myko/execution-observation/v2\0{}".to_vec()),
        ),
    ];
    let ballot = ballot(&key, 1);
    let promise = controller.prepare(anchor.genesis(), ballot, &key)?;
    let before = node.events_after(None)?;
    let chain = CertifiedControlChain::replay(&before, anchor.clone())?;
    let slot = chain.context_at(anchor.genesis())?.slot().clone();
    for transition in invalid {
        let value = transition.control_value()?;
        if controller
            .propose(
                anchor.genesis(),
                ballot,
                std::slice::from_ref(&promise),
                &value,
                &key,
            )
            .is_ok()
        {
            return Err("invalid observation reached durable proposal recording".into());
        }
        let message = ControlProposal {
            slot: slot.clone(),
            ballot,
            value,
            prepare_votes: vec![promise.clone()],
        };
        let signature = key.sign(&message.signing_bytes()?).to_bytes();
        if controller
            .accept(
                anchor.genesis(),
                &SignedControlProposal { message, signature },
                &key,
            )
            .is_ok()
            || node.events_after(None)? != before
        {
            return Err("invalid observation reached durable acceptance".into());
        }
    }
    choose_single(&controller, anchor.genesis(), &key, &valid.control_value()?)?;
    let observed = assignments(&node, &anchor)?;
    if observed.head() == anchor.genesis()
        || observed
            .exact(&ScopeId::new("work"), &ServiceId::new("records"))
            .is_some()
    {
        return Err("observation failed to advance or manufactured an assignment".into());
    }
    Ok(())
}

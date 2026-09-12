use std::{collections::BTreeSet, error::Error};

use chrono::Utc;
use ed25519_dalek::{Signer as _, SigningKey};
use myko_federation::{
    CertifiedControlChain, CommandId, ControlAnchor, ControlTransition, EventEnvelope, EventId,
    ExecutionAssignment, ExecutionAssignmentsAtHead, FrameworkControlEvent, LogPosition, NodeEvent,
    NodeId, ScopeId, ServiceId,
    control_quorum::{
        ControlBallot, ControlEpochId, ControlHead, ControlProposal, ControlSlot, ControlValue,
        ControlVote, ControlVoteKind, ControllerId, SignedControlProposal, SignedControlVote,
    },
};

type TestResult = Result<(), Box<dyn Error>>;

struct ControlHistory {
    anchor: ControlAnchor,
    keys: [SigningKey; 3],
    events: Vec<EventEnvelope>,
}

impl ControlHistory {
    fn new() -> Result<Self, String> {
        let keys = [71, 72, 73].map(|byte| SigningKey::from_bytes(&[byte; 32]));
        let anchor = ControlAnchor::new(
            ScopeId::new("mesh:test"),
            ControlEpochId([31; 32]),
            ControlHead([32; 32]),
            keys.iter().map(controller).collect(),
        )?;
        Ok(Self {
            anchor,
            keys,
            events: Vec::new(),
        })
    }

    fn proposal(
        &self,
        operation: CommandId,
        scope: &ScopeId,
        service: &ServiceId,
        executors: impl IntoIterator<Item = NodeId>,
    ) -> Result<ControlTransition, String> {
        ExecutionAssignment::new(
            operation,
            self.anchor.realm().clone(),
            scope.clone(),
            service.clone(),
            executors,
        )
        .transition()
    }

    fn chain(&self) -> Result<CertifiedControlChain, String> {
        CertifiedControlChain::replay(&self.events, self.anchor.clone())
    }

    fn choose(&mut self, transition: &ControlTransition) -> Result<ControlHead, Box<dyn Error>> {
        let chain = self.chain()?;
        let context = chain.context_at(chain.retained_head()?)?;
        let slot = context.slot();
        let verifier = context.verifier()?;
        let [first, second, _] = &self.keys;
        let ballot = ControlBallot {
            counter: 1,
            proposer: controller(first),
        };
        let promises = [first, second]
            .into_iter()
            .map(|key| {
                sign_vote(
                    slot,
                    key,
                    ballot,
                    ControlVoteKind::Promise { accepted: None },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let value = transition.control_value()?;
        let message = ControlProposal {
            slot: slot.clone(),
            ballot,
            value: value.clone(),
            prepare_votes: promises,
        };
        let signature = first.sign(&message.signing_bytes()?).to_bytes();
        let proposal = SignedControlProposal { message, signature };
        verifier.accept_request(&proposal)?;
        let accepts = [first, second]
            .into_iter()
            .map(|key| {
                sign_vote(
                    slot,
                    key,
                    ballot,
                    ControlVoteKind::Accept {
                        value: value.clone(),
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let head = verifier
            .verify_prepare(ballot, &proposal.message.prepare_votes)?
            .verify_chosen(&value, &accepts)?
            .head()?;
        self.events
            .push(record(FrameworkControlEvent::ControlProposal(proposal)));
        self.events.extend(
            accepts
                .into_iter()
                .map(|vote| record(FrameworkControlEvent::ControlVote(vote))),
        );
        Ok(head)
    }

    fn at(&self, head: ControlHead) -> Result<ExecutionAssignmentsAtHead, String> {
        ExecutionAssignmentsAtHead::replay(&self.chain()?, head)
    }
}

fn controller(key: &SigningKey) -> ControllerId {
    ControllerId(key.verifying_key().to_bytes())
}

fn sign_vote(
    slot: &ControlSlot,
    key: &SigningKey,
    ballot: ControlBallot,
    vote: ControlVoteKind,
) -> Result<SignedControlVote, serde_json::Error> {
    let message = ControlVote {
        slot: slot.clone(),
        ballot,
        controller: controller(key),
        vote,
    };
    let signature = key.sign(&message.signing_bytes()?).to_bytes();
    Ok(SignedControlVote { message, signature })
}

fn record(event: FrameworkControlEvent) -> EventEnvelope {
    EventEnvelope {
        position: LogPosition::FIRST,
        origin: EventId::new(NodeId::new(), LogPosition::FIRST),
        recorded_at: Utc::now(),
        event: NodeEvent::FrameworkControl(event),
    }
}

fn assert_error<T>(result: Result<T, String>, expected: &str) -> TestResult {
    match result {
        Err(error) if error.contains(expected) => Ok(()),
        Err(error) => Err(format!("expected {expected:?}, got {error:?}").into()),
        Ok(_) => Err(format!("expected error containing {expected:?}").into()),
    }
}

fn require_equal<T: PartialEq + std::fmt::Debug>(actual: &T, expected: &T) -> TestResult {
    if actual == expected {
        Ok(())
    } else {
        Err(format!("expected {expected:?}, got {actual:?}").into())
    }
}

#[test]
fn exact_head_recovers_replacement_without_resurrecting_repeated_old_evidence() -> TestResult {
    let mut history = ControlHistory::new()?;
    let scope = ScopeId::new("records/workspace:one");
    let service = ServiceId::new("records");
    let first = NodeId::new();
    let second = NodeId::new();
    let original = history.proposal(CommandId::new(), &scope, &service, [first, second])?;
    let original_head = history.choose(&original)?;
    let original_events = history.events.clone();
    let replacement = history.proposal(CommandId::new(), &scope, &service, [second])?;
    let replacement_head = history.choose(&replacement)?;
    history.events.extend(original_events);
    history.events.reverse();
    history.events = serde_json::from_slice(&serde_json::to_vec(&history.events)?)?;

    let old = history.at(original_head)?;
    require_equal(&old.head(), &original_head)?;
    require_equal(old.realm(), history.anchor.realm())?;
    require_equal(
        &old.exact(&scope, &service),
        &Some(&BTreeSet::from([first, second])),
    )?;
    let replaced = history.at(replacement_head)?;
    require_equal(
        &replaced.exact(&scope, &service),
        &Some(&BTreeSet::from([second])),
    )?;

    let removal = history.proposal(CommandId::new(), &scope, &service, [])?;
    let removed_head = history.choose(&removal)?;
    require_equal(
        &history.at(removed_head)?.exact(&scope, &service),
        &Some(&BTreeSet::new()),
    )?;
    require_equal(&history.at(original_head)?, &old)?;
    require_equal(&history.at(replacement_head)?, &replaced)?;
    Ok(())
}

#[test]
fn exact_records_are_isolated_by_scope_service_and_anchored_realm() -> TestResult {
    let mut history = ControlHistory::new()?;
    let scope = ScopeId::new("records/workspace:one");
    let other_scope = ScopeId::new("records/workspace:two");
    let service = ServiceId::new("records");
    let other_service = ServiceId::new("search");
    let first = NodeId::new();
    let second = NodeId::new();
    let initial = history.proposal(CommandId::new(), &scope, &service, [first])?;
    history.choose(&initial)?;
    let other = history.proposal(CommandId::new(), &other_scope, &service, [second])?;
    history.choose(&other)?;
    let different_service = history.proposal(CommandId::new(), &scope, &other_service, [second])?;
    let head = history.choose(&different_service)?;
    let assignments = history.at(head)?;
    require_equal(
        &assignments.exact(&scope, &service),
        &Some(&BTreeSet::from([first])),
    )?;
    require_equal(
        &assignments.exact(&scope, &other_service),
        &Some(&BTreeSet::from([second])),
    )?;
    require_equal(
        &assignments.exact(&other_scope, &service),
        &Some(&BTreeSet::from([second])),
    )?;
    require_equal(&assignments.exact(&other_scope, &other_service), &None)?;

    let foreign_anchor = ControlAnchor::new(
        ScopeId::new("mesh:foreign"),
        ControlEpochId([31; 32]),
        ControlHead([32; 32]),
        history.keys.iter().map(controller).collect(),
    )?;
    let foreign_chain = CertifiedControlChain::replay(&history.events, foreign_anchor.clone())?;
    let foreign = ExecutionAssignmentsAtHead::replay(&foreign_chain, foreign_anchor.genesis())?;
    require_equal(&foreign.exact(&scope, &service), &None)?;
    assert_error(
        ExecutionAssignmentsAtHead::replay(&foreign_chain, head),
        "not certified",
    )
}

#[test]
fn unchosen_proposal_and_minority_votes_do_not_establish_assignment() -> TestResult {
    let mut history = ControlHistory::new()?;
    let scope = ScopeId::new("records/workspace:one");
    let service = ServiceId::new("records");
    let proposal = history.proposal(CommandId::new(), &scope, &service, [NodeId::new()])?;
    let chosen = history.choose(&proposal)?;
    let removed = history.events.pop().ok_or("missing second accept")?;
    require_equal(
        &matches!(
            removed.event,
            NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(_))
        ),
        &true,
    )?;
    require_equal(
        &history
            .at(history.anchor.genesis())?
            .exact(&scope, &service),
        &None,
    )?;
    assert_error(history.at(chosen), "not certified")
}

#[test]
fn chosen_assignment_rejects_realm_operation_and_rotation_mismatches() -> TestResult {
    for case in ["realm", "operation", "rotate"] {
        let mut history = ControlHistory::new()?;
        let operation = CommandId::new();
        let proposal = ExecutionAssignment::new(
            operation,
            if case == "realm" {
                ScopeId::new("mesh:foreign")
            } else {
                history.anchor.realm().clone()
            },
            ScopeId::new("records/workspace:one"),
            ServiceId::new("records"),
            [NodeId::new()],
        )
        .transition()?;
        let (transition, reason) = match case {
            "realm" => (proposal, "different control realm"),
            "operation" => (
                ControlTransition::retain(CommandId::new(), proposal.payload().clone()),
                "operation does not match",
            ),
            _ => (
                ControlTransition::rotate(
                    operation,
                    history.keys.iter().map(controller).collect(),
                    proposal.payload().clone(),
                )?,
                "must not rotate",
            ),
        };
        let head = history.choose(&transition)?;
        assert_error(history.at(head), reason)?;
        history.at(history.anchor.genesis())?;
    }
    Ok(())
}

#[test]
fn chosen_assignment_rejects_noncanonical_bytes_and_unsupported_versions() -> TestResult {
    for case in ["whitespace", "duplicate", "unknown_field", "version"] {
        let mut history = ControlHistory::new()?;
        let executor = NodeId::new();
        let proposal = history.proposal(
            CommandId::new(),
            &ScopeId::new("scope:one"),
            &ServiceId::new("records"),
            [executor],
        )?;
        let encoded = String::from_utf8(proposal.payload().0.clone())?;
        let node = serde_json::to_string(&executor)?;
        let (changed, reason) = match case {
            "whitespace" => (format!("{encoded} "), "not canonical"),
            "duplicate" => (
                encoded.replace(&format!("[{node}]"), &format!("[{node},{node}]")),
                "not canonical",
            ),
            "unknown_field" => (
                encoded.replace('}', ",\"unexpected\":true}"),
                "unknown field",
            ),
            _ => (
                encoded.replace("/v1\0", "/v2\0"),
                "unsupported execution assignment version",
            ),
        };
        if changed.as_bytes() == proposal.payload().0 {
            return Err(format!("{case} did not change the payload").into());
        }
        let malformed =
            ControlTransition::retain(proposal.operation(), ControlValue(changed.into_bytes()));
        let head = history.choose(&malformed)?;
        assert_error(history.at(head), reason)?;
    }
    Ok(())
}

#[test]
fn other_control_payloads_and_controller_rotation_preserve_assignments() -> TestResult {
    let mut history = ControlHistory::new()?;
    let scope = ScopeId::new("scope:one");
    let service = ServiceId::new("records");
    let executor = NodeId::new();
    let proposal = history.proposal(CommandId::new(), &scope, &service, [executor])?;
    history.choose(&proposal)?;
    let other = ControlTransition::rotate(
        CommandId::new(),
        history.keys.iter().map(controller).collect(),
        ControlValue(b"other-control-domain".to_vec()),
    )?;
    let rotated = history.choose(&other)?;
    require_equal(
        &history.at(rotated)?.exact(&scope, &service),
        &Some(&BTreeSet::from([executor])),
    )?;
    let removal = history.proposal(CommandId::new(), &scope, &service, [])?;
    let removed = history.choose(&removal)?;
    require_equal(
        &history.at(removed)?.exact(&scope, &service),
        &Some(&BTreeSet::new()),
    )?;
    Ok(())
}

#[test]
fn reused_operation_cannot_restore_an_old_assignment() -> TestResult {
    let mut history = ControlHistory::new()?;
    let scope = ScopeId::new("scope:one");
    let service = ServiceId::new("records");
    let original = history.proposal(CommandId::new(), &scope, &service, [NodeId::new()])?;
    history.choose(&original)?;
    let removal = history.proposal(CommandId::new(), &scope, &service, [])?;
    let removed = history.choose(&removal)?;
    let reused = history.choose(&original)?;
    assert_error(history.at(reused), "operation was reused")?;
    require_equal(
        &history.at(removed)?.exact(&scope, &service),
        &Some(&BTreeSet::new()),
    )?;
    Ok(())
}

#[test]
fn proposal_set_order_and_repetition_do_not_change_control_value() -> TestResult {
    let history = ControlHistory::new()?;
    let operation = CommandId::new();
    let scope = ScopeId::new("scope:one");
    let service = ServiceId::new("records");
    let first = NodeId::new();
    let second = NodeId::new();
    require_equal(
        &history.proposal(operation, &scope, &service, [first, second])?,
        &history.proposal(operation, &scope, &service, [second, first, first])?,
    )?;
    Ok(())
}

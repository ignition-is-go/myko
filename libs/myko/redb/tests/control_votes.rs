use std::{error::Error, path::Path, sync::Arc};

use ed25519_dalek::{Signer as _, SigningKey};
use myko_federation::{
    EventEnvelope, EventId, EventJournal, FrameworkControlEvent, IngestStatus, LogPosition, Node,
    NodeError, NodeEvent, ScopeId, control_quorum::*,
};
use myko_redb::RedbJournal;

type TestResult = Result<(), Box<dyn Error>>;

fn keys() -> [SigningKey; 3] {
    [1, 2, 3].map(|seed| SigningKey::from_bytes(&[seed; 32]))
}

fn id(key: &SigningKey) -> ControllerId {
    ControllerId(key.verifying_key().to_bytes())
}

fn slot() -> ControlSlot {
    ControlSlot {
        realm: ScopeId::new("authority:control-votes"),
        epoch: ControlEpochId([4; 32]),
        predecessor: ControlHead([5; 32]),
    }
}

fn verifier(keys: [&SigningKey; 3]) -> Result<ControlQuorumVerifier, ControlQuorumError> {
    ControlQuorumVerifier::new(slot(), keys.into_iter().map(id))
}

fn ballot(counter: u64, key: &SigningKey) -> ControlBallot {
    ControlBallot {
        counter,
        proposer: id(key),
    }
}

fn value(bytes: &[u8]) -> ControlValue {
    ControlValue(bytes.to_vec())
}

fn open(path: &Path) -> Result<(Node, Arc<RedbJournal>), NodeError> {
    RedbJournal::open_node_with_journal(path)
}

fn promise_accepted(vote: &SignedControlVote) -> Result<Option<AcceptedControlValue>, String> {
    let ControlVoteKind::Promise { accepted } = &vote.message.vote else {
        return Err("vote was not a promise".to_owned());
    };
    Ok(accepted.clone())
}

fn vote_value(vote: &SignedControlVote) -> Result<ControlValue, String> {
    let ControlVoteKind::Accept { value } = &vote.message.vote else {
        return Err("vote was not an accept".to_owned());
    };
    Ok(value.clone())
}

fn control_votes(journal: &RedbJournal) -> Result<Vec<EventEnvelope>, NodeError> {
    Ok(journal
        .replay()?
        .into_iter()
        .filter(|event| {
            matches!(
                event.event,
                NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(_))
            )
        })
        .collect())
}

fn only_control_vote(journal: &RedbJournal) -> Result<EventEnvelope, Box<dyn Error>> {
    let votes = control_votes(journal)?;
    if votes.len() != 1 {
        return Err(format!("expected one control vote, found {}", votes.len()).into());
    }
    let Some(vote) = votes.into_iter().next() else {
        return Err("control vote disappeared after length check".into());
    };
    Ok(vote)
}

fn sign(
    key: &SigningKey,
    ballot: ControlBallot,
    vote: ControlVoteKind,
) -> Result<SignedControlVote, serde_json::Error> {
    let message = ControlVote {
        slot: slot(),
        ballot,
        controller: id(key),
        vote,
    };
    let signature = key.sign(&message.signing_bytes()?).to_bytes();
    Ok(SignedControlVote { message, signature })
}

fn sign_proposal(
    key: &SigningKey,
    ballot: ControlBallot,
    value: ControlValue,
    prepare_votes: Vec<SignedControlVote>,
) -> Result<SignedControlProposal, serde_json::Error> {
    let message = ControlProposal {
        slot: slot(),
        ballot,
        value,
        prepare_votes,
    };
    let signature = key.sign(&message.signing_bytes()?).to_bytes();
    Ok(SignedControlProposal { message, signature })
}

fn propose(
    proposer: &Node,
    proposer_key: &SigningKey,
    prepared: &PreparedControlQuorum<'_>,
    value: &ControlValue,
) -> Result<SignedControlProposal, Box<dyn Error>> {
    let request = prepared.proposal_request(value)?;
    Ok(proposer.propose_control(&request, proposer_key)?)
}

const fn wrapped_vote(
    template: &EventEnvelope,
    origin_node: myko_federation::NodeId,
    sequence: u64,
    vote: SignedControlVote,
) -> EventEnvelope {
    EventEnvelope {
        position: LogPosition::FIRST,
        origin: EventId::new(origin_node, LogPosition::new(sequence)),
        recorded_at: template.recorded_at,
        event: NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(vote)),
    }
}

#[test]
fn promise_survives_reopen_and_rejects_lower_ballot() -> TestResult {
    let [a_key, b_key, c_key] = keys();
    let verifier = verifier([&a_key, &b_key, &c_key])?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("promise.redb");
    let (node, journal) = open(&path)?;
    let promised = ballot(2, &a_key);
    let request = verifier.prepare_request(promised)?;
    let first = node.vote_control(&request, &a_key)?;
    if promise_accepted(&first)?.is_some() {
        return Err("new voter promise reported an accepted value".into());
    }
    if node.vote_control(&request, &a_key)? != first || control_votes(&journal)?.len() != 1 {
        return Err("exact promise retry appended another record or changed the signature".into());
    }
    drop(node);
    drop(journal);

    let (reopened, journal) = open(&path)?;
    if reopened.vote_control(&request, &a_key)? != first || control_votes(&journal)?.len() != 1 {
        return Err("reopen did not preserve idempotent promise retry".into());
    }
    let before = journal.replay()?;
    let lower = verifier.prepare_request(ballot(1, &a_key))?;
    let denied = reopened.vote_control(&lower, &a_key);
    if denied != Err(NodeError::ControlVote(ControlQuorumError::SupersededBallot))
        || journal.replay()? != before
    {
        return Err("lower ballot was signed or changed durable history after reopen".into());
    }
    Ok(())
}

#[test]
fn accepted_value_survives_reopen_and_is_reported_in_prepare() -> TestResult {
    let [a_key, b_key, c_key] = keys();
    let verifier = verifier([&a_key, &b_key, &c_key])?;
    let directory = tempfile::tempdir()?;
    let path_a = directory.path().join("a.redb");
    let path_b = directory.path().join("b.redb");
    let (a, _) = open(&path_a)?;
    let (b, _) = open(&path_b)?;
    let first_ballot = ballot(1, &a_key);
    let prepare = verifier.prepare_request(first_ballot)?;
    let promises = [
        a.vote_control(&prepare, &a_key)?,
        b.vote_control(&prepare, &b_key)?,
    ];
    let prepared = verifier.verify_prepare(first_ballot, &promises)?;
    let chosen = value(b"full accepted value");
    let proposal = propose(&a, &a_key, &prepared, &chosen)?;
    let accept = verifier.accept_request(&proposal)?;
    let accepted = a.vote_control(&accept, &a_key)?;
    if vote_value(&accepted)? != chosen {
        return Err("accepted vote did not contain the complete value".into());
    }
    drop(a);

    let (reopened, _) = open(&path_a)?;
    let retry_ballot = ballot(2, &c_key);
    let retry = verifier.prepare_request(retry_ballot)?;
    let promise = reopened.vote_control(&retry, &a_key)?;
    let Some(report) = promise_accepted(&promise)? else {
        return Err("reopened voter did not report its accepted value".into());
    };
    if report.ballot != first_ballot || report.value != chosen {
        return Err("reopened prepare reported the wrong accepted value".into());
    }
    Ok(())
}

#[test]
fn lost_reply_after_majority_recovers_same_value_on_higher_ballot() -> TestResult {
    let [a_key, b_key, c_key] = keys();
    let verifier = verifier([&a_key, &b_key, &c_key])?;
    let directory = tempfile::tempdir()?;
    let [path_a, path_b, path_c] = [
        directory.path().join("a.redb"),
        directory.path().join("b.redb"),
        directory.path().join("c.redb"),
    ];
    let (a, _) = open(&path_a)?;
    let (b, _) = open(&path_b)?;
    let first_ballot = ballot(1, &a_key);
    let prepare = verifier.prepare_request(first_ballot)?;
    let prepared = verifier.verify_prepare(
        first_ballot,
        &[
            a.vote_control(&prepare, &a_key)?,
            b.vote_control(&prepare, &b_key)?,
        ],
    )?;
    let chosen = value(b"chosen before reply loss");
    let proposal = propose(&a, &a_key, &prepared, &chosen)?;
    let accept = verifier.accept_request(&proposal)?;
    let accepted = [
        a.vote_control(&accept, &a_key)?,
        b.vote_control(&accept, &b_key)?,
    ];
    prepared.verify_chosen(&chosen, &accepted)?;
    drop(accepted);
    drop(a);
    drop(b);

    let (a, _) = open(&path_a)?;
    let (b, _) = open(&path_b)?;
    let higher = ballot(2, &c_key);
    let retry_prepare = verifier.prepare_request(higher)?;
    let retry_promises = [
        a.vote_control(&retry_prepare, &a_key)?,
        b.vote_control(&retry_prepare, &b_key)?,
    ];
    let recovered = verifier.verify_prepare(higher, &retry_promises)?;
    if recovered.check_value(&value(b"different value")).is_ok() {
        return Err("higher ballot accepted a replacement after majority acceptance".into());
    }
    let (c, _) = open(&path_c)?;
    let proposal = propose(&c, &c_key, &recovered, &chosen)?;
    let accept_same = verifier.accept_request(&proposal)?;
    let recovered_votes = [
        a.vote_control(&accept_same, &a_key)?,
        c.vote_control(&accept_same, &c_key)?,
    ];
    recovered.verify_chosen(&chosen, &recovered_votes)?;
    Ok(())
}

#[test]
fn accept_majority_may_include_voter_without_prior_prepare() -> TestResult {
    let [a_key, b_key, c_key] = keys();
    let verifier = verifier([&a_key, &b_key, &c_key])?;
    let directory = tempfile::tempdir()?;
    let [path_a, path_b, path_c] = [
        directory.path().join("a.redb"),
        directory.path().join("b.redb"),
        directory.path().join("c.redb"),
    ];
    let (a, _) = open(&path_a)?;
    let (b, _) = open(&path_b)?;
    let (c, journal_c) = open(&path_c)?;
    let current = ballot(1, &a_key);
    let prepare = verifier.prepare_request(current)?;
    let prepared = verifier.verify_prepare(
        current,
        &[
            a.vote_control(&prepare, &a_key)?,
            b.vote_control(&prepare, &b_key)?,
        ],
    )?;
    let chosen = value(b"accept quorum includes c without prepare");
    let proposal = propose(&a, &a_key, &prepared, &chosen)?;
    let accept = verifier.accept_request(&proposal)?;
    let accepted_a = a.vote_control(&accept, &a_key)?;
    let accepted_c = c.vote_control(&accept, &c_key)?;
    let accepted = [accepted_a, accepted_c.clone()];
    prepared.verify_chosen(&chosen, &accepted)?;
    let c_votes = control_votes(&journal_c)?;
    if c_votes.len() != 1 || vote_value(&accepted_c)? != chosen {
        return Err("C did not persist exactly one accept without a prior prepare".into());
    }
    let before_reopen = journal_c.replay()?;
    drop(c);
    drop(journal_c);

    let (c, journal_c) = open(&path_c)?;
    if journal_c.replay()? != before_reopen {
        return Err("C accept changed across reopen".into());
    }
    let lower = verifier.prepare_request(ballot(0, &b_key))?;
    let denied = c.vote_control(&lower, &c_key);
    if denied != Err(NodeError::ControlVote(ControlQuorumError::SupersededBallot))
        || journal_c.replay()? != before_reopen
    {
        return Err("C accept did not persist the implicit promise after reopen".into());
    }
    Ok(())
}

#[test]
fn competing_minorities_are_legal_through_voter_apis() -> TestResult {
    let [a_key, b_key, c_key] = keys();
    let verifier = verifier([&a_key, &b_key, &c_key])?;
    let directory = tempfile::tempdir()?;
    let [path_a, path_b, path_c] = [
        directory.path().join("a.redb"),
        directory.path().join("b.redb"),
        directory.path().join("c.redb"),
    ];
    let (a, _) = open(&path_a)?;
    let (b, _) = open(&path_b)?;
    let (c, _) = open(&path_c)?;
    let first_ballot = ballot(1, &a_key);
    let first_prepare = verifier.prepare_request(first_ballot)?;
    let first_prepared = verifier.verify_prepare(
        first_ballot,
        &[
            a.vote_control(&first_prepare, &a_key)?,
            b.vote_control(&first_prepare, &b_key)?,
        ],
    )?;
    let left = value(b"minority left");
    let first_proposal = propose(&a, &a_key, &first_prepared, &left)?;
    let first_accept = verifier.accept_request(&first_proposal)?;
    a.vote_control(&first_accept, &a_key)?;

    let second_ballot = ballot(2, &b_key);
    let second_prepare = verifier.prepare_request(second_ballot)?;
    let second_prepared = verifier.verify_prepare(
        second_ballot,
        &[
            b.vote_control(&second_prepare, &b_key)?,
            c.vote_control(&second_prepare, &c_key)?,
        ],
    )?;
    let right = value(b"minority right");
    let second_proposal = propose(&b, &b_key, &second_prepared, &right)?;
    let second_accept = verifier.accept_request(&second_proposal)?;
    c.vote_control(&second_accept, &c_key)?;

    let third_ballot = ballot(3, &c_key);
    let third_prepare = verifier.prepare_request(third_ballot)?;
    let reports = [
        a.vote_control(&third_prepare, &a_key)?,
        c.vote_control(&third_prepare, &c_key)?,
    ];
    let prepared = verifier.verify_prepare(third_ballot, &reports)?;
    if prepared.check_value(&left).is_ok() || prepared.check_value(&right).is_err() {
        return Err("higher prepare did not recover the highest accepted minority".into());
    }
    let recovered_proposal = propose(&c, &c_key, &prepared, &right)?;
    let recovered_accept = verifier.accept_request(&recovered_proposal)?;
    let recovered_votes = [
        a.vote_control(&recovered_accept, &a_key)?,
        c.vote_control(&recovered_accept, &c_key)?,
    ];
    prepared.verify_chosen(&right, &recovered_votes)?;
    drop(a);
    drop(c);

    let (a, _) = open(&path_a)?;
    let (c, _) = open(&path_c)?;
    let later_ballot = ballot(4, &b_key);
    let later_prepare = verifier.prepare_request(later_ballot)?;
    let later_a = a.vote_control(&later_prepare, &a_key)?;
    let later_c = c.vote_control(&later_prepare, &c_key)?;
    for vote in [&later_a, &later_c] {
        let Some(report) = promise_accepted(vote)? else {
            return Err(
                "reopened recovered minority voter did not report an accepted value".into(),
            );
        };
        if report.ballot != third_ballot || report.value != right {
            return Err("reopened recovered minority voter reported the wrong value".into());
        }
    }
    let later = verifier.verify_prepare(later_ballot, &[later_a, later_c])?;
    if later.check_value(&right).is_err() || later.check_value(&left).is_ok() {
        return Err("later prepare did not preserve the recovered minority value".into());
    }
    Ok(())
}

#[test]
fn same_ballot_conflict_is_rejected_after_independent_empty_prepare_certs() -> TestResult {
    let [a_key, b_key, c_key] = keys();
    let verifier = verifier([&a_key, &b_key, &c_key])?;
    let directory = tempfile::tempdir()?;
    let [path_a, path_b, path_c] = [
        directory.path().join("a.redb"),
        directory.path().join("b.redb"),
        directory.path().join("c.redb"),
    ];
    let (a, _) = open(&path_a)?;
    let (b, journal_b) = open(&path_b)?;
    let (c, _) = open(&path_c)?;
    let current = ballot(1, &a_key);
    let prepare = verifier.prepare_request(current)?;
    let first_promises = [
        a.vote_control(&prepare, &a_key)?,
        b.vote_control(&prepare, &b_key)?,
    ];
    verifier.verify_prepare(current, &first_promises)?;
    let second_promises = [
        b.vote_control(&prepare, &b_key)?,
        c.vote_control(&prepare, &c_key)?,
    ];
    verifier.verify_prepare(current, &second_promises)?;
    let first = value(b"first");
    let first_proposal = sign_proposal(&a_key, current, first, first_promises.to_vec())?;
    let first_accept = verifier.accept_request(&first_proposal)?;
    b.vote_control(&first_accept, &b_key)?;
    let before = journal_b.replay()?;
    let conflicting_proposal =
        sign_proposal(&a_key, current, value(b"second"), second_promises.to_vec())?;
    let conflicting_accept = verifier.accept_request(&conflicting_proposal)?;
    let denied = b.vote_control(&conflicting_accept, &b_key);
    if denied
        != Err(NodeError::ControlVote(
            ControlQuorumError::ConflictingAcceptedValues,
        ))
        || journal_b.replay()? != before
    {
        return Err("same-ballot accepted value conflict changed durable voter state".into());
    }
    Ok(())
}

#[test]
fn foreign_wrapper_of_valid_local_signature_raises_promise_after_reopen() -> TestResult {
    let [a_key, b_key, c_key] = keys();
    let verifier = verifier([&a_key, &b_key, &c_key])?;
    let directory = tempfile::tempdir()?;
    let source_path = directory.path().join("source.redb");
    let target_path = directory.path().join("target.redb");
    let (source, source_journal) = open(&source_path)?;
    let promised = ballot(5, &a_key);
    let request = verifier.prepare_request(promised)?;
    let signed = source.vote_control(&request, &a_key)?;
    let source_record = only_control_vote(&source_journal)?;
    if !matches!(
        &source_record.event,
        NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(_))
    ) {
        return Err("fixture did not record a control vote".into());
    }

    let (target, _) = open(&target_path)?;
    let foreign = wrapped_vote(
        &source_record,
        myko_federation::NodeId::new(),
        40,
        signed.clone(),
    );
    if target.ingest(foreign)?
        != (IngestStatus::Applied {
            position: LogPosition::FIRST,
        })
    {
        return Err("foreign wrapper was not durably ingested at the first local position".into());
    }
    drop(target);

    let (target, journal) = open(&target_path)?;
    if target.vote_control(&request, &a_key)? != signed || control_votes(&journal)?.len() != 1 {
        return Err("foreign-wrapped local signature did not become the exact retry record".into());
    }
    let lower = verifier.prepare_request(ballot(4, &a_key))?;
    let before = journal.replay()?;
    let denied = target.vote_control(&lower, &a_key);
    if denied != Err(NodeError::ControlVote(ControlQuorumError::SupersededBallot))
        || journal.replay()? != before
    {
        return Err("foreign-wrapped local signature did not raise the durable promise".into());
    }
    Ok(())
}

#[test]
fn forged_local_signature_never_becomes_voter_state() -> TestResult {
    let [a_key, b_key, c_key] = keys();
    let verifier = verifier([&a_key, &b_key, &c_key])?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("forged.redb");
    let (node, journal) = open(&path)?;
    let template_path = directory.path().join("template.redb");
    let (template_node, template_journal) = open(&template_path)?;
    let template_request = verifier.prepare_request(ballot(20, &c_key))?;
    template_node.vote_control(&template_request, &c_key)?;
    let template = only_control_vote(&template_journal)?;
    let high = ballot(9, &a_key);
    let forged = SignedControlVote {
        message: ControlVote {
            slot: slot(),
            ballot: high,
            controller: id(&a_key),
            vote: ControlVoteKind::Promise { accepted: None },
        },
        signature: [0; 64],
    };
    let local_wrapper = wrapped_vote(&template, node.node_id(), 1, forged);
    let before = journal.replay()?;
    if !matches!(
        node.ingest(local_wrapper),
        Err(NodeError::ControlVote(ControlQuorumError::InvalidSignature))
    ) || journal.replay()? != before
    {
        return Err("forged local-looking signature was retained".into());
    }
    let lower = verifier.prepare_request(ballot(1, &a_key))?;
    let signed_lower = node.vote_control(&lower, &a_key)?;
    if signed_lower.message.ballot != ballot(1, &a_key)
        || promise_accepted(&signed_lower)?.is_some()
    {
        return Err("forged local-looking signature raised promise state".into());
    }
    Ok(())
}

#[test]
fn conflicting_valid_local_signed_records_refuse_future_voting() -> TestResult {
    let [a_key, b_key, c_key] = keys();
    let verifier = verifier([&a_key, &b_key, &c_key])?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("conflict.redb");
    let (node, first_journal) = open(&path)?;
    let template_path = directory.path().join("template.redb");
    let (template_node, template_journal) = open(&template_path)?;
    let template_request = verifier.prepare_request(ballot(20, &c_key))?;
    template_node.vote_control(&template_request, &c_key)?;
    let template = only_control_vote(&template_journal)?;
    let accepted_ballot = ballot(5, &a_key);
    let left = sign(
        &a_key,
        accepted_ballot,
        ControlVoteKind::Accept {
            value: value(b"left"),
        },
    )?;
    let right = sign(
        &a_key,
        accepted_ballot,
        ControlVoteKind::Accept {
            value: value(b"right"),
        },
    )?;
    node.ingest(wrapped_vote(
        &template,
        myko_federation::NodeId::new(),
        1,
        left,
    ))?;
    node.ingest(wrapped_vote(
        &template,
        myko_federation::NodeId::new(),
        2,
        right,
    ))?;
    drop(node);
    drop(first_journal);

    let (reopened, journal) = open(&path)?;
    let before = journal.replay()?;
    let prepare = verifier.prepare_request(ballot(6, &b_key))?;
    let denied = reopened.vote_control(&prepare, &a_key);
    if denied
        != Err(NodeError::ControlVote(
            ControlQuorumError::ConflictingAcceptedValues,
        ))
        || journal.replay()? != before
    {
        return Err("conflicting valid local signatures allowed another vote".into());
    }
    Ok(())
}

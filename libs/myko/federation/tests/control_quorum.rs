use std::error::Error;

use ed25519_dalek::{Signer as _, SigningKey};
use myko_federation::{ScopeId, control_quorum::*};

type TestResult = Result<(), Box<dyn Error>>;

fn keys() -> [SigningKey; 3] {
    [1, 2, 3].map(|seed| SigningKey::from_bytes(&[seed; 32]))
}

fn id(key: &SigningKey) -> ControllerId {
    ControllerId(key.verifying_key().to_bytes())
}

fn slot() -> ControlSlot {
    ControlSlot {
        realm: ScopeId::new("authority:main"),
        epoch: ControlEpochId([4; 32]),
        predecessor: ControlHead([5; 32]),
    }
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

fn promise(
    key: &SigningKey,
    ballot: ControlBallot,
    accepted: Option<AcceptedControlValue>,
) -> Result<SignedControlVote, serde_json::Error> {
    sign(key, ballot, ControlVoteKind::Promise { accepted })
}

#[test]
fn empty_prepare_majority_allows_a_new_value() -> TestResult {
    let [a, b, c] = keys();
    let verifier = ControlQuorumVerifier::new(slot(), [id(&a), id(&b), id(&c)])?;
    let ballot = ballot(1, &a);
    let votes = [promise(&a, ballot, None)?, promise(&b, ballot, None)?];
    let prepared = verifier.verify_prepare(ballot, &votes)?;
    let proposed = value(b"new transition with full effect bytes");
    prepared.check_value(&proposed)?;
    if prepared.slot() != &slot()
        || prepared.ballot() != ballot
        || prepared.select_value(proposed.clone()) != proposed
    {
        return Err("prepare quorum changed the slot, ballot, or new value".into());
    }
    Ok(())
}

#[test]
fn recovery_requires_the_highest_accepted_full_value_regardless_of_reply_order() -> TestResult {
    let [a, b, c] = keys();
    let verifier = ControlQuorumVerifier::new(slot(), [id(&a), id(&b), id(&c)])?;
    let current = ballot(7, &c);
    let older = AcceptedControlValue {
        ballot: ballot(2, &a),
        value: value(b"older"),
    };
    let newest = AcceptedControlValue {
        ballot: ballot(5, &b),
        value: value(b"complete newer effect"),
    };
    let votes = [
        promise(&a, current, Some(older))?,
        promise(&b, current, Some(newest.clone()))?,
    ];
    let [first, second] = votes.clone();
    for ordered in [votes, [second, first]] {
        let prepared = verifier.verify_prepare(current, &ordered)?;
        prepared.check_value(&newest.value)?;
        if prepared.select_value(value(b"unrelated proposal")) != newest.value
            || !matches!(
                prepared.check_value(&value(b"unrelated proposal")),
                Err(ControlQuorumError::WrongValue)
            )
        {
            return Err(
                "prepare quorum allowed the proposer to discard the highest accepted value".into(),
            );
        }
    }
    Ok(())
}

#[test]
fn conflicting_or_future_accepted_reports_are_rejected() -> TestResult {
    let [a, b, c] = keys();
    let verifier = ControlQuorumVerifier::new(slot(), [id(&a), id(&b), id(&c)])?;
    let current = ballot(7, &c);
    let earlier = ballot(3, &a);
    let votes = [
        promise(
            &a,
            current,
            Some(AcceptedControlValue {
                ballot: earlier,
                value: value(b"x"),
            }),
        )?,
        promise(
            &b,
            current,
            Some(AcceptedControlValue {
                ballot: earlier,
                value: value(b"y"),
            }),
        )?,
    ];
    if !matches!(
        verifier.verify_prepare(current, &votes),
        Err(ControlQuorumError::ConflictingAcceptedValues)
    ) {
        return Err("conflicting accepted values were resolved arbitrarily".into());
    }
    let future = [
        promise(
            &a,
            current,
            Some(AcceptedControlValue {
                ballot: ballot(8, &a),
                value: value(b"x"),
            }),
        )?,
        promise(&b, current, None)?,
    ];
    if !matches!(
        verifier.verify_prepare(current, &future),
        Err(ControlQuorumError::InvalidAcceptedBallot)
    ) {
        return Err("future accepted ballot was trusted".into());
    }
    Ok(())
}

#[test]
fn duplicates_and_minority_cannot_manufacture_a_majority() -> TestResult {
    let [a, b, c] = keys();
    let verifier = ControlQuorumVerifier::new(slot(), [id(&a), id(&b), id(&c)])?;
    let current = ballot(1, &a);
    let vote = promise(&a, current, None)?;
    if !matches!(
        verifier.verify_prepare(current, std::slice::from_ref(&vote)),
        Err(ControlQuorumError::InsufficientQuorum)
    ) || !matches!(
        verifier.verify_prepare(current, &[vote.clone(), vote]),
        Err(ControlQuorumError::DuplicateController)
    ) {
        return Err("repeated or minority votes formed a quorum".into());
    }
    let two = ControlQuorumVerifier::new(slot(), [id(&a), id(&b)])?;
    if !matches!(
        two.verify_prepare(current, &[promise(&a, current, None)?]),
        Err(ControlQuorumError::InsufficientQuorum)
    ) {
        return Err("two-controller configuration did not require both votes".into());
    }
    two.verify_prepare(
        current,
        &[promise(&a, current, None)?, promise(&b, current, None)?],
    )?;
    Ok(())
}

#[test]
fn signatures_cover_value_phase_and_controller() -> TestResult {
    let [a, b, c] = keys();
    let verifier = ControlQuorumVerifier::new(slot(), [id(&a), id(&b), id(&c)])?;
    let current = ballot(1, &a);
    let first = promise(&a, current, None)?;
    let second = promise(&b, current, None)?;
    let mut changed_value = first.clone();
    changed_value.message.vote = ControlVoteKind::Promise {
        accepted: Some(AcceptedControlValue {
            ballot: current,
            value: value(b"injected"),
        }),
    };
    let mut changed_phase = first.clone();
    changed_phase.message.vote = ControlVoteKind::Accept {
        value: value(b"injected"),
    };
    let mut changed_controller = first;
    changed_controller.message.controller = id(&c);
    for tampered in [changed_value, changed_phase, changed_controller] {
        if !matches!(
            verifier.verify_prepare(current, &[tampered, second.clone()]),
            Err(ControlQuorumError::InvalidSignature)
        ) {
            return Err("tampered signed content was trusted".into());
        }
    }
    Ok(())
}

#[test]
fn independently_expected_slot_and_membership_reject_foreign_evidence() -> TestResult {
    let [a, b, c] = keys();
    let current = ballot(1, &a);
    let votes = [promise(&a, current, None)?, promise(&b, current, None)?];
    let mut wrong_realm = slot();
    wrong_realm.realm = ScopeId::new("authority:foreign");
    let mut wrong_epoch = slot();
    wrong_epoch.epoch = ControlEpochId([6; 32]);
    let mut wrong_head = slot();
    wrong_head.predecessor = ControlHead([7; 32]);
    for expected in [wrong_realm, wrong_epoch, wrong_head] {
        let verifier = ControlQuorumVerifier::new(expected, [id(&a), id(&b), id(&c)])?;
        if !matches!(
            verifier.verify_prepare(current, &votes),
            Err(ControlQuorumError::UnexpectedVote)
        ) {
            return Err("vote supplied its own trusted decision context".into());
        }
    }
    let verifier = ControlQuorumVerifier::new(slot(), [id(&a), id(&c)])?;
    if !matches!(
        verifier.verify_prepare(current, &votes),
        Err(ControlQuorumError::UnexpectedVote)
    ) || !matches!(
        verifier.verify_prepare(ballot(1, &b), &votes),
        Err(ControlQuorumError::UnknownProposer)
    ) || !matches!(
        verifier.verify_prepare(ballot(2, &a), &votes),
        Err(ControlQuorumError::UnexpectedVote)
    ) {
        return Err("unknown controller or mismatched ballot was trusted".into());
    }
    Ok(())
}

#[test]
fn changing_expected_context_does_not_make_old_signatures_valid() -> TestResult {
    let [a, b, c] = keys();
    let current = ballot(1, &a);
    let votes = [promise(&a, current, None)?, promise(&b, current, None)?];
    let mut changed_realm = slot();
    changed_realm.realm = ScopeId::new("authority:changed");
    let mut changed_epoch = slot();
    changed_epoch.epoch = ControlEpochId([9; 32]);
    let mut changed_head = slot();
    changed_head.predecessor = ControlHead([9; 32]);
    for context in [changed_realm, changed_epoch, changed_head] {
        let verifier = ControlQuorumVerifier::new(context.clone(), [id(&a), id(&b), id(&c)])?;
        let tampered = votes.clone().map(|mut vote| {
            vote.message.slot = context.clone();
            vote
        });
        if !matches!(
            verifier.verify_prepare(current, &tampered),
            Err(ControlQuorumError::InvalidSignature)
        ) {
            return Err("signature did not bind the decision context".into());
        }
    }
    let verifier = ControlQuorumVerifier::new(slot(), [id(&a), id(&b), id(&c)])?;
    let next = ballot(2, &c);
    let tampered = votes.map(|mut vote| {
        vote.message.ballot = next;
        vote
    });
    if !matches!(
        verifier.verify_prepare(next, &tampered),
        Err(ControlQuorumError::InvalidSignature)
    ) {
        return Err("signature did not bind the ballot".into());
    }
    Ok(())
}

#[test]
fn chosen_head_depends_on_slot_and_value_not_retry_ballot_or_signers() -> TestResult {
    let [a, b, c] = keys();
    let verifier = ControlQuorumVerifier::new(slot(), [id(&a), id(&b), id(&c)])?;
    let payload = value(b"stable operation id and complete authority effect");
    let first = ballot(1, &a);
    let retry = ballot(8, &c);
    let accept = ControlVoteKind::Accept {
        value: payload.clone(),
    };
    let initial_prepare = verifier.verify_prepare(
        first,
        &[promise(&a, first, None)?, promise(&b, first, None)?],
    )?;
    let initial = initial_prepare.verify_chosen(
        &payload,
        &[
            sign(&a, first, accept.clone())?,
            sign(&b, first, accept.clone())?,
        ],
    )?;
    let accepted = Some(AcceptedControlValue {
        ballot: first,
        value: payload.clone(),
    });
    let retry_prepare = verifier.verify_prepare(
        retry,
        &[promise(&b, retry, accepted)?, promise(&c, retry, None)?],
    )?;
    let recovered = retry_prepare.verify_chosen(
        &payload,
        &[sign(&b, retry, accept.clone())?, sign(&c, retry, accept)?],
    )?;
    if initial.head()? != recovered.head()?
        || recovered.value() != &payload
        || recovered.slot() != &slot()
        || recovered.ballot() != retry
    {
        return Err("the same recovered decision acquired a different head or payload".into());
    }
    Ok(())
}

#[test]
fn chosen_path_cannot_ignore_the_prepared_recovery_value() -> TestResult {
    let [a, b, c] = keys();
    let verifier = ControlQuorumVerifier::new(slot(), [id(&a), id(&b), id(&c)])?;
    let current = ballot(7, &c);
    let required = AcceptedControlValue {
        ballot: ballot(3, &a),
        value: value(b"must recover"),
    };
    let promises = [
        promise(&a, current, Some(required))?,
        promise(&b, current, None)?,
    ];
    let prepared = verifier.verify_prepare(current, &promises)?;
    let replacement = value(b"illegal replacement");
    if !matches!(
        prepared.check_value(&replacement),
        Err(ControlQuorumError::WrongValue)
    ) {
        return Err("fixture did not require recovery".into());
    }
    let accepts = [
        sign(
            &a,
            current,
            ControlVoteKind::Accept {
                value: replacement.clone(),
            },
        )?,
        sign(
            &b,
            current,
            ControlVoteKind::Accept {
                value: replacement.clone(),
            },
        )?,
    ];
    if !matches!(
        prepared.verify_chosen(&replacement, &accepts),
        Err(ControlQuorumError::WrongValue)
    ) {
        return Err("chosen verification ignored the prepared recovery value".into());
    }
    Ok(())
}

#[test]
fn promises_and_mismatched_values_are_not_chosen_certificates() -> TestResult {
    let [a, b, c] = keys();
    let verifier = ControlQuorumVerifier::new(slot(), [id(&a), id(&b), id(&c)])?;
    let current = ballot(1, &a);
    let payload = value(b"intended");
    let promises = [promise(&a, current, None)?, promise(&b, current, None)?];
    let prepared = verifier.verify_prepare(current, &promises)?;
    if !matches!(
        prepared.verify_chosen(&payload, &promises),
        Err(ControlQuorumError::WrongPhase)
    ) {
        return Err("promises were counted as a chosen value".into());
    }
    let accepts = [
        sign(
            &a,
            current,
            ControlVoteKind::Accept {
                value: payload.clone(),
            },
        )?,
        sign(
            &b,
            current,
            ControlVoteKind::Accept {
                value: value(b"other"),
            },
        )?,
    ];
    if !matches!(
        prepared.verify_chosen(&payload, &accepts),
        Err(ControlQuorumError::WrongValue)
    ) || !matches!(
        verifier.verify_prepare(current, &accepts),
        Err(ControlQuorumError::WrongPhase)
    ) {
        return Err("phase or value mismatch was accepted".into());
    }
    Ok(())
}

#[test]
fn invalid_controller_configurations_are_rejected() -> TestResult {
    let [a, _, _] = keys();
    for controllers in [vec![], vec![id(&a), id(&a)], vec![ControllerId([0; 32])]] {
        if !matches!(
            ControlQuorumVerifier::new(slot(), controllers),
            Err(ControlQuorumError::InvalidControllers)
        ) {
            return Err("invalid controller configuration was accepted".into());
        }
    }
    Ok(())
}

#[test]
fn serialized_votes_remain_unverified_until_signature_check() -> TestResult {
    let [a, b, c] = keys();
    let verifier = ControlQuorumVerifier::new(slot(), [id(&a), id(&b), id(&c)])?;
    let current = ballot(1, &a);
    let original = promise(&a, current, None)?;
    let bytes = serde_json::to_vec(&original)?;
    let decoded: SignedControlVote = serde_json::from_slice(&bytes)?;
    if decoded != original {
        return Err("signed vote bytes changed on round trip".into());
    }
    let mut forged = decoded;
    forged.signature = [0; 64];
    if !matches!(
        verifier.verify_prepare(current, &[forged, promise(&b, current, None)?]),
        Err(ControlQuorumError::InvalidSignature)
    ) {
        return Err("decoded wire container bypassed signature verification".into());
    }
    Ok(())
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

#[test]
fn accept_request_requires_proposer_signature_and_valid_prepare_majority() -> TestResult {
    let [a, b, c] = keys();
    let verifier = ControlQuorumVerifier::new(slot(), [id(&a), id(&b), id(&c)])?;
    let current = ballot(1, &a);
    let votes = vec![promise(&a, current, None)?, promise(&b, current, None)?];
    let proposal = sign_proposal(&a, current, value(b"value"), votes.clone())?;
    verifier.accept_request(&proposal)?;
    let wrong_key = sign_proposal(&b, current, value(b"value"), votes)?;
    if !matches!(
        verifier.accept_request(&wrong_key),
        Err(ControlQuorumError::InvalidSignature)
    ) {
        return Err("accept request trusted a key other than the proposer".into());
    }
    let missing = sign_proposal(&a, current, value(b"value"), Vec::new())?;
    if !matches!(
        verifier.accept_request(&missing),
        Err(ControlQuorumError::InsufficientQuorum)
    ) {
        return Err("proposer signature replaced a prepare quorum".into());
    }
    let repeated = promise(&a, current, None)?;
    let duplicates = sign_proposal(
        &a,
        current,
        value(b"value"),
        vec![repeated.clone(), repeated],
    )?;
    if !matches!(
        verifier.accept_request(&duplicates),
        Err(ControlQuorumError::DuplicateController)
    ) {
        return Err("proposal counted duplicate prepare signers".into());
    }
    let other_electorate = ControlQuorumVerifier::new(slot(), [id(&a), id(&c)])?;
    if !matches!(
        other_electorate.accept_request(&proposal),
        Err(ControlQuorumError::UnexpectedVote)
    ) {
        return Err("proposal supplied its own electorate".into());
    }
    Ok(())
}

#[test]
fn proposer_cannot_sign_away_the_highest_accepted_value() -> TestResult {
    let [a, b, c] = keys();
    let verifier = ControlQuorumVerifier::new(slot(), [id(&a), id(&b), id(&c)])?;
    let current = ballot(7, &c);
    let accepted = AcceptedControlValue {
        ballot: ballot(2, &a),
        value: value(b"must recover"),
    };
    let votes = vec![
        promise(&a, current, Some(accepted.clone()))?,
        promise(&b, current, None)?,
    ];
    let correct = sign_proposal(&c, current, accepted.value, votes.clone())?;
    verifier.accept_request(&correct)?;
    let replacement = sign_proposal(&c, current, value(b"replacement"), votes)?;
    if !matches!(
        verifier.accept_request(&replacement),
        Err(ControlQuorumError::WrongValue)
    ) {
        return Err("proposer signature bypassed the recovery rule".into());
    }
    Ok(())
}

#[test]
fn proposal_signature_binds_the_full_value_context_and_prepare_proof() -> TestResult {
    let [a, b, c] = keys();
    let current = ballot(1, &a);
    let original = sign_proposal(
        &a,
        current,
        value(b"original"),
        vec![promise(&a, current, None)?, promise(&b, current, None)?],
    )?;
    let mut changed_value = original.clone();
    changed_value.message.value = value(b"changed");
    let mut changed_proof = original.clone();
    changed_proof.message.prepare_votes = vec![promise(&c, current, None)?];
    let mut changed_slot = original.clone();
    changed_slot.message.slot.predecessor = ControlHead([8; 32]);
    let mut changed_ballot = original.clone();
    changed_ballot.message.ballot = ballot(2, &a);
    for changed in [changed_value, changed_proof, changed_slot, changed_ballot] {
        if !matches!(
            changed.verify_signature(),
            Err(ControlQuorumError::InvalidSignature)
        ) {
            return Err("proposal signature did not bind its entire body".into());
        }
    }
    let decoded: SignedControlProposal = serde_json::from_slice(&serde_json::to_vec(&original)?)?;
    if decoded != original {
        return Err("proposal proof changed on wire round trip".into());
    }
    decoded.verify_signature()?;
    Ok(())
}

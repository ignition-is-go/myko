use std::error::Error;
use std::sync::Arc;

use chrono::{Duration, Utc};
use ed25519_dalek::{Signer as _, SigningKey};
use myko::{ApplicationHost, MykoApplication};
use myko_authority::{
    AuthorityPolicy, RevocationKind, authority_realm_scope,
    certified::{AuthorityAnchor, AuthorityHistory, AuthoritySelection},
};
use myko_federation::{
    AccessAttempt, AccessOperation, AccessPolicy as _, AccessTarget, AuthorityConstraints,
    AuthorityGrant, AuthorityGrantId, AuthorityPresentation, AuthorityRealmId, CommandId,
    EventEnvelope, EventId, FederationPermission, FrameworkControlEvent, LogPosition, Node,
    NodeEvent, NodeId, Principal, PrincipalId, PrincipalKind, ScopeId, ScopeSelection,
    ScopeTopology, ServiceId,
    control_quorum::{
        ControlBallot, ControlEpochId, ControlHead, ControlQuorumVerifier, ControlSlot,
        ControlVoteKind, ControllerId,
    },
};
use myko_redb::RedbJournal;

type TestResult = Result<(), Box<dyn Error>>;

struct Fixture {
    events: Vec<EventEnvelope>,
    records: Vec<EventEnvelope>,
    head: ControlHead,
    revoked_head: ControlHead,
    request: AccessAttempt,
}

fn realm() -> AuthorityRealmId {
    AuthorityRealmId::new("certified-test")
}

fn keys() -> [SigningKey; 3] {
    [1, 2, 3].map(|seed| SigningKey::from_bytes(&[seed; 32]))
}

fn slot() -> ControlSlot {
    ControlSlot {
        realm: authority_realm_scope(&realm()),
        epoch: ControlEpochId([8; 32]),
        predecessor: ControlHead([9; 32]),
    }
}

fn anchor() -> Result<AuthorityAnchor, String> {
    AuthorityAnchor::new(
        realm(),
        slot().epoch,
        slot().predecessor,
        keys()
            .iter()
            .map(|key| ControllerId(key.verifying_key().to_bytes()))
            .collect(),
    )
}

fn fixture(max_uses: Option<u64>) -> Result<Fixture, Box<dyn Error>> {
    fixture_with_selection_ids(max_uses, [CommandId::new(), CommandId::new()])
}

fn fixture_with_selection_ids(
    max_uses: Option<u64>,
    [first_operation, second_operation]: [CommandId; 2],
) -> Result<Fixture, Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let source = RedbJournal::open_node(directory.path().join("founder.redb"))?;
    let other = RedbJournal::open_node(directory.path().join("other.redb"))?;
    let app = AuthorityPolicy::install(MykoApplication::new())?;
    let host = ApplicationHost::new(source.clone(), app)?;
    let policy = Arc::new(AuthorityPolicy::new(host, realm()));
    source.set_command_access_policy(policy.clone())?;
    let admin = Principal::new(PrincipalId::new("admin"), PrincipalKind::Node);
    policy.bootstrap(admin.clone())?;
    let reader = Principal::new(PrincipalId::new("reader"), PrincipalKind::Node);
    let scope = ScopeId::new("project:historical");
    policy.issue_grant(
        admin.clone(),
        AuthorityPresentation::direct(admin.clone()),
        AuthorityGrant {
            id: AuthorityGrantId::new("portable-grant"),
            realm_id: realm(),
            grantor: admin.clone(),
            grantee: reader.clone(),
            selection: ScopeSelection::Exact(scope.clone()),
            permissions: vec![FederationPermission::ReadState],
            operations: vec![AccessOperation::ReadItems],
            capabilities: Vec::new(),
            constraints: AuthorityConstraints::default(),
            obligations: Vec::new(),
            valid_from: Utc::now()
                .checked_sub_signed(Duration::seconds(10))
                .ok_or("grant time underflow")?,
            expires_at: None,
            max_uses,
        },
    )?;
    let mut request = AccessAttempt::scoped(
        reader.id.clone(),
        AuthorityPresentation::direct(reader),
        AccessOperation::ReadItems,
        scope.clone(),
    );
    request.target = AccessTarget::ServiceScope {
        service_id: ServiceId::new("test.service"),
        scope_id: scope,
    };
    if max_uses.is_none() && !policy.decide(&request)?.is_permit() {
        return Err("founder did not authorize its own grant".into());
    }
    let records = source
        .events_after(None)?
        .into_iter()
        .filter(|event| match &event.event {
            NodeEvent::CommandCommitted { command, .. } | NodeEvent::CommandLifecycle(command) => {
                command.request.scope_id == authority_realm_scope(&realm())
            }
            NodeEvent::FrameworkControl(_) => false,
        })
        .collect::<Vec<_>>();
    let head = choose(
        &source,
        &other,
        slot().predecessor,
        first_operation,
        &records,
    )?;
    let before_revoke = source.local_history_cut()?;
    policy.revoke(
        admin.clone(),
        AuthorityPresentation::direct(admin),
        RevocationKind::Grant,
        "portable-grant".to_owned(),
    )?;
    let revoked_records = source.events_after(before_revoke)?;
    let revoked_head = choose(&source, &other, head, second_operation, &revoked_records)?;
    let mut events = source.events_after(None)?;
    events.extend(other.events_after(None)?);
    source.set_command_access_policy(Arc::new(myko_federation::DenyAllAccessPolicy))?;
    drop(policy);
    drop(source);
    drop(other);
    let reopened = RedbJournal::open_node(directory.path().join("founder.redb"))?;
    drop(reopened);
    Ok(Fixture {
        events,
        records,
        head,
        revoked_head,
        request,
    })
}

fn choose(
    source: &Node,
    other: &Node,
    predecessor: ControlHead,
    operation: CommandId,
    records: &[EventEnvelope],
) -> Result<ControlHead, Box<dyn Error>> {
    let [a, b, c] = keys();
    let mut next = slot();
    next.predecessor = predecessor;
    let verifier = ControlQuorumVerifier::new(
        next,
        [&a, &b, &c].map(|key| ControllerId(key.verifying_key().to_bytes())),
    )?;
    let ballot = ControlBallot {
        counter: 1,
        proposer: ControllerId(a.verifying_key().to_bytes()),
    };
    let prepare = verifier.prepare_request(ballot)?;
    let promises = [
        source.vote_control(&prepare, &a)?,
        other.vote_control(&prepare, &b)?,
    ];
    let prepared = verifier.verify_prepare(ballot, &promises)?;
    let value = AuthoritySelection::new(operation, records)?.control_value()?;
    let proposal = source.propose_control(&prepared.proposal_request(&value)?, &a)?;
    let accept = verifier.accept_request(&proposal)?;
    let accepted = [
        source.vote_control(&accept, &a)?,
        other.vote_control(&accept, &b)?,
    ];
    Ok(prepared.verify_chosen(&value, &accepted)?.head()?)
}

fn ingest(node: &Node, events: &[EventEnvelope]) -> Result<(), Box<dyn Error>> {
    for event in events {
        node.ingest(event.clone())?;
    }
    Ok(())
}

#[test]
fn certified_foreign_grant_is_historical_after_founder_loss_and_reopen() -> TestResult {
    let fixture = fixture(None)?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("successor.redb");
    {
        let node = RedbJournal::open_node(&path)?;
        ingest(&node, &fixture.events)?;
    }
    let node = RedbJournal::open_node(&path)?;
    let app = AuthorityPolicy::install(MykoApplication::new())?;
    let policy = AuthorityPolicy::new(ApplicationHost::new(node.clone(), app)?, realm());
    if policy.decide(&fixture.request)?.is_permit() {
        return Err("retained certified history silently changed live local authority".into());
    }
    let history = AuthorityHistory::replay(&node, anchor()?)?;
    let assessment = history.assess_at(
        fixture.head,
        &fixture.request,
        Utc::now(),
        ScopeTopology::default(),
    )?;
    if assessment.head() != fixture.head
        || !assessment.decision_at_head().is_permit()
        || assessment.requires_certified_effect()
    {
        return Err("certified retained grant did not reconstruct historical authority".into());
    }
    if policy.decide(&fixture.request)?.is_permit() {
        return Err("historical assessment installed live permission".into());
    }
    let revoked = history.assess_at(
        fixture.revoked_head,
        &fixture.request,
        Utc::now(),
        ScopeTopology::default(),
    )?;
    if revoked.decision_at_head().is_permit() {
        return Err("later certified revocation did not replace the earlier grant".into());
    }
    Ok(())
}

#[test]
fn raw_records_and_missing_selected_bodies_cannot_prove_a_head() -> TestResult {
    let fixture = fixture(None)?;
    let raw = Node::in_memory();
    ingest(&raw, &fixture.records)?;
    if AuthorityHistory::replay(&raw, anchor()?)
        .and_then(|history| {
            history.assess_at(
                fixture.head,
                &fixture.request,
                Utc::now(),
                ScopeTopology::default(),
            )
        })
        .is_ok()
    {
        return Err("raw records created a certified head".into());
    }
    let missing = Node::in_memory();
    let Some(omitted) = fixture.records.last() else {
        return Err("fixture has no records".into());
    };
    let retained = fixture
        .events
        .iter()
        .filter(|event| event.origin != omitted.origin)
        .cloned()
        .collect::<Vec<_>>();
    ingest(&missing, &retained)?;
    if AuthorityHistory::replay(&missing, anchor()?)
        .and_then(|history| {
            history.assess_at(
                fixture.head,
                &fixture.request,
                Utc::now(),
                ScopeTopology::default(),
            )
        })
        .is_ok()
    {
        return Err("proposal bytes replaced a missing retained authority event".into());
    }
    Ok(())
}

#[test]
fn timestamp_mismatch_and_missing_accept_majority_are_rejected() -> TestResult {
    let fixture = fixture(None)?;
    let Some(record) = fixture.records.last() else {
        return Err("fixture has no records".into());
    };
    let mut changed_time = fixture.events.clone();
    for event in &mut changed_time {
        if event.origin == record.origin {
            event.recorded_at = event
                .recorded_at
                .checked_add_signed(Duration::seconds(1))
                .ok_or("record time overflow")?;
        }
    }
    let minority = fixture.events.iter().filter(|event| !matches!(&event.event,
        NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(vote))
            if matches!(vote.message.vote, ControlVoteKind::Accept { .. })
                && vote.message.controller == ControllerId(SigningKey::from_bytes(&[2; 32]).verifying_key().to_bytes())
    )).cloned().collect::<Vec<_>>();
    for events in [changed_time, minority] {
        let node = Node::in_memory();
        ingest(&node, &events)?;
        if AuthorityHistory::replay(&node, anchor()?)
            .and_then(|history| {
                history.assess_at(
                    fixture.head,
                    &fixture.request,
                    Utc::now(),
                    ScopeTopology::default(),
                )
            })
            .is_ok()
        {
            return Err("invalid retained evidence established a historical authority head".into());
        }
    }
    Ok(())
}

#[test]
fn retained_proposal_cannot_supply_its_own_anchor_electorate() -> TestResult {
    let fixture = fixture(None)?;
    let node = Node::in_memory();
    ingest(&node, &fixture.events)?;
    let stranger = SigningKey::from_bytes(&[55; 32]);
    let wrong = AuthorityAnchor::new(
        realm(),
        slot().epoch,
        slot().predecessor,
        vec![ControllerId(stranger.verifying_key().to_bytes())],
    )?;
    if AuthorityHistory::replay(&node, wrong)
        .and_then(|history| {
            history.assess_at(
                fixture.head,
                &fixture.request,
                Utc::now(),
                ScopeTopology::default(),
            )
        })
        .is_ok()
    {
        return Err("proposal selected an electorate not established by the anchor".into());
    }
    Ok(())
}

#[test]
fn historical_bounded_grant_is_not_a_consumed_permit() -> TestResult {
    let fixture = fixture(Some(1))?;
    let node = Node::in_memory();
    ingest(&node, &fixture.events)?;
    let before = node.events_after(None)?;
    let history = AuthorityHistory::replay(&node, anchor()?)?;
    for _ in 0..2 {
        let assessment = history.assess_at(
            fixture.head,
            &fixture.request,
            Utc::now(),
            ScopeTopology::default(),
        )?;
        if !assessment.decision_at_head().is_permit() || !assessment.requires_certified_effect() {
            return Err(
                "historical bounded grant hid its uncommitted consumption requirement".into(),
            );
        }
    }
    if node.events_after(None)? != before {
        return Err("historical assessment consumed or rewrote authority".into());
    }
    Ok(())
}

#[test]
fn outsider_proposal_does_not_poison_an_established_historical_head() -> TestResult {
    let fixture = fixture(None)?;
    let node = Node::in_memory();
    ingest(&node, &fixture.events)?;
    let mut outsider = fixture
        .events
        .iter()
        .find(|event| {
            matches!(
                &event.event,
                NodeEvent::FrameworkControl(FrameworkControlEvent::ControlProposal(_))
            )
        })
        .cloned()
        .ok_or("fixture has no proposal")?;
    let key = SigningKey::from_bytes(&[77; 32]);
    let NodeEvent::FrameworkControl(FrameworkControlEvent::ControlProposal(proposal)) =
        &mut outsider.event
    else {
        return Err("fixture did not select a proposal".into());
    };
    proposal.message.ballot.proposer = ControllerId(key.verifying_key().to_bytes());
    proposal.signature = key.sign(&proposal.message.signing_bytes()?).to_bytes();
    outsider.origin = EventId::new(NodeId::new(), LogPosition::FIRST);
    node.ingest(outsider)?;
    let assessment = AuthorityHistory::replay(&node, anchor()?)?.assess_at(
        fixture.head,
        &fixture.request,
        Utc::now(),
        ScopeTopology::default(),
    )?;
    if !assessment.decision_at_head().is_permit() {
        return Err("untrusted proposal poisoned independently certified history".into());
    }
    Ok(())
}

#[test]
fn malformed_duplicate_proof_cannot_erase_a_valid_chosen_head() -> TestResult {
    let fixture = fixture(None)?;
    let mut malformed = fixture
        .events
        .iter()
        .find(|event| {
            matches!(&event.event,
        NodeEvent::FrameworkControl(FrameworkControlEvent::ControlProposal(proposal))
            if proposal.message.slot.predecessor == slot().predecessor)
        })
        .cloned()
        .ok_or("fixture has no first proposal")?;
    let NodeEvent::FrameworkControl(FrameworkControlEvent::ControlProposal(proposal)) =
        &mut malformed.event
    else {
        return Err("fixture did not select a proposal".into());
    };
    proposal.message.prepare_votes.clear();
    proposal.signature = SigningKey::from_bytes(&[1; 32])
        .sign(&proposal.message.signing_bytes()?)
        .to_bytes();
    malformed.origin = EventId::new(NodeId::new(), LogPosition::FIRST);
    for malformed_first in [false, true] {
        let node = Node::in_memory();
        if malformed_first {
            node.ingest(malformed.clone())?;
        }
        ingest(&node, &fixture.events)?;
        if !malformed_first {
            node.ingest(malformed.clone())?;
        }
        let assessment = AuthorityHistory::replay(&node, anchor()?)?.assess_at(
            fixture.head,
            &fixture.request,
            Utc::now(),
            ScopeTopology::default(),
        )?;
        if !assessment.decision_at_head().is_permit() {
            return Err("malformed duplicate proof erased valid certified authority".into());
        }
    }
    Ok(())
}

#[test]
fn operation_identity_cannot_be_reused_at_a_later_head() -> TestResult {
    let operation = CommandId::new();
    let fixture = fixture_with_selection_ids(None, [operation, operation])?;
    let node = Node::in_memory();
    ingest(&node, &fixture.events)?;
    let history = AuthorityHistory::replay(&node, anchor()?)?;
    if !history
        .assess_at(
            fixture.head,
            &fixture.request,
            Utc::now(),
            ScopeTopology::default(),
        )?
        .decision_at_head()
        .is_permit()
    {
        return Err("later invalid identity erased valid historical predecessor".into());
    }
    let error = history
        .assess_at(
            fixture.revoked_head,
            &fixture.request,
            Utc::now(),
            ScopeTopology::default(),
        )
        .err()
        .ok_or("authority reused a selection operation at a later head")?;
    if error != "control operation was reused in the certified chain" {
        return Err(format!("reused identity failed for the wrong reason: {error}").into());
    }
    Ok(())
}

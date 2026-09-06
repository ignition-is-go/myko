use std::{error::Error, sync::Arc};

use chrono::{Duration, Utc};
use ed25519_dalek::SigningKey;
use myko::{ApplicationHost, MykoApplication};
use myko_authority::{
    AuthorityPolicy, RevocationKind, authority_realm_scope,
    certified::{
        AuthorityAnchor, AuthorityController, AuthorityHistory, AuthorityRotation,
        AuthoritySelection,
    },
};
use myko_federation::{
    AccessAttempt, AccessOperation, AccessTarget, AuthorityConstraints, AuthorityGrant,
    AuthorityGrantId, AuthorityPresentation, AuthorityRealmId, CommandId, DenyAllAccessPolicy,
    EventEnvelope, FederationPermission, Node, Principal, PrincipalId, PrincipalKind, ScopeId,
    ScopeSelection, ScopeTopology, ServiceId,
    control_quorum::{
        ControlBallot, ControlEpochId, ControlHead, ControlQuorumVerifier, ControlSlot,
        ControlValue, ControllerId,
    },
};
use myko_redb::RedbJournal;

type TestResult = Result<(), Box<dyn Error>>;

fn keys(seeds: [u8; 3]) -> [SigningKey; 3] {
    seeds.map(|seed| SigningKey::from_bytes(&[seed; 32]))
}

fn controllers(keys: &[SigningKey; 3]) -> Vec<ControllerId> {
    keys.iter()
        .map(|key| ControllerId(key.verifying_key().to_bytes()))
        .collect()
}

fn realm() -> AuthorityRealmId {
    AuthorityRealmId::new("rotation-test")
}

fn anchor() -> Result<AuthorityAnchor, String> {
    AuthorityAnchor::new(
        realm(),
        ControlEpochId([8; 32]),
        ControlHead([9; 32]),
        controllers(&keys([1, 2, 3])),
    )
}

fn ingest(node: &Node, events: &[EventEnvelope]) -> TestResult {
    for event in events {
        node.ingest(event.clone())?;
    }
    Ok(())
}

fn synchronize(nodes: &[Node; 2]) -> TestResult {
    let a = nodes[0].events_after(None)?;
    let b = nodes[1].events_after(None)?;
    ingest(&nodes[0], &b)?;
    ingest(&nodes[1], &a)
}

fn choose(
    nodes: &[Node; 2],
    keys: &[SigningKey; 3],
    head: ControlHead,
    value: &ControlValue,
) -> Result<ControlHead, Box<dyn Error>> {
    synchronize(nodes)?;
    let context = AuthorityHistory::replay(&nodes[0], anchor()?)?.context_at(head)?;
    let verifier = context.verifier()?;
    let ballot = ControlBallot {
        counter: 1,
        proposer: ControllerId(keys[0].verifying_key().to_bytes()),
    };
    let a = AuthorityController::new(nodes[0].clone(), anchor()?);
    let b = AuthorityController::new(nodes[1].clone(), anchor()?);
    let promises = [
        a.prepare(head, ballot, &keys[0])?,
        b.prepare(head, ballot, &keys[1])?,
    ];
    let proposal = a.propose(head, ballot, &promises, value, &keys[0])?;
    let accepts = [
        a.accept(head, &proposal, &keys[0])?,
        b.accept(head, &proposal, &keys[1])?,
    ];
    let chosen = verifier
        .verify_prepare(ballot, &promises)?
        .verify_chosen(value, &accepts)?
        .head()?;
    synchronize(nodes)?;
    Ok(chosen)
}

struct Handoff {
    directory: tempfile::TempDir,
    successors: [Node; 2],
    grant_head: ControlHead,
    rotation_head: ControlHead,
    pending: Vec<EventEnvelope>,
    selected: Vec<EventEnvelope>,
    request: AccessAttempt,
}

fn handoff() -> Result<Handoff, Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let old = [
        RedbJournal::open_node(directory.path().join("old-a.redb"))?,
        RedbJournal::open_node(directory.path().join("old-b.redb"))?,
    ];
    let app = AuthorityPolicy::install(MykoApplication::new())?;
    let policy = Arc::new(AuthorityPolicy::new(
        ApplicationHost::new(old[0].clone(), app)?,
        realm(),
    ));
    old[0].set_command_access_policy(policy.clone())?;
    let admin = Principal::new(PrincipalId::new("admin"), PrincipalKind::Node);
    let reader = Principal::new(PrincipalId::new("reader"), PrincipalKind::Node);
    let scope = ScopeId::new("rotation-data");
    policy.bootstrap(admin.clone())?;
    policy.issue_grant(
        admin.clone(),
        AuthorityPresentation::direct(admin.clone()),
        AuthorityGrant {
            id: AuthorityGrantId::new("surviving-grant"),
            realm_id: realm(),
            grantor: admin.clone(),
            grantee: reader.clone(),
            selection: ScopeSelection::Exact(scope.clone()),
            permissions: vec![FederationPermission::ReadState],
            operations: vec![AccessOperation::ReadItems],
            capabilities: vec![],
            constraints: AuthorityConstraints::default(),
            obligations: vec![],
            valid_from: Utc::now()
                .checked_sub_signed(Duration::seconds(10))
                .ok_or("time underflow")?,
            expires_at: None,
            max_uses: None,
        },
    )?;
    let selected = old[0].events_after(None)?;
    let grant_head = choose(
        &old,
        &keys([1, 2, 3]),
        anchor()?.genesis(),
        &AuthoritySelection::new(CommandId::new(), &selected)?.control_value()?,
    )?;
    let rotation =
        AuthorityRotation::new(CommandId::new(), realm(), controllers(&keys([4, 5, 6])))?;
    let rotation_head = choose(
        &old,
        &keys([1, 2, 3]),
        grant_head,
        &rotation.control_value()?,
    )?;
    let before = old[0].local_history_cut()?;
    policy.revoke(
        admin.clone(),
        AuthorityPresentation::direct(admin),
        RevocationKind::Grant,
        "surviving-grant".to_owned(),
    )?;
    let pending = old[0].events_after(before)?;
    synchronize(&old)?;
    let retained = old[0].events_after(None)?;
    for name in ["new-a.redb", "new-b.redb"] {
        let successor = RedbJournal::open_node(directory.path().join(name))?;
        ingest(&successor, &retained)?;
    }
    old[0].set_command_access_policy(Arc::new(DenyAllAccessPolicy))?;
    drop(policy);
    drop(old);
    // Opening each database proves the original stores are no longer held open.
    for name in ["old-a.redb", "old-b.redb"] {
        drop(RedbJournal::open_node(directory.path().join(name))?);
    }
    let successors = [
        RedbJournal::open_node(directory.path().join("new-a.redb"))?,
        RedbJournal::open_node(directory.path().join("new-b.redb"))?,
    ];
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
    Ok(Handoff {
        directory,
        successors,
        grant_head,
        rotation_head,
        pending,
        selected,
        request,
    })
}

#[test]
fn disjoint_reopened_successors_certify_revocation_after_old_stores_close() -> TestResult {
    let fixture = handoff()?;
    let history = AuthorityHistory::replay(&fixture.successors[0], anchor()?)?;
    if !history
        .assess_at(
            fixture.rotation_head,
            &fixture.request,
            Utc::now(),
            ScopeTopology::default(),
        )?
        .decision_at_head()
        .is_permit()
    {
        return Err("uncertified revocation affected historical grant".into());
    }
    let context = history.context_at(fixture.rotation_head)?;
    if context.slot().epoch == ControlEpochId([8; 32]) {
        return Err("rotation reused founder epoch".into());
    }
    let next = choose(
        &fixture.successors,
        &keys([4, 5, 6]),
        fixture.rotation_head,
        &AuthoritySelection::new(CommandId::new(), &fixture.pending)?.control_value()?,
    )?;
    drop(fixture.successors);
    let node = RedbJournal::open_node(fixture.directory.path().join("new-a.redb"))?;
    let history = AuthorityHistory::replay(&node, anchor()?)?;
    if !history
        .assess_at(
            fixture.grant_head,
            &fixture.request,
            Utc::now(),
            ScopeTopology::default(),
        )?
        .decision_at_head()
        .is_permit()
        || history
            .assess_at(next, &fixture.request, Utc::now(), ScopeTopology::default())?
            .decision_at_head()
            .is_permit()
    {
        return Err(
            "successor lost historical grant or failed to apply certified revocation".into(),
        );
    }
    Ok(())
}

#[test]
fn missing_selected_history_prevents_successor_context_and_vote() -> TestResult {
    let fixture = handoff()?;
    let node = RedbJournal::open_node(fixture.directory.path().join("missing.redb"))?;
    let omitted = fixture
        .selected
        .last()
        .ok_or("missing selected fixture")?
        .origin;
    for event in fixture.successors[0].events_after(None)? {
        if event.origin != omitted {
            node.ingest(event)?;
        }
    }
    if AuthorityHistory::replay(&node, anchor()?)?
        .context_at(fixture.rotation_head)
        .is_ok()
    {
        return Err("successor context ignored missing predecessor record".into());
    }
    let key = &keys([4, 5, 6])[0];
    let before = node.events_after(None)?;
    let controller = AuthorityController::new(node.clone(), anchor()?);
    if controller
        .prepare(
            fixture.rotation_head,
            ControlBallot {
                counter: 1,
                proposer: ControllerId(key.verifying_key().to_bytes()),
            },
            key,
        )
        .is_ok()
        || node.events_after(None)? != before
    {
        return Err("controller signed without complete selected authority history".into());
    }
    Ok(())
}

#[test]
fn successor_controller_rejects_old_epoch_proposal_before_append() -> TestResult {
    let fixture = handoff()?;
    let old_keys = keys([1, 2, 3]);
    // Old stores reopen only to manufacture stale low-level protocol evidence.
    let stale_nodes = [
        RedbJournal::open_node(fixture.directory.path().join("old-a.redb"))?,
        RedbJournal::open_node(fixture.directory.path().join("old-b.redb"))?,
    ];
    let verifier = ControlQuorumVerifier::new(
        ControlSlot {
            realm: authority_realm_scope(&realm()),
            epoch: ControlEpochId([8; 32]),
            predecessor: fixture.rotation_head,
        },
        controllers(&old_keys),
    )?;
    let ballot = ControlBallot {
        counter: 1,
        proposer: ControllerId(old_keys[0].verifying_key().to_bytes()),
    };
    let request = verifier.prepare_request(ballot)?;
    let promises = [
        stale_nodes[0].vote_control(&request, &old_keys[0])?,
        stale_nodes[1].vote_control(&request, &old_keys[1])?,
    ];
    let value = AuthoritySelection::new(CommandId::new(), &fixture.pending)?.control_value()?;
    let prepared = verifier.verify_prepare(ballot, &promises)?;
    let proposal =
        stale_nodes[0].propose_control(&prepared.proposal_request(&value)?, &old_keys[0])?;
    let before = fixture.successors[0].events_after(None)?;
    let issuer = AuthorityController::new(fixture.successors[0].clone(), anchor()?);
    if issuer
        .accept(fixture.rotation_head, &proposal, &keys([4, 5, 6])[0])
        .is_ok()
        || fixture.successors[0].events_after(None)? != before
    {
        return Err("authority issuer appended stale-epoch acceptance".into());
    }
    let accept = verifier.accept_request(&proposal)?;
    let accepts = [
        stale_nodes[0].vote_control(&accept, &old_keys[0])?,
        stale_nodes[1].vote_control(&accept, &old_keys[1])?,
    ];
    let stale_head = prepared.verify_chosen(&value, &accepts)?.head()?;
    synchronize(&stale_nodes)?;
    ingest(&fixture.successors[0], &stale_nodes[0].events_after(None)?)?;
    if AuthorityHistory::replay(&fixture.successors[0], anchor()?)?
        .context_at(stale_head)
        .is_ok()
    {
        return Err("low-level stale epoch majority activated authority context".into());
    }
    Ok(())
}

#[test]
fn authority_issuer_rejects_malformed_payload_without_persisting_it() -> TestResult {
    let fixture = handoff()?;
    let keys = keys([4, 5, 6]);
    let head = fixture.rotation_head;
    let a = AuthorityController::new(fixture.successors[0].clone(), anchor()?);
    let b = AuthorityController::new(fixture.successors[1].clone(), anchor()?);
    let ballot = ControlBallot {
        counter: 1,
        proposer: ControllerId(keys[0].verifying_key().to_bytes()),
    };
    let promises = [
        a.prepare(head, ballot, &keys[0])?,
        b.prepare(head, ballot, &keys[1])?,
    ];
    let value = ControlValue(b"not an authority transition".to_vec());
    let before = fixture.successors[0].events_after(None)?;
    if a.propose(head, ballot, &promises, &value, &keys[0]).is_ok()
        || fixture.successors[0].events_after(None)? != before
    {
        return Err("authority proposer persisted malformed transition bytes".into());
    }
    let context = AuthorityHistory::replay(&fixture.successors[0], anchor()?)?.context_at(head)?;
    let verifier = context.verifier()?;
    let prepared = verifier.verify_prepare(ballot, &promises)?;
    let raw =
        fixture.successors[0].propose_control(&prepared.proposal_request(&value)?, &keys[0])?;
    let before = fixture.successors[1].events_after(None)?;
    if b.accept(head, &raw, &keys[1]).is_ok() || fixture.successors[1].events_after(None)? != before
    {
        return Err("authority acceptor persisted malformed transition bytes".into());
    }
    Ok(())
}

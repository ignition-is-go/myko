use std::{error::Error, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use ed25519_dalek::SigningKey;
use myko::{ApplicationHost, MykoApplication};
use myko_authority::{
    AuthorityPolicy, RevocationKind, authority_realm_scope,
    certified::{
        AuthorityAnchor, AuthorityController, AuthorityDecisionRoot, AuthorityHistory,
        AuthoritySelection,
    },
};
use myko_federation::{
    AccessAttempt, AccessOperation, AccessTarget, AuthorityConstraints, AuthorityGrant,
    AuthorityGrantId, AuthorityPresentation, AuthorityRealmId, AuthorizationDecision,
    AuthorizationPhase, CommandId, EventEnvelope, FederationPermission, Node, NodeEvent, Principal,
    PrincipalId, PrincipalKind, ScopeId, ScopeSelection, ScopeTopology, ServiceId,
    control_quorum::{ControlBallot, ControlEpochId, ControlHead, ControlValue, ControllerId},
};
use myko_redb::RedbJournal;

type TestResult = Result<(), Box<dyn Error>>;

fn keys() -> [SigningKey; 3] {
    [1, 2, 3].map(|seed| SigningKey::from_bytes(&[seed; 32]))
}

fn controllers(keys: &[SigningKey; 3]) -> Vec<ControllerId> {
    keys.iter()
        .map(|key| ControllerId(key.verifying_key().to_bytes()))
        .collect()
}

fn realm() -> AuthorityRealmId {
    AuthorityRealmId::new("certified-consumption")
}

fn anchor() -> Result<AuthorityAnchor, String> {
    AuthorityAnchor::new(
        realm(),
        ControlEpochId([8; 32]),
        ControlHead([9; 32]),
        controllers(&keys()),
    )
}

fn ingest(node: &Node, events: &[EventEnvelope]) -> TestResult {
    for event in events {
        node.ingest(event.clone())?;
    }
    Ok(())
}

fn synchronize(a: &Node, b: &Node) -> TestResult {
    let a_events = a.events_after(None)?;
    let b_events = b.events_after(None)?;
    ingest(a, &b_events)?;
    ingest(b, &a_events)
}

fn choose(
    a_node: &Node,
    b_node: &Node,
    predecessor: ControlHead,
    value: &ControlValue,
) -> Result<ControlHead, Box<dyn Error>> {
    synchronize(a_node, b_node)?;
    let [a_key, b_key, _c_key] = keys();
    let context = AuthorityHistory::replay(a_node, anchor()?)?.context_at(predecessor)?;
    let verifier = context.verifier()?;
    let ballot = ControlBallot {
        counter: 1,
        proposer: ControllerId(a_key.verifying_key().to_bytes()),
    };
    let a = AuthorityController::new(a_node.clone(), anchor()?);
    let b = AuthorityController::new(b_node.clone(), anchor()?);
    let promises = [
        a.prepare(predecessor, ballot, &a_key)?,
        b.prepare(predecessor, ballot, &b_key)?,
    ];
    let proposal = a.propose(predecessor, ballot, &promises, value, &a_key)?;
    let accepts = [
        a.accept(predecessor, &proposal, &a_key)?,
        b.accept(predecessor, &proposal, &b_key)?,
    ];
    let chosen = verifier
        .verify_prepare(ballot, &promises)?
        .verify_chosen(value, &accepts)?
        .head()?;
    synchronize(a_node, b_node)?;
    Ok(chosen)
}

fn authority_events(node: &Node) -> Result<Vec<EventEnvelope>, Box<dyn Error>> {
    Ok(node
        .events_after(None)?
        .into_iter()
        .filter(|event| match &event.event {
            NodeEvent::CommandCommitted { command, .. } | NodeEvent::CommandLifecycle(command) => {
                command.request.scope_id == authority_realm_scope(&realm())
            }
            NodeEvent::FrameworkControl(_) => false,
        })
        .collect())
}

fn access_request(reader: Principal, scope: ScopeId, effect: &str) -> AccessAttempt {
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
    request.authorization_phase = AuthorizationPhase::Effect;
    request.effect_digest = Some(effect.to_owned());
    request
}

struct Fixture {
    directory: tempfile::TempDir,
    a_path: std::path::PathBuf,
    b_path: std::path::PathBuf,
    grant_head: ControlHead,
    reader: Principal,
    scope: ScopeId,
}

fn fixture() -> Result<Fixture, Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let a_path = directory.path().join("a.redb");
    let b_path = directory.path().join("b.redb");
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let app = AuthorityPolicy::install(MykoApplication::new())?;
    let policy = Arc::new(AuthorityPolicy::new(
        ApplicationHost::new(a.clone(), app)?,
        realm(),
    ));
    a.set_command_access_policy(policy.clone())?;
    let admin = Principal::new(PrincipalId::new("admin"), PrincipalKind::Node);
    let reader = Principal::new(PrincipalId::new("reader"), PrincipalKind::Node);
    let scope = ScopeId::new("consumption:data");
    policy.bootstrap(admin.clone())?;
    policy.issue_grant(
        admin.clone(),
        AuthorityPresentation::direct(admin.clone()),
        AuthorityGrant {
            id: AuthorityGrantId::new("single-use-grant"),
            realm_id: realm(),
            grantor: admin,
            grantee: reader.clone(),
            selection: ScopeSelection::Exact(scope.clone()),
            permissions: vec![FederationPermission::ReadState],
            operations: vec![AccessOperation::ReadItems],
            capabilities: Vec::new(),
            constraints: AuthorityConstraints::default(),
            obligations: Vec::new(),
            valid_from: fixed_time()?
                .checked_sub_signed(Duration::seconds(10))
                .ok_or("time underflow")?,
            expires_at: None,
            max_uses: Some(1),
        },
    )?;
    let selected = authority_events(&a)?;
    let grant_head = choose(
        &a,
        &b,
        anchor()?.genesis(),
        &AuthoritySelection::new(CommandId::new(), &selected)?.control_value()?,
    )?;
    drop(policy);
    drop(a);
    drop(b);
    Ok(Fixture {
        directory,
        a_path,
        b_path,
        grant_head,
        reader,
        scope,
    })
}

#[test]
fn chosen_decision_records_consume_single_use_authority_and_survive_reopen() -> TestResult {
    let fixture = fixture()?;
    let a = RedbJournal::open_node(&fixture.a_path)?;
    let b = RedbJournal::open_node(&fixture.b_path)?;
    let request_id = CommandId::new();
    let operation = CommandId::new();
    let now = fixed_time()?;
    let request = access_request(
        fixture.reader.clone(),
        fixture.scope.clone(),
        "sha256:first",
    );
    let history = AuthorityHistory::replay(&a, anchor()?)?;
    let decision = history.plan_decision_at(
        fixture.grant_head,
        operation,
        request_id,
        request.clone(),
        now,
        ScopeTopology::default(),
    )?;
    if !decision.decision().is_permit() {
        return Err("first certified decision did not permit the single use".into());
    }
    let value = decision.control_value()?;
    let decision_head = choose(&a, &b, fixture.grant_head, &value)?;
    let root = AuthorityDecisionRoot::new(realm(), request_id, AuthorizationPhase::Effect)?;
    drop(a);
    drop(b);

    let reopened = RedbJournal::open_node(&fixture.a_path)?;
    let reopened_b = RedbJournal::open_node(&fixture.b_path)?;
    let history = AuthorityHistory::replay(&reopened, anchor()?)?;
    let Some(recovered) = history.decision_at(decision_head, &root)? else {
        return Err("chosen certified decision was not recoverable after reopen".into());
    };
    if recovered.decision() != decision.decision() {
        return Err("recovered decision changed after durable replay".into());
    }
    if history
        .assess_at(
            decision_head,
            &request,
            now.checked_add_signed(Duration::seconds(1))
                .ok_or("time overflow")?,
            ScopeTopology::default(),
        )?
        .decision_at_head()
        .is_permit()
    {
        return Err("certified use record did not consume max_uses=1 authority".into());
    }
    let competing_request_id = CommandId::new();
    let competing = history.plan_decision_at(
        decision_head,
        CommandId::new(),
        competing_request_id,
        access_request(
            fixture.reader.clone(),
            fixture.scope.clone(),
            "sha256:competing",
        ),
        now.checked_add_signed(Duration::seconds(2))
            .ok_or("time overflow")?,
        ScopeTopology::default(),
    )?;
    if competing.decision().is_permit() {
        return Err("competing certified intent spent a consumed max_uses=1 grant".into());
    }
    let competing_head = choose(
        &reopened,
        &reopened_b,
        decision_head,
        &competing.control_value()?,
    )?;
    let competing_root =
        AuthorityDecisionRoot::new(realm(), competing_request_id, AuthorizationPhase::Effect)?;
    let Some(recovered_competing) = AuthorityHistory::replay(&reopened, anchor()?)?
        .decision_at(competing_head, &competing_root)?
    else {
        return Err("chosen competing certified denial was not recoverable".into());
    };
    if recovered_competing.decision().is_permit() {
        return Err("chosen competing intent recovered as a permit".into());
    }
    drop(reopened_b);
    drop(reopened);
    drop(fixture.directory);
    Ok(())
}

#[test]
fn controller_rejects_changed_binding_under_consumed_root_before_persisting() -> TestResult {
    let fixture = fixture()?;
    let a = RedbJournal::open_node(&fixture.a_path)?;
    let b = RedbJournal::open_node(&fixture.b_path)?;
    let request_id = CommandId::new();
    let now = fixed_time()?;
    let request = access_request(
        fixture.reader.clone(),
        fixture.scope.clone(),
        "sha256:first",
    );
    let original = AuthorityHistory::replay(&a, anchor()?)?.plan_decision_at(
        fixture.grant_head,
        CommandId::new(),
        request_id,
        request,
        now,
        ScopeTopology::default(),
    )?;
    let original_head = choose(&a, &b, fixture.grant_head, &original.control_value()?)?;
    let changed = access_request(
        fixture.reader.clone(),
        fixture.scope.clone(),
        "sha256:changed",
    );
    let changed_value = AuthorityHistory::replay(&a, anchor()?)?
        .plan_decision_at(
            fixture.grant_head,
            CommandId::new(),
            request_id,
            changed,
            now,
            ScopeTopology::default(),
        )?
        .control_value()?;
    let [a_key, b_key, _c_key] = keys();
    let ballot = ControlBallot {
        counter: 1,
        proposer: ControllerId(a_key.verifying_key().to_bytes()),
    };
    let a_controller = AuthorityController::new(a.clone(), anchor()?);
    let b_controller = AuthorityController::new(b.clone(), anchor()?);
    let promises = [
        a_controller.prepare(original_head, ballot, &a_key)?,
        b_controller.prepare(original_head, ballot, &b_key)?,
    ];
    let before = a.events_after(None)?;
    if a_controller
        .propose(original_head, ballot, &promises, &changed_value, &a_key)
        .is_ok()
        || a.events_after(None)? != before
    {
        return Err("controller persisted a changed binding under an already consumed root".into());
    }
    drop(a);
    drop(b);
    drop(fixture.directory);
    Ok(())
}

#[test]
fn decision_planned_after_certified_revocation_is_denied() -> TestResult {
    let fixture = fixture()?;
    let a = RedbJournal::open_node(&fixture.a_path)?;
    let b = RedbJournal::open_node(&fixture.b_path)?;
    let app = AuthorityPolicy::install(MykoApplication::new())?;
    let policy = Arc::new(AuthorityPolicy::new(
        ApplicationHost::new(a.clone(), app)?,
        realm(),
    ));
    a.set_command_access_policy(policy.clone())?;
    let before = a.local_history_cut()?;
    let admin = Principal::new(PrincipalId::new("admin"), PrincipalKind::Node);
    policy.revoke(
        admin.clone(),
        AuthorityPresentation::direct(admin),
        RevocationKind::Grant,
        "single-use-grant".to_owned(),
    )?;
    let revoked = a.events_after(before)?;
    let revoked_head = choose(
        &a,
        &b,
        fixture.grant_head,
        &AuthoritySelection::new(CommandId::new(), &revoked)?.control_value()?,
    )?;
    let planned = AuthorityHistory::replay(&a, anchor()?)?.plan_decision_at(
        revoked_head,
        CommandId::new(),
        CommandId::new(),
        access_request(
            fixture.reader.clone(),
            fixture.scope.clone(),
            "sha256:after-revoke",
        ),
        fixed_time()?,
        ScopeTopology::default(),
    )?;
    if !matches!(planned.decision(), AuthorizationDecision::Deny(_)) {
        return Err("certified revocation before request did not deny the decision".into());
    }
    drop(policy);
    drop(a);
    drop(b);
    drop(fixture.directory);
    Ok(())
}

fn fixed_time() -> Result<DateTime<Utc>, Box<dyn Error>> {
    DateTime::<Utc>::from_timestamp(1_700_000_000, 0).ok_or_else(|| "invalid time".into())
}

use std::{collections::BTreeMap, sync::Arc};

use chrono::{Duration, Utc};
use myko::{
    ApplicationHost, MykoApplication,
    core::request::RequestContext,
    view::{RegisteredViewOutput, ViewIdStatic},
};
use myko_authority::{AuthorityGrantsView, AuthorityPolicy, GrantRecord, authority_realm_scope};
use myko_federation::{
    AccessOperation, AuthorityConstraints, AuthorityGrant, AuthorityGrantId, AuthorityPresentation,
    AuthorityRealmId, FederationPermission, Node, Principal, PrincipalId, PrincipalKind, ScopeId,
    ScopeSelection, SubscriptionLiveness,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn principal(id: &str) -> Principal {
    Principal::new(PrincipalId::new(id), PrincipalKind::Node)
}

fn open_authority() -> Result<(ApplicationHost, Arc<AuthorityPolicy>, Principal), String> {
    let node = Node::in_memory();
    let application =
        AuthorityPolicy::install(MykoApplication::new()).map_err(|error| error.to_string())?;
    let application = ApplicationHost::new(node.clone(), application)?;
    let policy = Arc::new(AuthorityPolicy::new(
        application.clone(),
        AuthorityRealmId::new("main"),
    ));
    let administrator = principal("node:administrator");
    let installed: Arc<dyn myko_federation::AccessPolicy> = policy.clone();
    node.set_command_access_policy(installed)
        .map_err(|error| error.to_string())?;
    policy
        .bootstrap(administrator.clone())
        .map_err(|error| error.to_string())?;
    Ok((application, policy, administrator))
}

fn grant_record(
    state: &myko_federation::LiveSubscriptionState<
        BTreeMap<Arc<str>, Arc<dyn myko::item::AnyItem>>,
    >,
) -> Vec<GrantRecord> {
    state
        .value
        .as_ref()
        .into_iter()
        .flat_map(|rows| rows.values())
        .filter_map(|item| item.as_any().downcast_ref::<GrantRecord>().cloned())
        .collect()
}

#[test]
fn authority_grants_view_publishes_retained_liveness_for_native_clients() -> TestResult {
    let (application, policy, administrator) = open_authority()?;
    let request = Arc::new(RequestContext::internal(
        Arc::from("authority-grants-publication"),
        application.server().host_id,
        "test",
    ));
    let output = application.server().handler_registry.open_federated_view(
        AuthorityGrantsView::view_id_static().as_ref(),
        serde_json::to_value(AuthorityGrantsView {
            source_node: application.node_id(),
            realm_id: AuthorityRealmId::new("main"),
        })?,
        request,
        Arc::clone(application.server()),
        myko::server::federated_source::FederatedRequest {
            source_node: Some(application.node_id()),
            scope_id: Some(authority_realm_scope(&AuthorityRealmId::new("main"))),
        },
    )?;
    if !matches!(&output, RegisteredViewOutput::RetainedPublication(_)) {
        return Err("authority grants view returned a local map".into());
    }
    let RegisteredViewOutput::RetainedPublication(publication) = output else {
        return Err("authority grants view returned a local map".into());
    };
    let initial_liveness = publication.current().liveness;
    if initial_liveness != SubscriptionLiveness::Current {
        return Err(format!(
            "authority grants publication opened with liveness={initial_liveness:?}"
        )
        .into());
    }

    policy.issue_grant(
        administrator.clone(),
        AuthorityPresentation::direct(administrator),
        AuthorityGrant {
            id: AuthorityGrantId::new("grant:publication"),
            realm_id: AuthorityRealmId::new("main"),
            grantor: principal("node:administrator"),
            grantee: principal("node:reader"),
            selection: ScopeSelection::Exact(ScopeId::new("scope:publication")),
            permissions: vec![FederationPermission::ReadState],
            operations: vec![AccessOperation::ReadItems],
            capabilities: Vec::new(),
            constraints: AuthorityConstraints::default(),
            obligations: Vec::new(),
            valid_from: Utc::now()
                .checked_sub_signed(Duration::seconds(1))
                .ok_or("grant timestamp underflow")?,
            expires_at: None,
            max_uses: None,
        },
    )?;

    let deadline = std::time::Instant::now()
        .checked_add(std::time::Duration::from_secs(2))
        .ok_or("deadline overflowed")?;
    loop {
        let state = publication.current();
        let grants = grant_record(&state);
        if grants
            .iter()
            .any(|record| record.grant.id.as_str() == "grant:publication")
        {
            if state.liveness != SubscriptionLiveness::Current {
                return Err(format!(
                    "authority grants publication observed grant with liveness={:?}",
                    state.liveness
                )
                .into());
            }
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "retained authority publication did not observe grant; liveness={:?}",
                state.liveness
            )
            .into());
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

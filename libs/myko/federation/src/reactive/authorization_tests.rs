use std::sync::Arc;

use super::*;
use crate::{
    AccessOperation, AuthorityChallenge, AuthorityRealmId, AuthorizationBinding,
    AuthorizationDecision, AuthorizationExplanation, AuthorizationReport, ChallengeId,
    DenyDecision, ObligationId, Principal, PrincipalId, PrincipalKind, ResourceVisibility,
    ScopeTopology,
};

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now()
        .checked_add(std::time::Duration::from_secs(5))
        .unwrap_or_else(std::time::Instant::now);
    while !condition() && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
}

fn report() -> AuthorizationReport {
    AuthorizationReport {
        evaluated_at: chrono::Utc::now(),
        principal: Principal::new(PrincipalId::new("reader"), PrincipalKind::Person),
        executor: Principal::new(PrincipalId::new("node"), PrincipalKind::Node),
        operation: AccessOperation::FollowHandler,
        explanations: vec![AuthorizationExplanation {
            code: "revoked".to_owned(),
            message: "access revoked".to_owned(),
            grant_id: None,
            delegation_id: None,
            obligation_id: None,
            constraint: None,
        }],
    }
}

fn denied() -> AuthorizationBlock {
    AuthorizationBlock::Denied(Box::new(DenyDecision {
        report: report(),
        visibility: ResourceVisibility::Unauthorized,
    }))
}

fn challenged() -> Option<AuthorizationBlock> {
    let report = report();
    AuthorizationBlock::from_decision(AuthorizationDecision::Challenge {
        challenge: AuthorityChallenge {
            id: ChallengeId::new("challenge"),
            realm_id: AuthorityRealmId::new("realm"),
            obligation_id: ObligationId::new("approval"),
            kind: "approval".to_owned(),
            prompt: "approve access".to_owned(),
            binding: AuthorizationBinding {
                principal: report.principal.clone(),
                executor: report.executor.clone(),
                provenance: Vec::new(),
                operation: report.operation,
                service_id: None,
                command_id: None,
                command_type: None,
                resources: Vec::new(),
                capabilities: Vec::new(),
                arguments_digest: None,
                effect_digest: None,
                topology_proof: ScopeTopology::default(),
            },
            issued_at: chrono::Utc::now(),
            expires_at: chrono::Utc::now(),
        },
        report,
    })
}

#[test]
fn scalar_authorization_block_clears_and_outage_cannot_restore_value() {
    let (writer, live) = live_subscription(LiveSubscriptionState {
        value: Some("secret".to_owned()),
        through: Some(7_u64),
        liveness: SubscriptionLiveness::Current,
    });

    writer.interrupt(SubscriptionInterruption::AuthorizationBlocked { block: denied() });
    wait_until(|| live.current().value.is_none());
    let blocked = live.current();
    assert_eq!(blocked.value, None);
    assert_eq!(blocked.through, None);
    assert!(matches!(
        blocked.liveness,
        SubscriptionLiveness::AuthorizationBlocked { .. }
    ));

    writer.interrupt(SubscriptionInterruption::Resynchronizing {
        reason: "network outage".to_owned(),
    });
    wait_until(|| {
        matches!(
            live.current().liveness,
            SubscriptionLiveness::AuthorizationBlocked { .. }
        )
    });
    assert_eq!(live.current().value, None);
    assert_eq!(live.current().through, None);

    writer.publish("allowed".to_owned(), Some(8));
    wait_until(|| live.current().value.as_deref() == Some("allowed"));
    assert_eq!(live.current().value.as_deref(), Some("allowed"));
}

#[test]
fn maps_and_joins_clear_cached_protected_values() {
    let (left_writer, left) = live_subscription(LiveSubscriptionState {
        value: Some("secret".to_owned()),
        through: Some(1_u64),
        liveness: SubscriptionLiveness::Current,
    });
    let (_right_writer, right) = live_subscription(LiveSubscriptionState {
        value: Some("public".to_owned()),
        through: Some(1_u64),
        liveness: SubscriptionLiveness::Current,
    });
    let mapped = left.map_value(String::len);
    let fallible = left.try_map_value(|value| Ok::<_, &'static str>(value.len()));
    let coherent = left.join_coherent(&right);
    let frontier = left.join_frontiers(&right);

    left_writer.interrupt(SubscriptionInterruption::AuthorizationBlocked { block: denied() });

    wait_until(|| {
        mapped.current().value.is_none()
            && fallible.current().value.is_none()
            && coherent.current().value.is_none()
            && frontier.current().value.is_none()
    });

    assert_eq!(mapped.current().value, None);
    assert_eq!(fallible.current().value, None);
    assert_eq!(coherent.current().value, None);
    assert_eq!(frontier.current().value, None);
}

#[test]
fn collection_block_publishes_one_empty_reset_and_clears_scalar_projection() {
    let (writer, collection) = live_collection(
        vec![(Arc::<str>::from("secret"), Arc::new("payload".to_owned()))],
        LiveCollectionState {
            through: Some(3_u64),
            liveness: SubscriptionLiveness::Current,
        },
    );
    let revisions = collection.subscribe_revisions();
    let scalar = collection.as_subscription();
    revisions.discard_pending_revisions();

    writer.interrupt(SubscriptionInterruption::AuthorizationBlocked { block: denied() });

    wait_until(|| collection.rows().snapshot().is_empty() && scalar.current().value.is_none());

    assert!(collection.rows().snapshot().is_empty());
    assert_eq!(scalar.current().value, None);
    let revision = revisions.receiver().try_recv();
    assert!(revision.is_ok(), "missing blocked revision");
    let Ok(revision) = revision else {
        return;
    };
    assert!(matches!(
        revision.diff,
        Some(MapDiff::Initial { entries }) if entries.is_empty()
    ));
    assert_eq!(revision.state.through, None);
    assert!(matches!(
        revision.state.liveness,
        SubscriptionLiveness::AuthorizationBlocked { .. }
    ));
}

#[test]
fn collection_union_exposes_no_rows_from_a_blocked_source() {
    let (left_writer, left) = live_collection(
        vec![(Arc::<str>::from("secret"), Arc::new("payload".to_owned()))],
        LiveCollectionState {
            through: Some(1_u64),
            liveness: SubscriptionLiveness::Current,
        },
    );
    let (_right_writer, right) = live_collection(
        vec![(Arc::<str>::from("other"), Arc::new("other".to_owned()))],
        LiveCollectionState {
            through: Some(1_u64),
            liveness: SubscriptionLiveness::Current,
        },
    );
    let union = left.plan().union(right.plan()).materialize();

    left_writer.interrupt(SubscriptionInterruption::AuthorizationBlocked { block: denied() });

    wait_until(|| {
        union.rows().snapshot().is_empty()
            && matches!(
                union.current_state().liveness,
                SubscriptionLiveness::AuthorizationBlocked { .. }
            )
    });

    assert!(union.rows().snapshot().is_empty());
    assert!(matches!(
        union.current_state().liveness,
        SubscriptionLiveness::AuthorizationBlocked { .. }
    ));
}

#[test]
fn blocked_initial_states_discard_supplied_protected_data() {
    let block = denied();
    let (_writer, scalar) = live_subscription(LiveSubscriptionState {
        value: Some("secret".to_owned()),
        through: Some(1_u64),
        liveness: SubscriptionLiveness::AuthorizationBlocked {
            block: block.clone(),
        },
    });
    let (_writer, collection) = live_collection(
        vec![(Arc::<str>::from("secret"), Arc::new("payload".to_owned()))],
        LiveCollectionState {
            through: Some(1_u64),
            liveness: SubscriptionLiveness::AuthorizationBlocked { block },
        },
    );

    assert_eq!(scalar.current().value, None);
    assert_eq!(scalar.current().through, None);
    assert!(collection.rows().snapshot().is_empty());
    assert_eq!(collection.current_state().through, None);
}

#[test]
fn joins_recover_only_from_post_block_current_inputs() {
    let (left_writer, left) = live_subscription(LiveSubscriptionState {
        value: Some("secret-old".to_owned()),
        through: Some(1_u64),
        liveness: SubscriptionLiveness::Current,
    });
    let (right_writer, right) = live_subscription(LiveSubscriptionState {
        value: Some("right-old".to_owned()),
        through: Some(1_u64),
        liveness: SubscriptionLiveness::Current,
    });
    let coherent = left.join_coherent(&right);
    let frontier = left.join_frontiers(&right);

    left_writer.interrupt(SubscriptionInterruption::AuthorizationBlocked { block: denied() });
    left_writer.interrupt(SubscriptionInterruption::Resynchronizing {
        reason: "outage after denial".to_owned(),
    });
    right_writer.publish("right-new".to_owned(), Some(2));
    wait_until(|| coherent.current().value.is_none() && frontier.current().value.is_none());

    assert!(matches!(
        coherent.current().liveness,
        SubscriptionLiveness::AuthorizationBlocked { .. }
    ));
    assert!(matches!(
        frontier.current().liveness,
        SubscriptionLiveness::AuthorizationBlocked { .. }
    ));

    left_writer.publish("allowed-new".to_owned(), Some(2));
    wait_until(|| {
        coherent.current().value == Some(("allowed-new".to_owned(), "right-new".to_owned()))
            && frontier.current().value == Some(("allowed-new".to_owned(), "right-new".to_owned()))
    });

    assert_eq!(
        coherent.current().value,
        Some(("allowed-new".to_owned(), "right-new".to_owned()))
    );
    assert_eq!(
        frontier.current().value,
        Some(("allowed-new".to_owned(), "right-new".to_owned()))
    );
}

#[test]
fn union_recovers_only_from_post_block_current_rows() {
    let (left_writer, left) = live_collection(
        vec![(
            Arc::<str>::from("secret-old"),
            Arc::new("secret-old".to_owned()),
        )],
        LiveCollectionState {
            through: Some(1_u64),
            liveness: SubscriptionLiveness::Current,
        },
    );
    let (right_writer, right) = live_collection(
        vec![(
            Arc::<str>::from("right-old"),
            Arc::new("right-old".to_owned()),
        )],
        LiveCollectionState {
            through: Some(1_u64),
            liveness: SubscriptionLiveness::Current,
        },
    );
    let union = left.plan().union(right.plan()).materialize();

    left_writer.interrupt(SubscriptionInterruption::AuthorizationBlocked { block: denied() });
    wait_until(|| {
        union.rows().snapshot().is_empty()
            && matches!(
                union.current_state().liveness,
                SubscriptionLiveness::AuthorizationBlocked { .. }
            )
    });
    assert!(union.rows().snapshot().is_empty());
    left_writer.interrupt(SubscriptionInterruption::Resynchronizing {
        reason: "outage after denial".to_owned(),
    });
    right_writer.replace_all(
        vec![(
            Arc::<str>::from("right-new"),
            Arc::new("right-new".to_owned()),
        )],
        Some(2),
    );
    wait_until(|| union.rows().snapshot().is_empty());
    assert!(union.rows().snapshot().is_empty());

    left_writer.replace_all(
        vec![(
            Arc::<str>::from("secret-old"),
            Arc::new("secret-old".to_owned()),
        )],
        Some(2),
    );
    wait_until(|| {
        let rows = union.rows().snapshot();
        rows.iter().any(|(key, _)| key.as_ref() == "secret-old")
            && rows.iter().any(|(key, _)| key.as_ref() == "right-new")
    });
    let rows = union.rows().snapshot();
    assert!(rows.iter().any(|(key, _)| key.as_ref() == "secret-old"));
    assert!(rows.iter().any(|(key, _)| key.as_ref() == "right-new"));
    assert!(!rows.iter().any(|(key, _)| key.as_ref() == "right-old"));
}

#[test]
fn blocked_collection_plan_materializes_without_rows() {
    let rows = CellMap::new();
    rows.replace_all(vec![(
        Arc::<str>::from("secret"),
        Arc::new("payload".to_owned()),
    )]);
    let state = Cell::new(LiveCollectionState {
        through: Some(1_u64),
        liveness: SubscriptionLiveness::AuthorizationBlocked { block: denied() },
    });

    let materialized = MapCollectionPlan::new(rows.lock(), state.lock()).materialize();

    wait_until(|| {
        matches!(
            materialized.current_state().liveness,
            SubscriptionLiveness::AuthorizationBlocked { .. }
        )
    });
    assert!(materialized.rows().snapshot().is_empty());
    assert_eq!(materialized.current_state().through, None);
    assert!(matches!(
        materialized.current_state().liveness,
        SubscriptionLiveness::AuthorizationBlocked { .. }
    ));
}

#[test]
fn initially_blocked_collection_plan_recovers_from_current_revision() {
    let rows = CellMap::new();
    rows.replace_all(vec![(
        Arc::<str>::from("secret"),
        Arc::new("payload".to_owned()),
    )]);
    let state = Cell::new(LiveCollectionState {
        through: None::<u64>,
        liveness: SubscriptionLiveness::AuthorizationBlocked { block: denied() },
    });
    let materialized =
        MapCollectionPlan::new(rows.clone().lock(), state.clone().lock()).materialize();

    hyphae::batch(|| {
        rows.replace_all(vec![(
            Arc::<str>::from("allowed"),
            Arc::new("allowed".to_owned()),
        )]);
        state.set(LiveCollectionState {
            through: Some(2),
            liveness: SubscriptionLiveness::Current,
        });
    });
    wait_until(|| {
        materialized.rows().snapshot().len() == 1 && materialized.current_state().through == Some(2)
    });

    assert_eq!(
        materialized
            .rows()
            .snapshot()
            .first()
            .map(|(key, _)| key.as_ref()),
        Some("allowed")
    );
    assert_eq!(materialized.current_state().through, Some(2));
    assert_eq!(
        materialized.current_state().liveness,
        SubscriptionLiveness::Current
    );
}

#[test]
fn materialized_plan_clears_on_state_only_block_and_recovers_identical_rows() {
    let rows = CellMap::new();
    rows.replace_all(vec![(
        Arc::<str>::from("same"),
        Arc::new("same".to_owned()),
    )]);
    let state = Cell::new(LiveCollectionState {
        through: Some(1_u64),
        liveness: SubscriptionLiveness::Current,
    });
    let materialized = MapCollectionPlan::new(rows.lock(), state.clone().lock()).materialize();

    state.set(LiveCollectionState {
        through: None,
        liveness: SubscriptionLiveness::AuthorizationBlocked { block: denied() },
    });
    wait_until(|| materialized.rows().snapshot().is_empty());
    assert!(materialized.rows().snapshot().is_empty());

    state.set(LiveCollectionState {
        through: None,
        liveness: SubscriptionLiveness::Invalid {
            reason: "protocol failed".to_owned(),
        },
    });
    wait_until(|| {
        matches!(
            materialized.current_state().liveness,
            SubscriptionLiveness::Invalid { .. }
        )
    });
    assert!(materialized.rows().snapshot().is_empty());

    state.set(LiveCollectionState {
        through: Some(2),
        liveness: SubscriptionLiveness::Current,
    });
    wait_until(|| {
        materialized.rows().snapshot().len() == 1 && materialized.current_state().through == Some(2)
    });
    assert_eq!(
        materialized
            .rows()
            .snapshot()
            .first()
            .map(|(key, _)| key.as_ref()),
        Some("same")
    );
}

#[test]
fn challenge_remains_distinct_from_denial_and_clears_value() {
    let (writer, live) = live_subscription(LiveSubscriptionState {
        value: Some("secret".to_owned()),
        through: Some(1_u64),
        liveness: SubscriptionLiveness::Current,
    });

    let block = challenged();
    assert!(block.is_some(), "challenge decision unexpectedly permitted");
    let Some(block) = block else {
        return;
    };
    writer.interrupt(SubscriptionInterruption::AuthorizationBlocked { block });
    wait_until(|| live.current().value.is_none());

    assert!(matches!(
        live.current().liveness,
        SubscriptionLiveness::AuthorizationBlocked {
            block: AuthorizationBlock::Challenge { .. }
        }
    ));
}

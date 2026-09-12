#![allow(clippy::expect_used)]

use myko_federation::{
    AccessAttempt, AccessOperation, AuthorityPresentation, AuthorizationDecision, PrincipalId,
};

use super::*;

fn blocked() -> SubscriptionLiveness {
    let principal = PrincipalId::new("reader");
    let request = AccessAttempt::scoped(
        principal.clone(),
        AuthorityPresentation::direct_node(principal),
        AccessOperation::FollowHandler,
        ScopeId::new("protected"),
    );
    let decision = AuthorizationDecision::from_rule(&request, Err("revoked".to_owned()));
    SubscriptionLiveness::AuthorizationBlocked {
        block: AuthorizationBlock::from_decision(decision).expect("deny decision"),
    }
}

#[test]
fn handler_authorization_blocked_snapshot_discards_payload_and_cursor() {
    let liveness = blocked();
    let (state, keys) = decode_handler_state::<Vec<u64>, u64>(ErasedHandlerState {
        value: Some(serde_json::json!([42])),
        through: Some(serde_json::json!(7)),
        liveness: liveness.clone(),
        row_keys: Some(vec!["secret".to_owned()]),
    })
    .expect("decode blocked snapshot");
    assert_eq!(state.value, None);
    assert_eq!(state.through, None);
    assert_eq!(state.liveness, liveness);
    assert_eq!(keys, Some(Vec::new()));
}

#[test]
fn handler_authorization_blocked_delta_clears_prior_and_incoming_rows() {
    let liveness = blocked();
    let mut state = LiveSubscriptionState {
        value: Some(vec![42_u64]),
        through: Some(7_u64),
        liveness: SubscriptionLiveness::Current,
    };
    let mut keys = Some(vec!["secret".to_owned()]);
    apply_view_delta(
        &mut state,
        &mut keys,
        ErasedViewDelta {
            upserts: vec![myko_wire::ErasedKeyedValue {
                key: "secret".to_owned(),
                value: serde_json::json!(99),
            }],
            deletes: Vec::new(),
            order: None,
            through: Some(serde_json::json!(8)),
            liveness: liveness.clone(),
        },
    )
    .expect("decode blocked delta");
    assert_eq!(state.value, None);
    assert_eq!(state.through, None);
    assert_eq!(state.liveness, liveness);
    assert_eq!(keys, Some(Vec::new()));
}

#[test]
fn handler_authorization_catchup_cannot_repopulate_a_blocked_value() {
    let (writer, live) = live_subscription(LiveSubscriptionState {
        value: Some(42_u64),
        through: Some(7_u64),
        liveness: SubscriptionLiveness::Current,
    });
    let denial = blocked();
    publish_handler_state(
        &writer,
        LiveSubscriptionState {
            value: None,
            through: None,
            liveness: denial.clone(),
        },
    );
    for liveness in [
        SubscriptionLiveness::Connecting,
        SubscriptionLiveness::Resynchronizing {
            reason: "catching up after regrant".to_owned(),
        },
    ] {
        publish_handler_state(
            &writer,
            LiveSubscriptionState {
                value: Some(99),
                through: Some(8),
                liveness,
            },
        );
        let state = live.current();
        assert_eq!(state.value, None);
        assert_eq!(state.through, None);
        assert_eq!(state.liveness, denial);
    }
    publish_handler_state(
        &writer,
        LiveSubscriptionState {
            value: Some(100),
            through: Some(9),
            liveness: SubscriptionLiveness::Current,
        },
    );
    assert_eq!(live.current().value, Some(100));
    assert_eq!(live.current().through, Some(9));
    assert_eq!(live.current().liveness, SubscriptionLiveness::Current);
}

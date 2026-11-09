#![allow(clippy::panic_in_result_fn)]
use super::*;
use futures_util::future::BoxFuture;
use myko::wire::{QueryResponse, ReportResponse, WrappedQuery, WrappedReport, WrappedView};
use myko_auth::{
    AuthenticatedIdentity, Authenticator, Authorizer, Credentials, MykoAuthProvider, Principal,
    PrincipalKind,
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime},
};

struct Identity;
impl Authenticator for Identity {
    fn authenticate(
        &self,
        _: Credentials,
    ) -> BoxFuture<'_, Result<AuthenticatedIdentity, AuthError>> {
        Box::pin(async {
            Ok(AuthenticatedIdentity {
                principal: Principal {
                    issuer: "fixture".into(),
                    subject: "user".into(),
                    kind: PrincipalKind::User,
                },
                client_id: None,
                scopes: vec![],
                delegation: vec![],
                claims: Value::Null,
                expires_at: SystemTime::now()
                    .checked_add(Duration::from_mins(1))
                    .ok_or(AuthError::Unavailable)?,
            })
        })
    }
}
struct Policy(AtomicU64);
impl Authorizer for Policy {
    fn authorize(
        &self,
        _: &AuthenticatedIdentity,
        _: AuthorizationRequest<'_>,
    ) -> Result<(), AuthError> {
        if self.revision().is_multiple_of(2) {
            Ok(())
        } else {
            Err(AuthError::Forbidden)
        }
    }
    fn revision(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}
fn request(kind: OperationKind, tx: &str) -> MykoMessage {
    let args = serde_json::json!({"tx":tx});
    match kind {
        OperationKind::Query => MykoMessage::Query(WrappedQuery {
            query: args,
            query_id: "Query".into(),
            query_item_type: "Item".into(),
            window: None,
        }),
        OperationKind::View => MykoMessage::View(WrappedView {
            view: args,
            view_id: "View".into(),
            view_item_type: "Item".into(),
            window: None,
            sample_rate: None,
        }),
        _ => MykoMessage::Report(WrappedReport {
            report: args,
            report_id: "Report".into(),
        }),
    }
}
fn response(kind: OperationKind, tx: &str) -> MykoMessage {
    let response = QueryResponse {
        changes: vec![],
        deletes: vec![],
        upserts: vec![],
        sequence: 1,
        tx: tx.into(),
        total_count: None,
        window: None,
    };
    match kind {
        OperationKind::Query => MykoMessage::QueryResponse(response),
        OperationKind::View => MykoMessage::ViewResponse(response),
        _ => MykoMessage::ReportResponse(ReportResponse {
            tx: tx.into(),
            response: serde_json::json!({"private":"queued"}),
        }),
    }
}
#[tokio::test]
async fn queued_query_view_and_report_data_are_blocked_on_permission_loss() -> Result<(), AuthError>
{
    let policy = Arc::new(Policy(AtomicU64::new(0)));
    let session = MykoAuthProvider::new(Arc::new(Identity), policy.clone())
        .with_live_permissions()
        .authenticate(Credentials::default())
        .await?;
    for kind in [
        OperationKind::Query,
        OperationKind::View,
        OperationKind::Report,
    ] {
        policy.0.store(0, Ordering::Release);
        let (cancel, mut cancelled) = mpsc::unbounded_channel();
        let subscriptions = SubscriptionAuthorization::new(cancel);
        let request = request(kind, "queued");
        assert!(subscriptions.register(&request));
        let queued = response(kind, "queued");
        assert!(matches!(
            subscriptions.delivery(&queued, &session),
            Delivery::Allow
        ));
        policy.0.store(1, Ordering::Release);
        let Delivery::Error(error) = subscriptions.delivery(&queued, &session) else {
            return Err(AuthError::Unavailable);
        };
        assert!(matches!(
            (kind, error),
            (OperationKind::Query, MykoMessage::QueryError(_))
                | (OperationKind::View, MykoMessage::ViewError(_))
                | (OperationKind::Report, MykoMessage::ReportError(_))
        ));
        assert_eq!(
            cancelled
                .try_recv()
                .map_err(|_| AuthError::Unavailable)?
                .as_ref(),
            "queued"
        );
        assert!(matches!(
            subscriptions.delivery(&queued, &session),
            Delivery::Drop
        ));
        policy.0.store(2, Ordering::Release);
        assert!(matches!(
            subscriptions.delivery(&queued, &session),
            Delivery::Drop
        ));
        assert!(!subscriptions.register(&request));
        assert!(subscriptions.register(&self::request(kind, "new-request")));
        assert!(matches!(
            subscriptions.delivery(&response(kind, "new-request"), &session),
            Delivery::Allow
        ));
        subscriptions.cancel("new-request");
        assert!(!subscriptions.active("new-request"));
        assert!(matches!(
            subscriptions.delivery(&response(kind, "new-request"), &session),
            Delivery::Drop
        ));
    }
    session.check()?;
    Ok(())
}

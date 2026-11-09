//! Connection-local permissions for active and queued subscription output.
use myko::wire::{MykoMessage, QueryError, ReportError, ViewError};
use myko_auth::{AuthError, AuthSession, AuthorizationRequest, OperationKind};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;

struct Subscription {
    kind: OperationKind,
    operation: String,
    arguments: Value,
    active: bool,
    revision: Option<u64>,
}
impl Subscription {
    fn check(&mut self, auth: &AuthSession) -> Result<(), AuthError> {
        for _ in 0..3 {
            let revision = auth.policy_revision();
            if self.revision == Some(revision) {
                return Ok(());
            }
            let decision = auth.authorize(AuthorizationRequest::Execute {
                kind: self.kind,
                operation: &self.operation,
                arguments: &self.arguments,
            });
            if revision != auth.policy_revision() {
                continue;
            }
            decision?;
            self.revision = Some(revision);
            return Ok(());
        }
        Err(AuthError::Unavailable)
    }
    fn error(&self, tx: &str, error: AuthError) -> MykoMessage {
        match self.kind {
            OperationKind::Query => {
                MykoMessage::QueryError(QueryError::new(tx, &self.operation, error.to_string()))
            }
            OperationKind::View => {
                MykoMessage::ViewError(ViewError::new(tx, &self.operation, error.to_string()))
            }
            _ => MykoMessage::ReportError(ReportError::new(tx, &self.operation, error.to_string())),
        }
    }
}

pub struct SubscriptionAuthorization {
    subscriptions: Mutex<HashMap<Arc<str>, Subscription>>,
    cancel: mpsc::UnboundedSender<Arc<str>>,
}
impl SubscriptionAuthorization {
    pub fn new(cancel: mpsc::UnboundedSender<Arc<str>>) -> Self {
        Self {
            subscriptions: Mutex::new(HashMap::new()),
            cancel,
        }
    }
    /// Transactions identify one subscription for the life of the connection.
    /// Tombstones prevent queued or late builds from reviving cancelled data.
    pub fn register(&self, message: &MykoMessage) -> bool {
        let (kind, operation, arguments) = match message {
            MykoMessage::Query(w) => (OperationKind::Query, w.query_id.as_ref(), &w.query),
            MykoMessage::View(w) => (OperationKind::View, w.view_id.as_ref(), &w.view),
            MykoMessage::Report(w) => (OperationKind::Report, w.report_id.as_str(), &w.report),
            _ => return true,
        };
        let tx = arguments
            .get("tx")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let mut subscriptions = self
            .subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = subscriptions.get(tx) {
            return existing.active
                && existing.kind == kind
                && existing.operation == operation
                && existing.arguments == *arguments;
        }
        subscriptions.insert(
            tx.into(),
            Subscription {
                kind,
                operation: operation.into(),
                arguments: arguments.clone(),
                active: true,
                revision: None,
            },
        );
        true
    }
    pub fn cancel(&self, tx: &str) {
        if let Some(subscription) = self
            .subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(tx)
        {
            subscription.active = false;
            // Keep only a transaction tombstone, not potentially large arguments.
            subscription.arguments = Value::Null;
        }
    }
    pub fn active(&self, tx: &str) -> bool {
        self.subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(tx)
            .is_some_and(|subscription| subscription.active)
    }
    pub fn recheck(&self, auth: &AuthSession) -> Vec<MykoMessage> {
        let mut subscriptions = self
            .subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        subscriptions
            .iter_mut()
            .filter_map(|(tx, subscription)| {
                if !subscription.active {
                    return None;
                }
                let error = subscription.check(auth).err()?;
                subscription.active = false;
                subscription.arguments = Value::Null;
                let _ = self.cancel.send(tx.clone());
                Some(subscription.error(tx, error))
            })
            .collect()
    }
    /// Runs immediately before network delivery, including queued/sampled output.
    /// Denials replace the first response with a typed error; later output drops.
    pub fn delivery(&self, message: &MykoMessage, auth: &AuthSession) -> Delivery {
        let (tx, kind) = match message {
            MykoMessage::QueryResponse(response) => (response.tx.as_ref(), OperationKind::Query),
            MykoMessage::ViewResponse(response) => (response.tx.as_ref(), OperationKind::View),
            MykoMessage::ReportResponse(response) => (response.tx.as_str(), OperationKind::Report),
            _ => return Delivery::Allow,
        };
        let mut subscriptions = self
            .subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(subscription) = subscriptions.get_mut(tx) else {
            return Delivery::Drop;
        };
        if !subscription.active || subscription.kind != kind {
            return Delivery::Drop;
        }
        let delivery = match subscription.check(auth) {
            Ok(()) => Delivery::Allow,
            Err(error) => {
                subscription.active = false;
                subscription.arguments = Value::Null;
                let _ = self.cancel.send(tx.into());
                Delivery::Error(subscription.error(tx, error))
            }
        };
        drop(subscriptions);
        delivery
    }
}
pub enum Delivery {
    Allow,
    Drop,
    Error(MykoMessage),
}

#[cfg(test)]
mod tests;

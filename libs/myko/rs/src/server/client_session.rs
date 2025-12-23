//! Client session management for WebSocket connections
//!
//! Each WebSocket connection gets a ClientSession that manages:
//! - Active subscriptions via SubscriptionGuards
//! - Message sending to the client
//! - Automatic cleanup on disconnect

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use hypha::{Cell, CellImmutable, Signal, SubscriptionGuard, Watchable};

use crate::core::item::AnyItem;
use crate::report::AnyOutput;
use crate::wire::{MykoMessage, QueryResponse, ReportError, ReportResponse};

/// Trait for sending WebSocket messages.
///
/// Implemented by the actual WebSocket writer to allow abstraction
/// and easier testing.
pub trait WsWriter: Send + Sync + 'static {
    /// Send a message to the client.
    fn send(&self, msg: MykoMessage);
}

/// A WebSocket client session that manages subscriptions.
///
/// When dropped, all subscription guards are dropped, automatically
/// cleaning up all reactive subscriptions.
pub struct ClientSession<W: WsWriter> {
    /// Unique client identifier
    pub client_id: Arc<str>,
    /// WebSocket writer for sending messages
    writer: Arc<W>,
    /// Active subscriptions: tx -> guard
    subscriptions: HashMap<Arc<str>, SubscriptionGuard>,
}

impl<W: WsWriter> ClientSession<W> {
    /// Create a new client session.
    pub fn new(client_id: Arc<str>, writer: W) -> Self {
        Self {
            client_id,
            writer: Arc::new(writer),
            subscriptions: HashMap::new(),
        }
    }

    /// Subscribe to a CellMap from a query cell factory.
    ///
    /// This is used by WsHandler when the query registration provides a cell factory.
    pub fn subscribe_query(
        &mut self,
        tx: Arc<str>,
        cell: hypha::CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>,
    ) {
        let writer = self.writer.clone();
        let tx_clone = tx.clone();
        let sequence = Arc::new(AtomicU64::new(0));

        // subscribe_diffs sends Initial first, then subsequent diffs
        let guard = cell.subscribe_diffs(move |diff| {
            let seq = sequence.fetch_add(1, Ordering::SeqCst);
            let response = QueryResponse::from_diff(diff, tx_clone.clone(), seq);
            writer.send(MykoMessage::QueryResponse(response));
        });

        self.subscriptions.insert(tx, guard);
    }

    /// Subscribe to a report cell.
    pub fn subscribe_report(
        &mut self,
        tx: Arc<str>,
        report_id: Arc<str>,
        cell: Cell<Arc<dyn AnyOutput>, CellImmutable>,
    ) {
        let writer = self.writer.clone();
        let tx_clone = tx.clone();

        let guard = cell.subscribe(move |signal| match &signal {
            Signal::Value(output) => {
                let response = ReportResponse {
                    response: output.to_value(),
                    tx: tx_clone.to_string(),
                };
                writer.send(MykoMessage::ReportResponse(response));
            }
            Signal::Complete => {}
            Signal::Error(e) => {
                writer.send(MykoMessage::ReportError(ReportError {
                    tx: tx_clone.to_string(),
                    report_id: report_id.to_string(),
                    message: e.to_string(),
                }));
            }
        });

        self.subscriptions.insert(tx, guard);
    }

    /// Cancel a subscription by transaction ID.
    pub fn cancel(&mut self, tx: &Arc<str>) {
        self.subscriptions.remove(tx);
    }

    /// Cancel all subscriptions.
    pub fn cancel_all(&mut self) {
        self.subscriptions.clear();
    }

    /// Get the number of active subscriptions.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Check if a subscription exists.
    pub fn has_subscription(&self, tx: &Arc<str>) -> bool {
        self.subscriptions.contains_key(tx)
    }
}

impl<W: WsWriter> Drop for ClientSession<W> {
    fn drop(&mut self) {
        // All guards drop automatically
        log::debug!(
            "ClientSession dropped for client {}, cleaning up {} subscriptions",
            self.client_id,
            self.subscriptions.len()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::to_value::ToValue;
    use crate::common::with_id::WithId;
    use crate::store::StoreRegistry;
    use serde_json::Value;
    use std::sync::Mutex;

    // Mock writer that collects messages
    struct MockWriter {
        messages: Mutex<Vec<MykoMessage>>,
    }

    impl MockWriter {
        fn new() -> Self {
            Self {
                messages: Mutex::new(Vec::new()),
            }
        }

        fn message_count(&self) -> usize {
            self.messages.lock().unwrap().len()
        }

        fn last_message(&self) -> Option<MykoMessage> {
            self.messages.lock().unwrap().last().cloned()
        }

        fn messages(&self) -> Vec<MykoMessage> {
            self.messages.lock().unwrap().clone()
        }
    }

    impl WsWriter for MockWriter {
        fn send(&self, msg: MykoMessage) {
            self.messages.lock().unwrap().push(msg);
        }
    }

    // Need Arc wrapper for test
    struct ArcMockWriter(Arc<MockWriter>);

    impl WsWriter for ArcMockWriter {
        fn send(&self, msg: MykoMessage) {
            self.0.send(msg);
        }
    }

    // Test entity
    #[derive(Debug, Clone)]
    struct TestEntity {
        id: Arc<str>,
        name: String,
    }

    impl WithId for TestEntity {
        fn id(&self) -> Arc<str> {
            self.id.clone()
        }
    }

    impl ToValue for TestEntity {
        fn to_value(&self) -> Value {
            serde_json::json!({
                "id": self.id.as_ref(),
                "name": self.name
            })
        }
    }

    impl AnyItem for TestEntity {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn entity_type(&self) -> &'static str {
            "TestEntity"
        }
    }

    fn make_entity(id: &str, name: &str) -> Arc<dyn AnyItem> {
        Arc::new(TestEntity {
            id: id.into(),
            name: name.to_string(),
        }) as Arc<dyn AnyItem>
    }

    #[test]
    fn test_subscribe_query_cellmap() {
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));
        store.insert("b".into(), make_entity("b", "Bob"));

        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);

        let cellmap = store.select(|_| true);
        session.subscribe_query("tx-1".into(), cellmap);

        // Should have received initial data
        assert!(mock.message_count() >= 1);

        // Add an entity
        store.insert("c".into(), make_entity("c", "Charlie"));
        assert!(mock.message_count() >= 2);
    }

    #[test]
    fn test_cancel_subscription() {
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);

        let cellmap = store.select(|_| true);
        session.subscribe_query("tx-1".into(), cellmap);
        assert_eq!(session.subscription_count(), 1);

        session.cancel(&"tx-1".into());
        assert_eq!(session.subscription_count(), 0);
    }

    #[test]
    fn test_session_drop_cleanup() {
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));

        {
            let mock = Arc::new(MockWriter::new());
            let writer = ArcMockWriter(mock.clone());
            let mut session = ClientSession::new("client-1".into(), writer);

            let cellmap1 = store.select(|_| true);
            let cellmap2 = store.select(|_| true);
            session.subscribe_query("tx-1".into(), cellmap1);
            session.subscribe_query("tx-2".into(), cellmap2);

            // 2 subscriptions active
            assert_eq!(session.subscription_count(), 2);
        }
        // Session dropped - subscriptions should be cleaned up
    }

    #[test]
    fn test_subscribe_by_id() {
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));

        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);

        let id: Arc<str> = "a".into();
        let cellmap = store.select(move |item| *item.id() == *id);
        session.subscribe_query("tx-1".into(), cellmap);

        // Should have received initial data
        assert!(mock.message_count() >= 1);

        // Update the entity
        store.insert("a".into(), make_entity("a", "Alice Updated"));
        assert!(mock.message_count() >= 2);
    }

    #[test]
    fn test_delete_sends_deletes_not_upserts() {
        use crate::api::query::QueryResponse;

        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));
        store.insert("b".into(), make_entity("b", "Bob"));

        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);

        let cellmap = store.select(|_| true);
        session.subscribe_query("tx-1".into(), cellmap);

        let initial_count = mock.message_count();

        // Delete an entity
        store.remove(&"a".into());

        // Should have received a message with deletes
        assert!(mock.message_count() > initial_count);

        // Find the delete message (it should be the last one)
        let last_msg = mock.last_message().unwrap();
        if let MykoMessage::QueryResponse(QueryResponse { deletes, upserts, .. }) = last_msg {
            // The delete message should have "a" in deletes and empty upserts
            assert!(deletes.iter().any(|id| id.as_ref() == "a"), "Delete should contain 'a'");
            assert!(upserts.is_empty(), "Upserts should be empty for delete");
        } else {
            panic!("Expected QueryResponse");
        }
    }
}

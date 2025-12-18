use crate::{
    actors::ws::websocket_server::{SendToClientData, WebSocketServerMsg},
    api::query::{QueryError, QueryResponse},
    item::WrappedItem,
    message::MykoMessage,
    parsers::item::AnyItem,
    runtime::ActorRef,
};
use crossbeam::channel as crossbeam_channel;
use log::error;
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Debug)]
pub enum ProcessUpdateData {
    Set(Arc<dyn AnyItem>),
    Del(Arc<str>),
}

pub struct StartQueryData {
    pub query_id: Arc<str>,
    pub query_item_type: Arc<str>,
    pub query: Value,
}

/// Update sent via streaming query subscriptions.
/// Used for inter-actor query subscriptions without polling.
#[derive(Clone, Debug)]
pub enum QueryStreamUpdate {
    /// Initial snapshot of all matching items
    Initial(BTreeMap<Arc<str>, Arc<dyn AnyItem>>),
    /// An item was added or updated
    Upsert(Arc<str>, Arc<dyn AnyItem>),
    /// An item was removed
    Remove(Arc<str>),
    /// An error occurred processing this query
    Error(String),
}

/// Trait for receiving query result updates.
/// Implementations can forward results to channels, WebSockets, or other destinations.
pub trait QueryResultSink: Send + 'static {
    /// Push an update to the sink. Returns false if the sink is closed.
    fn push(&mut self, update: QueryStreamUpdate) -> bool;
}

/// Sink that forwards updates to a crossbeam channel.
/// Used for internal actor-to-actor query subscriptions.
pub struct ChannelSink {
    sender: crossbeam_channel::Sender<QueryStreamUpdate>,
}

impl ChannelSink {
    pub fn new(sender: crossbeam_channel::Sender<QueryStreamUpdate>) -> Self {
        Self { sender }
    }
}

impl QueryResultSink for ChannelSink {
    fn push(&mut self, update: QueryStreamUpdate) -> bool {
        self.sender.send(update).is_ok()
    }
}

/// Sink that sends query results directly to a WebSocket client.
/// Eliminates intermediate channels and async tasks for external subscriptions.
pub struct WebSocketSink {
    client_id: Arc<str>,
    ws_server: ActorRef<WebSocketServerMsg>,
    item_type: Arc<str>,
    query_id: Arc<str>,
    tx: Arc<str>,
    sequence: u64,
}

impl WebSocketSink {
    pub fn new(
        client_id: Arc<str>,
        ws_server: ActorRef<WebSocketServerMsg>,
        item_type: Arc<str>,
        query_id: Arc<str>,
        tx: Arc<str>,
    ) -> Self {
        Self {
            client_id,
            ws_server,
            item_type,
            query_id,
            tx,
            sequence: 0,
        }
    }
}

impl QueryResultSink for WebSocketSink {
    fn push(&mut self, update: QueryStreamUpdate) -> bool {
        let message = match update {
            QueryStreamUpdate::Initial(entries) => {
                self.sequence = 0;
                let upserts = entries
                    .values()
                    .map(|item| WrappedItem {
                        item: item.to_value(),
                        item_type: self.item_type.clone(),
                    })
                    .collect::<Vec<_>>();
                MykoMessage::QueryResponse(QueryResponse {
                    sequence: 0,
                    deletes: vec![],
                    upserts,
                    tx: self.tx.clone(),
                })
            }
            QueryStreamUpdate::Upsert(_id, value) => {
                self.sequence += 1;
                let upserts = vec![WrappedItem {
                    item: value.to_value(),
                    item_type: self.item_type.clone(),
                }];
                MykoMessage::QueryResponse(QueryResponse {
                    sequence: self.sequence,
                    deletes: vec![],
                    upserts,
                    tx: self.tx.clone(),
                })
            }
            QueryStreamUpdate::Remove(key) => {
                self.sequence += 1;
                MykoMessage::QueryResponse(QueryResponse {
                    sequence: self.sequence,
                    deletes: vec![key],
                    upserts: vec![],
                    tx: self.tx.clone(),
                })
            }
            QueryStreamUpdate::Error(message) => {
                error!("Query error [tx={}]: {}", self.tx, message);
                MykoMessage::QueryError(QueryError {
                    tx: self.tx.to_string(),
                    query_id: self.query_id.to_string(),
                    message,
                })
            }
        };

        self.ws_server
            .send_message(WebSocketServerMsg::SendToClient(SendToClientData {
                client_id: self.client_id.clone(),
                message,
            }))
            .is_ok()
    }
}

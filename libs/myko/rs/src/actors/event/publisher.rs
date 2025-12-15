//! Shared event publishing utilities.
//!
//! The [`EventPublisher`] provides a common interface for publishing events to the
//! [`EventManager`](super::event_manager::EventManager). It's used by:
//! - [`CommandContext`](crate::command::handler::CommandContext) for command-emitted events
//! - [`RelationshipManager`](crate::actors::relationship::RelationshipManager) for cascade events

#![allow(clippy::result_large_err)]

use super::{
    common::{PersistEvent, ProcessEventData},
    event_manager::EventManagerMsg,
};
use crate::{
    event::{EventOptions, MEvent, MEventType},
    item::Eventable,
    prelude::AnyItem,
    runtime::{ActorRef, SendError},
};
use serde::Serialize;
use std::sync::Arc;
use uuid::Uuid;

/// Helper for publishing events to EventManager.
///
/// Encapsulates the common logic for creating and sending events, reducing
/// duplication between CommandContext and RelationshipManager.
#[derive(Clone)]
pub struct EventPublisher {
    event_manager: ActorRef<EventManagerMsg>,
    host_id: Uuid,
}

impl EventPublisher {
    /// Create a new EventPublisher.
    pub fn new(event_manager: ActorRef<EventManagerMsg>, host_id: Uuid) -> Self {
        Self {
            event_manager,
            host_id,
        }
    }

    /// Publish a SET event with a typed item.
    ///
    /// The item is cloned and wrapped as `Arc<dyn AnyItem>` for efficient downstream processing.
    pub fn publish_set<T: Eventable + Serialize + Clone + 'static>(
        &self,
        item: &T,
        tx: &str,
        client_id: Option<Arc<str>>,
        options: Option<EventOptions>,
    ) -> Result<(), SendError<EventManagerMsg>> {
        let mut event = MEvent::from_item(item, MEventType::SET, tx.to_string());
        event.source_id = Some(self.host_id.to_string());
        if let Some(opts) = options {
            event.options = Some(opts);
        }

        let parsed_item: Arc<dyn AnyItem> = Arc::new(item.clone());

        self.event_manager
            .send_message(EventManagerMsg::ProcessEvent(ProcessEventData {
                event,
                persist: PersistEvent::Persist,
                parsed_item: Some(parsed_item),
                client_id,
            }))
    }

    /// Publish a DEL event with a typed item.
    ///
    /// The item is cloned and wrapped as `Arc<dyn AnyItem>` for efficient downstream processing.
    pub fn publish_del<T: Eventable + Serialize + Clone + 'static>(
        &self,
        item: &T,
        tx: &str,
        client_id: Option<Arc<str>>,
        options: Option<EventOptions>,
    ) -> Result<(), SendError<EventManagerMsg>> {
        let mut event = MEvent::from_item(item, MEventType::DEL, tx.to_string());
        event.source_id = Some(self.host_id.to_string());
        if let Some(opts) = options {
            event.options = Some(opts);
        }

        let parsed_item: Arc<dyn AnyItem> = Arc::new(item.clone());

        self.event_manager
            .send_message(EventManagerMsg::ProcessEvent(ProcessEventData {
                event,
                persist: PersistEvent::Persist,
                parsed_item: Some(parsed_item),
                client_id,
            }))
    }

    /// Publish a DEL event with a pre-parsed item.
    ///
    /// Used when you already have an `Arc<dyn AnyItem>` (e.g., from a query result).
    pub fn publish_del_item(
        &self,
        entity_type: &str,
        item: Arc<dyn AnyItem>,
        tx: &str,
        options: Option<EventOptions>,
    ) -> Result<(), SendError<EventManagerMsg>> {
        let event = MEvent {
            tx: tx.to_string(),
            item_type: entity_type.to_string(),
            item: item.to_value(),
            change_type: MEventType::DEL,
            created_at: chrono::Utc::now().to_rfc3339(),
            source_id: Some(self.host_id.to_string()),
            options,
        };

        self.event_manager
            .send_message(EventManagerMsg::ProcessEvent(ProcessEventData {
                event,
                persist: PersistEvent::Persist,
                parsed_item: Some(item),
                client_id: None,
            }))
    }

    /// Publish a SET event from a raw Value (no parsed item).
    ///
    /// Used when the item has been modified (e.g., array field updated) and
    /// can't be represented as a typed item anymore.
    pub fn publish_set_value(
        &self,
        entity_type: &str,
        item: serde_json::Value,
        tx: &str,
        options: Option<EventOptions>,
    ) -> Result<(), SendError<EventManagerMsg>> {
        let event = MEvent {
            tx: tx.to_string(),
            item_type: entity_type.to_string(),
            item,
            change_type: MEventType::SET,
            created_at: chrono::Utc::now().to_rfc3339(),
            source_id: Some(self.host_id.to_string()),
            options,
        };

        self.event_manager
            .send_message(EventManagerMsg::ProcessEvent(ProcessEventData {
                event,
                persist: PersistEvent::Persist,
                parsed_item: None,
                client_id: None,
            }))
    }
}

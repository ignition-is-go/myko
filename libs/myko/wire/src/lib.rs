//! Canonical, transport-independent wire messages for Myko nodes.
//!
//! Local sockets, native peer transports, and WebSocket compatibility layers
//! serialize these same request and response shapes. Each adapter remains
//! responsible for framing, authentication, and lifecycle supervision.

#![forbid(unsafe_code)]

use std::fmt;

use myko_app::{ErasedHandlerState, ErasedViewDelta, HandlerRequest};
use myko_federation::{
    CommandId, CommandResponse, CommandStatePage, CommandStateRequest, CommandStateUpdate,
    CommandSubmission, CommandWatchRequest, ItemFollowRequest, ItemStatePage, ItemStateRequest,
    ItemStateUpdate, LiveEvent, LogPosition, NodeId, ReplicationBatch, ScopeCatalogPage, ScopeId,
    ScopedReplicationBatch,
};
use serde::{Deserialize, Serialize};

/// Version of the canonical Myko node message schema.
///
/// Transport adapters may version their framing independently, but a peer must
/// not decode an envelope whose message schema it does not understand.
pub const WIRE_PROTOCOL_VERSION: u32 = 2;

/// A versioned message envelope for framed transports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireEnvelope<T> {
    /// Canonical message schema version.
    pub version: u32,
    /// The transport-independent message body.
    pub body: T,
}

impl<T> WireEnvelope<T> {
    /// Wraps a message in the current canonical schema version.
    #[must_use]
    pub const fn new(body: T) -> Self {
        Self {
            version: WIRE_PROTOCOL_VERSION,
            body,
        }
    }

    /// Returns the body when this envelope uses the supported schema version.
    ///
    /// # Errors
    ///
    /// Returns [`UnsupportedWireVersion`] when a peer sent a newer or older
    /// canonical message schema.
    pub fn into_current(self) -> Result<T, UnsupportedWireVersion> {
        if self.version == WIRE_PROTOCOL_VERSION {
            Ok(self.body)
        } else {
            Err(UnsupportedWireVersion {
                received: self.version,
            })
        }
    }
}

/// A peer sent a canonical message schema this node does not support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnsupportedWireVersion {
    /// The schema version received from the peer.
    pub received: u32,
}

impl fmt::Display for UnsupportedWireVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "unsupported Myko wire version {}; expected {}",
            self.received, WIRE_PROTOCOL_VERSION
        )
    }
}

impl std::error::Error for UnsupportedWireVersion {}

/// A canonical node request together with its transport-independent destination.
///
/// `None` addresses the node that accepted the connection. A concrete node id
/// asks that node to dispatch locally when it owns the id or forward the same
/// envelope through its federation router otherwise.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRequestEnvelope {
    /// Requested destination, or the node that accepted the connection.
    pub destination: Option<NodeId>,
    /// The unchanged canonical request to execute at the destination.
    pub request: NodeRequest,
}

impl NodeRequestEnvelope {
    /// Addresses the node that accepted the connection.
    #[must_use]
    pub const fn connected(request: NodeRequest) -> Self {
        Self {
            destination: None,
            request,
        }
    }

    /// Addresses one node through whichever node accepted the connection.
    #[must_use]
    pub const fn at(destination: NodeId, request: NodeRequest) -> Self {
        Self {
            destination: Some(destination),
            request,
        }
    }
}

/// Operations a Myko peer may request from another node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodeRequest {
    /// Identifies the node serving this connection.
    Identify,
    /// Lists application scopes visible to the caller.
    ListScopes {
        /// Exclusive scope cursor.
        after: Option<ScopeId>,
        /// Maximum scopes to return.
        limit: u32,
    },
    /// Pulls authoritative history from the serving node.
    Pull {
        /// Exclusive log cursor.
        after: Option<LogPosition>,
    },
    /// Pulls authoritative history for one scope.
    PullScope {
        /// Scope to pull.
        scope_id: ScopeId,
        /// Exclusive log cursor.
        after: Option<LogPosition>,
    },
    /// Follows authoritative history from the serving node.
    Follow {
        /// Exclusive log cursor.
        after: Option<LogPosition>,
    },
    /// Follows authoritative history for one scope.
    FollowScope {
        /// Scope to follow.
        scope_id: ScopeId,
        /// Exclusive log cursor.
        after: Option<LogPosition>,
    },
    /// Follows best-effort live events for explicit topics.
    FollowLive {
        /// Topics to follow.
        topics: Vec<String>,
    },
    /// Submits a command for durable admission.
    Submit {
        /// Command to admit.
        command: CommandSubmission,
    },
    /// Reads the current command snapshot.
    Command {
        /// Command identifier.
        command_id: CommandId,
    },
    /// Reads a page from the command-state catalog.
    CommandState {
        /// Command-state page request.
        request: CommandStateRequest,
    },
    /// Watches a command-state catalog.
    WatchCommands {
        /// Catalog follow request.
        request: CommandWatchRequest,
    },
    /// Watches one command's state.
    WatchCommand {
        /// Command identifier.
        command_id: CommandId,
    },
    /// Requests cancellation of a command.
    Cancel {
        /// Command identifier.
        command_id: CommandId,
        /// Durable cancellation reason.
        reason: String,
    },
    /// Reads a typed item-state page.
    ItemState {
        /// Item-state page request.
        request: ItemStateRequest,
    },
    /// Follows a typed item-state projection.
    FollowItems {
        /// Item projection follow request.
        request: ItemFollowRequest,
    },
    /// Follows one registered reactive application handler.
    FollowHandler {
        /// Handler lifecycle request.
        request: HandlerRequest,
    },
}

/// Frames a Myko node emits in response to a [`NodeRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodeFrame {
    /// Serving node identity.
    Hello { source_node: NodeId },
    /// Authoritative history batch.
    Batch { batch: Box<ReplicationBatch> },
    /// Authoritative scope-filtered history batch.
    ScopedBatch { batch: Box<ScopedReplicationBatch> },
    /// Scope catalog page.
    ScopeCatalog { page: Box<ScopeCatalogPage> },
    /// Command operation result.
    Command { response: Box<CommandResponse> },
    /// Command-state catalog page.
    CommandState { page: Box<CommandStatePage> },
    /// Command catalog is ready to stream updates.
    CommandWatchReady { request: Box<CommandWatchRequest> },
    /// Command-state stream update.
    CommandUpdate { update: Box<CommandStateUpdate> },
    /// Item-state page.
    ItemState { page: Box<ItemStatePage> },
    /// Item projection is ready to stream updates.
    ItemFollowReady { request: Box<ItemFollowRequest> },
    /// Item-state stream update.
    ItemUpdate { update: Box<ItemStateUpdate> },
    /// Reactive application handler lifecycle state.
    HandlerState { state: Box<ErasedHandlerState> },
    /// Incremental keyed application-view rows after its initial state.
    HandlerViewDelta { delta: Box<ErasedViewDelta> },
    /// Best-effort live event.
    Live { event: Box<LiveEvent> },
    /// Operation failure, represented as a portable diagnostic.
    Error { message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_uses_current_schema_version() {
        let envelope = WireEnvelope::new(NodeRequestEnvelope::connected(NodeRequest::Identify));
        assert_eq!(envelope.version, WIRE_PROTOCOL_VERSION);
    }

    #[test]
    fn envelope_rejects_an_unknown_schema_version() {
        let envelope = WireEnvelope {
            version: WIRE_PROTOCOL_VERSION.saturating_add(1),
            body: NodeRequestEnvelope::connected(NodeRequest::Identify),
        };
        assert_eq!(
            envelope.into_current(),
            Err(UnsupportedWireVersion {
                received: WIRE_PROTOCOL_VERSION.saturating_add(1),
            })
        );
    }

    #[test]
    fn messages_use_one_stable_discriminator() {
        assert!(matches!(
            serde_json::to_value(NodeRequest::Identify),
            Ok(serde_json::Value::Object(request))
                if request.get("type") == Some(&serde_json::Value::String("identify".to_owned()))
        ));
        assert!(matches!(
            serde_json::to_value(NodeFrame::Error {
                message: "failed".to_owned(),
            }),
            Ok(serde_json::Value::Object(frame))
                if frame.get("type") == Some(&serde_json::Value::String("error".to_owned()))
        ));
    }

    #[test]
    fn destination_does_not_change_the_request_shape() {
        let node_id = NodeId::new();
        let request = NodeRequest::FollowLive {
            topics: vec!["agent:test".to_owned()],
        };
        let connected = NodeRequestEnvelope::connected(request.clone());
        let routed = NodeRequestEnvelope::at(node_id, request.clone());
        assert_eq!(connected.request, request);
        assert_eq!(routed.request, request);
        assert_eq!(connected.destination, None);
        assert_eq!(routed.destination, Some(node_id));
    }
}

use myko_federation::{
    AccessTarget, AuthorityPresentation, ChallengeId, CommandId, CommandStateRequest,
    CommandSubmission, CommandWatchRequest, HandlerAccess, ItemFollowRequest, ItemStateRequest,
    LogPosition, NodeId, ProvenanceHop, ReplicationSelection, ScopeId,
};
use myko_wire::{HandlerRequest, NodeRequest, NodeRequestEnvelope};

/// Exact durable-history selection for one prepared request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistorySelection {
    All,
    Scope(ScopeId),
    Selected(ReplicationSelection),
}

/// One decoded request after its sole interpretation boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreparedRequest {
    Identify,
    ListScopes {
        after: Option<ScopeId>,
        limit: u32,
    },
    ReadHistory {
        selection: HistorySelection,
        after: Option<LogPosition>,
    },
    FollowHistory {
        selection: HistorySelection,
        after: Option<LogPosition>,
    },
    FollowLive {
        topics: Vec<String>,
    },
    SubmitCommand(CommandSubmission),
    ReadCommand(CommandId),
    ReadCommands(CommandStateRequest),
    WatchCommands(CommandWatchRequest),
    WatchCommand(CommandId),
    CancelCommand {
        command_id: CommandId,
        reason: String,
    },
    ReadItems(ItemStateRequest),
    FollowItems(ItemFollowRequest),
    FollowHandler(HandlerRequest),
    ApproveAuthority {
        challenge_id: ChallengeId,
        approved: bool,
    },
}

impl PreparedRequest {
    /// Return the exact authorization target derived at preparation time.
    #[must_use]
    pub fn access_target(&self) -> AccessTarget {
        match self {
            Self::Identify => AccessTarget::NodeIdentity,
            Self::ListScopes { .. } => AccessTarget::ScopeCatalog,
            Self::ReadHistory { selection, .. } | Self::FollowHistory { selection, .. } => {
                AccessTarget::History(match selection {
                    HistorySelection::All => ReplicationSelection::All,
                    HistorySelection::Scope(scope_id) => {
                        ReplicationSelection::Scopes(vec![myko_federation::ScopeSelection::Exact(
                            scope_id.clone(),
                        )])
                    }
                    HistorySelection::Selected(selection) => selection.clone(),
                })
            }
            Self::FollowLive { topics } => AccessTarget::LiveTopics(topics.clone()),
            Self::SubmitCommand(command) => AccessTarget::Command(command.id),
            Self::ReadCommand(command_id)
            | Self::WatchCommand(command_id)
            | Self::CancelCommand { command_id, .. } => AccessTarget::Command(*command_id),
            Self::ReadCommands(request) => AccessTarget::CommandCatalog {
                source_node: request.source_node,
                service_id: request.service_id.clone(),
                scope_id: request.scope_id.clone(),
                command_type: request.command_type.clone(),
            },
            Self::WatchCommands(request) => AccessTarget::CommandCatalog {
                source_node: Some(request.source_node),
                service_id: request.service_id.clone(),
                scope_id: request.scope_id.clone(),
                command_type: request.command_type.clone(),
            },
            Self::ReadItems(request) => AccessTarget::Items {
                source_node: request.source_node,
                service_id: request.service_id.clone(),
                scope_id: request.scope_id.clone(),
                item_type: request.item_type.clone(),
            },
            Self::FollowItems(request) => AccessTarget::Items {
                source_node: Some(request.source_node),
                service_id: request.service_id.clone(),
                scope_id: request.scope_id.clone(),
                item_type: request.item_type.clone(),
            },
            Self::FollowHandler(request) => AccessTarget::Handler {
                access: HandlerAccess {
                    kind: request.kind,
                    handler_id: request.handler_id.clone(),
                },
                source_node: request.source_node,
                scope_id: request.scope_id.clone(),
            },
            Self::ApproveAuthority { challenge_id, .. } => {
                AccessTarget::AuthorityApproval(challenge_id.clone())
            }
        }
    }
}

/// Prepared routing and authority provenance for one decoded envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedEnvelope {
    pub destination: Option<NodeId>,
    pub authority: Option<AuthorityPresentation>,
    pub forwarding_provenance: Vec<ProvenanceHop>,
    pub request: PreparedRequest,
    pub access_target: AccessTarget,
}

impl PreparedEnvelope {
    /// Interpret every wire request exactly once.
    #[must_use]
    pub fn from_wire(envelope: NodeRequestEnvelope) -> Self {
        let request = match envelope.request {
            NodeRequest::Identify => PreparedRequest::Identify,
            NodeRequest::ListScopes { after, limit } => {
                PreparedRequest::ListScopes { after, limit }
            }
            NodeRequest::Pull { after } => PreparedRequest::ReadHistory {
                selection: HistorySelection::All,
                after,
            },
            NodeRequest::PullScope { scope_id, after } => PreparedRequest::ReadHistory {
                selection: HistorySelection::Scope(scope_id),
                after,
            },
            NodeRequest::PullSelected { selection, after } => PreparedRequest::ReadHistory {
                selection: HistorySelection::Selected(selection),
                after,
            },
            NodeRequest::Follow { after } => PreparedRequest::FollowHistory {
                selection: HistorySelection::All,
                after,
            },
            NodeRequest::FollowScope { scope_id, after } => PreparedRequest::FollowHistory {
                selection: HistorySelection::Scope(scope_id),
                after,
            },
            NodeRequest::FollowSelected { selection, after } => PreparedRequest::FollowHistory {
                selection: HistorySelection::Selected(selection),
                after,
            },
            NodeRequest::FollowLive { topics } => PreparedRequest::FollowLive { topics },
            NodeRequest::Submit { command } => PreparedRequest::SubmitCommand(command),
            NodeRequest::Command { command_id } => PreparedRequest::ReadCommand(command_id),
            NodeRequest::CommandState { request } => PreparedRequest::ReadCommands(request),
            NodeRequest::WatchCommands { request } => PreparedRequest::WatchCommands(request),
            NodeRequest::WatchCommand { command_id } => PreparedRequest::WatchCommand(command_id),
            NodeRequest::Cancel { command_id, reason } => {
                PreparedRequest::CancelCommand { command_id, reason }
            }
            NodeRequest::ItemState { request } => PreparedRequest::ReadItems(request),
            NodeRequest::FollowItems { request } => PreparedRequest::FollowItems(request),
            NodeRequest::FollowHandler { request } => PreparedRequest::FollowHandler(request),
            NodeRequest::ApproveAuthority {
                challenge_id,
                approved,
            } => PreparedRequest::ApproveAuthority {
                challenge_id,
                approved,
            },
        };
        let access_target = request.access_target();
        Self {
            destination: envelope.destination,
            authority: envelope.authority,
            forwarding_provenance: envelope.forwarding_provenance,
            request,
            access_target,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preparation_derives_handler_access_once() {
        let handler = HandlerRequest {
            kind: myko_federation::HandlerKind::View,
            handler_id: "Projects".to_owned(),
            source_node: Some(NodeId::new()),
            scope_id: None,
            params: serde_json::json!({}),
        };
        let prepared = PreparedEnvelope::from_wire(NodeRequestEnvelope::connected(
            NodeRequest::FollowHandler {
                request: handler.clone(),
            },
        ));
        assert_eq!(
            prepared.access_target,
            AccessTarget::Handler {
                access: HandlerAccess {
                    kind: handler.kind,
                    handler_id: handler.handler_id,
                },
                source_node: handler.source_node,
                scope_id: handler.scope_id,
            }
        );
    }
}

//! Iroh transport adapter for Myko 7 immutable replication batches.
//!
//! This crate owns peer connectivity only. Command admission, history,
//! idempotency, and event ingestion remain in `myko-federation`. Explicit pulls
//! provide bounded catch-up, supervised follow streams replay then push, and
//! authenticated peers can submit, inspect, or cancel commands without
//! claiming them.
//! Exact-scope pull and follow streams let subscribers advance a source cursor
//! without receiving unrelated event bodies.
//! Every inbound operation is presented to Myko's transport-neutral
//! [`AccessPolicy`] using the authenticated Iroh endpoint as its principal.
//! Long-lived history and live streams re-evaluate that policy when it changes,
//! so revocation closes already-open streams rather than only blocking the next
//! connection.
//! The same endpoint carries filtered best-effort live events without turning
//! them into immutable history.

#![forbid(unsafe_code)]

mod attestation;
mod pairing;

pub use attestation::{
    RetainedHistorySignatureError, sign_retained_history_statement,
    verify_retained_history_statement,
};

pub use pairing::{
    MYKO_PAIRING_ALPN, MYKO_PAIRING_OFFER_ALPN, PairingInvitation, PairingReceipt,
    PairingReceiptSubscription,
};

use std::{
    collections::HashMap,
    fmt::Write as _,
    fs::{self, OpenOptions},
    io::Write,
    num::NonZeroUsize,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use iroh::{
    Endpoint,
    endpoint::{BindOpts, Connection, RecvStream, SendStream, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
pub use iroh::{EndpointAddr, EndpointId, SecretKey};
use myko::{
    ApplicationHost,
    client::{HandlerClientError, HandlerConnection, HandlerConnector, HandlerFrame, MykoClient},
    server::FederatedSession,
};
use myko_federation::{
    AccessPolicy, ApprovalDecision, AuthorityPresentation, ChallengeId, CommandClient,
    CommandClientFuture, CommandId, CommandResponse, CommandSnapshot, CommandStateClient,
    CommandStatePage, CommandStatePageFuture, CommandStateRequest, CommandStateSnapshot,
    CommandStateStream, CommandSubmission, CommandSubscription, CommandSubscriptionFuture,
    CommandWatchFuture, CommandWatchingClient, DenyAllAccessPolicy, ItemClient, ItemProjection,
    ItemQuery, ItemQueryResult, ItemQuerySnapshot, ItemQueryStream, ItemQueryUpdate, ItemStatePage,
    ItemStatePageFuture, ItemStateRequest, ItemStateSnapshot, LiveEvent, LivePublishReport,
    LiveSubscription, LiveSubscriptionState, LogPosition, Node, NodeId, PrincipalId, ProvenanceHop,
    ReconnectPolicy, ReplicationCheckpoint, ReplicationCursorKey, ReplicationCursorStore,
    ReplicationReport, ReplicationSelection, ScopeCatalogPage, ScopeId, ScopedReplicationBatch,
    ScopedReplicationCheckpoint, ScopedReplicationReport, SelectedReplicationBatch,
    SelectedReplicationCheckpoint, SelectedReplicationReport, SubscriptionLiveness,
    control_quorum::{
        ControlBallot, ControlHead, ControlValue, SignedControlProposal, SignedControlVote,
    },
    live_subscription,
};
use myko_wire::{
    HandlerRequest, NodeFrame as ReplicationFrame, NodeRequest as ReplicationRequest,
    NodeRequestEnvelope, WireEnvelope,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{sync::watch, task::JoinHandle};

mod identity;
pub use identity::*;

mod peer;
pub use peer::*;

mod client;
pub use client::*;

mod control_client;
mod evidence_client;
pub use evidence_client::IrohScopedEvidenceEndpoint;

mod protocol;
#[cfg(test)]
use protocol::replication_cursor_key;
use protocol::{
    read_command_frame, read_frame, write_request_envelope, write_request_with_authority,
};

#[cfg(test)]
#[allow(
    clippy::arithmetic_side_effects,
    clippy::panic_in_result_fn,
    clippy::too_many_lines,
    clippy::unwrap_used
)]
mod tests;

//! Owner-local Myko peer transport.
//!
//! A protected Unix socket carries the same typed snapshot/follow contracts as
//! native Iroh peers. The transport does not define application requests or
//! projections: a local TUI, desktop application, or service manager remains a
//! lightweight Myko node-facing participant rather than a special server API.

#![forbid(unsafe_code)]

use std::{
    fs,
    os::unix::fs::{FileTypeExt as _, PermissionsExt as _},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use myko::{
    ApplicationHost, MykoApplication,
    client::{HandlerClientError, HandlerConnection, HandlerConnector, HandlerFrame, MykoClient},
    server::FederatedSession,
};
use myko_federation::{
    AccessPolicy, ApprovalDecision, AuthorityPresentation, AuthorizationDecision, ChallengeId,
    CommandClient, CommandClientFuture, CommandId, CommandResponse, CommandSnapshot,
    CommandSubmission, CommandSubscription, CommandSubscriptionFuture, CommandWatchFuture,
    CommandWatchingClient, ItemClient, ItemQuery, ItemQueryResult, ItemQuerySnapshot,
    ItemQueryStream, ItemQueryUpdate, ItemStatePageFuture, ItemStateRequest, LiveEvent,
    LiveSubscription, LiveSubscriptionState, Node, NodeError, NodeId, Principal, PrincipalId,
    ProvenanceHop, ReconnectPolicy, ScopeId, SubscriptionLiveness, live_subscription,
};
use myko_wire::{
    HandlerRequest, NodeFrame as PeerFrame, NodeRequest as PeerRequest, NodeRequestEnvelope,
    WireEnvelope as Envelope,
};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{UnixListener, UnixStream},
    sync::{Semaphore, watch},
    task::{JoinHandle, JoinSet},
};
use tracing::Instrument as _;

const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
static NEXT_LOCAL_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);
const MAX_CONNECTIONS: usize = 64;

mod transport;
pub use transport::*;

#[cfg(test)]
#[allow(
    clippy::arithmetic_side_effects,
    clippy::expect_used,
    clippy::panic_in_result_fn,
    clippy::unwrap_used
)]
mod tests;

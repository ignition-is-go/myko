//! Transport-neutral federation primitives for Myko 7.
//!
//! This crate deliberately has no socket, HTTP, Iroh, Tokio, or storage-engine
//! dependency. A node is an authenticated command/history endpoint. It also
//! provides a bounded [`LiveEventHub`] for coalescible, non-authoritative state.
//! Network protocols and durable storage implement adapters around these
//! transport-neutral contracts.

#![forbid(unsafe_code)]
// Authorization denials carry the same structured report returned on the wire;
// boxing it would make every policy consumer allocate merely to propagate it.
#![allow(clippy::result_large_err)]

mod access_failure;
mod attestation;
mod authority;
mod causal;
mod control;
mod control_chain;
pub mod control_quorum;
mod prepared_effect;
mod publication;
mod reactive;
mod selected;
mod signed_statement;

pub use access_failure::{AuthorityUnavailable, AuthorizationFailure};
pub use attestation::RetainedHistoryStatement;
pub use authority::*;
pub use causal::causal_replay;
pub use control::FrameworkControlEvent;
pub use control_chain::{
    CertifiedControlChain, CertifiedControlContext, ControlAnchor, ControlTransition,
};
pub use prepared_effect::PreparedCommandEffect;
pub use publication::{LivePublication, LivePublicationStream};
pub use signed_statement::SignedRetainedHistoryStatement;

pub use reactive::{
    CollectionPlan, CompositeFrontier, LiveCollection, LiveCollectionError, LiveCollectionHandle,
    LiveCollectionRevision, LiveCollectionRevisionStream, LiveCollectionState,
    LiveCollectionWriter, LiveSubscription, LiveSubscriptionHandle, LiveSubscriptionState,
    LiveSubscriptionWriter, MapCollectionPlan, RuntimeCollection, SubscriptionLiveness,
    UnionCollectionPlan, live_collection, live_subscription,
};

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque, hash_map::Entry},
    fmt,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc, Mutex, RwLock, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use hyphae::{Cell, CellImmutable, MapDiff, SubscriptionGuard, Watchable as _};
use parking_lot::ReentrantMutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use uuid::Uuid;

pub use myko_items::{
    __snapshot_item_query, BelongsTo, ConcreteEndpoint, Directed, EdgeEnds, EndpointSpec,
    EntityRef, GeneratedItemQuery, GraphEdge, ItemMutation, ItemProjection, ItemQuery,
    ItemQueryResult, ItemScope, ItemState, MutationOperation, MykoCommand, MykoCommandContract,
    MykoItem, MykoOperation, MykoService, ServiceTypeId, TypedEdgeEnds, Undirected,
};

#[allow(clippy::redundant_pub_crate)]
mod access;
use access::digest_bytes;
pub use access::*;

#[allow(clippy::redundant_pub_crate)]
mod command;
pub use command::*;

mod commitment;
pub use commitment::*;

#[allow(clippy::redundant_pub_crate)]
mod history;
pub use history::*;
use history::{
    DURABLE_EVENT_PAGE_LIMIT, DURABLE_EVENT_PAGE_SIZE, DurableReplay, command_from_event,
    command_snapshot_supersedes, materialize_command_snapshot, materialize_pending_local_commands,
};
use selected::SelectedQueryRead;
pub use selected::{
    SelectedHistoryManifest, SelectedHistoryManifestError, SelectedHistorySnapshot,
};

#[allow(clippy::redundant_pub_crate)]
mod item;
pub use item::*;
use item::{
    SelectedQueryWake, materialize_command_state_entries, materialize_item_state_entries,
    next_command_state_request, validate_command_state_entry, validate_command_state_request,
};

#[allow(clippy::redundant_pub_crate)]
mod node;
#[cfg(test)]
use node::DeclaredCommand;
pub use node::*;
use node::{
    apply_item_envelope, decode_declared_body, decode_typed_command_state, project_item_history,
    validate_change_batch,
};

#[allow(clippy::redundant_pub_crate)]
mod memory;
use memory::MemoryState;
pub use memory::*;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;

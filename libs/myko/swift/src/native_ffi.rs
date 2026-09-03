//! Typed native-language contracts for an embedded Myko node.
//!
//! Application bridges provide access to their composed [`ApplicationNode`].
//! Myko owns the reusable discovery, pairing, peer, replication, and live
//! subscription surface exposed to Swift through `UniFFI`.

use myko_app::ApplicationNode;
use myko_authority::AuthorityPolicy;
use myko_federation::{AuthorityPresentation, AuthorityRealmId, Principal};
use myko_node::EndpointId;

use crate::{EmbeddedNodeError, EmbeddedNodeInfo};

mod authority;
mod federation;
mod subscriptions;

pub use authority::{
    MykoAccessOperation, MykoAuthority, MykoAuthorityConstraints, MykoAuthorityGrant,
    MykoAuthorityGrantInput, MykoAuthorityGrantRecord, MykoAuthorityGrantsSubscription,
    MykoAuthorityGrantsUpdate, MykoFederationPermission, MykoPrincipal, MykoPrincipalKind,
    MykoRevocationKind, MykoScopeSelection,
};
pub use federation::MykoFederation;
pub use subscriptions::{
    MykoNearbyNode, MykoNearbyNodesSubscription, MykoNearbyNodesUpdate, MykoPairedNode,
    MykoPairedNodesSubscription, MykoPairedNodesUpdate, MykoPairingInitiationSubscription,
    MykoPairingInitiationUpdate, MykoPairingReceipt, MykoPairingReceiptsSubscription,
    MykoPairingReceiptsUpdate, MykoPendingPairingReceipt,
};

/// Stable identity and foreground lifecycle state for an embedded Myko node.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct MykoNodeInfo {
    /// Stable authenticated native transport identity.
    pub endpoint_id: String,
    /// Stable Myko history identity stored in the node journal.
    pub node_id: String,
    /// Whether the composed application node is currently active.
    pub node_active: bool,
}

impl From<EmbeddedNodeInfo> for MykoNodeInfo {
    fn from(info: EmbeddedNodeInfo) -> Self {
        Self {
            endpoint_id: info.endpoint_id.to_string(),
            node_id: info.node_id.to_string(),
            node_active: info.active,
        }
    }
}

/// Framework-owned failure surfaced by a native federation binding.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum MykoFederationError {
    /// The composed application node is not currently available.
    #[error("Myko node unavailable: {message}")]
    Unavailable { message: String },
    /// A supplied node or peer identifier was malformed.
    #[error("invalid Myko identifier: {message}")]
    InvalidIdentifier { message: String },
    /// The identity-pinned pairing protocol failed.
    #[error("Myko pairing failed: {message}")]
    Pairing { message: String },
    /// A durable authority command or subscription failed.
    #[error("Myko authority failed: {message}")]
    Authority { message: String },
}

impl From<EmbeddedNodeError> for MykoFederationError {
    fn from(error: EmbeddedNodeError) -> Self {
        Self::Unavailable {
            message: error.to_string(),
        }
    }
}

/// Rust-side access to the composed application hosted by a native frontend.
///
/// This is deliberately not a foreign-language callback. An application crate
/// implements it once so the generated Myko component can execute the same
/// typed commands and subscriptions as every other transport.
pub trait NativeApplicationAccess: Send + Sync + 'static {
    /// Returns the active composed application node.
    ///
    /// # Errors
    ///
    /// Returns an error while the application node is inactive or unavailable.
    fn application(&self) -> Result<ApplicationNode, MykoFederationError>;

    /// Returns the stable native endpoint identity for receipt projection.
    fn endpoint_id(&self) -> EndpointId;
}

/// Authenticated application-owned context for native authority administration.
///
/// The application selects its realm and authenticated administrator once.
/// Myko owns grant persistence, command execution, and live projections.
#[derive(Clone)]
pub struct NativeAuthorityContext {
    policy: AuthorityPolicy,
    realm_id: AuthorityRealmId,
    authenticated: Principal,
    presentation: AuthorityPresentation,
}

impl NativeAuthorityContext {
    #[must_use]
    pub const fn new(
        policy: AuthorityPolicy,
        realm_id: AuthorityRealmId,
        authenticated: Principal,
        presentation: AuthorityPresentation,
    ) -> Self {
        Self {
            policy,
            realm_id,
            authenticated,
            presentation,
        }
    }
}

/// Application hook required by Myko's reusable native authority component.
pub trait NativeAuthorityAccess: NativeApplicationAccess {
    /// Returns the active node's authenticated authority context.
    ///
    /// # Errors
    ///
    /// Returns an error while the application authority is unavailable.
    fn authority_context(&self) -> Result<NativeAuthorityContext, MykoFederationError>;
}

fn transport_error(error: &(impl ToString + ?Sized)) -> MykoFederationError {
    MykoFederationError::Unavailable {
        message: error.to_string(),
    }
}

fn pairing_error(error: &(impl ToString + ?Sized)) -> MykoFederationError {
    MykoFederationError::Pairing {
        message: error.to_string(),
    }
}

fn authority_error(error: &(impl ToString + ?Sized)) -> MykoFederationError {
    MykoFederationError::Authority {
        message: error.to_string(),
    }
}

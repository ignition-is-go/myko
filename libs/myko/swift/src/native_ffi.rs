//! Typed native-language contracts for an embedded Myko node.
//!
//! Application bridges provide access to their composed [`ApplicationNode`].
//! Myko owns the reusable discovery, pairing, peer, replication, and live
//! subscription surface exposed to Swift through `UniFFI`.

use myko_app::ApplicationNode;
use myko_authority::AuthorityPolicy;
use myko_federation::{AuthorityPresentation, AuthorityRealmId, Principal};
use myko_node::EndpointId;
use std::ops::Deref;

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

/// Application state exposed by one active embedded native node.
///
/// Concrete applications implement this on their active runtime. Myko can
/// then provide the native federation adapter without an application-owned
/// wrapper or transport-specific forwarding implementation.
pub trait EmbeddedApplicationRuntime: Send + 'static {
    /// Returns the composed typed application node.
    fn application(&self) -> &ApplicationNode;
}

/// Authority state exposed by an active embedded native application.
pub trait EmbeddedAuthorityRuntime: EmbeddedApplicationRuntime {
    /// Returns the authenticated authority context for native administration.
    fn authority_context(&self) -> NativeAuthorityContext;
}

/// Generic native adapter around an [`EmbeddedNodeHost`].
///
/// The adapter implements Myko's federation and authority access traits once
/// for every embedded application runtime. Application crates retain their
/// start and stop functions but do not create a forwarding host type.
pub struct EmbeddedApplicationHost<Active> {
    host: crate::EmbeddedNodeHost<Active>,
}

impl<Active> EmbeddedApplicationHost<Active> {
    /// Wraps a stable-identity embedded node host.
    #[must_use]
    pub const fn new(host: crate::EmbeddedNodeHost<Active>) -> Self {
        Self { host }
    }
}

impl<Active> Deref for EmbeddedApplicationHost<Active> {
    type Target = crate::EmbeddedNodeHost<Active>;

    fn deref(&self) -> &Self::Target {
        &self.host
    }
}

impl<Active> NativeApplicationAccess for EmbeddedApplicationHost<Active>
where
    Active: EmbeddedApplicationRuntime,
{
    fn application(&self) -> Result<ApplicationNode, MykoFederationError> {
        self.host
            .with_active(MykoFederationError::from, |active, _runtime| {
                Ok(active.application().clone())
            })
    }

    fn endpoint_id(&self) -> EndpointId {
        self.host.info().endpoint_id
    }
}

impl<Active> NativeAuthorityAccess for EmbeddedApplicationHost<Active>
where
    Active: EmbeddedAuthorityRuntime,
{
    fn authority_context(&self) -> Result<NativeAuthorityContext, MykoFederationError> {
        self.host
            .with_active(MykoFederationError::from, |active, _runtime| {
                Ok(active.authority_context())
            })
    }
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

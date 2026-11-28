//! Durable records behind the built-in OAuth authority.
use std::time::SystemTime;

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::AuthenticatedIdentity;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum OAuthRecordKind {
    Client,
    Grant,
    Access,
    Refresh,
}
impl OAuthRecordKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Grant => "grant",
            Self::Access => "access",
            Self::Refresh => "refresh",
        }
    }
}

/// One durable fact, keyed by its kind and [`OAuthRecord::id`].
///
/// Tokens appear only as SHA-256 digests, so a reader of the store cannot
/// present a credential. A grant holds what every token in its family shares.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum OAuthRecord {
    /// A dynamically registered public client. Configured clients are not stored.
    Client {
        id: String,
        name: String,
        redirects: Vec<String>,
    },
    /// One approval: the login that granted it and what it granted.
    Grant {
        family: String,
        client: String,
        scopes: Vec<String>,
        identity: Box<AuthenticatedIdentity>,
    },
    Access {
        digest: String,
        family: String,
        expires_at: SystemTime,
    },
    /// A used refresh token stays stored so that replaying it revokes its family.
    Refresh {
        digest: String,
        family: String,
        used: bool,
    },
}
impl OAuthRecord {
    #[must_use]
    pub const fn kind(&self) -> OAuthRecordKind {
        match self {
            Self::Client { .. } => OAuthRecordKind::Client,
            Self::Grant { .. } => OAuthRecordKind::Grant,
            Self::Access { .. } => OAuthRecordKind::Access,
            Self::Refresh { .. } => OAuthRecordKind::Refresh,
        }
    }
    /// Unique within the record's kind.
    #[must_use]
    pub fn id(&self) -> &str {
        match self {
            Self::Client { id, .. } => id,
            Self::Grant { family, .. } => family,
            Self::Access { digest, .. } | Self::Refresh { digest, .. } => digest,
        }
    }
    /// The grant family a grant or token belongs to.
    #[must_use]
    pub fn family(&self) -> Option<&str> {
        match self {
            Self::Client { .. } => None,
            Self::Grant { family, .. }
            | Self::Access { family, .. }
            | Self::Refresh { family, .. } => Some(family),
        }
    }
}

#[derive(Clone, Debug)]
pub enum OAuthChange {
    /// Insert the record, or replace the one with the same kind and id.
    Put(Box<OAuthRecord>),
    Remove {
        kind: OAuthRecordKind,
        id: String,
    },
    /// Remove a grant and every token of its family.
    RevokeGrant {
        family: String,
    },
}

#[derive(Clone, Debug)]
pub struct OAuthStoreError(pub String);
impl std::fmt::Display for OAuthStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for OAuthStoreError {}

/// Durable storage for one authority's records.
///
/// The authority reads from memory. It calls [`load`](Self::load) once at
/// startup and [`record`](Self::record) for every later change.
pub trait OAuthStore: Send + Sync + 'static {
    /// Every stored record, reflecting all changes recorded before the call.
    fn load(&self) -> BoxFuture<'_, Result<Vec<OAuthRecord>, OAuthStoreError>>;
    /// Queue one change. Called while the authority holds its state lock, so
    /// this must return without blocking. Changes must be applied in call order.
    fn record(&self, change: OAuthChange);
}

/// What a startup restore did with the stored grants.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OAuthRestore {
    pub clients: usize,
    /// Grants whose login is still valid; their tokens work again.
    pub restored: usize,
    /// Grants whose login ended; removed from the store.
    pub dropped: usize,
    /// Grants whose login could not be checked; left stored for the next start.
    pub deferred: usize,
}

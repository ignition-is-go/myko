//! Stable application user IDs supplied by host authentication.
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A stable application-scoped profile identifier, never an authentication credential.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "codegen-ts", derive(ts_rs::TS))]
#[serde(transparent)]
pub struct UserId(pub String);

impl From<String> for UserId {
    fn from(value: String) -> Self {
        Self(value)
    }
}
impl From<&str> for UserId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}
impl From<Arc<str>> for UserId {
    fn from(value: Arc<str>) -> Self {
        Self(value.to_string())
    }
}
impl From<UserId> for Arc<str> {
    fn from(value: UserId) -> Self {
        value.0.into()
    }
}
impl AsRef<str> for UserId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Display for UserId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

//! Stable application user references supplied by a host authentication provider.
//! These IDs carry no grants and are distinct from permission principal IDs.
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A stable application-scoped profile identifier, never an authentication credential.
#[derive(
    Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize, crate::TS,
)]
#[ts(crate = "crate::ts_rs")]
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
impl crate::query::Filterable for UserId {
    type Filter = crate::query::EqFilter<Self>;
}

crate::register_typegen_type!(UserId);

#[cfg(test)]
mod tests {
    use super::UserId;
    fn accepts_filter<T: crate::query::Filterable>() {}
    #[test]
    fn user_reference_has_a_scalar_wire_shape_and_typed_filter() {
        let id = UserId::from("server-issued-user");
        let wire = serde_json::json!("server-issued-user");
        assert_eq!(serde_json::to_value(&id).ok(), Some(wire.clone()));
        assert_eq!(serde_json::from_value::<UserId>(wire).ok(), Some(id));
        accepts_filter::<UserId>();
    }
}

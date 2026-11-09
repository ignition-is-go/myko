//! Entity-wide restrictions on raw remote mutations, independent of app grants.
use crate::AuthError;
use std::{collections::HashSet, sync::OnceLock};

/// Raw clients cannot SET or DEL this entity. Trusted server events and commands
/// remain available; the application controls any explicitly exposed commands.
pub struct ServerOwnedEntityRegistration {
    pub entity_type: &'static str,
}
inventory::collect!(ServerOwnedEntityRegistration);

fn server_owned() -> &'static HashSet<&'static str> {
    static ENTITIES: OnceLock<HashSet<&'static str>> = OnceLock::new();
    ENTITIES.get_or_init(|| {
        inventory::iter::<ServerOwnedEntityRegistration>
            .into_iter()
            .map(|entry| entry.entity_type)
            .collect()
    })
}

/// Enforce the entity's raw-client mutation contract before application policy.
/// # Errors
/// Rejects raw mutations for server-owned entities regardless of caller grants.
pub fn authorize(entity_type: &str) -> Result<(), AuthError> {
    if server_owned().contains(entity_type) {
        Err(AuthError::Forbidden)
    } else {
        Ok(())
    }
}

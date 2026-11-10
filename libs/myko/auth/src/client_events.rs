//! Explicit per-operation opt-in for raw remote entity mutations.
use crate::{AuthError, EventChange};
use std::{collections::HashMap, sync::OnceLock};

/// Raw remote changes admitted by an entity's schema, subject to authorization.
pub struct ExternalEventRegistration {
    pub entity_type: &'static str,
    pub set: bool,
    pub delete: bool,
}
inventory::collect!(ExternalEventRegistration);

fn external_events() -> &'static HashMap<&'static str, (bool, bool)> {
    static ENTITIES: OnceLock<HashMap<&'static str, (bool, bool)>> = OnceLock::new();
    ENTITIES.get_or_init(|| {
        let mut entities = HashMap::<_, (bool, bool)>::new();
        for registration in inventory::iter::<ExternalEventRegistration> {
            let changes = entities.entry(registration.entity_type).or_default();
            changes.0 |= registration.set;
            changes.1 |= registration.delete;
        }
        entities
    })
}

/// Enforce per-operation opt-in before accepting raw remote entity events.
/// # Errors
/// Rejects events whose SET or DEL operation is not declared on the entity.
pub fn authorize(entity_type: &str, change: EventChange) -> Result<(), AuthError> {
    if external_events()
        .get(entity_type)
        .is_some_and(|(set, delete)| match change {
            EventChange::Set => *set,
            EventChange::Delete => *delete,
        })
    {
        Ok(())
    } else {
        Err(AuthError::Forbidden)
    }
}

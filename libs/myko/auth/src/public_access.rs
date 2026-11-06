//! Explicit anonymous operation and entity-event declarations.

use std::{
    collections::{HashMap, HashSet},
    sync::OnceLock,
};

use crate::{AuthError, AuthorizationRequest, EventChange, OperationKind};

pub struct PublicOperationRegistration {
    pub kind: OperationKind,
    pub operation: &'static str,
}
inventory::collect!(PublicOperationRegistration);

pub struct PublicEventRegistration {
    pub entity_type: &'static str,
    pub set: bool,
    pub delete: bool,
}
inventory::collect!(PublicEventRegistration);

#[derive(Default)]
struct Catalog {
    operations: HashMap<OperationKind, HashSet<&'static str>>,
    events: HashMap<&'static str, (bool, bool)>,
}

fn catalog() -> &'static Catalog {
    static CATALOG: OnceLock<Catalog> = OnceLock::new();
    CATALOG.get_or_init(|| {
        let mut catalog = Catalog::default();
        for registration in inventory::iter::<PublicOperationRegistration> {
            catalog
                .operations
                .entry(registration.kind)
                .or_default()
                .insert(registration.operation);
        }
        for registration in inventory::iter::<PublicEventRegistration> {
            let changes = catalog.events.entry(registration.entity_type).or_default();
            changes.0 |= registration.set;
            changes.1 |= registration.delete;
        }
        catalog
    })
}

#[must_use]
pub fn has_registrations() -> bool {
    !catalog().operations.is_empty() || !catalog().events.is_empty()
}

/// # Errors
/// Denies every operation or event not explicitly declared public.
pub fn authorize(request: AuthorizationRequest<'_>) -> Result<(), AuthError> {
    let allowed = match request {
        AuthorizationRequest::Discover { kind, operation }
        | AuthorizationRequest::Execute {
            kind, operation, ..
        } => catalog()
            .operations
            .get(&kind)
            .is_some_and(|operations| operations.contains(operation)),
        AuthorizationRequest::ClientEvent {
            entity_type,
            change,
            ..
        } => catalog()
            .events
            .get(entity_type)
            .is_some_and(|(set, delete)| match change {
                EventChange::Set => *set,
                EventChange::Delete => *delete,
            }),
        AuthorizationRequest::CommandResult { .. } => false,
    };
    if allowed {
        Ok(())
    } else {
        Err(AuthError::Unauthenticated)
    }
}

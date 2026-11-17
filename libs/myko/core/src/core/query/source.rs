//! Compatibility sources for evolving an entity's physical representation.
use super::{FilteredCellMap, QueryBuildContext};
use std::any::Any;

pub struct EntityQuerySourceRegistration {
    pub entity_type: &'static str,
    pub build: fn(&QueryBuildContext, &dyn Any) -> Option<FilteredCellMap>,
}
inventory::collect!(EntityQuerySourceRegistration);

#[doc(hidden)]
#[must_use]
pub fn has_entity_source(entity_type: &str) -> bool {
    inventory::iter::<EntityQuerySourceRegistration>
        .into_iter()
        .any(|source| source.entity_type == entity_type)
}

#[doc(hidden)]
#[must_use]
pub fn registered_entity_source(
    ctx: &QueryBuildContext,
    entity_type: &str,
    query: &dyn Any,
) -> Option<FilteredCellMap> {
    inventory::iter::<EntityQuerySourceRegistration>
        .into_iter()
        .filter(|source| source.entity_type == entity_type)
        .find_map(|source| (source.build)(ctx, query))
}

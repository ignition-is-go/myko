//! Read-only final-state overlays for application invariants and reversal policy.
use crate::{item::AnyItem, store::StoreRegistry};
use std::{collections::HashMap, sync::Arc};

pub struct ProposedState<'a> {
    pub server: Option<&'a super::MykoServerContext>,
    pub request: Option<Arc<crate::request::RequestContext>>,
    pub registry: &'a Arc<StoreRegistry>,
    pub host_id: crate::prelude::Uuid,
    pub upserts: &'a [Arc<dyn AnyItem>],
    pub deletes: &'a [Arc<dyn AnyItem>],
}

impl ProposedState<'_> {
    #[must_use]
    pub fn get(&self, kind: &str, id: &Arc<str>) -> Option<Arc<dyn AnyItem>> {
        self.upserts
            .iter()
            .find(|item| item.entity_type() == kind && &item.id() == id)
            .cloned()
            .or_else(|| {
                if self
                    .deletes
                    .iter()
                    .any(|item| item.entity_type() == kind && &item.id() == id)
                {
                    return None;
                }
                self.registry
                    .get(kind)
                    .and_then(|store| store.get_value(id))
            })
    }

    #[must_use]
    pub fn items(&self, kind: &str) -> Vec<Arc<dyn AnyItem>> {
        let mut items: HashMap<_, _> = self
            .registry
            .get(kind)
            .map_or_else(HashMap::new, |store| store.snapshot().into_iter().collect());
        for item in self
            .deletes
            .iter()
            .filter(|item| item.entity_type() == kind)
        {
            items.remove(&item.id());
        }
        for item in self
            .upserts
            .iter()
            .filter(|item| item.entity_type() == kind)
        {
            items.insert(item.id(), item.clone());
        }
        items.into_values().collect()
    }

    pub(crate) fn validate(&self, reversal: bool) -> Result<(), String> {
        for validator in inventory::iter::<MutationValidatorRegistration> {
            if (reversal && validator.validate_reversal_deletes && !self.deletes.is_empty())
                || validator.entity_types.iter().any(|kind| {
                    self.upserts
                        .iter()
                        .chain(self.deletes)
                        .any(|item| item.entity_type() == *kind)
                })
            {
                (validator.validate)(self, reversal)?;
            }
        }
        Ok(())
    }
}

/// Pure validators inspect the final overlay before authoritative publication.
/// Replay remains a decoding operation; it must preserve historical states.
pub struct MutationValidatorRegistration {
    pub validate_reversal_deletes: bool,
    pub entity_types: &'static [&'static str],
    pub validate: for<'a> fn(&ProposedState<'a>, bool) -> Result<(), String>,
}
inventory::collect!(MutationValidatorRegistration);

pub type MutationItems = Vec<Arc<dyn AnyItem>>;
pub struct MutationReplacement {
    pub upserts: MutationItems,
    pub deletes: MutationItems,
}

/// Canonicalize future authoritative writes during a representation migration.
/// Rewriters must be pure and idempotent. Replay and compensation bypass them.
pub struct MutationRewriterRegistration {
    pub entity_types: &'static [&'static str],
    pub rewrite: for<'a> fn(&ProposedState<'a>) -> Result<Option<MutationReplacement>, String>,
}
inventory::collect!(MutationRewriterRegistration);

impl ProposedState<'_> {
    pub(crate) fn rewrite(&self) -> Result<Option<MutationReplacement>, String> {
        for rewriter in inventory::iter::<MutationRewriterRegistration> {
            if rewriter.entity_types.iter().any(|kind| {
                self.upserts
                    .iter()
                    .chain(self.deletes)
                    .any(|item| item.entity_type() == *kind)
            }) && let Some(plan) = (rewriter.rewrite)(self)?
            {
                return Ok(Some(plan));
            }
        }
        Ok(None)
    }
}

pub fn requires_authority<'a>(items: impl Iterator<Item = &'a Arc<dyn AnyItem>>) -> bool {
    for item in items {
        if inventory::iter::<MutationValidatorRegistration>
            .into_iter()
            .any(|validator| validator.entity_types.contains(&item.entity_type()))
        {
            return true;
        }
        if inventory::iter::<MutationRewriterRegistration>
            .into_iter()
            .any(|rewriter| rewriter.entity_types.contains(&item.entity_type()))
        {
            return true;
        }
    }
    false
}

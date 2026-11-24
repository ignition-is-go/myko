//! Rows read back from a persisted event log.
//!
//! A backend reads an ordered stream of events and hands each one to the
//! context it serves. The context decides where each row lands: a shared row
//! in root's store, a partitioned row in the partition that holds it, or held
//! until one does. A server without partitions is the case with none.

use std::{collections::BTreeMap, sync::Arc};

use super::MykoServerContext;
use crate::{
    core::item::AnyItem,
    wire::{MEvent, MEventType},
};

type Row = (Arc<str>, Arc<dyn AnyItem>);

/// Rows a snapshot load gathers per type before writing them to a store.
const STORE_BATCH: usize = 512;

/// What a snapshot load did with the rows it was given.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadSummary {
    /// Rows placed in a store.
    pub loaded: usize,
    /// Rows this host wrote itself, which it already holds.
    pub own: usize,
    /// Rows that do not parse into the current shape of their type, or name
    /// a type this server does not know, by type. They stay in the log.
    pub unreadable: BTreeMap<Arc<str>, usize>,
    /// Rows no open partition takes yet, by type. They are kept, outside
    /// every partition, and land when one opens that takes them.
    pub held: BTreeMap<Arc<str>, usize>,
}

impl std::fmt::Display for LoadSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "loaded {} rows", self.loaded)?;
        let list = |map: &BTreeMap<Arc<str>, usize>| {
            map.iter()
                .map(|(entity_type, count)| format!("{entity_type}={count}"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        if !self.held.is_empty() {
            write!(f, "; held, waiting for a partition: {}", list(&self.held))?;
        }
        if !self.unreadable.is_empty() {
            write!(
                f,
                "; unreadable, left in the log: {}",
                list(&self.unreadable)
            )?;
        }
        Ok(())
    }
}

/// A snapshot being loaded into a context. Push every row, then
/// [`finish`](Self::finish).
pub struct LogLoad {
    ctx: MykoServerContext,
    host_id: String,
    batches: std::collections::HashMap<Arc<str>, Vec<Row>>,
    /// Rows a partition schema has a say in, applied after the shared rows.
    placed: Vec<Arc<dyn AnyItem>>,
    summary: LoadSummary,
}

impl LogLoad {
    pub(crate) fn new(ctx: &MykoServerContext) -> Self {
        Self {
            ctx: ctx.clone(),
            host_id: ctx.host_id.to_string(),
            batches: std::collections::HashMap::new(),
            placed: Vec::new(),
            summary: LoadSummary::default(),
        }
    }

    /// Take one row of the snapshot: the latest event of one item. Only a
    /// SET loads anything.
    pub fn push(&mut self, event: &MEvent) {
        if event.change_type != MEventType::SET {
            return;
        }
        if event.source_id.as_deref() == Some(self.host_id.as_str()) {
            self.summary.own = self.summary.own.saturating_add(1);
            return;
        }
        let Some(item) = self.ctx.parse_logged(event) else {
            let count = self
                .summary
                .unreadable
                .entry(Arc::clone(&event.item_type))
                .or_default();
            *count = count.saturating_add(1);
            return;
        };
        if self.ctx.places(item.entity_type()) {
            self.placed.push(item);
            return;
        }
        let entity_type: Arc<str> = Arc::from(item.entity_type());
        let entries = self.batches.entry(Arc::clone(&entity_type)).or_default();
        entries.push((item.id(), item));
        if entries.len() >= STORE_BATCH {
            let full = std::mem::take(entries);
            self.store(&entity_type, full);
        }
    }

    fn store(&mut self, entity_type: &str, entries: Vec<Row>) {
        self.summary.loaded = self.summary.loaded.saturating_add(entries.len());
        self.ctx
            .registry
            .get_or_create(entity_type)
            .insert_many(entries);
    }

    /// Land what is still gathered: the rest of the shared rows, then every
    /// row a partition schema places.
    ///
    /// # Errors
    ///
    /// Returns an error when a placed row is refused.
    pub fn finish(mut self) -> Result<LoadSummary, super::persister::PersistError> {
        for (entity_type, entries) in std::mem::take(&mut self.batches) {
            self.store(&entity_type, entries);
        }
        let placed = std::mem::take(&mut self.placed);
        if !placed.is_empty() {
            self.summary.loaded = self.summary.loaded.saturating_add(placed.len());
            self.ctx.place_logged(placed)?;
            let held = self.ctx.held_rows();
            self.summary.loaded = self
                .summary
                .loaded
                .saturating_sub(held.values().sum::<usize>());
            self.summary.held = held;
        }
        Ok(self.summary)
    }
}

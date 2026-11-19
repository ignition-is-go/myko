//! Disposable, bounded metadata projection. No entity payloads or inverse plans.
use super::{UndoConflict, UndoLogEntry, UndoLogPage};
use crate::{
    prelude::*,
    server::{EventRecord, HistoryEntityKey},
    wire::MEventType,
};
use hyphae::{Cell, CellImmutable, CellMutable, Gettable};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

pub const UNDO_LIST_CAPACITY: usize = 200;
pub const UNDO_LIST_KEYS_PER_TRANSACTION: usize = 256;
const USER_KEYS: usize = 8192;
const BLOCKERS: usize = 32;
const PENDING: usize = 1024;

#[myko_report_output]
pub struct UndoLogSnapshot {
    pub page: Option<UndoLogPage>,
    pub error: Option<String>,
}

/// Small committed-event description; payloads are discarded by the producer.
#[derive(Clone, Debug)]
pub struct UndoListChange {
    pub row_id: i64,
    pub transaction_id: Arc<str>,
    pub key: HistoryEntityKey,
    pub user_id: Option<Arc<str>>,
    pub command: Arc<str>,
    pub created_at: Arc<str>,
    pub undo: bool,
    pub receipt: Option<Arc<str>>,
    pub label: Option<String>,
    pub attributed: bool,
}
impl UndoListChange {
    #[must_use]
    pub fn from_record(row_id: i64, record: &EventRecord) -> Self {
        let p = record.provenance.as_ref();
        let tx = record.event.tx.clone();
        let original = record
            .event
            .item
            .get("transactionId")
            .and_then(serde_json::Value::as_str);
        let receipt = (record.event.item_type.as_ref() == "TransactionUndo"
            && record.event.change_type == MEventType::SET
            && original.is_some()
            && p.is_some_and(|p| p.undo_of_tx.as_deref() == original && p.request_tx == tx)
            && record
                .event
                .item
                .get("undoTransactionId")
                .and_then(serde_json::Value::as_str)
                == Some(tx.as_ref()))
        .then(|| original.map(Arc::from))
        .flatten();
        Self {
            row_id,
            transaction_id: tx.clone(),
            key: HistoryEntityKey::new(
                record.event.item_type.clone(),
                record
                    .event
                    .item
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown"),
            ),
            user_id: p
                .and_then(|p| p.actor_user_id.as_ref())
                .map(|id| Arc::from(id.as_ref())),
            command: p.map_or_else(|| Arc::from("Edit"), |p| p.command_id.clone()),
            created_at: record.event.created_at.clone(),
            undo: p.is_some_and(|p| p.undo_of_tx.is_some()),
            receipt,
            label: record
                .event
                .item
                .get("name")
                .and_then(serde_json::Value::as_str)
                .map(|name| name.chars().take(160).collect()),
            attributed: p.is_some_and(|p| p.request_tx == tx && p.actor_user_id.is_some()),
        }
    }
}

/// Bounded metadata loaded once for an actor; no before-images are loaded.
pub struct UndoListSeed {
    pub entry: UndoLogEntry,
    pub keys: HashMap<HistoryEntityKey, i64>,
    pub mixed: bool,
    pub blockers: Vec<UndoListChange>,
}
impl UndoListSeed {
    #[must_use]
    pub fn from_changes(first: i64, changes: &[UndoListChange], truncated: bool) -> Option<Self> {
        let mut accumulator = Accumulator::new(changes.first()?);
        for change in changes {
            accumulator.add(change);
        }
        accumulator.truncated |= truncated;
        let cached = accumulator.finish(first);
        Some(Self {
            entry: cached.entry,
            keys: cached.keys,
            mixed: cached.mixed,
            blockers: Vec::new(),
        })
    }
}
struct Cached {
    entry: UndoLogEntry,
    keys: HashMap<HistoryEntityKey, i64>,
    mixed: bool,
    blockers: HashMap<Arc<str>, UndoListChange>,
}
struct User {
    stream: Box<dyn Fn() -> Option<Cell<Arc<UndoLogSnapshot>, CellMutable>> + Send + Sync>,
    cutoff: u64,
    ready: bool,
    rows: BTreeMap<i64, Cached>,
    by_key: HashMap<HistoryEntityKey, Vec<i64>>,
    more: bool,
}
#[allow(clippy::struct_excessive_bools)]
struct Accumulator {
    first: UndoListChange,
    keys: HashMap<HistoryEntityKey, i64>,
    kinds: HashSet<Arc<str>>,
    mixed: bool,
    opted: bool,
    undo: bool,
    valid: bool,
    truncated: bool,
    receipts: HashSet<Arc<str>>,
    label: Option<String>,
}
impl Accumulator {
    fn new(first: &UndoListChange) -> Self {
        Self {
            first: first.clone(),
            keys: HashMap::new(),
            kinds: HashSet::new(),
            mixed: false,
            opted: false,
            undo: false,
            valid: true,
            truncated: false,
            receipts: HashSet::new(),
            label: None,
        }
    }
    fn add(&mut self, change: &UndoListChange) {
        self.valid &= change.attributed && change.user_id == self.first.user_id;
        self.opted |= crate::item::is_undoable_entity(&change.key.item_type);
        self.mixed |= !crate::item::is_undoable_entity(&change.key.item_type);
        self.undo |= change.undo;
        if self.kinds.len() < 32 {
            self.kinds.insert(change.key.item_type.clone());
        }
        if self.keys.contains_key(&change.key) || self.keys.len() < UNDO_LIST_KEYS_PER_TRANSACTION {
            self.keys.insert(change.key.clone(), change.row_id);
        } else {
            self.truncated = true;
        }
        if self.label.is_none() {
            self.label.clone_from(&change.label);
        }
        if let Some(original) = &change.receipt
            && self.receipts.len() < UNDO_LIST_KEYS_PER_TRANSACTION
        {
            self.receipts.insert(original.clone());
        }
    }
    fn finish(self, first_id: i64) -> Cached {
        let mut kinds: Vec<_> = self.kinds.into_iter().collect();
        kinds.sort();
        let affected_entities = self
            .keys
            .keys()
            .take(3)
            .map(|key| crate::graph::EntityRef::new(key.item_type.clone(), key.item_id.clone()))
            .collect();
        Cached {
            entry: UndoLogEntry {
                transaction_id: self.first.transaction_id,
                first_id,
                command: self.first.command,
                context: self.label,
                created_at: self.first.created_at,
                entity_types: kinds,
                entities_changed: self.keys.len(),
                blocked_reason: None,
                conflicts: Vec::new(),
                validation_deferred: true,
                details_truncated: self.truncated,
                affected_entities,
            },
            keys: self.keys,
            mixed: self.mixed,
            blockers: HashMap::new(),
        }
    }
}

/// Publish only after releasing the engine lock; subscribers must never run in it.
pub struct UndoListPublication {
    cell: Cell<Arc<UndoLogSnapshot>, CellMutable>,
    value: Arc<UndoLogSnapshot>,
}
impl UndoListPublication {
    pub fn publish(self) {
        self.cell.set(self.value);
    }
}

#[derive(Default)]
pub struct UndoListEngine {
    users: HashMap<Arc<str>, User>,
    pending: HashMap<Arc<str>, Accumulator>,
}
impl UndoListEngine {
    /// Returns a shared actor stream and whether it needs initial hydration.
    pub fn watch(&mut self, actor: Arc<str>) -> (Cell<Arc<UndoLogSnapshot>, CellImmutable>, bool) {
        if let Some(cell) = self.users.get(&actor).and_then(|user| (user.stream)()) {
            let retry = cell.get().error.is_some();
            return (cell.lock(), retry);
        }
        let cell = Cell::new(Arc::new(UndoLogSnapshot {
            page: None,
            error: None,
        }));
        let weak = cell.downgrade();
        self.users.insert(
            actor,
            User {
                stream: Box::new(move || weak.upgrade()),
                cutoff: 0,
                ready: false,
                rows: BTreeMap::new(),
                by_key: HashMap::new(),
                more: false,
            },
        );
        (cell.lock(), true)
    }
    pub fn hydrate(
        &mut self,
        actor: &str,
        cutoff: u64,
        seeds: Vec<UndoListSeed>,
        more: bool,
    ) -> Vec<UndoListPublication> {
        let Some(user) = self.users.get_mut(actor) else {
            return Vec::new();
        };
        user.cutoff = cutoff;
        user.ready = true;
        user.rows = seeds
            .into_iter()
            .map(|seed| {
                (
                    seed.entry.first_id,
                    Cached {
                        entry: seed.entry,
                        keys: seed.keys,
                        mixed: seed.mixed,
                        blockers: seed
                            .blockers
                            .into_iter()
                            .map(|change| (change.transaction_id.clone(), change))
                            .take(BLOCKERS)
                            .collect(),
                    },
                )
            })
            .collect();
        user.more = more;
        Self::index(user);
        Self::publication(user, actor).into_iter().collect()
    }
    #[must_use]
    pub fn fail(&mut self, actor: &str, error: String) -> Vec<UndoListPublication> {
        if let Some(user) = self.users.get_mut(actor) {
            user.ready = false;
        }
        self.users
            .get(actor)
            .and_then(|user| (user.stream)())
            .map(|cell| UndoListPublication {
                cell,
                value: Arc::new(UndoLogSnapshot {
                    page: None,
                    error: Some(error),
                }),
            })
            .into_iter()
            .collect()
    }
    /// Consume already committed event metadata. No database work occurs here.
    pub fn records(
        &mut self,
        sequence: u64,
        changes: &[UndoListChange],
    ) -> Vec<UndoListPublication> {
        self.users.retain(|_, user| (user.stream)().is_some());
        let mut dirty = HashSet::new();
        for change in changes {
            if change.user_id.is_some() {
                if !self.pending.contains_key(&change.transaction_id)
                    && self.pending.len() >= PENDING
                    && let Some(oldest) = self
                        .pending
                        .iter()
                        .min_by_key(|(_, value)| value.first.row_id)
                        .map(|(tx, _)| tx.clone())
                {
                    self.pending.remove(&oldest);
                }
                self.pending
                    .entry(change.transaction_id.clone())
                    .or_insert_with(|| Accumulator::new(change))
                    .add(change);
            }
            for (actor, user) in &mut self.users {
                if sequence <= user.cutoff {
                    continue;
                }
                if let Some(affected) = user.by_key.get(&change.key) {
                    for first in affected {
                        let Some(row) = user.rows.get_mut(first) else {
                            continue;
                        };
                        if row.entry.transaction_id == change.transaction_id {
                            continue;
                        }
                        if row
                            .keys
                            .get(&change.key)
                            .is_some_and(|last| change.row_id > *last)
                        {
                            if row.blockers.contains_key(&change.transaction_id)
                                || row.blockers.len() < BLOCKERS
                            {
                                row.blockers
                                    .insert(change.transaction_id.clone(), change.clone());
                            } else {
                                row.entry.details_truncated = true;
                            }
                            if change.user_id.as_deref() != Some(actor.as_ref()) {
                                dirty.insert(actor.clone());
                            }
                        }
                    }
                }
            }
        }
        dirty
            .into_iter()
            .filter_map(|actor| {
                self.users
                    .get(&actor)
                    .and_then(|user| Self::publication(user, &actor))
            })
            .collect()
    }
    pub fn complete(
        &mut self,
        sequence: u64,
        tx: &str,
        actor: &str,
        first: i64,
    ) -> (Vec<UndoListPublication>, Vec<Arc<str>>) {
        let Some(pending) = self.pending.remove(tx) else {
            return (
                Vec::new(),
                self.users
                    .contains_key(actor)
                    .then(|| Arc::from(actor))
                    .into_iter()
                    .collect(),
            );
        };
        if !pending.valid || pending.first.user_id.as_deref() != Some(actor) {
            return (Vec::new(), Vec::new());
        }
        let originals = pending.receipts.clone();
        let insert = pending.opted && !pending.undo;
        let cached = insert.then(|| pending.finish(first));
        let mut refill = Vec::new();
        let mut dirty = HashSet::new();
        for (user_id, user) in &mut self.users {
            if sequence <= user.cutoff {
                continue;
            }
            let before = user.rows.len();
            user.rows
                .retain(|_, row| !originals.contains(&row.entry.transaction_id));
            for row in user.rows.values_mut() {
                let n = row.blockers.len();
                row.blockers.retain(|blocker, _| {
                    (originals.is_empty() || blocker.as_ref() != tx) && !originals.contains(blocker)
                });
                if row.blockers.len() != n {
                    dirty.insert(user_id.clone());
                }
            }
            if user.rows.len() != before {
                dirty.insert(user_id.clone());
                if user.more {
                    refill.push(user_id.clone());
                }
            }
        }
        if let Some(row) = cached
            && let Some(user) = self.users.get_mut(actor)
            && sequence > user.cutoff
        {
            user.rows
                .retain(|_, old| old.entry.transaction_id.as_ref() != tx);
            user.rows.insert(first, row);
            Self::index(user);
            dirty.insert(Arc::from(actor));
        }
        let publications = dirty
            .into_iter()
            .filter_map(|actor| {
                self.users.get_mut(&actor).and_then(|user| {
                    Self::index(user);
                    Self::publication(user, &actor)
                })
            })
            .collect();
        (publications, refill)
    }
    fn index(user: &mut User) {
        while user.rows.len() > UNDO_LIST_CAPACITY {
            user.rows.pop_first();
            user.more = true;
        }
        let mut keys: usize = user.rows.values().map(|row| row.keys.len()).sum();
        for row in user.rows.values_mut() {
            if keys <= USER_KEYS {
                break;
            }
            keys = keys.saturating_sub(row.keys.len());
            row.keys.clear();
            row.entry.details_truncated = true;
        }
        user.by_key.clear();
        for (first, row) in &user.rows {
            for key in row.keys.keys() {
                user.by_key.entry(key.clone()).or_default().push(*first);
            }
        }
    }
    fn publication(user: &User, actor: &str) -> Option<UndoListPublication> {
        if !user.ready {
            return None;
        }
        let cell = (user.stream)()?;
        let mut selected = HashSet::new();
        let mut blockers: HashMap<Arc<str>, UndoListChange> = HashMap::new();
        let mut mixed = false;
        let mut entries = Vec::new();
        for row in user.rows.values().rev() {
            selected.insert(row.entry.transaction_id.clone());
            blockers.remove(&row.entry.transaction_id);
            for (tx, change) in &row.blockers {
                if !selected.contains(tx) {
                    blockers.insert(tx.clone(), change.clone());
                }
            }
            mixed |= row.mixed;
            let mut entry = row.entry.clone();
            entry.blocked_reason = if mixed {
                Some("This range includes entities that are not opted into undo.".into())
            } else if blockers.is_empty() {
                None
            } else {
                Some("This range includes entities changed by a later transaction.".into())
            };
            entry.conflicts = blockers
                .values()
                .take(BLOCKERS)
                .map(|change| UndoConflict {
                    transaction_id: change.transaction_id.clone(),
                    user_id: change.user_id.clone(),
                    is_other_user: change.user_id.as_deref().is_some_and(|id| id != actor),
                    entity_type: change.key.item_type.clone(),
                    entity_id: change.key.item_id.clone(),
                    created_at: change.created_at.clone(),
                })
                .collect();
            entries.push(entry);
        }
        let next_before = user
            .more
            .then(|| user.rows.first_key_value().map(|(first, _)| *first))
            .flatten();
        Some(UndoListPublication {
            cell,
            value: Arc::new(UndoLogSnapshot {
                page: Some(UndoLogPage {
                    entries,
                    next_before,
                }),
                error: None,
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyphae::Gettable;
    inventory::submit! { crate::item::UndoableRegistration { entity_type: "UndoListTest" } }
    fn change(id: i64, tx: &str, actor: &str, key: &str) -> UndoListChange {
        UndoListChange {
            row_id: id,
            transaction_id: tx.into(),
            key: HistoryEntityKey::new("UndoListTest", key),
            user_id: Some(actor.into()),
            command: "Edit".into(),
            created_at: "now".into(),
            undo: false,
            receipt: None,
            label: None,
            attributed: true,
        }
    }
    fn publish(values: Vec<UndoListPublication>) {
        for value in values {
            value.publish();
        }
    }
    fn commit(engine: &mut UndoListEngine, change: UndoListChange) {
        publish(engine.records(
            u64::try_from(change.row_id).unwrap(),
            std::slice::from_ref(&change),
        ));
        let (updates, _) = engine.complete(
            u64::try_from(change.row_id).unwrap(),
            &change.transaction_id,
            change.user_id.as_deref().unwrap(),
            change.row_id,
        );
        publish(updates);
    }
    #[test]
    fn bounded_completed_stack_and_shared_user_stream() {
        let mut engine = UndoListEngine::default();
        let (alice, hydrate) = engine.watch("alice".into());
        assert!(hydrate);
        let (other, hydrate) = engine.watch("alice".into());
        assert!(!hydrate);
        publish(engine.hydrate("alice", 0, vec![], false));
        for id in 1..=205 {
            commit(
                &mut engine,
                change(id, &id.to_string(), "alice", &id.to_string()),
            );
        }
        assert_eq!(alice.get().page.as_ref().unwrap().entries.len(), 200);
        assert_eq!(
            alice.get().page.as_ref().unwrap().entries[0]
                .transaction_id
                .as_ref(),
            "205"
        );
        assert_eq!(alice.get(), other.get());
        let row = change(206, "in-flight", "alice", "a");
        publish(engine.records(206, &[row]));
        assert_eq!(
            alice.get().page.as_ref().unwrap().entries[0]
                .transaction_id
                .as_ref(),
            "205"
        );
    }
    #[test]
    fn interleaved_foreign_transaction_survives_completion_and_prefix_selection() {
        let mut engine = UndoListEngine::default();
        let (alice, _) = engine.watch("alice".into());
        publish(engine.hydrate("alice", 0, vec![], false));
        commit(&mut engine, change(1, "old", "alice", "a"));
        commit(&mut engine, change(2, "bob", "bob", "a"));
        commit(&mut engine, change(3, "new", "alice", "a"));
        let value = alice.get();
        let entries = &value.page.as_ref().unwrap().entries;
        assert!(entries[0].blocked_reason.is_none());
        assert!(
            entries[1]
                .conflicts
                .iter()
                .any(|conflict| conflict.transaction_id.as_ref() == "bob")
        );
        assert!(
            !entries[1]
                .conflicts
                .iter()
                .any(|conflict| conflict.transaction_id.as_ref() == "new")
        );
        let mut undo = change(4, "undo-bob", "bob", "a");
        undo.undo = true;
        undo.receipt = Some("bob".into());
        commit(&mut engine, undo);
        assert!(
            alice.get().page.as_ref().unwrap().entries[1]
                .blocked_reason
                .is_none()
        );
    }
    #[test]
    fn failed_hydration_cannot_publish_a_partial_stack_as_ready() {
        let mut engine = UndoListEngine::default();
        let (alice, _) = engine.watch("alice".into());
        publish(engine.fail("alice", "offline".into()));
        commit(&mut engine, change(1, "new", "alice", "a"));
        assert!(alice.get().page.is_none());
        assert_eq!(alice.get().error.as_deref(), Some("offline"));
        let seed = UndoListSeed::from_changes(1, &[change(1, "new", "alice", "a")], false).unwrap();
        publish(engine.hydrate("alice", 1, vec![seed], false));
        assert!(alice.get().error.is_none());
        assert_eq!(alice.get().page.as_ref().unwrap().entries.len(), 1);
    }
    #[test]
    fn excluded_activity_is_silent_and_large_transactions_defer_details() {
        let mut engine = UndoListEngine::default();
        let (alice, _) = engine.watch("alice".into());
        publish(engine.hydrate("alice", 0, vec![], false));
        let mut excluded = change(1, "presence", "alice", "a");
        excluded.key.item_type = "Presence".into();
        commit(&mut engine, excluded);
        assert!(alice.get().page.as_ref().unwrap().entries.is_empty());
        let changes: Vec<_> = (2..=302)
            .map(|id| change(id, "large", "alice", &id.to_string()))
            .collect();
        publish(engine.records(303, &changes));
        let (updates, _) = engine.complete(304, "large", "alice", 2);
        publish(updates);
        let value = alice.get();
        let entry = &value.page.as_ref().unwrap().entries[0];
        assert!(entry.validation_deferred && entry.details_truncated);
        assert_eq!(entry.entities_changed, UNDO_LIST_KEYS_PER_TRANSACTION);
    }
    #[test]
    fn hydration_cutoff_and_unrelated_writes_do_not_republish_or_resurrect() {
        let mut engine = UndoListEngine::default();
        let (alice, _) = engine.watch("alice".into());
        let row = change(1, "old", "alice", "a");
        publish(engine.records(1, std::slice::from_ref(&row)));
        publish(engine.hydrate("alice", 2, vec![], false));
        let (updates, _) = engine.complete(2, "old", "alice", 1);
        assert!(updates.is_empty());
        assert!(
            engine
                .records(3, &[change(3, "foreign", "bob", "unrelated")])
                .is_empty()
        );
        assert!(alice.get().page.as_ref().unwrap().entries.is_empty());
        // A command spanning the initial snapshot must still appear at completion.
        let row = change(4, "spanning", "alice", "a");
        publish(engine.records(4, &[row]));
        publish(engine.hydrate("alice", 4, vec![], false));
        let (updates, _) = engine.complete(5, "spanning", "alice", 4);
        publish(updates);
        assert_eq!(alice.get().page.as_ref().unwrap().entries.len(), 1);
    }
}

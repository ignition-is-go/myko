//! Disposable undo metadata, fed by successful producer commits.
use super::{PostgresConfig, connect_pg_client, qi};
use myko::{
    entities::undo::{
        UNDO_LIST_CAPACITY, UNDO_LIST_KEYS_PER_TRANSACTION, UndoListChange, UndoListEngine,
        UndoListSeed, UndoLogSnapshot,
    },
    hyphae::{Cell, CellImmutable},
    server::HistoryEntityKey,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, OnceLock, Weak},
};

enum Job {
    Records(u64, Vec<UndoListChange>),
    Complete(u64, String, String, i64),
    Hydrate(String),
}
pub(super) struct Source {
    pub gate: Mutex<u64>,
    engine: Mutex<UndoListEngine>,
    sender: flume::Sender<Job>,
}
impl Source {
    pub fn shared(config: &PostgresConfig) -> Arc<Self> {
        static SOURCES: OnceLock<Mutex<HashMap<String, Weak<Source>>>> = OnceLock::new();
        let mut sources = SOURCES
            .get_or_init(Mutex::default)
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        sources.retain(|_, source| source.strong_count() > 0);
        let key = format!("{}:{}", config.url, config.table);
        if let Some(source) = sources.get(&key).and_then(Weak::upgrade) {
            return source;
        }
        let (sender, receiver) = flume::unbounded();
        let source = Arc::new(Self {
            gate: Mutex::new(0),
            engine: Mutex::default(),
            sender,
        });
        sources.insert(key, Arc::downgrade(&source));
        drop(sources);
        let weak = Arc::downgrade(&source);
        let config = config.clone();
        std::thread::spawn(move || {
            let mut failed = HashSet::new();
            while let Ok(job) = receiver.recv() {
                let Some(source) = weak.upgrade() else {
                    break;
                };
                let committed = matches!(&job, Job::Records(..) | Job::Complete(..));
                let publications = match job {
                    Job::Records(sequence, changes) => source
                        .engine
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .records(sequence, &changes),
                    Job::Complete(sequence, tx, actor, first) => {
                        let (publications, refill) = source
                            .engine
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .complete(sequence, &tx, &actor, first);
                        for actor in refill {
                            let _ = source.sender.send(Job::Hydrate(actor.to_string()));
                        }
                        publications
                    }
                    Job::Hydrate(actor) => match hydrate(&source, &config, &actor) {
                        Ok((cutoff, seeds, more)) => {
                            failed.remove(&actor);
                            source
                                .engine
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .hydrate(&actor, cutoff, seeds, more)
                        }
                        Err(error) => {
                            failed.insert(actor.clone());
                            source
                                .engine
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .fail(&actor, error)
                        }
                    },
                };
                for publication in publications {
                    publication.publish();
                }
                // A new successful commit is a recovery signal after an outage.
                // Healthy streams never schedule another hydration here.
                if committed {
                    for actor in failed.drain() {
                        let _ = source.sender.send(Job::Hydrate(actor));
                    }
                }
            }
        });
        source
    }
    pub fn watch(&self, actor: &str) -> Cell<Arc<UndoLogSnapshot>, CellImmutable> {
        let (cell, hydrate) = self
            .engine
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .watch(Arc::from(actor));
        if hydrate {
            let _ = self.sender.send(Job::Hydrate(actor.to_owned()));
        }
        cell
    }
    pub fn records(&self, sequence: &mut u64, changes: Vec<UndoListChange>) {
        if changes.is_empty() {
            return;
        }
        *sequence = sequence.saturating_add(1);
        let _ = self.sender.send(Job::Records(*sequence, changes));
    }
    pub fn complete(&self, sequence: &mut u64, tx: &str, actor: &str, first: i64) {
        *sequence = sequence.saturating_add(1);
        let _ = self.sender.send(Job::Complete(
            *sequence,
            tx.to_owned(),
            actor.to_owned(),
            first,
        ));
    }
}

// Project only small metadata columns; never transfer historical entity bodies.
fn columns(alias: &str) -> String {
    format!(
        "{alias}.id, {alias}.tx, {alias}.item_type, {alias}.item_id, COALESCE({alias}.event->>'createdAt',{alias}.created_at::text),
        {alias}.event->'provenance'->>'actorUserId', {alias}.event->'provenance'->>'commandId',
        {alias}.event->'provenance'->>'requestTx', {alias}.event->'provenance'->>'undoOfTx',
        LEFT({alias}.event->'item'->>'name',160)"
    )
}
fn change(row: &postgres::Row) -> UndoListChange {
    let tx: String = row.get(1);
    let actor: Option<String> = row.get(5);
    let request: Option<String> = row.get(7);
    UndoListChange {
        row_id: row.get(0),
        transaction_id: Arc::from(tx.as_str()),
        key: HistoryEntityKey::new(row.get::<_, String>(2), row.get::<_, String>(3)),
        created_at: Arc::from(row.get::<_, String>(4)),
        attributed: actor.is_some() && request.as_deref() == Some(tx.as_str()),
        user_id: actor.map(Arc::from),
        command: Arc::from(
            row.get::<_, Option<String>>(6)
                .unwrap_or_else(|| "Edit".into()),
        ),
        undo: row.get::<_, Option<String>>(8).is_some(),
        receipt: None,
        label: row.get(9),
        preview: None,
    }
}
fn compensated(table: &str, completions: &str, transaction: &str) -> String {
    format!(
        "EXISTS (SELECT 1 FROM {table} u JOIN {completions} f ON f.tx = u.tx
        WHERE u.item_type='TransactionUndo' AND u.change_type='SET'
        AND u.event->'provenance'->>'undoOfTx'=u.event->'item'->>'transactionId'
        AND u.event->'provenance'->>'requestTx'=u.tx
        AND u.event->'item'->>'undoTransactionId'=u.tx
        AND u.event->'provenance'->>'actorUserId'=f.user_id
        AND f.last_id=(SELECT MAX(id) FROM {table} WHERE tx=f.tx)
        AND (u.event->'item'->>'transactionId'={transaction} OR u.tx={transaction}))"
    )
}
#[allow(clippy::too_many_lines)]
fn hydrate(
    source: &Source,
    config: &PostgresConfig,
    actor: &str,
) -> Result<(u64, Vec<UndoListSeed>, bool), String> {
    let mut client = connect_pg_client(config, "undo metadata hydration")?;
    let table = qi(&config.table);
    let completions = qi(&format!("{}_transactions", config.table));
    let gate = source
        .gate
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut snapshot = client
        .build_transaction()
        .isolation_level(postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .map_err(|e| e.to_string())?;
    snapshot
        .query_one(&format!("SELECT MAX(id) FROM {table}"), &[])
        .map_err(|e| e.to_string())?;
    let cutoff = *gate;
    drop(gate);
    let opted: Vec<String> = inventory::iter::<myko::item::UndoableRegistration>
        .into_iter()
        .map(|entry| entry.entity_type.to_string())
        .collect();
    let undone = compensated(&table, &completions, "c.tx");
    let candidates = snapshot.query(&format!("SELECT c.tx,c.first_id, EXISTS(SELECT 1 FROM {table} e WHERE e.tx=c.tx AND NOT(e.item_type=ANY($2))), c.last_id FROM {completions} c
        WHERE c.user_id=$1 AND c.last_id=(SELECT MAX(id) FROM {table} WHERE tx=c.tx)
        AND c.first_id>COALESCE((SELECT MAX(id) FROM {table} WHERE {}),0)
        AND EXISTS (SELECT 1 FROM {table} e WHERE e.tx=c.tx AND e.item_type=ANY($2))
        AND NOT EXISTS (SELECT 1 FROM {table} e WHERE e.tx=c.tx AND e.event->'provenance'->>'undoOfTx' IS NOT NULL)
        AND NOT {undone} ORDER BY c.first_id DESC LIMIT 201",super::transactions::boundary_filter()), &[&actor,&opted]).map_err(|e|e.to_string())?;
    let more = candidates.len() > UNDO_LIST_CAPACITY;
    let candidates: Vec<_> = candidates.into_iter().take(UNDO_LIST_CAPACITY).collect();
    let transactions: Vec<String> = candidates.iter().map(|row| row.get(0)).collect();
    let metadata = snapshot
        .query(
            &format!(
                "SELECT {} FROM unnest($1::text[]) c(tx)
        CROSS JOIN LATERAL (SELECT * FROM {table} WHERE tx=c.tx ORDER BY id LIMIT 257) e ORDER BY e.id",
                columns("e")
            ),
            &[&transactions],
        )
        .map_err(|e| e.to_string())?;
    let mut changes: HashMap<Arc<str>, Vec<UndoListChange>> = HashMap::new();
    for row in &metadata {
        let change = change(row);
        changes
            .entry(change.transaction_id.clone())
            .or_default()
            .push(change);
    }
    let mut seeds = Vec::new();
    let mut budget = 8192_usize;
    for candidate in candidates {
        let tx: String = candidate.get(0);
        let Some(mut changes) = changes.remove(tx.as_str()) else {
            continue;
        };
        let truncated = changes.len() > UNDO_LIST_KEYS_PER_TRANSACTION;
        changes.truncate(UNDO_LIST_KEYS_PER_TRANSACTION);
        let Some(mut seed) = UndoListSeed::from_changes(candidate.get(1), &changes, truncated)
        else {
            continue;
        };
        seed.mixed = candidate.get(2);
        if truncated {
            // Sampling must not invent a hard conflict with a writer that the
            // same original transaction subsequently overwrote.
            for last in seed.keys.values_mut() {
                *last = candidate.get(3);
            }
        }
        if seed.keys.len() > budget {
            seed.keys.clear();
            seed.entry.details_truncated = true;
        }
        budget = budget.saturating_sub(seed.keys.len());
        seeds.push(seed);
    }
    let mut owners = Vec::new();
    let mut types = Vec::new();
    let mut ids = Vec::new();
    let mut last = Vec::new();
    for seed in &seeds {
        for (key, revision) in &seed.keys {
            owners.push(seed.entry.first_id);
            types.push(key.item_type.to_string());
            ids.push(key.item_id.to_string());
            last.push(*revision);
        }
    }
    let firsts: Vec<_> = seeds.iter().map(|seed| seed.entry.first_id).collect();
    let txs: Vec<_> = seeds
        .iter()
        .map(|seed| seed.entry.transaction_id.to_string())
        .collect();
    let compensated = compensated(&table, &completions, "e.tx");
    let blockers = snapshot
        .query(
            &format!(
                "SELECT {}, c.owner FROM unnest($5::bigint[],$6::text[]) c(owner,tx)
        CROSS JOIN LATERAL (SELECT DISTINCT ON (e.tx) e.*
            FROM unnest($1::bigint[],$2::text[],$3::text[],$4::bigint[]) k(owner,kind,item,last)
            JOIN {table} e ON e.item_type=k.kind AND e.item_id=k.item AND e.id>k.last
            WHERE k.owner=c.owner AND e.tx<>c.tx AND NOT {compensated}
            ORDER BY e.tx,e.id DESC LIMIT 33) e",
                columns("e")
            ),
            &[&owners, &types, &ids, &last, &firsts, &txs],
        )
        .map_err(|e| e.to_string())?;
    let mut by_first: HashMap<_, _> = seeds
        .iter_mut()
        .map(|seed| (seed.entry.first_id, seed))
        .collect();
    for row in &blockers {
        if let Some(seed) = by_first.get_mut(&row.get::<_, i64>(10)) {
            if seed.blockers.len() < 32 {
                seed.blockers.push(change(row));
            } else {
                seed.entry.details_truncated = true;
            }
        }
    }
    hydrate_diffs(&mut snapshot, &table, &mut seeds)?;
    snapshot.commit().map_err(|e| e.to_string())?;
    Ok((cutoff, seeds, more))
}

fn hydrate_diffs(
    snapshot: &mut postgres::Transaction<'_>,
    table: &str,
    seeds: &mut [UndoListSeed],
) -> Result<(), String> {
    use myko::entities::undo::{UNDO_DIFF_ENTITIES, UndoPreviewAccumulator, UndoPreviewInput};
    let mut txs = Vec::new();
    let mut types = Vec::new();
    let mut ids = Vec::new();
    let mut accumulators = HashMap::new();
    for seed in seeds.iter_mut() {
        let mut keys: Vec<_> = seed
            .keys
            .keys()
            .filter(|key| myko::item::is_undoable_entity(&key.item_type))
            .collect();
        keys.sort();
        seed.entry.diffs_complete =
            !seed.mixed && !seed.entry.details_truncated && keys.len() <= UNDO_DIFF_ENTITIES;
        for key in keys.into_iter().take(UNDO_DIFF_ENTITIES) {
            txs.push(seed.entry.transaction_id.to_string());
            types.push(key.item_type.to_string());
            ids.push(key.item_id.to_string());
        }
        accumulators.insert(
            seed.entry.transaction_id.clone(),
            UndoPreviewAccumulator::default(),
        );
    }
    let rows=snapshot.query(&format!("WITH states AS MATERIALIZED (
        SELECT k.tx,k.kind,k.item,
            COALESCE(previous.change_type='SET',false) bp, final.change_type='SET' ap,
            CASE WHEN previous.change_type='SET' THEN previous.event->'item' END b,
            CASE WHEN final.change_type='SET' THEN final.event->'item' END a
        FROM unnest($1::text[],$2::text[],$3::text[]) k(tx,kind,item)
        CROSS JOIN LATERAL (SELECT MIN(id) first_id,MAX(id) last_id FROM {table} WHERE tx=k.tx AND item_type=k.kind AND item_id=k.item) bounds
        JOIN {table} final ON final.id=bounds.last_id
        LEFT JOIN LATERAL (SELECT change_type,event FROM {table} WHERE item_type=k.kind AND item_id=k.item AND id<bounds.first_id ORDER BY id DESC LIMIT 1) previous ON true
    ), sized AS MATERIALIZED (
        SELECT *, COALESCE(octet_length(b::text),0) bl,COALESCE(octet_length(a::text),0) al FROM states
    ), budgeted AS (
        SELECT *, SUM(CASE WHEN bl<=16384 AND al<=16384 THEN bl+al ELSE 0 END) OVER(PARTITION BY tx ORDER BY kind,item) total FROM sized
    ) SELECT tx,kind,item,bp,ap,
        CASE WHEN bl<=16384 AND al<=16384 AND total<=65536 THEN b::text END,
        CASE WHEN bl<=16384 AND al<=16384 AND total<=65536 THEN a::text END,
        bl<=16384 AND al<=16384 AND total<=65536 FROM budgeted"), &[&txs,&types,&ids]).map_err(|error|error.to_string())?;
    for row in rows {
        let tx: String = row.get(0);
        let key = HistoryEntityKey::new(row.get::<_, String>(1), row.get::<_, String>(2));
        let Some(accumulator) = accumulators.get_mut(tx.as_str()) else {
            continue;
        };
        if !row.get::<_, bool>(7) {
            accumulator.record(&key, None);
            continue;
        }
        let before = row
            .get::<_, Option<String>>(5)
            .map(|text| serde_json::from_str(&text))
            .transpose()
            .map_err(|error| error.to_string())?;
        let after = row
            .get::<_, Option<String>>(6)
            .map(|text| serde_json::from_str(&text))
            .transpose()
            .map_err(|error| error.to_string())?;
        let input = UndoPreviewInput::capture(&key, before.as_ref(), after.as_ref());
        accumulator.record(&key, Some(&input));
    }
    for seed in seeds {
        if let Some(accumulator) = accumulators.remove(&seed.entry.transaction_id) {
            let (diffs, complete) = accumulator.finish();
            seed.entry.entity_diffs = Arc::new(diffs);
            seed.entry.diffs_complete &= complete;
        }
    }
    Ok(())
}

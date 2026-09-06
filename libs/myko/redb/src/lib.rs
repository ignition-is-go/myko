//! Durable embedded event journal for transport-neutral Myko 7 nodes.
//!
//! The database stores only immutable node-local history and stable node
//! identity. Myko rebuilds command and graph projections from that history on
//! startup, keeping storage layout out of the federation protocol.

#![forbid(unsafe_code)]

use std::{fmt, path::Path, sync::Arc};

use myko_federation::{
    EventEnvelope, EventJournal, Node, NodeError, NodeId, ReplicationCheckpoint,
    ReplicationCursorKey, ReplicationCursorStore, StorageIncarnationId,
};
use rayon::prelude::*;
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("myko_meta");
const EVENTS: TableDefinition<u64, &[u8]> = TableDefinition::new("myko_events");
const ORIGINS: TableDefinition<&[u8], u64> = TableDefinition::new("myko_event_origins");
const REPLICATION_CHECKPOINTS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("myko_replication_checkpoints");
const NODE_ID_KEY: &str = "node_id";
const STORAGE_INCARNATION_KEY: &str = "storage_incarnation";

/// A crash-safe Redb implementation of Myko's immutable event journal.
pub struct RedbJournal {
    database: Arc<Database>,
    node_id: NodeId,
    storage_incarnation: StorageIncarnationId,
}

impl fmt::Debug for RedbJournal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedbJournal")
            .field("node_id", &self.node_id)
            .field("storage_incarnation", &self.storage_incarnation)
            .finish_non_exhaustive()
    }
}

impl RedbJournal {
    /// Opens or creates a journal, preserving its stable node identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the database cannot be opened, initialized, or read.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, NodeError> {
        let path = path.as_ref();
        let started = std::time::Instant::now();
        let database = Arc::new(Database::create(path).map_err(backend_error)?);
        tracing::debug!(
            path = %path.display(),
            elapsed_ms = started.elapsed().as_millis(),
            "redb database opened"
        );
        let initialized = std::time::Instant::now();
        let (node_id, storage_incarnation) = initialize(&database)?;
        tracing::debug!(
            path = %path.display(),
            elapsed_ms = initialized.elapsed().as_millis(),
            "redb journal metadata initialized"
        );
        Ok(Self {
            database,
            node_id,
            storage_incarnation,
        })
    }

    /// Opens a complete event-sourced Myko node over this journal.
    ///
    /// # Errors
    ///
    /// Returns an error when the journal cannot be opened or replayed.
    pub fn open_node(path: impl AsRef<Path>) -> Result<Node, NodeError> {
        Self::open_node_with_journal(path).map(|(node, _journal)| node)
    }

    /// Opens a node while retaining a shared handle for local metadata such as
    /// durable, source-aware replication checkpoints.
    ///
    /// # Errors
    ///
    /// Returns an error when the journal cannot be opened or replayed.
    pub fn open_node_with_journal(path: impl AsRef<Path>) -> Result<(Node, Arc<Self>), NodeError> {
        let journal = Arc::new(Self::open(path)?);
        let node = Node::from_journal(journal.clone())?;
        Ok((node, journal))
    }
}

impl EventJournal for RedbJournal {
    fn node_id(&self) -> Result<NodeId, NodeError> {
        Ok(self.node_id)
    }

    fn storage_incarnation(&self) -> Result<StorageIncarnationId, NodeError> {
        Ok(self.storage_incarnation)
    }

    fn replay(&self) -> Result<Vec<EventEnvelope>, NodeError> {
        let started = std::time::Instant::now();
        let read = self.database.begin_read().map_err(backend_error)?;
        let table = read.open_table(EVENTS).map_err(backend_error)?;
        let mut encoded_events = Vec::new();
        let mut encoded_bytes = 0_u64;
        for entry in table.iter().map_err(backend_error)? {
            let (_, encoded) = entry.map_err(backend_error)?;
            encoded_bytes = encoded_bytes
                .saturating_add(u64::try_from(encoded.value().len()).unwrap_or(u64::MAX));
            encoded_events.push(encoded.value().to_vec());
        }
        let read_elapsed = started.elapsed();
        let decode_started = std::time::Instant::now();
        let events = encoded_events
            .par_iter()
            .map(|encoded| serde_json::from_slice(encoded).map_err(backend_error))
            .collect::<Result<Vec<EventEnvelope>, NodeError>>()?;
        tracing::debug!(
            events = events.len(),
            encoded_bytes,
            read_ms = read_elapsed.as_millis(),
            decode_ms = decode_started.elapsed().as_millis(),
            elapsed_ms = started.elapsed().as_millis(),
            "redb journal replay decoded"
        );
        Ok(events)
    }

    fn append(&self, event: &EventEnvelope) -> Result<(), NodeError> {
        let encoded = serde_json::to_vec(event).map_err(backend_error)?;
        let origin = serde_json::to_vec(&event.origin).map_err(backend_error)?;
        let mut write = self.database.begin_write().map_err(backend_error)?;
        write
            .set_durability(Durability::Immediate)
            .map_err(backend_error)?;
        {
            let mut events = write.open_table(EVENTS).map_err(backend_error)?;
            let expected =
                events
                    .last()
                    .map_err(backend_error)?
                    .map_or(Ok(1), |(position, _)| {
                        position.value().checked_add(1).ok_or_else(|| {
                            NodeError::Backend("event position exhausted".to_owned())
                        })
                    })?;
            if event.position.get() != expected {
                return Err(NodeError::Backend(format!(
                    "journal expected position {expected}, received {}",
                    event.position.get()
                )));
            }
            events
                .insert(event.position.get(), encoded.as_slice())
                .map_err(backend_error)?;

            let mut origins = write.open_table(ORIGINS).map_err(backend_error)?;
            if origins
                .get(origin.as_slice())
                .map_err(backend_error)?
                .is_some()
            {
                return Err(NodeError::Backend(format!(
                    "event origin {:?} is already durable",
                    event.origin
                )));
            }
            origins
                .insert(origin.as_slice(), event.position.get())
                .map_err(backend_error)?;
        }
        write.commit().map_err(backend_error)
    }
}

impl ReplicationCursorStore for RedbJournal {
    fn load_checkpoint(
        &self,
        key: &ReplicationCursorKey,
    ) -> Result<Option<ReplicationCheckpoint>, NodeError> {
        let encoded = serde_json::to_vec(key).map_err(backend_error)?;
        let read = self.database.begin_read().map_err(backend_error)?;
        let table = read
            .open_table(REPLICATION_CHECKPOINTS)
            .map_err(backend_error)?;
        table
            .get(encoded.as_slice())
            .map_err(backend_error)?
            .map(|value| serde_json::from_slice(value.value()).map_err(backend_error))
            .transpose()
    }

    fn save_checkpoint(
        &self,
        key: &ReplicationCursorKey,
        checkpoint: ReplicationCheckpoint,
    ) -> Result<(), NodeError> {
        let encoded_key = serde_json::to_vec(key).map_err(backend_error)?;
        let encoded_checkpoint = serde_json::to_vec(&checkpoint).map_err(backend_error)?;
        let mut write = self.database.begin_write().map_err(backend_error)?;
        write
            .set_durability(Durability::Immediate)
            .map_err(backend_error)?;
        {
            let mut checkpoints = write
                .open_table(REPLICATION_CHECKPOINTS)
                .map_err(backend_error)?;
            let existing = checkpoints
                .get(encoded_key.as_slice())
                .map_err(backend_error)?
                .map(|value| serde_json::from_slice(value.value()).map_err(backend_error))
                .transpose()?;
            if existing.is_some_and(|existing: ReplicationCheckpoint| {
                existing.source_node == checkpoint.source_node
                    && existing.selection == checkpoint.selection
                    && match (existing.position, checkpoint.position) {
                        (Some(_), None) => true,
                        (Some(existing), Some(next)) => existing > next,
                        (None, _) => false,
                    }
            }) {
                return Err(NodeError::Backend(format!(
                    "replication checkpoint for {}/{} cannot move backwards",
                    key.transport(),
                    key.peer()
                )));
            }
            checkpoints
                .insert(encoded_key.as_slice(), encoded_checkpoint.as_slice())
                .map_err(backend_error)?;
        }
        write.commit().map_err(backend_error)
    }
}

fn initialize(database: &Database) -> Result<(NodeId, StorageIncarnationId), NodeError> {
    let mut write = database.begin_write().map_err(backend_error)?;
    write
        .set_durability(Durability::Immediate)
        .map_err(backend_error)?;
    let (node_id, storage_incarnation) = {
        let has_durable_state = write
            .open_table(EVENTS)
            .map_err(backend_error)?
            .last()
            .map_err(backend_error)?
            .is_some()
            || write
                .open_table(ORIGINS)
                .map_err(backend_error)?
                .last()
                .map_err(backend_error)?
                .is_some()
            || write
                .open_table(REPLICATION_CHECKPOINTS)
                .map_err(backend_error)?
                .last()
                .map_err(backend_error)?
                .is_some();
        let mut meta = write.open_table(META).map_err(backend_error)?;
        let encoded_incarnation = metadata_value(&meta, STORAGE_INCARNATION_KEY)?;
        let node_id = match metadata_value(&meta, NODE_ID_KEY)? {
            Some(encoded) => serde_json::from_slice(&encoded).map_err(backend_error)?,
            None if has_durable_state || encoded_incarnation.is_some() => {
                return Err(NodeError::Backend(
                    "established journal has no node identity".to_owned(),
                ));
            }
            None => {
                let node_id = NodeId::new();
                let encoded = serde_json::to_vec(&node_id).map_err(backend_error)?;
                meta.insert(NODE_ID_KEY, encoded.as_slice())
                    .map_err(backend_error)?;
                node_id
            }
        };
        let storage_incarnation = if let Some(encoded) = encoded_incarnation {
            serde_json::from_slice(&encoded).map_err(backend_error)?
        } else {
            let incarnation = StorageIncarnationId::new();
            let encoded = serde_json::to_vec(&incarnation).map_err(backend_error)?;
            meta.insert(STORAGE_INCARNATION_KEY, encoded.as_slice())
                .map_err(backend_error)?;
            incarnation
        };
        (node_id, storage_incarnation)
    };
    drop(write.open_table(ORIGINS).map_err(backend_error)?);
    drop(
        write
            .open_table(REPLICATION_CHECKPOINTS)
            .map_err(backend_error)?,
    );
    write.commit().map_err(backend_error)?;
    Ok((node_id, storage_incarnation))
}

fn metadata_value(
    meta: &redb::Table<'_, &str, &[u8]>,
    key: &str,
) -> Result<Option<Vec<u8>>, NodeError> {
    let value = meta
        .get(key)
        .map_err(backend_error)?
        .map(|encoded| encoded.value().to_vec());
    Ok(value)
}

fn backend_error(error: impl fmt::Display) -> NodeError {
    NodeError::Backend(error.to_string())
}

#[cfg(test)]
mod tests {
    use myko_federation::{
        AccessPolicy, AllowAllAccessPolicy, BatchId, ChangeBatch, CommandId, CommandRequest,
        CommandState, EventJournal, InMemoryBackend, IngestStatus, LogPosition, NodeBackend,
        NodeEvent, PrincipalId, ScopeId, ServiceId,
    };

    use super::*;

    fn allow_commands(node: &Node) -> Result<Arc<dyn AccessPolicy>, String> {
        let policy: Arc<dyn AccessPolicy> = Arc::new(AllowAllAccessPolicy);
        node.set_command_access_policy(policy.clone())
            .map_err(|error| error.to_string())?;
        Ok(policy)
    }

    fn request(id: CommandId) -> CommandRequest {
        let principal_id = PrincipalId::new("human:test");
        CommandRequest {
            id,
            service_id: ServiceId::new("agent"),
            scope_id: ScopeId::new("session:durable"),
            principal_id: principal_id.clone(),
            authority: myko_federation::AuthorityPresentation::direct_node(principal_id),
            resource_claims: Vec::new(),
            application_capabilities: Vec::new(),
            arguments_digest: None,
            command_type: "prompt".to_owned(),
            payload: b"remember me".to_vec(),
        }
    }

    fn committed_envelope(
        origin: myko_federation::EventId,
        request: &CommandRequest,
        causal_parents: Vec<myko_federation::EventId>,
        timestamp_source: &EventEnvelope,
    ) -> EventEnvelope {
        let batch_id = BatchId::new();
        EventEnvelope {
            position: LogPosition::new(1),
            origin,
            recorded_at: timestamp_source.recorded_at,
            event: NodeEvent::CommandCommitted {
                command: myko_federation::CommandSnapshot {
                    request: request.clone(),
                    state: CommandState::CommittedLocally {
                        batch_id,
                        position: origin,
                    },
                    result: Some(b"durable result".to_vec()),
                    updated_at: origin,
                },
                batch: ChangeBatch {
                    id: batch_id,
                    command_id: request.id,
                    service_id: request.service_id.clone(),
                    scope_id: request.scope_id.clone(),
                    causal_parents,
                    changes: Vec::new(),
                },
            },
        }
    }

    fn timestamp_source() -> Result<EventEnvelope, String> {
        let node = Node::in_memory();
        let _policy = allow_commands(&node)?;
        node.submit(request(CommandId::new()))
            .map_err(|error| error.to_string())?;
        node.events_after(None)
            .map_err(|error| error.to_string())?
            .into_iter()
            .next()
            .ok_or_else(|| "timestamp source did not emit a lifecycle event".to_owned())
    }

    fn require_causal_origins(
        backend: &impl NodeBackend,
        cut: LogPosition,
        expected: &[myko_federation::EventId],
    ) -> Result<(), String> {
        let actual = backend
            .causal_events_through(cut)
            .map_err(|error| error.to_string())?
            .into_iter()
            .map(|event| event.origin)
            .collect::<Vec<_>>();
        if actual.as_slice() != expected {
            return Err(format!(
                "unexpected causal history at {}: {actual:?}, expected {expected:?}",
                cut.get()
            ));
        }
        Ok(())
    }

    fn require_replayed_bodies(
        journal: &RedbJournal,
        expected: &[EventEnvelope],
    ) -> Result<(), String> {
        let actual = journal.replay().map_err(|error| error.to_string())?;
        if actual.len() != expected.len()
            || actual.iter().zip(expected).any(|(actual, expected)| {
                actual.origin != expected.origin
                    || actual.recorded_at != expected.recorded_at
                    || actual.event != expected.event
            })
        {
            return Err("replayed event bodies differ from retained immutable inputs".to_owned());
        }
        Ok(())
    }

    fn require_command_blocked(
        backend: &impl NodeBackend,
        journal: &RedbJournal,
        command: &CommandRequest,
        phase: &str,
    ) -> Result<(), String> {
        if backend
            .command(command.id)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Err(format!("{phase} exposed the blocked command result"));
        }
        let before = journal.replay().map_err(|error| error.to_string())?.len();
        if !matches!(
            backend.admit(command.clone()),
            Err(NodeError::CommandHistoryIncomplete(id)) if id == command.id
        ) {
            return Err(format!("{phase} allowed duplicate command admission"));
        }
        if journal.replay().map_err(|error| error.to_string())?.len() != before {
            return Err(format!("{phase} duplicate admission grew the journal"));
        }
        Ok(())
    }

    #[test]
    fn retained_history_verification_checks_exact_immutable_inclusion() -> Result<(), String> {
        let source = Node::in_memory();
        let _source_policy = allow_commands(&source)?;
        source
            .submit(request(CommandId::new()))
            .map_err(|error| error.to_string())?;
        source
            .submit(request(CommandId::new()))
            .map_err(|error| error.to_string())?;
        let required = source
            .events_after(None)
            .map_err(|error| error.to_string())?;
        let first = required
            .first()
            .cloned()
            .ok_or_else(|| "source first event is missing".to_owned())?;
        let higher = required
            .get(1)
            .cloned()
            .ok_or_else(|| "source higher event is missing".to_owned())?;
        if higher.origin.node_id != first.origin.node_id
            || higher.origin.sequence <= first.origin.sequence
        {
            return Err("fixture events are not increasing on one origin".to_owned());
        }

        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let path = directory.path().join("retained-history.redb");
        let target = RedbJournal::open_node(&path).map_err(|error| error.to_string())?;
        let _target_policy = allow_commands(&target)?;
        target
            .submit(request(CommandId::new()))
            .map_err(|error| error.to_string())?;
        for event in &required {
            target
                .ingest(event.clone())
                .map_err(|error| error.to_string())?;
        }
        drop(target);

        let reopened = RedbJournal::open(&path).map_err(|error| error.to_string())?;
        reopened
            .verify_retained_history(&required)
            .map_err(|error| error.to_string())?;
        reopened
            .verify_retained_history(&[higher.clone(), first.clone()])
            .map_err(|error| error.to_string())?;
        reopened
            .verify_retained_history(&[first.clone(), first.clone()])
            .map_err(|error| error.to_string())?;
        reopened
            .verify_retained_history(&[])
            .map_err(|error| error.to_string())?;
        let replayed = reopened.replay().map_err(|error| error.to_string())?;
        let imported = replayed
            .iter()
            .find(|event| event.origin == first.origin)
            .ok_or_else(|| "reopened imported event is missing".to_owned())?;
        if imported.position == first.position {
            return Err("fixture did not change the imported observer position".to_owned());
        }

        let mut conflict = first.clone();
        conflict.recorded_at += std::time::Duration::from_secs(1);
        if !matches!(
            reopened.verify_retained_history(&[first.clone(), conflict]),
            Err(NodeError::EventConflict(origin)) if origin == first.origin
        ) {
            return Err("conflicting duplicate requirement was not rejected".to_owned());
        }
        let mut changed_body = first.clone();
        match &mut changed_body.event {
            NodeEvent::FrameworkControl(_) => return Err("expected command fixture".to_owned()),
            NodeEvent::CommandLifecycle(command) | NodeEvent::CommandCommitted { command, .. } => {
                command.request.payload = b"changed retained body".to_vec();
            }
        }
        if !matches!(
            reopened.verify_retained_history(std::slice::from_ref(&changed_body)),
            Err(NodeError::EventConflict(origin)) if origin == first.origin
        ) {
            return Err("changed immutable event body was not rejected".to_owned());
        }

        let sparse_path = directory.path().join("sparse-retained-history.redb");
        let sparse = RedbJournal::open_node(&sparse_path).map_err(|error| error.to_string())?;
        sparse
            .ingest(higher.clone())
            .map_err(|error| error.to_string())?;
        drop(sparse);
        let sparse = RedbJournal::open(&sparse_path).map_err(|error| error.to_string())?;
        sparse
            .verify_retained_history(&[higher])
            .map_err(|error| error.to_string())?;
        if !matches!(
            sparse.verify_retained_history(std::slice::from_ref(&first)),
            Err(NodeError::MissingRetainedEvent(origin)) if origin == first.origin
        ) {
            return Err("omitted lower event was inferred from a retained higher event".to_owned());
        }
        sparse
            .verify_retained_history(&[])
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    #[test]
    fn restart_preserves_identity_history_and_idempotency() -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let path = directory.path().join("node.redb");
        let command = request(CommandId::new());
        let first_node = RedbJournal::open_node(&path).map_err(|error| error.to_string())?;
        let _policy = allow_commands(&first_node)?;
        let node_id = first_node.node_id();

        first_node
            .submit(command.clone())
            .map_err(|error| error.to_string())?;
        let claimed = first_node
            .claim(command.id)
            .map_err(|error| error.to_string())?;
        first_node
            .commit(
                command.id,
                ChangeBatch {
                    id: BatchId::new(),
                    command_id: command.id,
                    service_id: command.service_id.clone(),
                    scope_id: command.scope_id.clone(),
                    causal_parents: vec![claimed.snapshot().updated_at],
                    changes: Vec::new(),
                },
                b"finished".to_vec(),
            )
            .map_err(|error| error.to_string())?;
        drop(first_node);

        let reopened = RedbJournal::open_node(&path).map_err(|error| error.to_string())?;
        if reopened.node_id() != node_id {
            return Err("node identity changed across restart".to_owned());
        }
        let snapshot = reopened
            .command(command.id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "committed command disappeared after restart".to_owned())?;
        if !matches!(snapshot.state, CommandState::CommittedLocally { .. })
            || snapshot.result.as_deref() != Some(b"finished".as_slice())
        {
            return Err("committed command did not recover exactly".to_owned());
        }
        if reopened
            .claim(command.id)
            .map_err(|error| error.to_string())?
            .should_execute()
        {
            return Err("reopened node granted duplicate execution".to_owned());
        }
        if reopened
            .events_after(None)
            .map_err(|error| error.to_string())?
            .len()
            != 3
        {
            return Err("durable history did not replay completely".to_owned());
        }
        Ok(())
    }

    #[test]
    fn reopened_journal_rejects_conflicting_foreign_event_origins() -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let path = directory.path().join("foreign-origin.redb");

        let source = Node::in_memory();
        let _source_policy = allow_commands(&source)?;
        let foreign_command = request(CommandId::new());
        let admission = source
            .admit(foreign_command.clone())
            .map_err(|error| error.to_string())?;
        source
            .commit(
                foreign_command.id,
                ChangeBatch {
                    id: BatchId::new(),
                    command_id: foreign_command.id,
                    service_id: foreign_command.service_id.clone(),
                    scope_id: foreign_command.scope_id.clone(),
                    causal_parents: vec![admission.snapshot().updated_at],
                    changes: Vec::new(),
                },
                b"original result".to_vec(),
            )
            .map_err(|error| error.to_string())?;
        let foreign_events = source
            .events_after(None)
            .map_err(|error| error.to_string())?;
        let foreign_commit = foreign_events
            .last()
            .cloned()
            .ok_or_else(|| "source produced no foreign committed event".to_owned())?;

        let target = RedbJournal::open_node(&path).map_err(|error| error.to_string())?;
        let _target_policy = allow_commands(&target)?;
        target
            .submit(request(CommandId::new()))
            .map_err(|error| error.to_string())?;
        for event in &foreign_events {
            target
                .ingest(event.clone())
                .map_err(|error| error.to_string())?;
        }
        drop(target);

        let reopened = RedbJournal::open_node(&path).map_err(|error| error.to_string())?;
        let imported = reopened
            .events_after(None)
            .map_err(|error| error.to_string())?
            .into_iter()
            .find(|event| event.origin == foreign_commit.origin)
            .ok_or_else(|| "foreign event disappeared after journal reopen".to_owned())?;
        if imported.position == foreign_commit.position {
            return Err(
                "fixture did not give the foreign event a distinct observer position".to_owned(),
            );
        }

        let mut duplicate = foreign_commit.clone();
        duplicate.position = LogPosition::new(9_999);
        if reopened
            .ingest(duplicate)
            .map_err(|error| error.to_string())?
            != IngestStatus::Duplicate
        {
            return Err("identical foreign event was not idempotent after reopen".to_owned());
        }

        let mut changed_body = foreign_commit.clone();
        let NodeEvent::CommandCommitted { command, .. } = &mut changed_body.event else {
            return Err("fixture did not produce a committed foreign event".to_owned());
        };
        command.request.payload = b"forged payload".to_vec();
        if !matches!(
            reopened.ingest(changed_body),
            Err(NodeError::EventConflict(origin)) if origin == foreign_commit.origin
        ) {
            return Err("changed foreign event body was accepted after reopen".to_owned());
        }

        let mut changed_timestamp = foreign_commit.clone();
        changed_timestamp.recorded_at += std::time::Duration::from_secs(1);
        if !matches!(
            reopened.ingest(changed_timestamp),
            Err(NodeError::EventConflict(origin)) if origin == foreign_commit.origin
        ) {
            return Err("changed foreign event timestamp was accepted after reopen".to_owned());
        }
        drop(reopened);

        let recovered = RedbJournal::open_node(&path).map_err(|error| error.to_string())?;
        let snapshot = recovered
            .command(foreign_command.id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "original foreign command disappeared after conflicts".to_owned())?;
        if !matches!(snapshot.state, CommandState::CommittedLocally { .. })
            || snapshot.result.as_deref() != Some(b"original result".as_slice())
        {
            return Err("conflicting replay changed recovered foreign command state".to_owned());
        }
        if recovered
            .events_after(None)
            .map_err(|error| error.to_string())?
            .len()
            != 3
        {
            return Err("conflicting replay changed durable journal history".to_owned());
        }
        Ok(())
    }

    #[test]
    fn causal_index_preserves_an_unresolved_child_across_redb_restarts() -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let path = directory.path().join("causal-index.redb");

        let source = Node::in_memory();
        let _source_policy = allow_commands(&source)?;
        let command = request(CommandId::new());
        let admission = source
            .admit(command.clone())
            .map_err(|error| error.to_string())?;
        source
            .commit(
                command.id,
                ChangeBatch {
                    id: BatchId::new(),
                    command_id: command.id,
                    service_id: command.service_id.clone(),
                    scope_id: command.scope_id.clone(),
                    causal_parents: vec![admission.snapshot().updated_at],
                    changes: Vec::new(),
                },
                b"recorded result".to_vec(),
            )
            .map_err(|error| error.to_string())?;
        let source_events = source
            .events_after(None)
            .map_err(|error| error.to_string())?;
        let parent = source_events
            .first()
            .cloned()
            .ok_or_else(|| "source did not emit an executing parent".to_owned())?;
        let child = source_events
            .last()
            .cloned()
            .ok_or_else(|| "source did not emit a committed child".to_owned())?;

        let journal = Arc::new(RedbJournal::open(&path).map_err(|error| error.to_string())?);
        let target =
            InMemoryBackend::from_journal(journal.clone()).map_err(|error| error.to_string())?;
        let child_position = match target
            .ingest(child.clone())
            .map_err(|error| error.to_string())?
        {
            IngestStatus::Applied { position } => position,
            IngestStatus::Duplicate => {
                return Err("empty target reported child as duplicate".to_owned());
            }
        };
        require_causal_origins(&target, child_position, &[])?;
        let persisted = journal.replay().map_err(|error| error.to_string())?;
        if persisted.len() != 1
            || !persisted.first().is_some_and(|event| {
                event.origin == child.origin
                    && event.recorded_at == child.recorded_at
                    && event.event == child.event
            })
        {
            return Err("blocked child was not retained durably at its local cut".to_owned());
        }
        require_command_blocked(&target, &journal, &command, "initial ingest")?;
        drop(target);
        drop(journal);

        let journal = Arc::new(RedbJournal::open(&path).map_err(|error| error.to_string())?);
        let reopened =
            InMemoryBackend::from_journal(journal.clone()).map_err(|error| error.to_string())?;
        require_causal_origins(&reopened, child_position, &[])?;
        require_command_blocked(&reopened, &journal, &command, "first reopen")?;
        let parent_position = match reopened
            .ingest(parent.clone())
            .map_err(|error| error.to_string())?
        {
            IngestStatus::Applied { position } => position,
            IngestStatus::Duplicate => return Err("late parent was already present".to_owned()),
        };
        require_causal_origins(&reopened, parent_position, &[parent.origin, child.origin])?;
        require_causal_origins(&reopened, child_position, &[])?;
        let released = reopened
            .command(command.id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "late parent did not release the command result".to_owned())?;
        if released.result.as_deref() != Some(b"recorded result".as_slice()) {
            return Err("late parent released the wrong command result".to_owned());
        }
        drop(reopened);
        drop(journal);

        let journal = Arc::new(RedbJournal::open(&path).map_err(|error| error.to_string())?);
        let recovered =
            InMemoryBackend::from_journal(journal).map_err(|error| error.to_string())?;
        require_causal_origins(&recovered, parent_position, &[parent.origin, child.origin])?;
        require_causal_origins(&recovered, child_position, &[])?;
        let recovered_command = recovered
            .command(command.id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "third reopen lost the released command".to_owned())?;
        if recovered_command.result.as_deref() != Some(b"recorded result".as_slice()) {
            return Err("third reopen recovered the wrong command result".to_owned());
        }
        Ok(())
    }

    #[test]
    fn sparse_same_origin_author_order_survives_redb_restarts() -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let path = directory.path().join("sparse-author-order.redb");
        let author = NodeId::new();
        let timestamp_source = timestamp_source()?;
        let dependency_origin = myko_federation::EventId::new(NodeId::new(), LogPosition::new(1));
        let earlier_origin = myko_federation::EventId::new(author, LogPosition::new(10));
        let later_origin = myko_federation::EventId::new(author, LogPosition::new(30));
        let mut dependency_request = request(CommandId::new());
        dependency_request.scope_id = ScopeId::new("session:sparse-dependency");
        let dependency = committed_envelope(
            dependency_origin,
            &dependency_request,
            Vec::new(),
            &timestamp_source,
        );
        let earlier = committed_envelope(
            earlier_origin,
            &request(CommandId::new()),
            vec![dependency_origin],
            &timestamp_source,
        );
        let later = committed_envelope(
            later_origin,
            &request(CommandId::new()),
            Vec::new(),
            &timestamp_source,
        );
        let expected = [later.clone(), earlier.clone(), dependency.clone()];

        let journal = Arc::new(RedbJournal::open(&path).map_err(|error| error.to_string())?);
        let backend =
            InMemoryBackend::from_journal(journal.clone()).map_err(|error| error.to_string())?;
        let later_cut = match backend.ingest(later).map_err(|error| error.to_string())? {
            IngestStatus::Applied { position } => position,
            IngestStatus::Duplicate => return Err("later event was already durable".to_owned()),
        };
        require_causal_origins(&backend, later_cut, &[later_origin])?;
        backend.ingest(earlier).map_err(|error| error.to_string())?;
        let dependency_cut = match backend
            .ingest(dependency)
            .map_err(|error| error.to_string())?
        {
            IngestStatus::Applied { position } => position,
            IngestStatus::Duplicate => return Err("dependency was already durable".to_owned()),
        };
        require_causal_origins(
            &backend,
            dependency_cut,
            &[dependency_origin, earlier_origin, later_origin],
        )?;
        require_causal_origins(&backend, later_cut, &[later_origin])?;
        require_replayed_bodies(&journal, &expected)?;
        drop(backend);
        drop(journal);

        for phase in ["first reopen", "second reopen"] {
            let journal = Arc::new(RedbJournal::open(&path).map_err(|error| error.to_string())?);
            let backend = InMemoryBackend::from_journal(journal.clone())
                .map_err(|error| error.to_string())?;
            require_causal_origins(&backend, later_cut, &[later_origin])?;
            require_causal_origins(
                &backend,
                dependency_cut,
                &[dependency_origin, earlier_origin, later_origin],
            )?;
            require_replayed_bodies(&journal, &expected)
                .map_err(|error| format!("{phase} changed an immutable journal event: {error}"))?;
        }
        Ok(())
    }

    #[test]
    fn redb_rejects_a_cycle_between_explicit_and_inferred_author_edges() -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let path = directory.path().join("combined-author-cycle.redb");
        let author = NodeId::new();
        let timestamp_source = timestamp_source()?;
        let earlier_origin = myko_federation::EventId::new(author, LogPosition::new(10));
        let later_origin = myko_federation::EventId::new(author, LogPosition::new(30));
        let later = committed_envelope(
            later_origin,
            &request(CommandId::new()),
            Vec::new(),
            &timestamp_source,
        );
        let earlier = committed_envelope(
            earlier_origin,
            &request(CommandId::new()),
            vec![later_origin],
            &timestamp_source,
        );
        let journal = Arc::new(RedbJournal::open(&path).map_err(|error| error.to_string())?);
        let backend =
            InMemoryBackend::from_journal(journal.clone()).map_err(|error| error.to_string())?;
        let expected = [later.clone()];
        let later_cut = match backend.ingest(later).map_err(|error| error.to_string())? {
            IngestStatus::Applied { position } => position,
            IngestStatus::Duplicate => return Err("later event was already durable".to_owned()),
        };
        if !matches!(
            backend.ingest(earlier),
            Err(NodeError::CorruptHistory(reason)) if reason.contains("dependency cycle")
        ) {
            return Err("combined inferred and explicit cycle was accepted".to_owned());
        }
        require_causal_origins(&backend, later_cut, &[later_origin])?;
        require_replayed_bodies(&journal, &expected)?;
        drop(backend);
        drop(journal);

        let journal = Arc::new(RedbJournal::open(&path).map_err(|error| error.to_string())?);
        let reopened =
            InMemoryBackend::from_journal(journal.clone()).map_err(|error| error.to_string())?;
        require_causal_origins(&reopened, later_cut, &[later_origin])?;
        require_replayed_bodies(&journal, &expected)
    }

    #[test]
    fn restart_requeues_a_locally_abandoned_claim() -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let path = directory.path().join("abandoned.redb");
        let command = request(CommandId::new());
        let first_node = RedbJournal::open_node(&path).map_err(|error| error.to_string())?;
        let _policy = allow_commands(&first_node)?;
        first_node
            .submit(command.clone())
            .map_err(|error| error.to_string())?;
        if !first_node
            .claim(command.id)
            .map_err(|error| error.to_string())?
            .should_execute()
        {
            return Err("initial handler did not claim the command".to_owned());
        }
        drop(first_node);

        let reopened = RedbJournal::open_node(&path).map_err(|error| error.to_string())?;
        let recovered = reopened
            .command(command.id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "abandoned command disappeared".to_owned())?;
        if !matches!(recovered.state, CommandState::Submitted) {
            return Err("abandoned local execution was not requeued".to_owned());
        }
        if !reopened
            .claim(command.id)
            .map_err(|error| error.to_string())?
            .should_execute()
        {
            return Err("recovered command could not be claimed".to_owned());
        }
        Ok(())
    }

    #[test]
    fn restart_preserves_a_durable_handler_retry() -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let path = directory.path().join("retrying.redb");
        let command = request(CommandId::new());
        let first_node = RedbJournal::open_node(&path).map_err(|error| error.to_string())?;
        let _policy = allow_commands(&first_node)?;
        first_node
            .submit(command.clone())
            .map_err(|error| error.to_string())?;
        first_node
            .claim(command.id)
            .map_err(|error| error.to_string())?;
        first_node
            .retry(command.id, "workspace registry temporarily unavailable")
            .map_err(|error| error.to_string())?;
        drop(first_node);

        let reopened = RedbJournal::open_node(&path).map_err(|error| error.to_string())?;
        let recovered = reopened
            .command(command.id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "retrying command disappeared".to_owned())?;
        if !matches!(
            recovered.state,
            CommandState::Retrying { reason }
                if reason == "workspace registry temporarily unavailable"
        ) {
            return Err("retry reason did not survive restart".to_owned());
        }
        if !reopened
            .claim(command.id)
            .map_err(|error| error.to_string())?
            .should_execute()
        {
            return Err("retrying command could not be claimed after restart".to_owned());
        }
        Ok(())
    }

    #[test]
    fn restart_preserves_terminal_cancellation() -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let path = directory.path().join("cancelled.redb");
        let command = request(CommandId::new());
        let first_node = RedbJournal::open_node(&path).map_err(|error| error.to_string())?;
        let _policy = allow_commands(&first_node)?;
        first_node
            .submit(command.clone())
            .map_err(|error| error.to_string())?;
        first_node
            .claim(command.id)
            .map_err(|error| error.to_string())?;
        first_node
            .cancel(command.id, "operator cancelled")
            .map_err(|error| error.to_string())?;
        drop(first_node);

        let reopened = RedbJournal::open_node(&path).map_err(|error| error.to_string())?;
        let recovered = reopened
            .command(command.id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "cancelled command disappeared".to_owned())?;
        if !matches!(
            recovered.state,
            CommandState::Cancelled { reason } if reason == "operator cancelled"
        ) {
            return Err("cancelled command was requeued after restart".to_owned());
        }
        if reopened
            .claim(command.id)
            .map_err(|error| error.to_string())?
            .should_execute()
        {
            return Err("cancelled command was executable after restart".to_owned());
        }
        Ok(())
    }

    #[test]
    fn restart_preserves_source_aware_replication_checkpoints() -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let path = directory.path().join("replication-cursors.redb");
        let key = ReplicationCursorKey::new("iroh", "peer:test");
        let first_source = NodeId::new();
        let first_checkpoint = ReplicationCheckpoint::new(first_source, Some(LogPosition::new(41)));
        let journal = RedbJournal::open(&path).map_err(|error| error.to_string())?;
        if journal
            .load_checkpoint(&key)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Err("new journal contained a replication checkpoint".to_owned());
        }
        journal
            .save_checkpoint(&key, first_checkpoint.clone())
            .map_err(|error| error.to_string())?;
        drop(journal);

        let reopened = RedbJournal::open(&path).map_err(|error| error.to_string())?;
        if reopened
            .load_checkpoint(&key)
            .map_err(|error| error.to_string())?
            != Some(first_checkpoint)
        {
            return Err("replication checkpoint did not survive restart".to_owned());
        }
        if reopened
            .save_checkpoint(
                &key,
                ReplicationCheckpoint::new(first_source, Some(LogPosition::new(40))),
            )
            .is_ok()
        {
            return Err("durable replication checkpoint moved backwards".to_owned());
        }

        let replacement = ReplicationCheckpoint::new(NodeId::new(), None);
        reopened
            .save_checkpoint(&key, replacement.clone())
            .map_err(|error| error.to_string())?;
        if reopened
            .load_checkpoint(&key)
            .map_err(|error| error.to_string())?
            != Some(replacement)
        {
            return Err("new source identity did not reset the peer checkpoint".to_owned());
        }
        Ok(())
    }

    fn replace_metadata(path: &Path, key: &str, value: Option<&[u8]>) -> Result<(), String> {
        let database = Database::create(path).map_err(|error| error.to_string())?;
        let write = database.begin_write().map_err(|error| error.to_string())?;
        {
            let mut meta = write.open_table(META).map_err(|error| error.to_string())?;
            match value {
                Some(value) => {
                    meta.insert(key, value).map_err(|error| error.to_string())?;
                }
                None => {
                    let _removed = meta.remove(key).map_err(|error| error.to_string())?;
                }
            }
        }
        write.commit().map_err(|error| error.to_string())
    }

    fn read_metadata(path: &Path, key: &str) -> Result<Option<Vec<u8>>, String> {
        let database = Database::create(path).map_err(|error| error.to_string())?;
        let read = database.begin_read().map_err(|error| error.to_string())?;
        let meta = match read.open_table(META) {
            Ok(meta) => meta,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(error.to_string()),
        };
        let value = meta
            .get(key)
            .map_err(|error| error.to_string())?
            .map(|value| value.value().to_vec());
        Ok(value)
    }

    fn read_events(path: &Path) -> Result<Vec<EventEnvelope>, String> {
        let database = Database::create(path).map_err(|error| error.to_string())?;
        let read = database.begin_read().map_err(|error| error.to_string())?;
        let events = read.open_table(EVENTS).map_err(|error| error.to_string())?;
        events
            .iter()
            .map_err(|error| error.to_string())?
            .map(|entry| {
                let (_, encoded) = entry.map_err(|error| error.to_string())?;
                serde_json::from_slice(encoded.value()).map_err(|error| error.to_string())
            })
            .collect()
    }

    #[test]
    fn storage_incarnation_is_stable_per_store_and_distinct_between_stores() -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let first_path = directory.path().join("first.redb");
        let second_path = directory.path().join("second.redb");
        let first = RedbJournal::open(&first_path).map_err(|error| error.to_string())?;
        let incarnation = first
            .storage_incarnation()
            .map_err(|error| error.to_string())?;
        drop(first);

        let reopened = RedbJournal::open(&first_path).map_err(|error| error.to_string())?;
        if reopened
            .storage_incarnation()
            .map_err(|error| error.to_string())?
            != incarnation
        {
            return Err("storage incarnation changed across reopen".to_owned());
        }
        let independent = RedbJournal::open(&second_path).map_err(|error| error.to_string())?;
        if independent
            .storage_incarnation()
            .map_err(|error| error.to_string())?
            == incarnation
        {
            return Err("independent stores shared a storage incarnation".to_owned());
        }
        Ok(())
    }

    #[test]
    fn copied_database_preserves_node_and_storage_identities() -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let source_path = directory.path().join("source.redb");
        let copy_path = directory.path().join("copy.redb");
        let source = RedbJournal::open(&source_path).map_err(|error| error.to_string())?;
        let expected = (
            source.node_id().map_err(|error| error.to_string())?,
            source
                .storage_incarnation()
                .map_err(|error| error.to_string())?,
        );
        drop(source);
        std::fs::copy(&source_path, &copy_path).map_err(|error| error.to_string())?;

        let copied = RedbJournal::open(&copy_path).map_err(|error| error.to_string())?;
        let actual = (
            copied.node_id().map_err(|error| error.to_string())?,
            copied
                .storage_incarnation()
                .map_err(|error| error.to_string())?,
        );
        if actual != expected {
            return Err("database copy unexpectedly changed persisted identities".to_owned());
        }
        Ok(())
    }

    #[test]
    fn legacy_store_upgrade_adds_incarnation_without_changing_identity_or_history()
    -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let path = directory.path().join("legacy.redb");
        let command = request(CommandId::new());
        let (node, journal) =
            RedbJournal::open_node_with_journal(&path).map_err(|error| error.to_string())?;
        let _policy = allow_commands(&node)?;
        node.submit(command).map_err(|error| error.to_string())?;
        let node_id = node.node_id();
        let history = journal.replay().map_err(|error| error.to_string())?;
        drop(node);
        drop(journal);
        replace_metadata(&path, STORAGE_INCARNATION_KEY, None)?;

        let upgraded = RedbJournal::open(&path).map_err(|error| error.to_string())?;
        if upgraded.node_id().map_err(|error| error.to_string())? != node_id
            || upgraded.replay().map_err(|error| error.to_string())? != history
        {
            return Err("legacy upgrade changed node identity or immutable history".to_owned());
        }
        let incarnation = upgraded
            .storage_incarnation()
            .map_err(|error| error.to_string())?;
        drop(upgraded);
        let reopened = RedbJournal::open(&path).map_err(|error| error.to_string())?;
        if reopened
            .storage_incarnation()
            .map_err(|error| error.to_string())?
            != incarnation
        {
            return Err("upgraded incarnation was not durable".to_owned());
        }
        Ok(())
    }

    #[test]
    fn malformed_incarnation_and_missing_node_identity_are_not_replaced() -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        for key in [NODE_ID_KEY, STORAGE_INCARNATION_KEY] {
            let malformed_path = directory.path().join(format!("malformed-{key}.redb"));
            drop(RedbJournal::open(&malformed_path).map_err(|error| error.to_string())?);
            replace_metadata(&malformed_path, key, Some(b"not-json"))?;
            if RedbJournal::open(&malformed_path).is_ok() {
                return Err(format!("malformed {key} was silently replaced"));
            }
            if read_metadata(&malformed_path, key)?.as_deref() != Some(b"not-json".as_slice()) {
                return Err(format!("failed open changed malformed {key} metadata"));
            }
        }

        let partial_empty_path = directory.path().join("incarnation-without-node.redb");
        drop(RedbJournal::open(&partial_empty_path).map_err(|error| error.to_string())?);
        replace_metadata(&partial_empty_path, NODE_ID_KEY, None)?;
        if RedbJournal::open(&partial_empty_path).is_ok() {
            return Err("partial established metadata received a replacement node ID".to_owned());
        }
        if read_metadata(&partial_empty_path, NODE_ID_KEY)?.is_some() {
            return Err("failed partial-metadata open persisted a node identity".to_owned());
        }

        let partial_path = directory.path().join("missing-node.redb");
        let command = request(CommandId::new());
        let (node, journal) = RedbJournal::open_node_with_journal(&partial_path)
            .map_err(|error| error.to_string())?;
        let _policy = allow_commands(&node)?;
        node.submit(command).map_err(|error| error.to_string())?;
        let history = journal.replay().map_err(|error| error.to_string())?;
        drop(node);
        drop(journal);
        replace_metadata(&partial_path, NODE_ID_KEY, None)?;
        if RedbJournal::open(&partial_path).is_ok() {
            return Err("nonempty journal invented a replacement node identity".to_owned());
        }
        if read_metadata(&partial_path, NODE_ID_KEY)?.is_some() {
            return Err("failed nonempty open persisted a node identity".to_owned());
        }
        if read_events(&partial_path)? != history {
            return Err("failed partial-metadata open changed durable history".to_owned());
        }
        Ok(())
    }

    #[test]
    fn orphaned_checkpoint_or_origin_state_cannot_receive_a_new_node_identity() -> Result<(), String>
    {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let checkpoint_path = directory.path().join("checkpoint-only.redb");
        let key = ReplicationCursorKey::new("iroh", "peer:orphaned");
        let checkpoint = ReplicationCheckpoint::new(NodeId::new(), Some(LogPosition::new(7)));
        let journal = RedbJournal::open(&checkpoint_path).map_err(|error| error.to_string())?;
        journal
            .save_checkpoint(&key, checkpoint.clone())
            .map_err(|error| error.to_string())?;
        drop(journal);
        replace_metadata(&checkpoint_path, NODE_ID_KEY, None)?;
        replace_metadata(&checkpoint_path, STORAGE_INCARNATION_KEY, None)?;
        if RedbJournal::open(&checkpoint_path).is_ok() {
            return Err("checkpoint-only store received a new node identity".to_owned());
        }
        let database = Database::create(&checkpoint_path).map_err(|error| error.to_string())?;
        let read = database.begin_read().map_err(|error| error.to_string())?;
        let checkpoints = read
            .open_table(REPLICATION_CHECKPOINTS)
            .map_err(|error| error.to_string())?;
        let encoded_key = serde_json::to_vec(&key).map_err(|error| error.to_string())?;
        let retained = checkpoints
            .get(encoded_key.as_slice())
            .map_err(|error| error.to_string())?
            .map(|value| serde_json::from_slice(value.value()).map_err(|error| error.to_string()))
            .transpose()?;
        if retained != Some(checkpoint) {
            return Err("failed checkpoint-only open changed durable state".to_owned());
        }
        drop((checkpoints, read, database));
        if read_metadata(&checkpoint_path, NODE_ID_KEY)?.is_some() {
            return Err("failed checkpoint-only open persisted a node identity".to_owned());
        }

        let origin_path = directory.path().join("origin-only.redb");
        let database = Database::create(&origin_path).map_err(|error| error.to_string())?;
        let write = database.begin_write().map_err(|error| error.to_string())?;
        {
            let mut origins = write
                .open_table(ORIGINS)
                .map_err(|error| error.to_string())?;
            origins
                .insert(b"orphaned-origin".as_slice(), 41)
                .map_err(|error| error.to_string())?;
        }
        write.commit().map_err(|error| error.to_string())?;
        drop(database);
        if RedbJournal::open(&origin_path).is_ok() {
            return Err("origin-only store received a new node identity".to_owned());
        }
        let database = Database::create(&origin_path).map_err(|error| error.to_string())?;
        let read = database.begin_read().map_err(|error| error.to_string())?;
        let origins = read
            .open_table(ORIGINS)
            .map_err(|error| error.to_string())?;
        let retained_position = origins
            .get(b"orphaned-origin".as_slice())
            .map_err(|error| error.to_string())?
            .map(|position| position.value());
        if retained_position != Some(41) {
            return Err("failed origin-only open changed durable state".to_owned());
        }
        drop((origins, read, database));
        if read_metadata(&origin_path, NODE_ID_KEY)?.is_some() {
            return Err("failed origin-only open persisted a node identity".to_owned());
        }
        Ok(())
    }
}

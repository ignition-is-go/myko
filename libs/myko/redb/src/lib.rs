//! Durable embedded event journal for transport-neutral Myko 7 nodes.
//!
//! The database stores only immutable node-local history and stable node
//! identity. Myko rebuilds command and graph projections from that history on
//! startup, keeping storage layout out of the federation protocol.

#![forbid(unsafe_code)]

use std::{fmt, path::Path, sync::Arc};

use myko_federation::{
    EventEnvelope, EventJournal, Node, NodeError, NodeId, ReplicationCheckpoint,
    ReplicationCursorKey, ReplicationCursorStore,
};
use rayon::prelude::*;
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition, TableError};

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("myko_meta");
const EVENTS: TableDefinition<u64, &[u8]> = TableDefinition::new("myko_events");
const ORIGINS: TableDefinition<&[u8], u64> = TableDefinition::new("myko_event_origins");
const REPLICATION_CHECKPOINTS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("myko_replication_checkpoints");
const NODE_ID_KEY: &str = "node_id";

/// A crash-safe Redb implementation of Myko's immutable event journal.
pub struct RedbJournal {
    database: Arc<Database>,
    node_id: NodeId,
}

impl fmt::Debug for RedbJournal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedbJournal")
            .field("node_id", &self.node_id)
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
        let node_id = initialize(&database)?;
        tracing::debug!(
            path = %path.display(),
            elapsed_ms = initialized.elapsed().as_millis(),
            "redb journal metadata initialized"
        );
        Ok(Self { database, node_id })
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

fn initialize(database: &Database) -> Result<NodeId, NodeError> {
    if let Some(node_id) = read_existing_node_id(database)? {
        return Ok(node_id);
    }

    let mut write = database.begin_write().map_err(backend_error)?;
    write
        .set_durability(Durability::Immediate)
        .map_err(backend_error)?;
    let node_id = {
        let mut meta = write.open_table(META).map_err(backend_error)?;
        let encoded = meta
            .get(NODE_ID_KEY)
            .map_err(backend_error)?
            .map(|value| value.value().to_vec());
        if let Some(encoded) = encoded {
            serde_json::from_slice(&encoded).map_err(backend_error)?
        } else {
            let node_id = NodeId::new();
            let encoded = serde_json::to_vec(&node_id).map_err(backend_error)?;
            meta.insert(NODE_ID_KEY, encoded.as_slice())
                .map_err(backend_error)?;
            node_id
        }
    };
    drop(write.open_table(EVENTS).map_err(backend_error)?);
    drop(write.open_table(ORIGINS).map_err(backend_error)?);
    drop(
        write
            .open_table(REPLICATION_CHECKPOINTS)
            .map_err(backend_error)?,
    );
    write.commit().map_err(backend_error)?;
    Ok(node_id)
}

fn read_existing_node_id(database: &Database) -> Result<Option<NodeId>, NodeError> {
    let read = database.begin_read().map_err(backend_error)?;
    let meta = match read.open_table(META) {
        Ok(meta) => meta,
        Err(TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(error) => return Err(backend_error(error)),
    };
    meta.get(NODE_ID_KEY)
        .map_err(backend_error)?
        .map(|encoded| serde_json::from_slice(encoded.value()).map_err(backend_error))
        .transpose()
}

fn backend_error(error: impl fmt::Display) -> NodeError {
    NodeError::Backend(error.to_string())
}

#[cfg(test)]
mod tests {
    use myko_federation::{
        AccessPolicy, AllowAllAccessPolicy, BatchId, ChangeBatch, CommandId, CommandRequest,
        CommandState, LogPosition, PrincipalId, ScopeId, ServiceId,
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
}

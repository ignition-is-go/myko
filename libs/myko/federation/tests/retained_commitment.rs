use std::error::Error;

use chrono::{DateTime, Utc};
use myko_federation::*;
use sha2::{Digest, Sha256};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn Error>>;

fn event(sequence: u64, command_byte: u8, scope: &ScopeId, payload: &[u8]) -> EventEnvelope {
    let node_id = NodeId::from_uuid(Uuid::from_bytes([1; 16]));
    let origin = EventId::new(node_id, LogPosition::new(sequence));
    let principal = PrincipalId::new("test:commitment");
    let mut command_id = [0; 16];
    command_id[15] = command_byte;
    EventEnvelope {
        position: LogPosition::new(sequence),
        origin,
        recorded_at: DateTime::<Utc>::from_timestamp(
            1_700_000_000_i64.saturating_add(i64::from(command_byte)),
            123_000_000,
        )
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
        event: NodeEvent::CommandLifecycle(CommandSnapshot {
            request: CommandRequest {
                id: CommandId::from_uuid(Uuid::from_bytes(command_id)),
                service_id: ServiceId::new("commitment"),
                scope_id: scope.clone(),
                principal_id: principal.clone(),
                authority: AuthorityPresentation::direct_node(principal),
                resource_claims: Vec::new(),
                application_capabilities: Vec::new(),
                arguments_digest: None,
                command_type: "commitment.fixture".to_owned(),
                payload: payload.to_vec(),
            },
            state: CommandState::Submitted,
            result: None,
            updated_at: origin,
        }),
    }
}

fn manifest(
    events: &[EventEnvelope],
    selection: &ScopeSelection,
    prefix: Option<EventEnvelope>,
) -> Result<SelectedHistoryManifest, Box<dyn Error>> {
    let node = Node::in_memory();
    if let Some(prefix) = prefix {
        node.ingest(prefix)?;
    }
    for event in events {
        node.ingest(event.clone())?;
    }
    Ok(SelectedHistorySnapshot::current(&node)?.retained_manifest(selection)?)
}

fn commitment(
    events: &[EventEnvelope],
    selection: &ScopeSelection,
    prefix: Option<EventEnvelope>,
) -> Result<RetainedHistoryCommitment, Box<dyn Error>> {
    Ok(manifest(events, selection, prefix)?.commitment()?)
}

#[test]
fn retained_history_statement_signing_bytes_match_version_one_golden() -> TestResult {
    let scope = ScopeId::new("commitment:selected");
    let events = [event(1, 1, &scope, b"one"), event(2, 2, &scope, b"two")];
    let manifest = manifest(&events, &ScopeSelection::Exact(scope), None)?;
    let statement = RetainedHistoryStatement::new(
        NodeId::from_uuid(Uuid::from_bytes([3; 16])),
        StorageIncarnationId::from_uuid(Uuid::from_bytes([4; 16])),
        EventId::new(
            NodeId::from_uuid(Uuid::from_bytes([5; 16])),
            LogPosition::new(42),
        ),
        &manifest,
    )?;
    let signing_bytes = statement.signing_bytes()?;
    if !signing_bytes.starts_with(b"myko.retained-history-statement.v1\0")
        || signing_bytes.starts_with(b"myko.retained-history-commitment")
    {
        return Err("statement signing domain is not distinct from the commitment domain".into());
    }
    let actual: [u8; 32] = Sha256::digest(&signing_bytes).into();
    let expected = [
        0x03, 0x18, 0x02, 0x36, 0x25, 0x69, 0x97, 0x96, 0xb4, 0x76, 0x65, 0x6c, 0x18, 0xc2, 0xce,
        0x39, 0xe8, 0x4b, 0x91, 0xbf, 0x27, 0x64, 0xb6, 0x3d, 0xec, 0x0a, 0x5f, 0xce, 0xa6, 0x54,
        0xe9, 0x6e,
    ];
    if actual != expected {
        return Err(format!("statement signing bytes golden mismatch: {actual:02x?}").into());
    }
    Ok(())
}

#[test]
fn commitment_ignores_observer_positions_cut_and_input_order() -> TestResult {
    let scope = ScopeId::new("commitment:selected");
    let first = event(1, 1, &scope, b"one");
    let second = event(2, 2, &scope, b"two");
    let direct = commitment(
        &[first.clone(), second.clone()],
        &ScopeSelection::Exact(scope.clone()),
        None,
    )?;
    let mut prefix = event(1, 9, &ScopeId::new("commitment:unrelated"), b"prefix");
    let prefix_origin = EventId::new(
        NodeId::from_uuid(Uuid::from_bytes([9; 16])),
        LogPosition::new(1),
    );
    prefix.origin = prefix_origin;
    let NodeEvent::CommandLifecycle(prefix_command) = &mut prefix.event else {
        return Err("prefix fixture event is not lifecycle".into());
    };
    prefix_command.updated_at = prefix_origin;
    let reordered = commitment(
        &[second, first],
        &ScopeSelection::Exact(scope),
        Some(prefix),
    )?;
    if direct != reordered || direct.event_count() != 2 {
        return Err("equivalent immutable event sets produced different commitments".into());
    }
    Ok(())
}

#[test]
fn commitment_binds_every_immutable_field_selection_and_event() -> TestResult {
    let scope = ScopeId::new("commitment:selected");
    let first = event(1, 1, &scope, b"one");
    let second = event(2, 2, &scope, b"two");
    let baseline = commitment(
        &[first.clone(), second.clone()],
        &ScopeSelection::Exact(scope.clone()),
        None,
    )?;
    let mut changed_body = second.clone();
    let NodeEvent::CommandLifecycle(command) = &mut changed_body.event else {
        return Err("fixture event is not lifecycle".into());
    };
    command.request.payload = b"changed".to_vec();
    let mut changed_timestamp = second.clone();
    changed_timestamp.recorded_at += std::time::Duration::from_secs(1);
    let mut changed_origin = second.clone();
    changed_origin.origin = EventId::new(
        NodeId::from_uuid(Uuid::from_bytes([2; 16])),
        changed_origin.origin.sequence,
    );
    let variants = [
        commitment(
            &[first.clone(), changed_body],
            &ScopeSelection::Exact(scope.clone()),
            None,
        )?,
        commitment(
            &[first.clone(), changed_timestamp],
            &ScopeSelection::Exact(scope.clone()),
            None,
        )?,
        commitment(
            &[first.clone(), changed_origin],
            &ScopeSelection::Exact(scope.clone()),
            None,
        )?,
        commitment(
            &[first.clone(), second],
            &ScopeSelection::Subtree(scope.clone()),
            None,
        )?,
        commitment(
            std::slice::from_ref(&first),
            &ScopeSelection::Exact(scope),
            None,
        )?,
    ];
    if variants.iter().any(|variant| variant == &baseline) {
        return Err("commitment ignored changed immutable manifest content".into());
    }
    let expected = [
        0xb4, 0x0a, 0xd8, 0x08, 0xbe, 0xf5, 0x71, 0x11, 0x26, 0x92, 0xc6, 0x5f, 0x71, 0x6d, 0x54,
        0x86, 0x88, 0x8b, 0xd0, 0x06, 0xc3, 0xcd, 0x80, 0x24, 0x03, 0x28, 0xf3, 0xe4, 0xbc, 0xa1,
        0xcd, 0xd5,
    ];
    if baseline.digest() != &expected {
        return Err("commitment golden mismatch".into());
    }
    Ok(())
}

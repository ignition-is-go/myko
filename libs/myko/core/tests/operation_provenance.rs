pub use myko::*;
use myko::{
    command::CommandContext,
    entities::{
        client::Client,
        server::{Server, ServerId},
    },
    prelude::*,
    request::RequestContext,
    server::{
        EventRecord, HandlerRegistry, MykoServerContext, MykoServerRuntime, PersistError,
        Persister, RelationshipManager, persister::PersisterRouter,
    },
    store::StoreRegistry,
    wire::{MEvent, MEventType},
};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Default)]
struct Recording(Mutex<Vec<EventRecord>>, Mutex<Option<CommandContext>>);
impl Persister for Recording {
    fn persist(&self, event: MEvent) -> Result<(), PersistError> {
        self.persist_record(EventRecord {
            event,
            provenance: None,
        })
    }
    fn persist_record(&self, record: EventRecord) -> Result<(), PersistError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(record);
        let nested = self
            .1
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(nested) = nested {
            nested
                .emit_set(&child("reentrant"))
                .map_err(|e| PersistError {
                    entity_type: "Client".into(),
                    message: e.message,
                })?;
        }
        Ok(())
    }
    fn supports_provenance(&self) -> bool {
        true
    }
}
fn setup() -> (
    Arc<MykoServerContext>,
    Arc<Recording>,
    flume::Receiver<MEvent>,
) {
    let recorder = Arc::new(Recording::default());
    let mut router = PersisterRouter::default();
    router.set_default(Some(recorder.clone()));
    let (tx, rx) = flume::unbounded();
    let server = Arc::new(MykoServerContext::new(
        Uuid::new_v4(),
        Arc::new(StoreRegistry::new()),
        Arc::new(HandlerRegistry::new()),
        Arc::new(RelationshipManager::new()),
        Arc::new(router),
        Arc::new(myko::search::SearchIndex::new()),
        MykoServerRuntime {
            peer_clients: Arc::new(dashmap::DashMap::new()),
            event_sink: Some(tx),
            history_replay: None,
        },
    ));
    (server, recorder, rx)
}
fn context(server: Arc<MykoServerContext>) -> CommandContext {
    let req = Arc::new(RequestContext::from_client(
        "caller-chosen-tx".into(),
        "unverified-connection".into(),
        server.host_id,
    ));
    CommandContext::new("EditTree".into(), req, server)
}
fn parent() -> Server {
    Server {
        id: "parent".into(),
        version: "test".into(),
        address: "test".into(),
        port: 0,
        started_at: "test".into(),
    }
}
fn child(id: &str) -> Client {
    Client {
        id: id.into(),
        server_id: ServerId::from(Arc::<str>::from("parent")),
        address: None,
        windback: None,
    }
}

#[myko_command]
pub struct NestedEdit {}
impl CommandHandler for NestedEdit {
    fn execute(self, ctx: CommandContext) -> Result<(), myko::command::CommandError> {
        ctx.emit_set(&child("nested"))
    }
}

#[test]
fn command_emissions_batches_replacements_and_cascades_share_operation() -> anyhow::Result<()> {
    let (server, recorder, sink) = setup();
    let ctx = context(server.clone());
    let cloned = ctx.clone(); // Cloning before the first emit must keep one operation.
    ctx.emit_set(&parent())
        .map_err(|e| anyhow::anyhow!(e.message))?;
    ctx.emit_set_batch(&[child("one"), child("two")])
        .map_err(|e| anyhow::anyhow!(e.message))?;
    let mixed: Vec<Arc<dyn AnyItem>> = vec![Arc::new(child("three")), Arc::new(parent())];
    cloned
        .emit_set_any_batch(mixed)
        .map_err(|e| anyhow::anyhow!(e.message))?;
    ctx.emit_replace_batch(&[child("four")], &[Arc::new(child("one"))])
        .map_err(|e| anyhow::anyhow!(e.message))?;
    let raw = MEvent::from_item(&child("five"), MEventType::SET, "imported-host");
    ctx.emit_event_batch(vec![raw])
        .map_err(|e| anyhow::anyhow!(e.message))?;
    ctx.execute_command(NestedEdit {})
        .map_err(|e| anyhow::anyhow!(e.message))?;
    ctx.emit_del(&parent())
        .map_err(|e| anyhow::anyhow!(e.message))?;
    let records = recorder
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    anyhow::ensure!(
        records
            .iter()
            .any(|r| r.event.change_type == MEventType::DEL
                && r.event.item.get("id").and_then(serde_json::Value::as_str) == Some("five")),
        "subtree cascade must be recorded"
    );
    let first = records
        .first()
        .and_then(|r| r.provenance.as_ref())
        .ok_or_else(|| anyhow::anyhow!("missing provenance"))?;
    anyhow::ensure!(first.operation_id.as_ref() != "caller-chosen-tx");
    for record in &records {
        anyhow::ensure!(record.provenance.as_ref() == Some(first));
        anyhow::ensure!(record.event.tx.as_ref() == "caller-chosen-tx");
        let json = serde_json::to_string(record)?;
        anyhow::ensure!(serde_json::from_str::<EventRecord>(&json)? == *record);
        anyhow::ensure!(
            serde_json::from_str::<MEvent>(&json)? == record.event,
            "old readers ignore sidecar"
        );
    }
    anyhow::ensure!(sink.try_iter().all(|e| e.tx.as_ref() == "caller-chosen-tx"));
    drop(records);
    server.set(&parent())?;
    let records = recorder
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    anyhow::ensure!(
        records.last().is_some_and(|r| r.provenance.is_none()),
        "scope must end after command emission"
    );
    Ok(())
}

#[test]
fn independent_contexts_do_not_trust_reused_request_tx() -> anyhow::Result<()> {
    let (server, recorder, _) = setup();
    context(server.clone())
        .emit_set(&parent())
        .map_err(|e| anyhow::anyhow!(e.message))?;
    context(server)
        .emit_set(&parent())
        .map_err(|e| anyhow::anyhow!(e.message))?;
    let records = recorder
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let ids: Vec<_> = records
        .iter()
        .filter_map(|r| r.provenance.as_ref().map(|p| p.operation_id.clone()))
        .collect();
    anyhow::ensure!(ids.len() == 2 && ids.first() != ids.last());
    Ok(())
}

#[test]
fn legacy_events_and_custom_persisters_remain_compatible() -> anyhow::Result<()> {
    struct Legacy(Mutex<Vec<MEvent>>);
    impl Persister for Legacy {
        fn persist(&self, event: MEvent) -> Result<(), PersistError> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(event);
            Ok(())
        }
    }
    let event = MEvent::from_item(&parent(), MEventType::SET, "legacy");
    let record: EventRecord = serde_json::from_value(serde_json::to_value(&event)?)?;
    anyhow::ensure!(record.provenance.is_none());
    let legacy = Legacy(Mutex::new(Vec::new()));
    anyhow::ensure!(!legacy.supports_provenance());
    legacy.persist_record(record)?;
    anyhow::ensure!(
        legacy
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .first()
            == Some(&event)
    );
    Ok(())
}

#[test]
fn queued_reentrant_command_retains_its_own_operation() -> anyhow::Result<()> {
    let (server, recorder, _) = setup();
    let outer = context(server.clone());
    outer
        .emit_set(&parent())
        .map_err(|e| anyhow::anyhow!(e.message))?;
    let nested = context(server);
    *recorder
        .1
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(nested);
    outer
        .emit_del(&parent())
        .map_err(|e| anyhow::anyhow!(e.message))?;
    let records = recorder
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let root = records.first().and_then(|r| r.provenance.as_ref());
    let nested = records
        .iter()
        .find(|r| r.event.item.get("id").and_then(serde_json::Value::as_str) == Some("reentrant"))
        .and_then(|r| r.provenance.as_ref());
    anyhow::ensure!(
        root.is_some() && nested.is_some() && root != nested,
        "queued emission must retain the enqueueing operation"
    );
    Ok(())
}

#[test]
fn serialized_dispatch_uses_context_identity_not_wire_transaction() -> anyhow::Result<()> {
    use myko::command::{CommandExecutorAdapter, DynCommandExecutor};
    let (server, recorder, _) = setup();
    CommandExecutorAdapter::<NestedEdit>::new()
        .execute_from_value(
            serde_json::json!({ "tx": "forged-wire-label" }),
            context(server),
        )
        .map_err(|e| anyhow::anyhow!(e.message))?;
    let records = recorder
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    anyhow::ensure!(records.len() == 1);
    anyhow::ensure!(records.first().is_some_and(|r| {
        r.event.tx.as_ref() == "caller-chosen-tx"
            && r.provenance
                .as_ref()
                .is_some_and(|p| p.request_tx.as_ref() == "caller-chosen-tx")
    }));
    Ok(())
}

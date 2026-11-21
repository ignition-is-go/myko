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
                .emit_set(child("reentrant"))
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
    setup_with_recording(true)
}

fn setup_with_recording(
    record: bool,
) -> (
    Arc<MykoServerContext>,
    Arc<Recording>,
    flume::Receiver<MEvent>,
) {
    let recorder = Arc::new(Recording::default());
    let mut router = PersisterRouter::default();
    if record {
        router.set_default(Some(recorder.clone()));
    }
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
        ctx.emit_set(child("nested"))
    }
}

#[test]
fn command_emissions_batches_replacements_and_cascades_share_transaction() -> anyhow::Result<()> {
    let (server, recorder, sink) = setup();
    let ctx = context(server.clone());
    let cloned = ctx.clone(); // Cloning before the first emit must keep one operation.
    ctx.emit_set(parent())
        .map_err(|e| anyhow::anyhow!(e.message))?;
    ctx.emit_set_batch([child("one"), child("two")])
        .map_err(|e| anyhow::anyhow!(e.message))?;
    let mixed: Vec<Arc<dyn AnyItem>> = vec![Arc::new(child("three")), Arc::new(parent())];
    cloned
        .emit_set_any_batch(mixed)
        .map_err(|e| anyhow::anyhow!(e.message))?;
    ctx.emit_replace_batch([child("four")], &[Arc::new(child("one"))])
        .map_err(|e| anyhow::anyhow!(e.message))?;
    let raw = MEvent::from_item(&child("five"), MEventType::SET, "imported-host");
    ctx.emit_event_batch(vec![raw])
        .map_err(|e| anyhow::anyhow!(e.message))?;
    ctx.execute_command(NestedEdit {})
        .map_err(|e| anyhow::anyhow!(e.message))?;
    ctx.emit_del(parent())
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
    server.set(parent())?;
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
fn independent_contexts_can_share_transaction() -> anyhow::Result<()> {
    let (server, recorder, _) = setup();
    context(server.clone())
        .emit_set(parent())
        .map_err(|e| anyhow::anyhow!(e.message))?;
    context(server)
        .emit_set(parent())
        .map_err(|e| anyhow::anyhow!(e.message))?;
    let records = recorder
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    anyhow::ensure!(records.len() == 2);
    anyhow::ensure!(
        records
            .iter()
            .all(|record| record.event.tx.as_ref() == "caller-chosen-tx")
    );
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
        .emit_set(parent())
        .map_err(|e| anyhow::anyhow!(e.message))?;
    let mut nested = context(server);
    Arc::make_mut(&mut nested.req).tx = "reentrant-tx".into();
    *recorder
        .1
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(nested);
    outer
        .emit_del(parent())
        .map_err(|e| anyhow::anyhow!(e.message))?;
    let records = recorder
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    anyhow::ensure!(records.iter().all(|record| {
        let reentrant = record
            .event
            .item
            .get("id")
            .and_then(serde_json::Value::as_str)
            == Some("reentrant");
        record.event.tx.as_ref()
            == if reentrant {
                "reentrant-tx"
            } else {
                "caller-chosen-tx"
            }
    }));
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

#[myko_query(Server)]
pub struct TransactionQuery {
    pub expected_tx: String,
}

impl QueryHandler for TransactionQuery {
    fn test_entity(ctx: QueryTestContext<Self>) -> bool {
        ctx.query_context.tx() == ctx.query.expected_tx
    }
}

#[myko_report(String)]
pub struct TransactionLeafReport {}

impl ReportHandler for TransactionLeafReport {
    type Output = String;

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<String>, Definite> {
        let matches = ctx.query_map(TransactionQuery {
            expected_tx: ctx.tx().to_owned(),
        });
        assert_eq!(matches.snapshot().len(), 1);
        hyphae::Cell::new(Arc::new(ctx.tx().to_owned())).lock()
    }
}

#[myko_report(String)]
pub struct TransactionReport {}

impl ReportHandler for TransactionReport {
    type Output = String;

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<String>, Definite> {
        ctx.report(TransactionLeafReport {})
    }
}

#[myko_command]
pub struct ReadThenWrite {}

impl CommandHandler for ReadThenWrite {
    fn execute(self, ctx: CommandContext) -> Result<(), myko::command::CommandError> {
        let query = TransactionQuery {
            expected_tx: ctx.tx().to_owned(),
        };
        let rows = ctx.exec_query(query.clone())?;
        let first = ctx.exec_query_first(query)?;
        let report_tx = ctx.exec_report(TransactionReport {})?;
        if rows.len() != 1 || first.is_none() || report_tx != ctx.tx() {
            return Err(myko::command::CommandError::new(
                ctx.tx().to_owned(),
                ctx.command_id.to_string(),
                "query or report lost the originating transaction".to_owned(),
            ));
        }
        ctx.execute_command(NestedEdit {})
    }
}

#[test]
fn command_queries_reports_and_nested_writes_keep_originating_transaction() -> anyhow::Result<()> {
    let (server, recorder, _) = setup();
    server.set(parent())?;
    context(server)
        .execute_command(ReadThenWrite {})
        .map_err(|error| anyhow::anyhow!(error.message))?;
    let records = recorder
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    anyhow::ensure!(
        records
            .last()
            .is_some_and(|record| record.event.tx.as_ref() == "caller-chosen-tx")
    );
    drop(records);
    Ok(())
}

#[test]
fn sink_only_commands_keep_originating_transaction() -> anyhow::Result<()> {
    let (server, _, sink) = setup_with_recording(false);
    let ctx = context(server);
    ctx.emit_set(parent())
        .map_err(|error| anyhow::anyhow!(error.message))?;
    ctx.emit_set_batch([child("one"), child("two")])
        .map_err(|error| anyhow::anyhow!(error.message))?;
    let events: Vec<_> = sink.try_iter().collect();
    anyhow::ensure!(events.len() == 3);
    anyhow::ensure!(
        events
            .iter()
            .all(|event| event.tx.as_ref() == "caller-chosen-tx")
    );
    Ok(())
}

#[myko_saga]
pub struct TransactionSaga;

impl myko::saga::SagaHandler for TransactionSaga {
    type EventItem = Server;
    type Command = ReadThenWrite;
    const EVENT_TYPE: MEventType = MEventType::SET;

    fn handle(
        _item: Server,
        _event: MEvent,
        _ctx: Arc<myko::saga::SagaContext>,
    ) -> Option<Self::Command> {
        Some(ReadThenWrite {})
    }
}

#[test]
fn saga_commands_keep_event_transaction_across_async_dispatch() -> anyhow::Result<()> {
    use myko::futures::StreamExt;
    use myko::saga::{AnySaga, SagaContext};

    let (server, recorder, _) = setup();
    let ctx = context(server.clone());
    ctx.emit_set(parent())
        .map_err(|error| anyhow::anyhow!(error.message))?;
    let event = recorder
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .first()
        .ok_or_else(|| anyhow::anyhow!("missing originating event"))?
        .event
        .clone();
    let events = Box::pin(myko::futures::stream::iter(vec![event]));
    let saga_ctx = Arc::new(SagaContext::new(server.host_id, server.registry.clone()));
    let mut commands = TransactionSaga.build_boxed(events, saga_ctx);
    let command = myko::futures::executor::block_on(commands.next())
        .ok_or_else(|| anyhow::anyhow!("missing saga command"))?;
    anyhow::ensure!(
        command
            .originating_tx()
            .is_some_and(|tx| tx.as_ref() == "caller-chosen-tx")
    );
    let request = Arc::new(RequestContext::internal(
        "unrelated-dispatch-tx".into(),
        server.host_id,
        "saga",
    ));
    let dispatch_ctx = CommandContext::new("ReadThenWrite".into(), request, server);
    std::thread::spawn(move || command.execute_boxed(dispatch_ctx))
        .join()
        .map_err(|_| anyhow::anyhow!("saga dispatch thread panicked"))?
        .map_err(|error| anyhow::anyhow!(error.message))?;
    let records = recorder
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    anyhow::ensure!(
        records
            .iter()
            .all(|record| record.event.tx.as_ref() == "caller-chosen-tx")
    );
    drop(records);
    Ok(())
}

//! Real `PostgreSQL` acceptance tests for authenticated causal undo.
pub use myko::*;
use myko::{
    command::CommandContext,
    entities::{
        framework_reports::EventsForTransaction,
        undo::{
            GetUndoTransactions, TransactionUndo, UndoLastTransaction, UndoTransaction,
            UndoTransactions,
        },
    },
    prelude::*,
    request::RequestContext,
    server::{
        HandlerRegistry, HistoryReplayProvider, MykoServerContext, MykoServerRuntime, Persister,
        RelationshipManager, persister::PersisterRouter,
    },
    store::StoreRegistry,
};
use myko_auth::{
    AuthError, AuthSession, AuthenticatedIdentity, AuthorizationRequest, Authorizer, Principal,
    PrincipalKind,
};
use myko_server::postgres::{
    PostgresConfig, PostgresHistoryReplayProvider, PostgresProducer, PostgresProducerHandle,
};
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

mod scene {
    use myko::prelude::*;

    #[myko_item(undoable)]
    pub struct UndoScene {
        pub name: String,
    }
}
pub use scene::*;

mod node {
    use super::{UndoScene, UndoSceneId};
    use myko::prelude::*;
    #[myko_item(undoable)]
    pub struct UndoNode {
        #[belongs_to(UndoScene)]
        pub scene_id: UndoSceneId,
        pub name: String,
    }
}
pub use node::*;

mod position {
    use super::{UndoNode, UndoNodeId};
    use myko::prelude::*;
    #[myko_item(undoable)]
    pub struct UndoPosition {
        #[ensure_for(UndoNode)]
        pub node_id: UndoNodeId,
        #[serde(default)]
        pub x: Option<f64>,
    }
}
pub use position::*;

mod value {
    use super::{UndoNode, UndoNodeId};
    use myko::prelude::*;
    #[myko_item(undoable)]
    pub struct UndoValue {
        #[belongs_to(UndoNode)]
        pub node_id: UndoNodeId,
        pub value: String,
    }
}
pub use value::*;

struct Allow;
impl Authorizer for Allow {
    fn authorize(
        &self,
        _: &AuthenticatedIdentity,
        _: AuthorizationRequest<'_>,
    ) -> Result<(), AuthError> {
        Ok(())
    }
}
fn session(user: &str) -> AuthSession {
    AuthSession::new(
        AuthenticatedIdentity {
            user_id: Some(format!("profile:{user}").into()),
            principal: Principal {
                issuer: "trusted-test-provider".into(),
                subject: user.into(),
                kind: PrincipalKind::User,
            },
            client_id: None,
            scopes: vec![],
            delegation: vec![],
            claims: serde_json::Value::Null,
            expires_at: SystemTime::now()
                .checked_add(Duration::from_hours(1))
                .unwrap_or(SystemTime::UNIX_EPOCH),
        },
        Arc::new(Allow),
    )
}

struct Fixture {
    backend: Arc<PostgresProducerHandle>,
    provider: Arc<PostgresHistoryReplayProvider>,
    server: Arc<MykoServerContext>,
}
impl Fixture {
    fn new() -> anyhow::Result<Self> {
        let table = format!("undo_{}", uuid::Uuid::new_v4().simple());
        let config = PostgresConfig {
            url: std::env::var("MYKO_HISTORY_TEST_URL")?,
            channel: table.clone(),
            table,
        };
        let host = uuid::Uuid::new_v4();
        let producer = PostgresProducer::new(&config, host).map_err(anyhow::Error::msg)?;
        // An empty barrier also ensures the background producer has initialized its schema.
        let backend = Arc::new(producer.handle());
        backend.flush().map_err(anyhow::Error::msg)?;
        let provider = Arc::new(PostgresHistoryReplayProvider::new(config));
        let server = Self::server(
            host,
            Arc::new(StoreRegistry::new()),
            backend.clone(),
            provider.clone(),
            None,
        );
        Ok(Self {
            backend,
            provider,
            server,
        })
    }
    fn server(
        host: uuid::Uuid,
        registry: Arc<StoreRegistry>,
        backend: Arc<PostgresProducerHandle>,
        provider: Arc<PostgresHistoryReplayProvider>,
        event_sink: Option<flume::Sender<myko::wire::MEvent>>,
    ) -> Arc<MykoServerContext> {
        let mut router = PersisterRouter::default();
        router.set_default(Some(backend));
        Arc::new(MykoServerContext::new(
            host,
            registry,
            Arc::new(HandlerRegistry::new()),
            Arc::new(RelationshipManager::new()),
            Arc::new(router),
            Arc::new(myko::search::SearchIndex::new()),
            MykoServerRuntime {
                peer_clients: Arc::new(dashmap::DashMap::new()),
                event_sink,
                history_replay: Some(provider),
            },
        ))
    }
    fn context(&self, user: &str, tx: &str) -> CommandContext {
        let req = RequestContext::from_client(
            tx.into(),
            format!("socket:{user}").into(),
            self.server.host_id,
        )
        .with_auth(Some(session(user)));
        CommandContext::new("SceneEdit".into(), Arc::new(req), self.server.clone())
    }
    fn seed(&self) -> anyhow::Result<()> {
        self.server.set(&UndoScene {
            id: "scene".into(),
            name: "Scene".into(),
        })?;
        for id in ["a", "b"] {
            self.server.set(&UndoPosition {
                id: id.into(),
                node_id: id.into(),
                x: None,
            })?;
            self.server.set(&UndoNode {
                id: id.into(),
                scene_id: "scene".into(),
                name: id.into(),
            })?;
        }
        self.backend.flush().map_err(anyhow::Error::msg)
    }
    fn moved(&self, user: &str, tx: &str, moves: &[(&str, f64)]) -> anyhow::Result<()> {
        let positions = moves
            .iter()
            .map(|(id, x)| UndoPosition {
                id: (*id).into(),
                node_id: (*id).into(),
                x: Some(*x),
            })
            .collect::<Vec<_>>();
        self.context(user, tx)
            .emit_set_batch(&positions)
            .map_err(command_error)?;
        self.backend.flush().map_err(anyhow::Error::msg)
    }
    fn node(&self, id: &str) -> anyhow::Result<UndoNode> {
        self.item("UndoNode", id)
    }
    fn position(&self, id: &str) -> anyhow::Result<UndoPosition> {
        self.item("UndoPosition", id)
    }
    fn item<T: serde::de::DeserializeOwned>(&self, kind: &str, id: &str) -> anyhow::Result<T> {
        let item = self
            .server
            .registry
            .get(kind)
            .and_then(|store| store.get_value(&Arc::from(id)))
            .ok_or_else(|| anyhow::anyhow!("missing {kind}:{id}"))?;
        Ok(serde_json::from_value(item.to_value())?)
    }
    fn restart(&mut self) -> anyhow::Result<()> {
        self.backend.flush().map_err(anyhow::Error::msg)?;
        let registry = self
            .provider
            .replay_to_store(&chrono::Utc::now().to_rfc3339(), &HandlerRegistry::new())
            .map_err(anyhow::Error::msg)?;
        self.server = Self::server(
            self.server.host_id,
            registry,
            self.backend.clone(),
            self.provider.clone(),
            None,
        );
        Ok(())
    }
    fn records(&self, tx: &str) -> anyhow::Result<myko::server::TransactionHistory> {
        self.backend.flush().map_err(anyhow::Error::msg)?;
        self.provider
            .transaction_history(tx)
            .map_err(anyhow::Error::msg)
    }
}
fn command_error(error: myko::command::CommandError) -> anyhow::Error {
    anyhow::anyhow!(error.message)
}

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn alice_undoes_move_after_restart_and_preserves_bobs_rename() -> anyhow::Result<()> {
    let mut fixture = Fixture::new()?;
    fixture.seed()?;
    fixture.moved("alice", "alice-move", &[("a", 10.0), ("b", 20.0)])?;
    let mut node = fixture.node("a")?;
    node.name = "Bob's name".into();
    fixture
        .context("bob", "bob-rename")
        .emit_set(&node)
        .map_err(command_error)?;
    fixture.restart()?;
    let result = UndoLastTransaction {}
        .execute(fixture.context("alice", "alice-undo"))
        .map_err(command_error)?
        .ok_or_else(|| anyhow::anyhow!("missing undo result"))?;
    anyhow::ensure!(result.transaction_id.as_ref() == "alice-move");
    anyhow::ensure!(result.events_applied == 2);
    anyhow::ensure!(fixture.position("a")?.x.is_none() && fixture.position("b")?.x.is_none());
    anyhow::ensure!(fixture.node("a")?.name == "Bob's name");
    fixture.restart()?;
    anyhow::ensure!(fixture.position("a")?.x.is_none() && fixture.node("a")?.name == "Bob's name");
    anyhow::ensure!(
        UndoLastTransaction {}
            .execute(fixture.context("alice", "nothing-left"))
            .map_err(command_error)?
            .is_none()
    );
    let bob = UndoLastTransaction {}
        .execute(fixture.context("bob", "bob-undo"))
        .map_err(command_error)?;
    anyhow::ensure!(bob.is_some() && fixture.node("a")?.name == "a");
    let history = fixture.records("alice-move")?;
    anyhow::ensure!(
        history.complete
            && history.already_undone
            && history.author().map_err(anyhow::Error::msg)?.as_ref() == "profile:alice"
    );
    Ok(())
}

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn later_same_entity_edit_rejects_entire_undo_without_writes() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    fixture.seed()?;
    fixture.moved("alice", "alice-move", &[("a", 10.0), ("b", 20.0)])?;
    // Even an equal-value later write is a distinct revision owned by Bob.
    fixture.moved("bob", "bob-move", &[("a", 10.0)])?;
    let error = UndoTransaction {
        transaction_id: "alice-move".into(),
    }
    .execute(fixture.context("alice", "rejected-undo"))
    .err()
    .ok_or_else(|| anyhow::anyhow!("conflicting undo was accepted"))?;
    anyhow::ensure!(error.message.contains("UndoPosition:a"));
    anyhow::ensure!(
        fixture.position("a")?.x == Some(10.0) && fixture.position("b")?.x == Some(20.0)
    );
    anyhow::ensure!(!fixture.records("alice-move")?.already_undone);
    anyhow::ensure!(
        fixture
            .provider
            .transaction_history("rejected-undo")
            .is_err()
    );
    Ok(())
}

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn undo_enforces_authenticated_author_for_reads_and_writes() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    fixture.seed()?;
    fixture.moved("alice", "alice-move", &[("a", 10.0)])?;
    anyhow::ensure!(
        UndoTransaction {
            transaction_id: "alice-move".into()
        }
        .execute(fixture.context("bob", "bob-forged-undo"))
        .is_err()
    );
    let mut anonymous = fixture.context("alice", "anonymous-undo");
    Arc::make_mut(&mut anonymous.req).auth = None;
    anyhow::ensure!(UndoLastTransaction {}.execute(anonymous).is_err());
    let bob = fixture
        .context("bob", "bob-history")
        .exec_report(EventsForTransaction {
            transaction_id: "alice-move".into(),
        })
        .map_err(command_error)?;
    anyhow::ensure!(bob.is_err());
    let alice = fixture
        .context("alice", "alice-history")
        .exec_report(EventsForTransaction {
            transaction_id: "alice-move".into(),
        })
        .map_err(command_error)?;
    anyhow::ensure!(alice.map_err(anyhow::Error::msg)?.len() == 1);
    anyhow::ensure!(fixture.position("a")?.x == Some(10.0));
    Ok(())
}

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn deletion_undo_resurrects_entities_and_original_relationship_ids() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    fixture.seed()?;
    let original = fixture.node("a")?;
    fixture
        .context("alice", "delete-node")
        .emit_del(&original)
        .map_err(command_error)?;
    anyhow::ensure!(fixture.node("a").is_err() && fixture.position("a").is_err());
    let result = UndoTransaction {
        transaction_id: "delete-node".into(),
    }
    .execute(fixture.context("alice", "revive-node"))
    .map_err(command_error)?;
    anyhow::ensure!(result.events_applied == 2);
    anyhow::ensure!(fixture.node("a")?.scene_id == UndoSceneId::from("scene"));
    anyhow::ensure!(fixture.position("a")?.node_id == UndoNodeId::from("a"));
    Ok(())
}

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn undo_creation_cannot_cascade_delete_a_newer_child() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    fixture.seed()?;
    fixture
        .context("alice", "create-node")
        .emit_set(&UndoNode {
            id: "new".into(),
            scene_id: "scene".into(),
            name: "New".into(),
        })
        .map_err(command_error)?;
    fixture
        .context("bob", "add-value")
        .emit_set(&UndoValue {
            id: "value".into(),
            node_id: "new".into(),
            value: "Bob's value".into(),
        })
        .map_err(command_error)?;
    let error = UndoTransaction {
        transaction_id: "create-node".into(),
    }
    .execute(fixture.context("alice", "reject-delete"))
    .err()
    .ok_or_else(|| anyhow::anyhow!("new dependent was deleted"))?;
    anyhow::ensure!(error.message.contains("newer dependent"));
    anyhow::ensure!(
        fixture.node("new").is_ok()
            && fixture.item::<UndoValue>("UndoValue", "value")?.value == "Bob's value"
    );
    Ok(())
}

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn sequential_undo_walks_the_users_stack_after_restart() -> anyhow::Result<()> {
    let mut fixture = Fixture::new()?;
    fixture.seed()?;
    fixture.moved("alice", "first", &[("a", 1.0)])?;
    fixture.moved("alice", "second", &[("a", 2.0)])?;
    UndoLastTransaction {}
        .execute(fixture.context("alice", "undo-second"))
        .map_err(command_error)?;
    anyhow::ensure!(fixture.position("a")?.x == Some(1.0));
    fixture.restart()?;
    UndoLastTransaction {}
        .execute(fixture.context("alice", "undo-first"))
        .map_err(command_error)?;
    anyhow::ensure!(fixture.position("a")?.x.is_none());
    Ok(())
}

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn incomplete_transaction_remains_ineligible_after_restart() -> anyhow::Result<()> {
    let mut fixture = Fixture::new()?;
    fixture.seed()?;
    let unfinished = fixture.context("alice", "unfinished");
    unfinished
        .emit_set(&UndoPosition {
            id: "a".into(),
            node_id: "a".into(),
            x: Some(5.0),
        })
        .map_err(command_error)?;
    fixture.restart()?;
    anyhow::ensure!(!fixture.records("unfinished")?.complete);
    anyhow::ensure!(
        UndoTransaction {
            transaction_id: "unfinished".into()
        }
        .execute(fixture.context("alice", "reject-incomplete"))
        .is_err()
    );
    anyhow::ensure!(fixture.position("a")?.x == Some(5.0));
    drop(unfinished);
    Ok(())
}

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn concurrent_undo_applies_once() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    fixture.seed()?;
    fixture.moved("alice", "move", &[("a", 1.0)])?;
    let first = fixture.context("alice", "undo-one");
    let second = fixture.context("alice", "undo-two");
    let results = std::thread::scope(|scope| {
        let one = scope.spawn(|| {
            UndoTransaction {
                transaction_id: "move".into(),
            }
            .execute(first)
        });
        let two = scope.spawn(|| {
            UndoTransaction {
                transaction_id: "move".into(),
            }
            .execute(second)
        });
        (one.join(), two.join())
    });
    let one = results
        .0
        .map_err(|_| anyhow::anyhow!("undo thread panicked"))?;
    let two = results
        .1
        .map_err(|_| anyhow::anyhow!("undo thread panicked"))?;
    anyhow::ensure!(one.is_ok() != two.is_ok());
    anyhow::ensure!(fixture.position("a")?.x.is_none());
    Ok(())
}

mod trigger {
    use super::UndoNodeId;
    use myko::prelude::*;
    #[myko_item(undoable)]
    pub struct UndoTrigger {
        pub node_id: UndoNodeId,
    }
}
use trigger::*;

#[myko_command]
pub struct SagaMove {
    pub node_id: UndoNodeId,
}
impl CommandHandler for SagaMove {
    fn execute(self, ctx: CommandContext) -> Result<(), myko::command::CommandError> {
        ctx.emit_set(&UndoPosition {
            id: self.node_id.0.clone().into(),
            node_id: self.node_id,
            x: Some(7.0),
        })
    }
}
#[myko_saga]
pub struct AttributionSaga;
impl myko::saga::SagaHandler for AttributionSaga {
    type EventItem = UndoTrigger;
    type Command = SagaMove;
    const EVENT_TYPE: myko::wire::MEventType = myko::wire::MEventType::SET;
    fn handle(
        item: UndoTrigger,
        _: myko::wire::MEvent,
        _: Arc<myko::saga::SagaContext>,
    ) -> Option<Self::Command> {
        Some(SagaMove {
            node_id: item.node_id,
        })
    }
}

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn saga_retains_verified_actor_and_delays_completion_until_causal_work_finishes()
-> anyhow::Result<()> {
    use myko::futures::StreamExt;
    use myko::saga::{AnySaga, SagaContext};
    let mut fixture = Fixture::new()?;
    fixture.seed()?;
    let (sink, events) = flume::unbounded();
    fixture.server = Fixture::server(
        fixture.server.host_id,
        fixture.server.registry.clone(),
        fixture.backend.clone(),
        fixture.provider.clone(),
        Some(sink),
    );
    fixture
        .context("alice", "saga-root")
        .emit_set(&UndoTrigger {
            id: "trigger".into(),
            node_id: "a".into(),
        })
        .map_err(command_error)?;
    anyhow::ensure!(!fixture.records("saga-root")?.complete);
    let event = events.recv_timeout(Duration::from_secs(1))?;
    let saga_context = Arc::new(SagaContext::new(
        fixture.server.host_id,
        fixture.server.registry.clone(),
    ));
    let stream = Box::pin(myko::futures::stream::iter(vec![event]));
    let mut commands = AttributionSaga.build_boxed(stream, saga_context);
    let command = myko::futures::executor::block_on(commands.next())
        .ok_or_else(|| anyhow::anyhow!("saga produced no command"))?;
    anyhow::ensure!(
        UndoTransaction {
            transaction_id: "saga-root".into()
        }
        .execute(fixture.context("alice", "too-early"))
        .is_err()
    );
    let request = Arc::new(RequestContext::internal(
        "saga-root".into(),
        fixture.server.host_id,
        "saga",
    ));
    let context = CommandContext::new("SagaMove".into(), request, fixture.server.clone());
    std::thread::spawn(move || command.execute_boxed(context))
        .join()
        .map_err(|_| anyhow::anyhow!("saga thread panicked"))?
        .map_err(command_error)?;
    let history = fixture.records("saga-root")?;
    anyhow::ensure!(history.complete && history.records.len() == 2);
    anyhow::ensure!(history.author().map_err(anyhow::Error::msg)?.as_ref() == "profile:alice");
    anyhow::ensure!(fixture.position("a")?.x == Some(7.0));
    UndoTransaction {
        transaction_id: "saga-root".into(),
    }
    .execute(fixture.context("alice", "undo-saga"))
    .map_err(command_error)?;
    anyhow::ensure!(fixture.position("a")?.x.is_none());
    Ok(())
}

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn mixed_authors_and_legacy_history_are_not_undo_authority() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    fixture.seed()?;
    fixture.moved("alice", "reused-label", &[("a", 1.0)])?;
    fixture.moved("bob", "reused-label", &[("a", 2.0)])?;
    anyhow::ensure!(
        UndoTransaction {
            transaction_id: "reused-label".into()
        }
        .execute(fixture.context("alice", "reject-mixed"))
        .is_err()
    );
    let mut context = fixture.context("alice", "legacy");
    Arc::make_mut(&mut context.req).auth = None;
    context
        .emit_set(&UndoPosition {
            id: "b".into(),
            node_id: "b".into(),
            x: Some(3.0),
        })
        .map_err(command_error)?;
    drop(context);
    anyhow::ensure!(
        UndoTransaction {
            transaction_id: "legacy".into()
        }
        .execute(fixture.context("alice", "reject-legacy"))
        .is_err()
    );
    anyhow::ensure!(fixture.position("a")?.x == Some(2.0) && fixture.position("b")?.x == Some(3.0));
    Ok(())
}

mod owned {
    use super::UndoNodeId;
    use myko::prelude::*;
    #[myko_item(undoable)]
    pub struct UndoGroup {
        #[owns_many(UndoNode)]
        pub nodes: Vec<UndoNodeId>,
    }
}
use owned::*;

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn undo_cannot_rewrite_a_newer_owning_parent_or_remove_a_new_ensured_child() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    fixture.seed()?;
    fixture
        .context("alice", "create-owned")
        .emit_set(&UndoNode {
            id: "owned".into(),
            scene_id: "scene".into(),
            name: "Owned".into(),
        })
        .map_err(command_error)?;
    fixture
        .context("bob", "own-node")
        .emit_set(&UndoGroup {
            id: "group".into(),
            nodes: vec!["owned".into()],
        })
        .map_err(command_error)?;
    anyhow::ensure!(
        UndoTransaction {
            transaction_id: "create-owned".into()
        }
        .execute(fixture.context("alice", "reject-parent-fixup"))
        .is_err()
    );
    anyhow::ensure!(
        fixture.item::<UndoGroup>("UndoGroup", "group")?.nodes == vec![UndoNodeId::from("owned")]
    );
    fixture
        .context("alice", "create-ensured")
        .emit_set(&UndoNode {
            id: "ensured".into(),
            scene_id: "scene".into(),
            name: "Ensured".into(),
        })
        .map_err(command_error)?;
    fixture
        .context("bob", "add-ensured-row")
        .emit_set(&UndoPosition {
            id: "new-position".into(),
            node_id: "ensured".into(),
            x: Some(9.0),
        })
        .map_err(command_error)?;
    anyhow::ensure!(
        UndoTransaction {
            transaction_id: "create-ensured".into()
        }
        .execute(fixture.context("alice", "reject-ensured"))
        .is_err()
    );
    anyhow::ensure!(
        fixture.node("ensured").is_ok() && fixture.position("new-position")?.x == Some(9.0)
    );
    Ok(())
}

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn active_and_invalid_undo_requests_publish_no_inverse_writes() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    fixture.seed()?;
    fixture.moved("alice", "move", &[("a", 1.0)])?;
    let active = fixture.context("alice", "still-reading");
    anyhow::ensure!(
        UndoLastTransaction {}
            .execute(fixture.context("alice", "reject-active"))
            .is_err()
    );
    drop(active);
    anyhow::ensure!(
        UndoTransaction {
            transaction_id: "move".into()
        }
        .execute(fixture.context("alice", "bad\0tx"))
        .is_err()
    );
    anyhow::ensure!(fixture.position("a")?.x == Some(1.0));
    anyhow::ensure!(!fixture.records("move")?.already_undone);
    Ok(())
}

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn ordinary_entity_writes_cannot_forge_undo_receipts() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    fixture.seed()?;
    fixture.moved("alice", "move", &[("a", 1.0)])?;
    fixture
        .context("bob", "forged-receipt")
        .emit_set(&TransactionUndo {
            id: "move".into(),
            transaction_id: "move".into(),
            undo_transaction_id: "forged-receipt".into(),
            user_id: "profile:alice".into(),
            events_applied: 1,
        })
        .map_err(command_error)?;
    anyhow::ensure!(!fixture.records("move")?.already_undone);
    let result = UndoLastTransaction {}
        .execute(fixture.context("alice", "real-undo"))
        .map_err(command_error)?
        .ok_or_else(|| anyhow::anyhow!("forged receipt hid original transaction"))?;
    anyhow::ensure!(result.transaction_id.as_ref() == "move");
    anyhow::ensure!(fixture.position("a")?.x.is_none());
    Ok(())
}

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn late_causal_writes_do_not_reorder_the_originating_user_actions() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    fixture.seed()?;
    let early = fixture.context("alice", "early-action");
    early
        .emit_set(&UndoPosition {
            id: "a".into(),
            node_id: "a".into(),
            x: Some(1.0),
        })
        .map_err(command_error)?;
    fixture.moved("alice", "later-action", &[("b", 2.0)])?;
    early
        .emit_set(&UndoPosition {
            id: "a".into(),
            node_id: "a".into(),
            x: Some(3.0),
        })
        .map_err(command_error)?;
    drop(early);
    let result = UndoLastTransaction {}
        .execute(fixture.context("alice", "undo-later"))
        .map_err(command_error)?
        .ok_or_else(|| anyhow::anyhow!("missing latest transaction"))?;
    anyhow::ensure!(result.transaction_id.as_ref() == "later-action");
    anyhow::ensure!(fixture.position("a")?.x == Some(3.0) && fixture.position("b")?.x.is_none());
    Ok(())
}

mod reference {
    use myko::prelude::*;
    use std::sync::Arc;
    #[myko_item(undoable)]
    pub struct UndoReference {
        pub target_type: Arc<str>,
        pub target_id: Arc<str>,
    }
}
use reference::*;
myko::submit! { myko::relationship::ReferenceRegistration {
    local_type: UndoReference::ENTITY_NAME_STATIC,
    extract_refs: |item| item.downcast_ref::<UndoReference>().map_or_else(Vec::new, |item| {
        vec![myko::graph::EntityRef::new(item.target_type.clone(), item.target_id.clone())]
    }),
} }

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn non_owning_reference_survives_target_deletion_and_blocks_missing_target_restoration()
-> anyhow::Result<()> {
    let mut fixture = Fixture::new()?;
    fixture.seed()?;
    let reference = UndoReference {
        id: "ref".into(),
        target_type: UndoNode::ENTITY_NAME_STATIC.into(),
        target_id: "a".into(),
    };
    fixture.server.set(&reference)?;
    fixture
        .context("alice", "retarget")
        .emit_set(&UndoReference {
            target_id: "b".into(),
            ..reference
        })
        .map_err(command_error)?;
    fixture
        .context("bob", "remove-a")
        .emit_del(&fixture.node("a")?)
        .map_err(command_error)?;
    fixture.restart()?;
    anyhow::ensure!(
        fixture
            .item::<UndoReference>(UndoReference::ENTITY_NAME_STATIC, "ref")?
            .target_id
            .as_ref()
            == "b"
    );
    let error = UndoTransaction {
        transaction_id: "retarget".into(),
    }
    .execute(fixture.context("alice", "reject-ref-undo"))
    .err()
    .ok_or_else(|| anyhow::anyhow!("missing reference was restored"))?;
    anyhow::ensure!(error.message.contains("missing reference"));
    anyhow::ensure!(
        !fixture
            .provider
            .transaction_exists("reject-ref-undo")
            .map_err(anyhow::Error::msg)?
    );
    Ok(())
}

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn undo_target_creation_cannot_break_a_later_non_owning_reference() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    fixture.seed()?;
    fixture
        .context("alice", "create-target")
        .emit_set(&UndoScene {
            id: "shared".into(),
            name: "Shared".into(),
        })
        .map_err(command_error)?;
    fixture
        .context("bob", "add-reference")
        .emit_set(&UndoReference {
            id: "ref".into(),
            target_type: UndoScene::ENTITY_NAME_STATIC.into(),
            target_id: "shared".into(),
        })
        .map_err(command_error)?;
    anyhow::ensure!(
        UndoTransaction {
            transaction_id: "create-target".into()
        }
        .execute(fixture.context("alice", "reject-delete-target"))
        .is_err()
    );
    fixture
        .context("bob", "ordinary-delete")
        .emit_del(&UndoScene {
            id: "shared".into(),
            name: "Shared".into(),
        })
        .map_err(command_error)?;
    anyhow::ensure!(
        fixture
            .item::<UndoReference>(UndoReference::ENTITY_NAME_STATIC, "ref")
            .is_ok()
    );
    Ok(())
}

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn startup_boundary_is_durable_and_excludes_old_transactions_without_rewriting_history()
-> anyhow::Result<()> {
    use myko::entities::undo::{UndoHistoryBoundary, begin_undo_history};
    let mut fixture = Fixture::new()?;
    fixture.seed()?;
    fixture.moved("alice", "before-migration", &[("a", 1.0)])?;
    // Ordinary entity writes cannot establish an internal migration boundary.
    fixture
        .context("bob", "forged-boundary")
        .emit_set(&UndoHistoryBoundary {
            id: "forged".into(),
        })
        .map_err(command_error)?;
    fixture.backend.flush().map_err(anyhow::Error::msg)?;
    anyhow::ensure!(
        !fixture
            .provider
            .undo_boundary_exists("forged")
            .map_err(anyhow::Error::msg)?
    );
    anyhow::ensure!(
        begin_undo_history(&fixture.server, "migration-v1").map_err(anyhow::Error::msg)?
    );
    fixture.moved("alice", "after-migration", &[("a", 2.0)])?;
    fixture.restart()?;
    anyhow::ensure!(
        !begin_undo_history(&fixture.server, "migration-v1").map_err(anyhow::Error::msg)?
    );
    let result = UndoLastTransaction {}
        .execute(fixture.context("alice", "undo-new"))
        .map_err(command_error)?
        .ok_or_else(|| anyhow::anyhow!("new edit not undoable"))?;
    anyhow::ensure!(result.transaction_id.as_ref() == "after-migration");
    anyhow::ensure!(fixture.position("a")?.x == Some(1.0));
    anyhow::ensure!(
        UndoLastTransaction {}
            .execute(fixture.context("alice", "no-old-history"))
            .map_err(command_error)?
            .is_none()
    );
    let error = UndoTransaction {
        transaction_id: "before-migration".into(),
    }
    .execute(fixture.context("alice", "explicit-old"))
    .err()
    .ok_or_else(|| anyhow::anyhow!("old edit was undoable"))?;
    anyhow::ensure!(error.message.contains("predates"));
    anyhow::ensure!(fixture.records("before-migration")?.records.len() == 1);
    Ok(())
}

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn range_undo_reverses_overlapping_edits_and_persists_all_receipts() -> anyhow::Result<()> {
    let mut fixture = Fixture::new()?;
    fixture.seed()?;
    fixture.moved("alice", "first", &[("a", 1.0)])?;
    fixture.moved("alice", "second", &[("a", 2.0), ("b", 3.0)])?;
    let page = GetUndoTransactions { limit: 20 }
        .execute(fixture.context("alice", "log"))
        .map_err(command_error)?;
    anyhow::ensure!(page.entries.len() == 2);
    anyhow::ensure!(
        page.entries
            .iter()
            .all(|entry| entry.blocked_reason.is_none())
    );
    let result = UndoTransactions {
        transaction_ids: vec!["second".into(), "first".into()],
    }
    .execute(fixture.context("alice", "undo-range"))
    .map_err(command_error)?;
    anyhow::ensure!(result.transactions_undone == 2);
    anyhow::ensure!(fixture.position("a")?.x.is_none() && fixture.position("b")?.x.is_none());
    fixture.restart()?;
    anyhow::ensure!(
        fixture.records("first")?.already_undone && fixture.records("second")?.already_undone
    );
    anyhow::ensure!(fixture.position("a")?.x.is_none());
    let page = GetUndoTransactions { limit: 20 }
        .execute(fixture.context("alice", "empty-log"))
        .map_err(command_error)?;
    anyhow::ensure!(page.entries.is_empty());
    Ok(())
}

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn foreign_edit_blocks_older_prefix_and_is_visible_without_partial_writes() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    fixture.seed()?;
    fixture.moved("alice", "alice-first", &[("a", 1.0)])?;
    fixture.moved("bob", "bob-between", &[("a", 2.0)])?;
    fixture.moved("alice", "alice-second", &[("a", 3.0)])?;
    let page = GetUndoTransactions { limit: 20 }
        .execute(fixture.context("alice", "log"))
        .map_err(command_error)?;
    anyhow::ensure!(page.entries.len() == 2);
    let first = page
        .entries
        .first()
        .ok_or_else(|| anyhow::anyhow!("missing latest edit"))?;
    anyhow::ensure!(first.blocked_reason.is_none());
    let older = page
        .entries
        .get(1)
        .ok_or_else(|| anyhow::anyhow!("missing older edit"))?;
    anyhow::ensure!(older.blocked_reason.is_some());
    let blocker = older
        .conflicts
        .first()
        .ok_or_else(|| anyhow::anyhow!("missing blocker"))?;
    anyhow::ensure!(blocker.transaction_id.as_ref() == "bob-between" && blocker.is_other_user);
    anyhow::ensure!(blocker.user_id.as_deref() == Some("profile:bob"));
    anyhow::ensure!(
        blocker.entity_type.as_ref() == "UndoPosition" && blocker.entity_id.as_ref() == "a"
    );
    let rejected = UndoTransactions {
        transaction_ids: vec!["alice-second".into(), "alice-first".into()],
    }
    .execute(fixture.context("alice", "rejected-range"));
    anyhow::ensure!(rejected.is_err() && fixture.position("a")?.x == Some(3.0));
    anyhow::ensure!(!fixture.records("alice-second")?.already_undone);
    anyhow::ensure!(
        fixture
            .provider
            .transaction_history("rejected-range")
            .is_err()
    );
    UndoTransactions {
        transaction_ids: vec!["alice-second".into()],
    }
    .execute(fixture.context("alice", "undo-latest"))
    .map_err(command_error)?;
    anyhow::ensure!(fixture.position("a")?.x == Some(2.0));
    Ok(())
}

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn range_revalidates_stale_and_invalid_selections() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    fixture.seed()?;
    fixture.moved("alice", "first", &[("a", 1.0)])?;
    let _ = GetUndoTransactions { limit: 20 }
        .execute(fixture.context("alice", "log"))
        .map_err(command_error)?;
    fixture.moved("alice", "newer", &[("b", 2.0)])?;
    for ids in [
        vec!["first"],
        vec!["newer", "newer"],
        vec![],
        vec!["foreign"],
    ] {
        let result = UndoTransactions {
            transaction_ids: ids.into_iter().map(Arc::from).collect(),
        }
        .execute(fixture.context("alice", &uuid::Uuid::new_v4().to_string()));
        anyhow::ensure!(result.is_err());
    }
    anyhow::ensure!(fixture.position("a")?.x == Some(1.0) && fixture.position("b")?.x == Some(2.0));
    anyhow::ensure!(
        !fixture.records("first")?.already_undone && !fixture.records("newer")?.already_undone
    );
    Ok(())
}

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn older_prefix_can_restore_a_dependency_missing_from_latest_undo() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    fixture.seed()?;
    let reference = UndoReference {
        id: "ref".into(),
        target_type: UndoNode::ENTITY_NAME_STATIC.into(),
        target_id: "a".into(),
    };
    fixture.server.set(&reference)?;
    fixture
        .context("alice", "delete-a")
        .emit_del(&fixture.node("a")?)
        .map_err(command_error)?;
    fixture
        .context("alice", "retarget-b")
        .emit_set(&UndoReference {
            target_id: "b".into(),
            ..reference
        })
        .map_err(command_error)?;
    let page = GetUndoTransactions { limit: 20 }
        .execute(fixture.context("alice", "log"))
        .map_err(command_error)?;
    anyhow::ensure!(
        page.entries
            .first()
            .is_some_and(|e| e.blocked_reason.is_some())
    );
    anyhow::ensure!(
        page.entries
            .get(1)
            .is_some_and(|e| e.blocked_reason.is_none())
    );
    UndoTransactions {
        transaction_ids: vec!["retarget-b".into(), "delete-a".into()],
    }
    .execute(fixture.context("alice", "restore-both"))
    .map_err(command_error)?;
    anyhow::ensure!(fixture.node("a").is_ok());
    anyhow::ensure!(
        fixture
            .item::<UndoReference>(UndoReference::ENTITY_NAME_STATIC, "ref")?
            .target_id
            .as_ref()
            == "a"
    );
    Ok(())
}

mod excluded_state {
    use myko::prelude::*;
    #[myko_item]
    pub struct UndoPresence {
        pub value: String,
    }
}
mod excluded_status {
    use myko::prelude::*;
    #[myko_item(undoable = false, ingest_buffer_ms = 10)]
    pub struct ExcludedStatus {
        pub value: String,
    }
}
use excluded_state::*;
use excluded_status::*;

#[test]
#[ignore = "requires an isolated MYKO_HISTORY_TEST_URL database"]
fn entity_opt_in_excludes_presence_and_blocks_mixed_transactions_without_partial_undo()
-> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    fixture.seed()?;
    fixture.moved("alice", "edit", &[("a", 1.0)])?;
    let presence = UndoPresence {
        id: "presence".into(),
        value: "online".into(),
    };
    fixture
        .context("alice", "presence-only")
        .emit_set(&presence)
        .map_err(command_error)?;
    let page = GetUndoTransactions { limit: 20 }
        .execute(fixture.context("alice", "log"))
        .map_err(command_error)?;
    anyhow::ensure!(
        page.entries.len() == 1
            && page
                .entries
                .first()
                .is_some_and(|e| e.transaction_id.as_ref() == "edit")
    );
    anyhow::ensure!(
        UndoTransaction {
            transaction_id: "presence-only".into()
        }
        .execute(fixture.context("alice", "reject-presence"))
        .is_err()
    );
    {
        let ctx = fixture.context("alice", "mixed");
        ctx.emit_set(&UndoPosition {
            id: "b".into(),
            node_id: "b".into(),
            x: Some(2.0),
        })
        .map_err(command_error)?;
        ctx.emit_set(&UndoPresence {
            value: "away".into(),
            ..presence
        })
        .map_err(command_error)?;
    }
    let page = GetUndoTransactions { limit: 20 }
        .execute(fixture.context("alice", "mixed-log"))
        .map_err(command_error)?;
    anyhow::ensure!(page.entries.first().is_some_and(|e| {
        e.blocked_reason
            .as_ref()
            .is_some_and(|reason| reason.contains("not opted into undo"))
    }));
    anyhow::ensure!(
        UndoTransactions {
            transaction_ids: vec!["mixed".into()]
        }
        .execute(fixture.context("alice", "reject-mixed"))
        .is_err()
    );
    anyhow::ensure!(fixture.position("b")?.x == Some(2.0));
    anyhow::ensure!(
        fixture
            .item::<UndoPresence>(UndoPresence::ENTITY_NAME_STATIC, "presence")?
            .value
            == "away"
    );
    for kind in [
        UndoPresence::ENTITY_NAME_STATIC,
        ExcludedStatus::ENTITY_NAME_STATIC,
    ] {
        anyhow::ensure!(!myko::item::is_undoable_entity(kind));
    }
    Ok(())
}

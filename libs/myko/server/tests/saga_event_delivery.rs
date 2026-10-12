use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use myko::{
    command::{CommandContext, CommandError},
    entities::demo::{DemoStatus, DemoStatusId},
    event::{MEvent, MEventType},
    myko_saga,
    saga::{SagaCommand, SagaContext, SagaHandler},
};
use myko_server::{MykoServer, NullPersister};

static OBSERVED: LazyLock<(flume::Sender<String>, flume::Receiver<String>)> =
    LazyLock::new(|| flume::bounded(2));

struct RecordStatus(String);

impl SagaCommand for RecordStatus {
    fn execute_boxed(self: Box<Self>, _ctx: CommandContext) -> Result<(), CommandError> {
        OBSERVED
            .0
            .try_send(self.0)
            .map_err(|error| CommandError::new("saga-test", "RecordStatus", error.to_string()))
    }

    fn command_name(&self) -> &'static str {
        "RecordStatus"
    }
}

#[myko_saga]
struct StatusSaga;

impl SagaHandler for StatusSaga {
    type EventItem = DemoStatus;
    type Command = RecordStatus;
    const EVENT_TYPE: MEventType = MEventType::SET;

    fn handle(item: DemoStatus, _event: MEvent, _ctx: Arc<SagaContext>) -> Option<Self::Command> {
        Some(RecordStatus(item.name))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registered_saga_receives_prestart_and_running_updates()
-> Result<(), Box<dyn std::error::Error>> {
    let server = Arc::new(
        MykoServer::builder()
            .with_bind_addr(([127, 0, 0, 1], 0).into())
            .with_default_persister(Arc::new(NullPersister))
            .build(),
    );
    let ctx = server.ctx();
    let mut status = DemoStatus {
        id: DemoStatusId::from("observed-status"),
        name: "before-start".into(),
        color: String::new(),
        emoji: String::new(),
    };
    ctx.set(&status)?;
    let running = server.clone();
    let task = tokio::spawn(async move { running.run().await.map_err(|error| error.to_string()) });
    let first = tokio::time::timeout(Duration::from_secs(5), OBSERVED.1.recv_async()).await;
    assert_eq!(first??, "before-start");
    status.name = "after-start".into();
    ctx.set(&status)?;
    let second = tokio::time::timeout(Duration::from_secs(5), OBSERVED.1.recv_async()).await;
    task.abort();
    assert_eq!(second??, "after-start");
    Ok(())
}

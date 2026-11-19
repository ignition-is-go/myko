//! A subscription the client cancels while it is still being built must not be
//! installed when the build finishes. Installed, it has no owner: the client
//! will not cancel it a second time, so it keeps recomputing until the socket
//! closes.

use std::{
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use myko::{
    prelude::*,
    report::{ReportContext, ReportHandler},
    wire::{CancelSubscription, MykoMessage, PingData, WrappedReport},
};
use myko_server::{MykoServer, NullPersister, router::route_connection};
use tokio::{net::TcpListener, time::timeout};
use tokio_tungstenite::tungstenite::Message;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

static BUILD_STARTED: LazyLock<(flume::Sender<()>, flume::Receiver<()>)> =
    LazyLock::new(|| flume::bounded(1));
static RELEASE_BUILD: LazyLock<(flume::Sender<()>, flume::Receiver<()>)> =
    LazyLock::new(|| flume::bounded(1));
static SOURCE: LazyLock<Cell<u64, CellMutable>> = LazyLock::new(|| Cell::new(0));
static RECOMPUTES: AtomicUsize = AtomicUsize::new(0);

#[myko_report_output]
#[derive(Eq)]
pub struct HeldOutput {
    pub value: u64,
}

/// Its build blocks until the test releases it, then follows [`SOURCE`].
#[myko_report(HeldOutput)]
pub struct HeldReport {}

impl ReportHandler for HeldReport {
    type Output = HeldOutput;

    fn compute(&self, _ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        let _ = BUILD_STARTED.0.send(());
        let _ = RELEASE_BUILD.1.recv();
        SOURCE
            .clone()
            .lock()
            .map(|value| {
                RECOMPUTES.fetch_add(1, Ordering::SeqCst);
                Arc::new(HeldOutput { value: *value })
            })
            .materialize()
    }
}

async fn send(socket: &mut Socket, message: &MykoMessage) -> TestResult {
    socket
        .send(Message::Text(serde_json::to_string(message)?.into()))
        .await?;
    Ok(())
}

/// Returns once the server has handled everything sent before this call.
async fn round_trip(socket: &mut Socket, id: &str) -> TestResult {
    send(
        socket,
        &MykoMessage::Ping(PingData {
            id: id.into(),
            timestamp: 0,
        }),
    )
    .await?;
    loop {
        let frame = timeout(Duration::from_secs(5), socket.next())
            .await?
            .ok_or("socket closed")??;
        let Message::Text(text) = frame else {
            continue;
        };
        if matches!(serde_json::from_str(&text)?, MykoMessage::Ping(pong) if pong.id == id) {
            return Ok(());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_report_cancelled_while_it_is_being_built_is_never_installed() -> TestResult {
    let server = Arc::new(
        MykoServer::builder()
            .with_default_persister(Arc::new(NullPersister))
            .build(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let runtime = server.clone();
    let accept = tokio::spawn(async move {
        while let Ok((stream, peer)) = listener.accept().await {
            let ctx = Arc::new(runtime.ctx());
            let info = runtime.server_info();
            tokio::spawn(async move { route_connection(stream, peer, ctx, info).await });
        }
    });
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/myko")).await?;

    send(
        &mut socket,
        &MykoMessage::Report(WrappedReport {
            report: serde_json::json!({"tx": "held"}),
            report_id: "HeldReport".into(),
        }),
    )
    .await?;
    timeout(Duration::from_secs(5), BUILD_STARTED.1.recv_async()).await??;

    send(
        &mut socket,
        &MykoMessage::ReportCancel(CancelSubscription { tx: "held".into() }),
    )
    .await?;
    round_trip(&mut socket, "after-cancel").await?;

    RELEASE_BUILD.0.send_async(()).await?;
    let built = timeout(Duration::from_secs(5), async {
        while RECOMPUTES.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    if built.is_err() {
        return Err("the held build never finished".into());
    }
    // The read loop takes the finished build off its channel between messages.
    for id in ["settle-1", "settle-2", "settle-3"] {
        round_trip(&mut socket, id).await?;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let before = RECOMPUTES.load(Ordering::SeqCst);
    for value in 1..=5 {
        SOURCE.set(value);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let recomputed = RECOMPUTES.load(Ordering::SeqCst).saturating_sub(before);

    accept.abort();
    if recomputed != 0 {
        return Err(format!(
            "the cancelled report was installed and recomputed {recomputed} of 5 source changes"
        )
        .into());
    }
    Ok(())
}

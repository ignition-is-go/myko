#![allow(clippy::panic_in_result_fn)]

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use myko::{
    entities::demo::{DemoStatusId, DemoTask, DemoTaskId},
    server::{PersistError, Persister},
    wire::{MEvent, MEventType, MykoMessage, command::WrappedCommand},
};
use myko_server::{GatewayAccess, MykoServer, router::route_connection_with_access};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Default)]
struct Journal(Mutex<Vec<MEvent>>);
impl Persister for Journal {
    fn persist(&self, event: MEvent) -> std::result::Result<(), PersistError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event);
        Ok(())
    }
}

async fn gateway(
    server: Arc<MykoServer>,
    access: GatewayAccess,
) -> Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let task = tokio::spawn(async move {
        while let Ok((stream, peer)) = listener.accept().await {
            let ctx = Arc::new(server.ctx());
            let info = server.server_info();
            tokio::spawn(async move {
                route_connection_with_access(stream, peer, ctx, info, access).await
            });
        }
    });
    Ok((addr, task))
}

async fn send(socket: &mut Socket, message: &MykoMessage, binary: bool) -> Result {
    let frame = if binary {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(message, &mut bytes)?;
        Message::Binary(bytes.into())
    } else {
        Message::Text(serde_json::to_string(message)?.into())
    };
    socket.send(frame).await?;
    Ok(())
}

async fn next(socket: &mut Socket) -> Result<MykoMessage> {
    loop {
        match timeout(Duration::from_secs(3), socket.next())
            .await?
            .ok_or("closed")??
        {
            Message::Text(text) => return Ok(serde_json::from_str(&text)?),
            Message::Binary(bytes) => return Ok(ciborium::de::from_reader(bytes.as_ref())?),
            _ => {}
        }
    }
}

async fn closed(socket: &mut Socket) -> Result {
    match timeout(Duration::from_secs(3), socket.next()).await? {
        None | Some(Err(_) | Ok(Message::Close(_))) => Ok(()),
        other => Err(format!("expected connection termination, got {other:?}").into()),
    }
}

fn task(id: &str) -> DemoTask {
    DemoTask {
        id: DemoTaskId::from(id),
        title: id.to_owned(),
        completed: false,
        status_id: DemoStatusId::from("todo"),
    }
}

fn count(server: &MykoServer) -> usize {
    server
        .ctx()
        .registry
        .get_or_create("DemoTask")
        .snapshot()
        .len()
}

#[tokio::test]
async fn readonly_denies_valid_mutations_in_both_encodings_before_store_or_journal() -> Result {
    let journal = Arc::new(Journal::default());
    let server = Arc::new(
        MykoServer::builder()
            .with_default_persister(journal.clone())
            .build(),
    );
    let (readonly, ro_task) = gateway(server.clone(), GatewayAccess::ReadOnly).await?;
    let (full, full_task) = gateway(server.clone(), GatewayAccess::Full).await?;
    for binary in [false, true] {
        let messages = [
            MykoMessage::Command(WrappedCommand {
                command_id: "CreateDemoTask".into(),
                command: serde_json::json!({"id":format!("command-{binary}"),"title":"created", "completed":false,"statusId":"todo","tx":"command"}),
            }),
            MykoMessage::Event(MEvent::from_item(
                &task(&format!("event-{binary}")),
                MEventType::SET,
                "fixture",
            )),
            MykoMessage::EventBatch(vec![
                MEvent::from_item(
                    &task(&format!("batch-a-{binary}")),
                    MEventType::SET,
                    "fixture",
                ),
                MEvent::from_item(
                    &task(&format!("batch-b-{binary}")),
                    MEventType::SET,
                    "fixture",
                ),
            ]),
        ];
        for message in messages {
            let (mut socket, _) =
                tokio_tungstenite::connect_async(format!("ws://{readonly}/myko")).await?;
            let before_count = count(&server);
            let before_journal = journal
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .filter(|event| event.item_type.as_ref() == "DemoTask")
                .count();
            send(&mut socket, &message, binary).await?;
            closed(&mut socket).await?;
            assert_eq!(count(&server), before_count);
            assert_eq!(
                journal
                    .0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .iter()
                    .filter(|event| event.item_type.as_ref() == "DemoTask")
                    .count(),
                before_journal
            );
            let (mut trusted, _) =
                tokio_tungstenite::connect_async(format!("ws://{full}/myko")).await?;
            send(&mut trusted, &message, binary).await?;
            let expected_delta = if matches!(message, MykoMessage::EventBatch(_)) {
                2
            } else {
                1
            };
            timeout(Duration::from_secs(3), async {
                while count(&server) != before_count + expected_delta {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            if matches!(message, MykoMessage::Command(_)) {
                assert!(matches!(
                    next(&mut trusted).await?,
                    MykoMessage::CommandResponse(_)
                ));
            }
            timeout(Duration::from_secs(3), async {
                while journal
                    .0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .iter()
                    .filter(|event| event.item_type.as_ref() == "DemoTask")
                    .count()
                    != before_journal + expected_delta
                {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            trusted.close(None).await?;
        }
    }
    ro_task.abort();
    full_task.abort();
    Ok(())
}

#[tokio::test]
async fn readonly_rejects_all_mcp_transports() -> Result {
    let server = Arc::new(MykoServer::builder().build());
    let (addr, task) = gateway(server, GatewayAccess::ReadOnly).await?;
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/myko/mcp?transport=test");
    assert_eq!(
        client
            .post(&url)
            .json(&serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize"}))
            .send()
            .await?
            .status(),
        403
    );
    assert_eq!(
        client
            .get(&url)
            .header("Accept", "text/event-stream")
            .send()
            .await?
            .status(),
        403
    );
    match tokio_tungstenite::connect_async(format!("ws://{addr}/myko/mcp")).await {
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            assert_eq!(response.status(), 403);
        }
        other => return Err(format!("expected forbidden handshake, got {other:?}").into()),
    }
    task.abort();
    Ok(())
}

#[tokio::test]
async fn readonly_query_updates_controls_cancel_and_reconnect() -> Result {
    use myko::wire::{
        message::{CancelSubscription, PingData},
        query::{QueryWindow, QueryWindowUpdate, WrappedQuery},
    };
    let server = Arc::new(MykoServer::builder().build());
    server.ctx().set(&task("initial"))?;
    let (addr, task_handle) = gateway(server.clone(), GatewayAccess::ReadOnly).await?;
    let url = format!("ws://{addr}/myko");
    let (mut socket, _) = tokio_tungstenite::connect_async(&url).await?;
    let query = MykoMessage::Query(WrappedQuery {
        query: serde_json::json!({"tx":"live"}),
        query_id: "GetDemoTasks".into(),
        query_item_type: "DemoTask".into(),
        window: None,
    });
    send(&mut socket, &query, false).await?;
    let initial = next(&mut socket).await?;
    assert!(
        matches!(initial, MykoMessage::QueryResponse(ref response) if !response.upserts.is_empty())
    );
    server.ctx().set(&task("live-update"))?;
    assert!(
        matches!(next(&mut socket).await?, MykoMessage::QueryResponse(ref response) if !response.upserts.is_empty())
    );
    send(
        &mut socket,
        &MykoMessage::QueryWindow(QueryWindowUpdate {
            tx: "live".into(),
            window: Some(QueryWindow {
                offset: 0,
                limit: 1,
            }),
        }),
        true,
    )
    .await?;
    assert!(matches!(
        next(&mut socket).await?,
        MykoMessage::QueryResponse(_)
    ));
    send(
        &mut socket,
        &MykoMessage::QueryCancel(CancelSubscription { tx: "live".into() }),
        true,
    )
    .await?;
    let ping = MykoMessage::Ping(PingData {
        id: "barrier".into(),
        timestamp: 0,
    });
    send(&mut socket, &ping, true).await?;
    assert!(matches!(next(&mut socket).await?, MykoMessage::Ping(_)));
    server.ctx().set(&task("after-cancel"))?;
    send(&mut socket, &ping, true).await?;
    assert!(matches!(next(&mut socket).await?, MykoMessage::Ping(_)));
    socket.close(None).await?;
    let (mut socket, _) = tokio_tungstenite::connect_async(&url).await?;
    send(&mut socket, &query, true).await?;
    assert!(
        matches!(next(&mut socket).await?, MykoMessage::QueryResponse(ref response) if response.upserts.len() == 3)
    );
    socket.close(None).await?;
    task_handle.abort();
    Ok(())
}

#[tokio::test]
async fn readonly_denies_result_injection_and_benchmark() -> Result {
    let journal = Arc::new(Journal::default());
    let server = Arc::new(
        MykoServer::builder()
            .with_default_persister(journal.clone())
            .build(),
    );
    let (addr, task_handle) = gateway(server.clone(), GatewayAccess::ReadOnly).await?;
    let fixtures = [
        serde_json::json!({"event":"ws:m:command-response","data":{"tx":"injected","response":true}}),
        serde_json::json!({"event":"ws:m:command-error","data":{"tx":"injected","commandId":"CreateDemoTask","message":"injected"}}),
        serde_json::json!({"event":"ws:m:query-response","data":{"tx":"injected","sequence":1,"deletes":[],"upserts":[]}}),
        serde_json::json!({"event":"ws:m:report-response","data":{"tx":"injected","response":null}}),
        serde_json::json!({"event":"ws:m:benchmark","data":{"bytes":"payload"}}),
    ];
    for fixture in fixtures {
        let message: MykoMessage = serde_json::from_value(fixture)?;
        for binary in [false, true] {
            let (mut socket, _) =
                tokio_tungstenite::connect_async(format!("ws://{addr}/myko")).await?;
            send(&mut socket, &message, binary).await?;
            closed(&mut socket).await?;
            assert_eq!(count(&server), 0);
            assert!(
                journal
                    .0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .iter()
                    .all(|event| event.item_type.as_ref() != "DemoTask")
            );
        }
    }
    task_handle.abort();
    Ok(())
}

#[tokio::test]
async fn typed_client_receives_readonly_initial_and_live_query_data() -> Result {
    use myko::{client::MykoClient, entities::demo::GetDemoTasks, hyphae::Gettable};
    let server = Arc::new(MykoServer::builder().build());
    server.ctx().set(&task("typed-initial"))?;
    let (addr, task_handle) = gateway(server.clone(), GatewayAccess::ReadOnly).await?;
    let client = MykoClient::new_with_auto_reconnect(false);
    client.set_address(Some(format!("ws://{addr}/myko")));
    let watch = client.watch_query_state(GetDemoTasks {});
    timeout(Duration::from_secs(3), async {
        while !watch.ready().get() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(watch.items().get().len(), 1);
    server.ctx().set(&task("typed-live"))?;
    timeout(Duration::from_secs(3), async {
        while watch.items().get().len() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    drop(watch);
    client.close();
    task_handle.abort();
    Ok(())
}

static LIFETIME_REQUESTS: Mutex<Vec<std::sync::Weak<myko::request::RequestContext>>> =
    Mutex::new(Vec::new());

#[myko::myko_query(DemoTask)]
struct GatewayLifetimeQuery {}

impl myko::query::QueryHandler for GatewayLifetimeQuery {
    fn test_entity(ctx: myko::query::QueryTestContext<Self>) -> bool {
        LIFETIME_REQUESTS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Arc::downgrade(&ctx.query_context.req));
        true
    }
}

#[tokio::test]
async fn policy_router_owns_connection_lifetime_and_cancellation_cleanup() -> Result {
    use myko::wire::query::WrappedQuery;
    let server = Arc::new(MykoServer::builder().build());
    let ctx = Arc::new(server.ctx());
    ctx.set(&task("lifetime"))?;
    for cancel in [false, true, false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let connection_ctx = ctx.clone();
        let info = server.server_info();
        let connection = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await?;
            route_connection_with_access(
                stream,
                peer,
                connection_ctx,
                info,
                GatewayAccess::ReadOnly,
            )
            .await
        });
        let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/myko")).await?;
        send(
            &mut socket,
            &MykoMessage::Query(WrappedQuery {
                query: serde_json::json!({"tx":"lifetime"}),
                query_id: "GatewayLifetimeQuery".into(),
                query_item_type: "DemoTask".into(),
                window: None,
            }),
            false,
        )
        .await?;
        assert!(matches!(
            next(&mut socket).await?,
            MykoMessage::QueryResponse(_)
        ));
        assert!(
            !connection.is_finished(),
            "route future must own the live connection"
        );
        assert_eq!(ctx.registry.get_or_create("Client").snapshot().len(), 1);
        assert!(
            LIFETIME_REQUESTS
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .any(|request| request.upgrade().is_some())
        );
        if cancel {
            connection.abort();
            assert!(connection.await.is_err());
            closed(&mut socket).await?;
        } else {
            socket.close(None).await?;
            timeout(Duration::from_secs(3), connection).await???;
        }
        timeout(Duration::from_secs(3), async {
            while LIFETIME_REQUESTS
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .any(|request| request.upgrade().is_some())
                || !ctx.registry.get_or_create("Client").snapshot().is_empty()
            {
                tokio::task::yield_now().await;
            }
        })
        .await?;
    }
    Ok(())
}

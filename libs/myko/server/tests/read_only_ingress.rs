#![allow(clippy::arithmetic_side_effects, clippy::expect_used)]

use std::{sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use hyphae::Gettable;
use myko::{
    client::{ConnectionStatus, MykoClient, MykoProtocol},
    entities::demo::{
        CreateDemoTask, DeleteDemoTask, DemoStatusId, DemoTask, DemoTaskId, GetDemoTasks,
    },
    wire::{MEvent, MEventType, MykoMessage, WrappedQuery, WrappedReport, WrappedView},
};
use myko_server::MykoServer;

async fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while !condition() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition timed out"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn read_only_server_streams_allowed_query_and_denies_json_cbor_and_mcp_mutations() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let address = listener.local_addr().expect("listener address");
    let server = Arc::new(
        MykoServer::builder()
            .with_read_only_queries(["GetDemoTasks"])
            .build(),
    );
    let ctx = server.ctx();
    let run_server = server.clone();
    let server_task = tokio::spawn(async move {
        run_server.run_ws_listener(listener).await.expect("serve");
    });

    let allowed = MykoClient::new();
    allowed.set_address(Some(format!("ws://{address}/myko")));
    let tasks = allowed.watch_query(GetDemoTasks {});
    wait_until(|| {
        matches!(
            allowed.connection_status_sync(),
            ConnectionStatus::Connected(_)
        )
    })
    .await;
    let task = DemoTask {
        id: DemoTaskId::from("direct-update"),
        title: "published directly".to_owned(),
        completed: false,
        status_id: DemoStatusId::from("current"),
    };
    ctx.set(&task).expect("direct server update");
    wait_until(|| tasks.get().iter().any(|item| item.id == task.id)).await;
    let updated = DemoTask {
        title: "second live update".to_owned(),
        ..task.clone()
    };
    ctx.set(&updated).expect("second direct server update");
    wait_until(|| {
        tasks
            .get()
            .iter()
            .any(|item| item.id == updated.id && item.title == updated.title)
    })
    .await;

    for protocol in [MykoProtocol::JSON, MykoProtocol::CBOR] {
        let client = MykoClient::new();
        client.set_protocol(protocol);
        client.set_address(Some(format!("ws://{address}/myko")));
        wait_until(|| {
            matches!(
                client.connection_status_sync(),
                ConnectionStatus::Connected(_)
            )
        })
        .await;
        let denied = client.send_command::<_, bool>(&CreateDemoTask {
            id: DemoTaskId::from(format!("denied-{protocol:?}")),
            title: "must not exist".to_owned(),
            completed: false,
            status_id: DemoStatusId::from("current"),
        });
        wait_until(|| denied.get().is_some()).await;
        assert!(
            denied.get().is_some_and(|result| result.is_err()),
            "{protocol:?} mutation unexpectedly succeeded"
        );

        let denied_delete = client.send_command::<_, serde_json::Value>(&DeleteDemoTask {
            id: updated.id.clone(),
        });
        wait_until(|| denied_delete.get().is_some()).await;
        assert!(denied_delete.get().is_some_and(|result| result.is_err()));
    }

    assert_raw_ingress_denials(address, &updated).await;

    let response = reqwest::Client::new()
        .post(format!("http://{address}/myko/mcp"))
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "execute",
                "arguments": {
                    "code": "return await myko.command('CreateDemoTask', {id:'mcp-denied', title:'no', completed:false, statusId:'current'});"
                }
            }
        }))
        .send()
        .await
        .expect("MCP response")
        .json::<serde_json::Value>()
        .await
        .expect("MCP JSON");
    assert_eq!(
        response.pointer("/result/isError"),
        Some(&serde_json::json!(true))
    );
    assert!(
        !tasks
            .get()
            .iter()
            .any(|item| item.id.as_ref() == "mcp-denied")
    );

    server_task.abort();
}

async fn assert_raw_ingress_denials(address: std::net::SocketAddr, task: &DemoTask) {
    let (mut query_socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/myko"))
        .await
        .expect("connect query socket");
    let denied_query = MykoMessage::Query(WrappedQuery {
        query: serde_json::json!({"tx": "denied-query"}),
        query_id: Arc::from("GetAllServers"),
        query_item_type: Arc::from("Server"),
        window: None,
    });
    query_socket
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&denied_query)
                .expect("serialize denied query")
                .into(),
        ))
        .await
        .expect("send denied query");
    let response = query_socket
        .next()
        .await
        .expect("query response")
        .expect("query frame");
    let text = response.into_text().expect("query error text");
    let message: MykoMessage = serde_json::from_str(&text).expect("query error message");
    assert!(matches!(message, MykoMessage::QueryError(_)));

    let event = MEvent::from_item(task, MEventType::SET, "test-client");
    let denied = [
        MykoMessage::Report(WrappedReport {
            report: serde_json::json!({"tx": "report"}),
            report_id: "ClientStatus".to_owned(),
        }),
        MykoMessage::View(WrappedView {
            view: serde_json::json!({"tx": "view"}),
            view_id: Arc::from("ConnectedClients"),
            view_item_type: Arc::from("Client"),
            window: None,
            sample_rate: None,
        }),
        MykoMessage::Event(event.clone()),
        MykoMessage::EventBatch(vec![event]),
    ];
    for message in denied {
        let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/myko"))
            .await
            .expect("connect denial socket");
        socket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                serde_json::to_string(&message)
                    .expect("serialize denied message")
                    .into(),
            ))
            .await
            .expect("send denied message");
        let closed = tokio::time::timeout(Duration::from_secs(1), socket.next())
            .await
            .expect("server must answer or close promptly");
        assert!(
            closed.is_none_or(|frame| frame.is_err() || frame.is_ok_and(|frame| frame.is_close())),
            "disallowed message did not close the connection"
        );
    }
}

#[tokio::test]
async fn read_only_allowlist_is_validated_at_listener_startup() {
    let server = MykoServer::builder()
        .with_read_only_queries(["NotARegisteredQuery"])
        .build();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let error = server
        .run_ws_listener(listener)
        .await
        .expect_err("unknown query must reject listener startup");
    assert!(
        error
            .to_string()
            .contains("unregistered queries: NotARegisteredQuery")
    );
}

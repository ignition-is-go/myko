use std::{sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use myko::{
    prelude::*,
    request::RequestContext,
    wire::{MEvent, MEventType, MykoMessage, PingData, WrappedCommand},
};
use myko_server::{ClientMutationPolicy, MykoServer, mcp::exec::Executor};
use serde_json::{Value, json};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[myko_item]
pub struct IngressRecord {
    pub value: String,
}

#[myko_command(bool)]
pub struct AllowedIngressWrite {
    pub value: String,
}

impl CommandHandler for AllowedIngressWrite {
    fn execute(self, ctx: CommandContext) -> Result<bool, CommandError> {
        ctx.emit_set(&IngressRecord {
            id: "one".into(),
            value: self.value,
        })?;
        Ok(true)
    }
}

fn internal(server: &MykoServer) -> CommandContext {
    CommandContext::new(
        "test".into(),
        Arc::new(RequestContext::internal(
            "test".into(),
            server.host_id,
            "test",
        )),
        Arc::new(server.ctx()),
    )
}

fn value(server: &MykoServer) -> Result<Option<String>, String> {
    internal(server)
        .exec_query_first(GetIngressRecordsByIds {
            ids: vec!["one".into()],
        })
        .map(|row| row.map(|row| row.value.clone()))
        .map_err(|error| error.message)
}

fn ensure(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

async fn connect(url: &str) -> Result<Socket, Box<dyn std::error::Error + Send + Sync>> {
    let now = tokio::time::Instant::now();
    let deadline = now.checked_add(Duration::from_secs(3)).unwrap_or(now);
    loop {
        match tokio_tungstenite::connect_async(url).await {
            Ok((socket, _)) => return Ok(socket),
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn send(socket: &mut Socket, message: MykoMessage) -> TestResult {
    socket
        .send(Message::Text(serde_json::to_string(&message)?.into()))
        .await?;
    Ok(())
}

async fn receive(
    socket: &mut Socket,
) -> Result<MykoMessage, Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await?
            .ok_or("socket closed")??;
        if let Message::Text(text) = message {
            return Ok(serde_json::from_str(text.as_str())?);
        }
    }
}

fn command(id: &str, tx: &str, args: Value) -> Result<MykoMessage, String> {
    let mut args = args;
    args.as_object_mut()
        .ok_or("Expected command object")?
        .insert("tx".into(), json!(tx));
    Ok(MykoMessage::Command(WrappedCommand {
        command_id: id.into(),
        command: args,
    }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restricted_clients_cannot_bypass_commands_with_events_or_mcp() -> TestResult {
    let probe = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = probe.local_addr()?;
    drop(probe);
    let server = Arc::new(
        MykoServer::builder()
            .with_bind_addr(address)
            .with_client_mutation_policy(ClientMutationPolicy::commands_only([
                "AllowedIngressWrite",
            ]))
            .build(),
    );
    internal(&server)
        .emit_set(&IngressRecord {
            id: "one".into(),
            value: "original".into(),
        })
        .map_err(|error| error.message)?;
    let running = server.clone();
    let task = tokio::spawn(async move { running.run_ws_loop().await });
    let mut socket = connect(&format!("ws://{address}/myko")).await?;

    send(
        &mut socket,
        command("DeleteIngressRecord", "denied", json!({"id":"one"}))?,
    )
    .await?;
    let denied = loop {
        if let MykoMessage::CommandError(error) = receive(&mut socket).await? {
            break error;
        }
    };
    ensure(denied.tx == "denied", "Denial lost transaction ID")?;
    ensure(
        denied.message.contains("mutation policy"),
        "Missing denial message",
    )?;
    ensure(
        value(&server)?.as_deref() == Some("original"),
        "Expected original state",
    )?;

    let event = MEvent::from_item(
        &IngressRecord {
            id: "one".into(),
            value: "raw".into(),
        },
        MEventType::SET,
        "client",
    );
    send(&mut socket, MykoMessage::Event(event.clone())).await?;
    send(&mut socket, MykoMessage::EventBatch(vec![event])).await?;
    send(
        &mut socket,
        MykoMessage::Ping(PingData {
            id: "barrier".into(),
            timestamp: 0,
        }),
    )
    .await?;
    loop {
        if let MykoMessage::Ping(payload) = receive(&mut socket).await? {
            ensure(payload.id == "barrier", "Unexpected ping")?;
            break;
        }
    }
    ensure(
        value(&server)?.as_deref() == Some("original"),
        "Expected original state",
    )?;

    send(
        &mut socket,
        command("AllowedIngressWrite", "allowed", json!({"value":"allowed"}))?,
    )
    .await?;
    loop {
        if matches!(receive(&mut socket).await?, MykoMessage::CommandResponse(_)) {
            break;
        }
    }
    ensure(
        value(&server)?.as_deref() == Some("allowed"),
        "Expected allowed state",
    )?;

    check_mcp(&server, address).await?;
    task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trusted_default_preserves_peer_writes_and_internal_execution() -> TestResult {
    let probe = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = probe.local_addr()?;
    drop(probe);
    let server = Arc::new(MykoServer::builder().with_bind_addr(address).build());
    let running = server.clone();
    let task = tokio::spawn(async move { running.run_ws_loop().await });
    let mut socket = connect(&format!("ws://{address}/myko")).await?;
    let event = MEvent::from_item(
        &IngressRecord {
            id: "one".into(),
            value: "raw trusted".into(),
        },
        MEventType::SET,
        "client",
    );
    send(&mut socket, MykoMessage::Event(event.clone())).await?;
    send(&mut socket, MykoMessage::EventBatch(vec![event])).await?;
    send(
        &mut socket,
        MykoMessage::Ping(PingData {
            id: "barrier".into(),
            timestamp: 0,
        }),
    )
    .await?;
    loop {
        if matches!(receive(&mut socket).await?, MykoMessage::Ping(_)) {
            break;
        }
    }
    ensure(
        value(&server)?.as_deref() == Some("raw trusted"),
        "Expected raw trusted state",
    )?;
    ensure(
        server.ctx().client_mutation_policy().allows_events(),
        "Default must trust peers",
    )?;
    let executor = Executor::InProcess(Arc::new(server.ctx()));
    executor
        .execute_command("AllowedIngressWrite", json!({"value":"trusted"}))
        .await?;
    executor
        .execute_command("DeleteIngressRecord", json!({"id":"one"}))
        .await?;
    ensure(value(&server)?.is_none(), "Trusted delete did not run")?;
    let restricted = MykoServer::builder()
        .with_client_mutation_policy(ClientMutationPolicy::commands_only(Vec::<String>::new()))
        .build();
    AllowedIngressWrite {
        value: "internal".into(),
    }
    .execute(internal(&restricted))
    .map_err(|error| error.message)?;
    ensure(
        value(&restricted)?.as_deref() == Some("internal"),
        "Expected internal state",
    )?;
    task.abort();
    Ok(())
}

async fn check_mcp(server: &MykoServer, address: std::net::SocketAddr) -> TestResult {
    let executor = Executor::InProcess(Arc::new(server.ctx()));
    ensure(
        executor
            .execute_command("DeleteIngressRecord", json!({"id":"one"}))
            .await
            .err()
            .is_some_and(|error| error.contains("mutation policy")),
        "MCP did not reject command",
    )?;
    executor
        .execute_command("AllowedIngressWrite", json!({"value":"mcp"}))
        .await?;
    ensure(
        value(server)?.as_deref() == Some("mcp"),
        "Expected mcp state",
    )?;
    let query = executor
        .execute_query("GetIngressRecordsByIds", json!({"ids":["one"]}))
        .await?;
    ensure(query.to_string().contains("mcp"), "Reads were blocked")?;

    let request = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"execute","arguments":{"code":"return await myko.command('DeleteIngressRecord', {id: 'one'});"}}});
    let response: Value = reqwest::Client::new()
        .post(format!("http://{address}/myko/mcp"))
        .json(&request)
        .send()
        .await?
        .json()
        .await?;
    ensure(
        response.pointer("/result/isError") == Some(&json!(true)),
        &response.to_string(),
    )?;
    ensure(
        response.to_string().contains("mutation policy"),
        "MCP transport lost denial",
    )?;
    let mut mcp_socket = connect(&format!("ws://{address}/myko/mcp")).await?;
    mcp_socket
        .send(Message::Text(request.to_string().into()))
        .await?;
    let response = tokio::time::timeout(Duration::from_secs(5), mcp_socket.next())
        .await?
        .ok_or("MCP closed")??;
    let response: Value = serde_json::from_str(response.to_text()?)?;
    ensure(
        response.pointer("/result/isError") == Some(&json!(true)),
        &response.to_string(),
    )?;
    ensure(
        value(server)?.as_deref() == Some("mcp"),
        "Expected mcp state",
    )?;
    Ok(())
}

//! Asserts that a `ServerInfo` set on `CellServerBuilder` reaches
//! `handle_initialize` over HTTP — the wire shape pulse-mcp depends on.

use std::sync::Arc;
use std::time::Duration;

use myko_server::{CellServer, mcp::dispatch::ServerInfo};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_initialize_uses_threaded_server_info() {
    // Pick a free port to avoid colliding with other tests.
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let actual = probe.local_addr().unwrap();
    drop(probe);

    let info = ServerInfo {
        name: "pulse-mcp".into(),
        version: "0.2.0".into(),
        instructions: Some("teach me".into()),
    };

    let server = Arc::new(
        CellServer::builder()
            .with_bind_addr(actual)
            .with_server_info(info)
            .build(),
    );

    let server_for_run = server.clone();
    let handle = tokio::spawn(async move {
        let _ = server_for_run.run_ws_loop().await;
    });

    // Give the listener a beat to come up.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "test", "version": "0" }
        }
    });

    let client = reqwest::Client::new();
    let url = format!("http://{}/myko/mcp", actual);
    let resp: serde_json::Value = client
        .post(&url)
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .json(&body)
        .send()
        .await
        .expect("POST initialize")
        .json()
        .await
        .expect("parse JSON");

    let result = &resp["result"];
    assert_eq!(result["serverInfo"]["name"], "pulse-mcp");
    assert_eq!(result["serverInfo"]["version"], "0.2.0");
    assert_eq!(result["instructions"], "teach me");

    handle.abort();
}

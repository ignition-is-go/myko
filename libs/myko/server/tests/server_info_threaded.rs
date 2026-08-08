//! Asserts that a `ServerInfo` set on `MykoServerBuilder` reaches
//! `handle_initialize` over HTTP — the wire shape pulse-mcp depends on.

use std::{sync::Arc, time::Duration};

use myko_server::{MykoServer, mcp::dispatch::ServerInfo};

/// POST the `initialize` payload, retrying transient connect errors for up
/// to ~2 seconds while the listener is still coming up. Avoids the flake
/// of a fixed sleep on loaded CI runners.
async fn post_initialize_with_retry(
    client: &reqwest::Client,
    url: &str,
    body: &serde_json::Value,
) -> Option<serde_json::Value> {
    let now = tokio::time::Instant::now();
    let deadline = now.checked_add(Duration::from_secs(2)).unwrap_or(now);
    loop {
        match client
            .post(url)
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .json(body)
            .send()
            .await
        {
            Ok(resp) => return resp.json().await.ok(),
            Err(err) if err.is_connect() && tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => {
                eprintln!("POST initialize: {error:?}");
                return None;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_initialize_uses_threaded_server_info() {
    // Pick a free port to avoid colliding with other tests.
    let probe = std::net::TcpListener::bind("127.0.0.1:0");
    assert!(probe.is_ok(), "bind test listener");
    let Ok(probe) = probe else {
        return;
    };
    let actual = probe.local_addr();
    assert!(actual.is_ok(), "read test listener address");
    let Ok(actual) = actual else {
        return;
    };
    drop(probe);

    let info = ServerInfo {
        name: "pulse-mcp".into(),
        version: "0.2.0".into(),
        instructions: Some("teach me".into()),
        ..Default::default()
    };

    let server = Arc::new(
        MykoServer::builder()
            .with_bind_addr(actual)
            .with_server_info(info)
            .build(),
    );

    let server_for_run = server.clone();
    let handle = tokio::spawn(async move {
        let _ = server_for_run.run_ws_loop().await;
    });

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
    let url = format!("http://{actual}/myko/mcp");
    let resp = post_initialize_with_retry(&client, &url, &body).await;
    assert!(resp.is_some(), "initialize response");
    let Some(resp) = resp else {
        return;
    };
    assert_eq!(
        resp.pointer("/result/serverInfo/name"),
        Some(&serde_json::json!("pulse-mcp"))
    );
    assert_eq!(
        resp.pointer("/result/serverInfo/version"),
        Some(&serde_json::json!("0.2.0"))
    );
    assert_eq!(
        resp.pointer("/result/instructions"),
        Some(&serde_json::json!("teach me"))
    );

    handle.abort();
}

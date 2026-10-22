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
async fn http_initialize_uses_threaded_server_info() -> TestResult {
    check_endpoints("/myko", "/myko/mcp", true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn custom_websocket_and_mcp_paths() -> TestResult {
    check_endpoints("/custom/socket", "/api/control", true).await
}

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

async fn check_endpoints(websocket_path: &str, mcp_path: &str, explicit: bool) -> TestResult {
    // Pick a free port to avoid colliding with other tests.
    let probe = std::net::TcpListener::bind("127.0.0.1:0");
    assert!(probe.is_ok(), "bind test listener");
    let Ok(probe) = probe else {
        return Ok(());
    };
    let actual = probe.local_addr();
    assert!(actual.is_ok(), "read test listener address");
    let Ok(actual) = actual else {
        return Ok(());
    };
    drop(probe);

    let info = ServerInfo {
        name: "pulse-mcp".into(),
        version: "0.2.0".into(),
        instructions: Some("teach me".into()),
        ..Default::default()
    };

    let mut builder = MykoServer::builder()
        .with_bind_addr(actual)
        .with_server_info(info);
    if explicit {
        builder = builder
            .with_websocket_path(websocket_path)
            .with_mcp_path(mcp_path);
    }
    let server = Arc::new(builder.build());

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
    let url = format!("http://{actual}{mcp_path}?test=1");
    let resp = post_initialize_with_retry(&client, &url, &body).await;
    assert!(resp.is_some(), "initialize response");
    let Some(resp) = resp else {
        return Ok(());
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

    check_transports(&client, &url, actual, websocket_path, mcp_path).await?;
    handle.abort();
    Ok(())
}

async fn check_transports(
    client: &reqwest::Client,
    url: &str,
    actual: std::net::SocketAddr,
    websocket_path: &str,
    mcp_path: &str,
) -> TestResult {
    let status = client
        .get(url)
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;
    assert_eq!(
        status.get("endpoint").and_then(serde_json::Value::as_str),
        Some(mcp_path)
    );
    let sse = client
        .get(url)
        .header("accept", "text/event-stream")
        .send()
        .await?;
    assert_eq!(sse.status(), 200);
    assert_eq!(
        sse.headers()
            .get("content-type")
            .ok_or("missing content type")?,
        "text/event-stream"
    );
    for path in [websocket_path, mcp_path] {
        let (socket, response) =
            tokio_tungstenite::connect_async(format!("ws://{actual}{path}?test=1")).await?;
        assert_eq!(response.status(), 101);
        drop(socket);
    }
    if websocket_path != "/myko" {
        for path in [
            "/myko",
            "/myko/mcp",
            "/custom/socket/extra",
            "/api/control/extra",
        ] {
            assert_eq!(
                client
                    .get(format!("http://{actual}{path}"))
                    .send()
                    .await?
                    .status(),
                404
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn invalid_endpoint_paths_fail_before_binding() -> TestResult {
    for (websocket, mcp) in [
        ("relative", "/mcp"),
        ("/ws?query=1", "/mcp"),
        ("/ws", "/mcp#fragment"),
        ("/ws", "/bad path"),
        ("//host/ws", "/mcp"),
        ("/same", "/same"),
    ] {
        let server = MykoServer::builder()
            .with_bind_addr(([127, 0, 0, 1], 0).into())
            .with_websocket_path(websocket)
            .with_mcp_path(mcp)
            .build();
        let error = server
            .run_ws_loop()
            .await
            .err()
            .ok_or("expected invalid path error")?;
        if error
            .downcast_ref::<std::io::Error>()
            .ok_or("expected IO error")?
            .kind()
            != std::io::ErrorKind::InvalidInput
        {
            return Err("expected InvalidInput".into());
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn environment_paths_from_process() -> TestResult {
    if std::env::var_os("MYKO_PATH_TEST_CHILD").is_some() {
        check_endpoints("/custom/socket", "/api/control", false).await?;
        check_endpoints("/explicit/ws", "/explicit/mcp", true).await?;
        return Ok(());
    }
    let status = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", "environment_paths_from_process", "--nocapture"])
        .env("MYKO_PATH_TEST_CHILD", "1")
        .env("MYKO_WS_PATH", "/custom/socket")
        .env("MYKO_MCP_PATH", "/api/control")
        .status()?;
    if !status.success() {
        return Err("environment path subprocess failed".into());
    }
    Ok(())
}

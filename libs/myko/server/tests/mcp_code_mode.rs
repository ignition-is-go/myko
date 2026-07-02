//! End-to-end verification of the MCP "Code Mode" `search`/`execute` tools
//! over a real HTTP connection, against this repo's own built-in `Server`
//! entity — not just the in-memory `dispatch::handle_request` unit tests.

use std::sync::Arc;
use std::time::Duration;

use myko_server::{CellServer, mcp::dispatch::ServerInfo};

async fn post_with_retry(
    client: &reqwest::Client,
    url: &str,
    body: &serde_json::Value,
) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        match client
            .post(url)
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .json(body)
            .send()
            .await
        {
            Ok(resp) => return resp.json().await.expect("parse JSON"),
            Err(err) if err.is_connect() && tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(err) => panic!("POST {url}: {err:?}"),
        }
    }
}

fn tool_call(id: i64, name: &str, arguments: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/call",
        "params": { "name": name, "arguments": arguments }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_then_execute_round_trip_over_http() {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);

    // ServerInfo::default() builds the operation index automatically from
    // inventory-registered operations — no bindings-dir wiring needed.
    let server = Arc::new(
        CellServer::builder()
            .with_bind_addr(addr)
            .with_server_info(ServerInfo::default())
            .build(),
    );
    let server_for_run = server.clone();
    let handle = tokio::spawn(async move {
        let _ = server_for_run.run_ws_loop().await;
    });

    let client = reqwest::Client::new();
    let url = format!("http://{addr}/myko/mcp");

    // 1. tools/list: only search/execute/connection_status.
    let list_resp = post_with_retry(
        &client,
        &url,
        &serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }),
    )
    .await;
    let tool_names: Vec<&str> = list_resp["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(tool_names.contains(&"search"));
    assert!(tool_names.contains(&"execute"));
    assert_eq!(
        tool_names.len(),
        3,
        "expected exactly search/execute/connection_status, got {tool_names:?}"
    );

    // 2. search finds the real, inventory-registered GetAllServers query.
    let search_resp = post_with_retry(
        &client,
        &url,
        &tool_call(2, "search", serde_json::json!({ "query": "GetAllServers" })),
    )
    .await;
    let search_text = search_resp["result"]["content"][0]["text"]
        .as_str()
        .expect("text content");
    let search_body: serde_json::Value = serde_json::from_str(search_text).expect("valid JSON");
    let ops = search_body["operations"]
        .as_array()
        .expect("operations array");
    assert!(
        ops.iter()
            .any(|op| op["id"] == "GetAllServers" && op["kind"] == "query"),
        "expected GetAllServers in search results, got {ops:?}"
    );

    // 3. execute runs a script chaining a real query call through the
    // in-process Executor and returns its JSON result.
    let execute_resp = post_with_retry(
        &client,
        &url,
        &tool_call(
            3,
            "execute",
            serde_json::json!({ "code": "const r = await myko.query('GetAllServers', {}); return r.count;" }),
        ),
    )
    .await;
    let execute_result = &execute_resp["result"];
    assert_ne!(
        execute_result["isError"],
        serde_json::json!(true),
        "execute should succeed, got {execute_result:?}"
    );
    let count_text = execute_result["content"][0]["text"]
        .as_str()
        .expect("text content");
    let count: i64 = count_text
        .trim()
        .parse()
        .expect("execute should return the query's item count");
    assert!(count >= 0);

    handle.abort();
}

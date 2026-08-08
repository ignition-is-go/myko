//! End-to-end verification of the MCP "Code Mode" `search`/`execute` tools
//! over a real HTTP connection, against this repo's own built-in `Server`
//! entity — not just the in-memory `dispatch::handle_request` unit tests.

use std::{sync::Arc, time::Duration};

use myko::{
    command::{CommandContext, CommandError, CommandHandler},
    myko_command,
};
use myko_server::{MykoServer, mcp::dispatch::ServerInfo};

#[myko_command(String)]
#[serde(deny_unknown_fields)]
struct StrictEcho {
    value: String,
}

impl CommandHandler for StrictEcho {
    fn execute(self, _ctx: CommandContext) -> Result<String, CommandError> {
        Ok(self.value)
    }
}

async fn post_with_retry(
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
                eprintln!("POST {url}: {error:?}");
                return None;
            }
        }
    }
}

macro_rules! require_some {
    ($value:expr, $message:literal) => {
        match {
            let value = $value;
            assert!(value.is_some(), $message);
            value
        } {
            Some(value) => value,
            None => return,
        }
    };
}

fn tool_call(id: i64, name: &str, arguments: serde_json::Value) -> serde_json::Value {
    let call = serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/call",
        "params": { "name": name, "arguments": arguments }
    });
    drop(arguments);
    call
}

fn spawn_test_server(addr: std::net::SocketAddr) -> tokio::task::JoinHandle<()> {
    let server = Arc::new(
        MykoServer::builder()
            .with_bind_addr(addr)
            .with_server_info(ServerInfo::default())
            .build(),
    );
    tokio::spawn(async move {
        let _ = server.run_ws_loop().await;
    })
}

async fn assert_tool_discovery(client: &reqwest::Client, url: &str) {
    let list_resp = require_some!(
        post_with_retry(
            client,
            url,
            &serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }),
        )
        .await,
        "tools/list response"
    );
    let tools = require_some!(
        list_resp
            .pointer("/result/tools")
            .and_then(serde_json::Value::as_array),
        "tools array"
    );
    let tool_names: Vec<&str> = tools
        .iter()
        .filter_map(|tool| tool.get("name").and_then(serde_json::Value::as_str))
        .collect();
    assert!(tool_names.contains(&"search"));
    assert!(tool_names.contains(&"execute"));
    assert_eq!(
        tool_names.len(),
        3,
        "expected exactly search/execute/connection_status, got {tool_names:?}"
    );

    let search_resp = require_some!(
        post_with_retry(
            client,
            url,
            &tool_call(2, "search", serde_json::json!({ "query": "GetAllServers" })),
        )
        .await,
        "search response"
    );
    let search_text = require_some!(
        search_resp
            .pointer("/result/content/0/text")
            .and_then(serde_json::Value::as_str),
        "text content"
    );
    let search_body = require_some!(
        serde_json::from_str::<serde_json::Value>(search_text).ok(),
        "valid JSON"
    );
    let ops = require_some!(
        search_body
            .get("operations")
            .and_then(serde_json::Value::as_array),
        "operations array"
    );
    assert!(
        ops.iter().any(
            |op| op.get("id") == Some(&serde_json::json!("GetAllServers"))
                && op.get("kind") == Some(&serde_json::json!("query"))
        ),
        "expected GetAllServers in search results, got {ops:?}"
    );
}

async fn assert_query_execution(client: &reqwest::Client, url: &str) {
    let execute_resp = require_some!(post_with_retry(
        client,
        url,
        &tool_call(
            3,
            "execute",
            serde_json::json!({ "code": "const r = await myko.query('GetAllServers', {}); return r.count;" }),
        ),
    )
    .await, "execute response");
    let execute_result = require_some!(execute_resp.get("result"), "execute result");
    assert_ne!(
        execute_result.get("isError"),
        Some(&serde_json::json!(true)),
        "execute should succeed, got {execute_result:?}"
    );
    let count_text = require_some!(
        execute_result
            .pointer("/content/0/text")
            .and_then(serde_json::Value::as_str),
        "text content"
    );
    let count = require_some!(
        count_text.trim().parse::<i64>().ok(),
        "execute should return the query's item count"
    );
    assert!(count >= 0);
}

async fn assert_strict_commands(client: &reqwest::Client, url: &str) {
    let strict_resp = require_some!(post_with_retry(
        client,
        url,
        &tool_call(
            4,
            "execute",
            serde_json::json!({ "code": "return await myko.command('StrictEcho', {value: 'ok'});" }),
        ),
    )
    .await, "strict command response");
    let strict_text = require_some!(
        strict_resp
            .pointer("/result/content/0/text")
            .and_then(serde_json::Value::as_str),
        "strict command text content"
    );
    let strict_body = require_some!(
        serde_json::from_str::<serde_json::Value>(strict_text).ok(),
        "strict command wrapper JSON"
    );
    assert_eq!(strict_body.get("success"), Some(&serde_json::json!(true)));
    assert_eq!(strict_body.get("result"), Some(&serde_json::json!("ok")));

    let unknown_resp = require_some!(post_with_retry(
        client,
        url,
        &tool_call(
            5,
            "execute",
            serde_json::json!({
                "code": "try { await myko.command('StrictEcho', {value: 'ok', unexpected: true}); return 'accepted'; } catch (e) { return e.message; }"
            }),
        ),
    )
    .await, "unknown field response");
    let unknown_text = require_some!(
        unknown_resp
            .pointer("/result/content/0/text")
            .and_then(serde_json::Value::as_str),
        "strict rejection text content"
    );
    assert!(
        unknown_text.contains("unknown field `unexpected`"),
        "strict command must keep rejecting user fields: {unknown_text}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_then_execute_round_trip_over_http() {
    let _strict_args_type_is_used = StrictEchoArgs {
        value: "probe".to_string(),
    };

    let probe = std::net::TcpListener::bind("127.0.0.1:0");
    assert!(probe.is_ok(), "bind test listener");
    let Ok(probe) = probe else {
        return;
    };
    let addr = probe.local_addr();
    assert!(addr.is_ok(), "read test listener address");
    let Ok(addr) = addr else {
        return;
    };
    drop(probe);

    let handle = spawn_test_server(addr);

    let client = reqwest::Client::new();
    let url = format!("http://{addr}/myko/mcp");

    assert_tool_discovery(&client, &url).await;

    assert_query_execution(&client, &url).await;

    assert_strict_commands(&client, &url).await;

    handle.abort();
}

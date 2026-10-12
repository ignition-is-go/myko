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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct NestedInput {
    label: String,
}

#[myko_command(String)]
#[serde(deny_unknown_fields)]
pub struct WireEcho {
    #[serde(rename = "wireValue", alias = "oldValue")]
    rust_value: String,
    nested: NestedInput,
    #[serde(default)]
    max_count: u32,
}

impl CommandHandler for WireEcho {
    fn execute(self, _ctx: CommandContext) -> Result<String, CommandError> {
        Ok(format!(
            "{}:{}:{}",
            self.rust_value, self.nested.label, self.max_count
        ))
    }
}

myko::register_operation_example!(
    "command",
    WireEcho,
    serde_json::json!({
        "wireValue": "example", "nested": {"label": "nested"}
    })
);

#[myko_command(Vec<serde_json::Value>)]
pub struct Records {}

impl CommandHandler for Records {
    fn execute(self, _ctx: CommandContext) -> Result<Vec<serde_json::Value>, CommandError> {
        Ok(vec![
            serde_json::json!({"id": "a"}),
            serde_json::json!({"id": "b"}),
        ])
    }
}

#[myko_command(Vec<serde_json::Value>)]
pub struct References {}

impl CommandHandler for References {
    fn execute(self, _ctx: CommandContext) -> Result<Vec<serde_json::Value>, CommandError> {
        Ok(vec![
            serde_json::json!({"group": "one", "recordId": "a"}),
            serde_json::json!({"group": "one", "recordId": "a"}),
            serde_json::json!({"group": "one", "recordId": "missing"}),
            serde_json::json!({"group": "two", "recordId": "b"}),
        ])
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

async fn execute_code(
    client: &reqwest::Client,
    url: &str,
    code: &str,
) -> Result<serde_json::Value, String> {
    let response = post_with_retry(
        client,
        url,
        &tool_call(20, "execute", serde_json::json!({"code": code})),
    )
    .await
    .ok_or("missing execute response")?;
    assert_ne!(
        response.pointer("/result/isError"),
        Some(&serde_json::json!(true)),
        "{response}"
    );
    let text = response
        .pointer("/result/content/0/text")
        .and_then(serde_json::Value::as_str)
        .ok_or("missing result text")?;
    serde_json::from_str(text).map_err(|error| error.to_string())
}

async fn assert_contract_discovery(client: &reqwest::Client, url: &str) -> Result<(), String> {
    let response = post_with_retry(
        client,
        url,
        &tool_call(
            21,
            "search",
            serde_json::json!({
                "query": "WireEcho", "kind": "command", "details": true
            }),
        ),
    )
    .await
    .ok_or("missing search response")?;
    let text = response
        .pointer("/result/content/0/text")
        .and_then(serde_json::Value::as_str)
        .ok_or("missing search text")?;
    let body: serde_json::Value = serde_json::from_str(text).map_err(|error| error.to_string())?;
    let description = body.pointer("/operations/0").ok_or("missing description")?;
    assert_eq!(
        description.get("outputType"),
        Some(&serde_json::json!({
            "command_id": "string", "success": "true", "result": "string"
        }))
    );
    assert_eq!(
        description.pointer("/inputType/0/name"),
        Some(&serde_json::json!("wireValue"))
    );
    assert_eq!(
        description.pointer("/inputType/0/aliases"),
        Some(&serde_json::json!(["oldValue"]))
    );
    assert_eq!(
        description.pointer("/inputType/2/name"),
        Some(&serde_json::json!("maxCount"))
    );
    assert_eq!(
        description.pointer("/inputType/2/optional"),
        Some(&serde_json::json!(true))
    );
    let example = description
        .get("example")
        .and_then(serde_json::Value::as_str)
        .ok_or("missing validated example")?;
    assert_eq!(
        execute_code(client, url, example).await?.get("result"),
        Some(&serde_json::json!("example:nested:0"))
    );
    Ok(())
}

async fn assert_validation_and_computation(
    client: &reqwest::Client,
    url: &str,
) -> Result<(), String> {
    let diagnostics = execute_code(
        client,
        url,
        r"
        const failures = await Promise.all([
          myko.command('WireEcho', {wireValue: 'x', nested: {label: 7}}).catch(e => e.message),
          myko.command('WireEcho', []).catch(e => e.message),
          myko.command('WireEcho', {rust_value: 'x', nested: {label: 'y'}}).catch(e => e.message),
        ]);
        const valid = await myko.command('WireEcho', {oldValue: 'x', nested: {label: 'y'}});
        return {failures, valid: valid.result};
    ",
    )
    .await?;
    let nested = diagnostics
        .pointer("/failures/0")
        .and_then(serde_json::Value::as_str)
        .ok_or("missing nested diagnostic")?;
    let array = diagnostics
        .pointer("/failures/1")
        .and_then(serde_json::Value::as_str)
        .ok_or("missing array diagnostic")?;
    let unknown = diagnostics
        .pointer("/failures/2")
        .and_then(serde_json::Value::as_str)
        .ok_or("missing unknown field diagnostic")?;
    assert!(nested.contains("nested.label"), "{nested}");
    assert!(array.contains("expected an argument object"), "{array}");
    assert!(unknown.contains("rust_value"), "{unknown}");
    assert_eq!(diagnostics.get("valid"), Some(&serde_json::json!("x:y:0")));

    let computed = execute_code(
        client,
        url,
        r"
        const [records, references] = await Promise.all([
          myko.command('Records', {}), myko.command('References', {})
        ]);
        const ids = new Set(records.result.map(record => record.id));
        const members = new Map();
        for (const reference of references.result) {
          if (!ids.has(reference.recordId)) continue;
          if (!members.has(reference.group)) members.set(reference.group, new Set());
          members.get(reference.group).add(reference.recordId);
        }
        return Object.fromEntries([...members].map(([group, ids]) => [group, ids.size]));
    ",
    )
    .await?;
    assert_eq!(computed, serde_json::json!({"one": 1, "two": 1}));
    Ok(())
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
    let discovery = assert_contract_discovery(&client, &url).await;
    assert!(discovery.is_ok(), "{discovery:?}");
    let computation = assert_validation_and_computation(&client, &url).await;
    assert!(computation.is_ok(), "{computation:?}");

    handle.abort();
}

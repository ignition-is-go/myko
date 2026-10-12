//! End-to-end verification of the MCP "Code Mode" `search`/`execute` tools
//! over a real HTTP connection, against this repo's own built-in `Server`
//! entity — not just the in-memory `dispatch::handle_request` unit tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

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

static CONTRACT_CALLS: AtomicUsize = AtomicUsize::new(0);
static DESERIALIZER_CALLS: AtomicUsize = AtomicUsize::new(0);

fn parse_values<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<String>, D::Error> {
    DESERIALIZER_CALLS.fetch_add(1, Ordering::SeqCst);
    serde::Deserialize::deserialize(deserializer)
}

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, myko::schemars::JsonSchema, myko::TS,
)]
#[schemars(crate = "myko::schemars")]
#[ts(crate = "myko::ts_rs")]
struct ProbeConfig {
    revision: u32,
    mode: ProbeMode,
}

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, myko::schemars::JsonSchema, myko::TS,
)]
#[schemars(crate = "myko::schemars")]
#[ts(crate = "myko::ts_rs")]
#[serde(rename_all = "snake_case")]
enum ProbeMode {
    Fast,
    Precise,
}

#[myko_command(String)]
#[serde(deny_unknown_fields)]
struct ContractProbe {
    scene_id: String,
    #[serde(
        rename(serialize = "storedValue", deserialize = "wireValue"),
        alias = "legacyValue"
    )]
    input_value: String,
    #[serde(deserialize_with = "parse_values")]
    values: Vec<String>,
    #[serde(default)]
    retries: u32,
    optional_value: Option<String>,
    config: Option<ProbeConfig>,
    #[serde(skip_deserializing)]
    ignored: String,
}

impl CommandHandler for ContractProbe {
    fn execute(self, _ctx: CommandContext) -> Result<String, CommandError> {
        CONTRACT_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(format!(
            "{}:{}:{}:{}:{:?}:{}:{:?}",
            self.scene_id,
            self.input_value,
            self.values.len(),
            self.retries,
            self.optional_value,
            self.ignored,
            self.config
        ))
    }
}

#[myko_command(String)]
struct FlattenProbe {
    #[serde(flatten)]
    extras: std::collections::BTreeMap<String, String>,
}

impl CommandHandler for FlattenProbe {
    fn execute(self, _ctx: CommandContext) -> Result<String, CommandError> {
        Ok(self.extras.len().to_string())
    }
}

#[myko_command(String)]
#[serde(rename_all = "snake_case")]
struct SnakeEcho {
    scene_id: String,
}

impl CommandHandler for SnakeEcho {
    fn execute(self, _ctx: CommandContext) -> Result<String, CommandError> {
        Ok(self.scene_id)
    }
}

mod computation_record {
    use myko::prelude::{EventPublishing, Querying, RegistryScoped, RequestScoped};

    #[myko::myko_item]
    pub struct ComputationRecord {
        pub payload: String,
    }
}

mod computation_reference {
    use myko::prelude::{EventPublishing, Querying, RegistryScoped, RequestScoped};

    use super::computation_record::ComputationRecordId;

    #[myko::myko_item]
    pub struct ComputationReference {
        pub entity_id: ComputationRecordId,
        pub group: String,
    }
}

use computation_record::ComputationRecord;
use computation_reference::ComputationReference;

fn seed_computation_records(server: &MykoServer) {
    let ctx = server.ctx();
    for id in 0..36 {
        assert!(
            ctx.set(&ComputationRecord {
                id: format!("item-{id}").into(),
                payload: "x".repeat(4096),
            })
            .is_ok()
        );
    }
    let deleted = (18..24)
        .map(|id| ComputationRecord {
            id: format!("missing-{id}").into(),
            payload: "x".repeat(4096),
        })
        .collect::<Vec<_>>();
    assert!(ctx.batch_set(&deleted).is_ok());
    let mut references = (0..18)
        .map(|id| (format!("item-{id}"), "shared"))
        .collect::<Vec<_>>();
    references.extend((18..29).map(|id| (format!("item-{id}"), "a")));
    references.extend((29..36).map(|id| (format!("item-{id}"), "b")));
    references.extend((18..24).map(|id| (format!("missing-{id}"), "shared")));
    references.push(("item-0".to_string(), "shared"));
    for (id, (entity_id, group)) in references.into_iter().enumerate() {
        assert!(
            ctx.set(&ComputationReference {
                id: format!("reference-{id}").into(),
                entity_id: entity_id.into(),
                group: group.to_string(),
            })
            .is_ok()
        );
    }
    for record in deleted {
        assert!(ctx.del(&record).is_ok());
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

fn spawn_test_server(
    addr: std::net::SocketAddr,
    setup: impl FnOnce(&MykoServer) + Send + 'static,
) -> tokio::task::JoinHandle<()> {
    let server = Arc::new(
        MykoServer::builder()
            .with_bind_addr(addr)
            .with_server_info(ServerInfo::default())
            .build(),
    );
    setup(&server);
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

fn assert_contract_shape(contract: &serde_json::Value) {
    let args = require_some!(
        contract
            .pointer("/operation/args")
            .and_then(serde_json::Value::as_array),
        "argument list"
    );
    assert!(
        args.iter()
            .any(|arg| arg.get("name") == Some(&serde_json::json!("sceneId")))
    );
    let wire = require_some!(
        args.iter()
            .find(|arg| arg.get("name") == Some(&serde_json::json!("wireValue"))),
        "wireValue argument"
    );
    assert_eq!(
        wire.get("aliases"),
        Some(&serde_json::json!(["legacyValue"]))
    );
    assert!(
        args.iter()
            .any(|arg| arg.get("name") == Some(&serde_json::json!("retries"))
                && arg.get("optional") == Some(&serde_json::json!(true)))
    );
    assert!(
        !args
            .iter()
            .any(|arg| arg.get("name") == Some(&serde_json::json!("ignored")))
    );
    assert_eq!(
        contract.pointer("/resultShape/result"),
        Some(&serde_json::json!("string"))
    );
}

fn assert_generated_input_schema(contract: &serde_json::Value) {
    let schema = require_some!(contract.get("inputSchema"), "input schema");
    assert_eq!(
        schema.pointer("/properties/wireValue/$ref"),
        schema.pointer("/properties/legacyValue/$ref")
    );
    let validator = jsonschema::validator_for(schema);
    assert!(validator.is_ok());
    let Ok(validator) = validator else {
        return;
    };
    assert!(validator.is_valid(&serde_json::json!({"sceneId":"s", "wireValue":"v", "values":[]})));
    assert!(validator.is_valid(&serde_json::json!({"sceneId":"s", "legacyValue":"v", "values":[], "config":{"revision":1, "mode":"fast"}})));
    assert!(!validator.is_valid(&serde_json::json!({"sceneId":"s", "values":[]})));
    let reference = require_some!(
        schema
            .pointer("/properties/values/$ref")
            .and_then(serde_json::Value::as_str),
        "opaque field reference"
    );
    assert_eq!(
        schema
            .pointer(reference.trim_start_matches('#'))
            .and_then(|value| value.get("x-myko-opaque")),
        Some(&serde_json::json!(true))
    );
}

async fn assert_operation_contracts(client: &reqwest::Client, url: &str) {
    let description = require_some!(
        post_with_retry(
            client,
            url,
            &tool_call(
                6,
                "search",
                serde_json::json!({"kind": "command", "id": "ContractProbe", "details": true})
            )
        )
        .await,
        "contract response"
    );
    let text = require_some!(
        description
            .pointer("/result/content/0/text")
            .and_then(serde_json::Value::as_str),
        "contract text"
    );
    let contract = require_some!(
        serde_json::from_str::<serde_json::Value>(text).ok(),
        "contract JSON"
    );
    assert_contract_shape(&contract);
    assert_generated_input_schema(&contract);
    assert_eq!(DESERIALIZER_CALLS.load(Ordering::SeqCst), 0);

    let execution = require_some!(post_with_retry(client, url, &tool_call(7, "execute", serde_json::json!({"code": r"
        const contract = myko.describe('command', 'ContractProbe');
        const errors = [];
        for (const args of [
            {scene_id: 's', wireValue: 'v', values: []},
            {sceneId: 's', wireValue: 'v', values: [123]},
            {sceneId: 's', wireValue: 'v', values: [], unexpected: true},
            []
        ]) {
            try { await myko.command('ContractProbe', args); errors.push('accepted'); }
            catch (e) { errors.push(e.message); }
        }
        const valid = await myko.command('ContractProbe', {sceneId: 's', legacyValue: 'v', values: []});
        return {contract, errors, valid};
    "}))).await, "contract execution response");
    let text = require_some!(
        execution
            .pointer("/result/content/0/text")
            .and_then(serde_json::Value::as_str),
        "execution text"
    );
    let result = require_some!(
        serde_json::from_str::<serde_json::Value>(text).ok(),
        "execution JSON"
    );
    assert_eq!(result.get("contract"), Some(&contract));
    let errors = require_some!(
        result.get("errors").and_then(serde_json::Value::as_array),
        "validation errors"
    );
    assert!(
        errors
            .first()
            .and_then(serde_json::Value::as_str)
            .is_some_and(|error| error.contains("sceneId"))
    );
    assert!(
        errors
            .get(1)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|error| error.contains("values[0]") && error.contains("string"))
    );
    assert!(
        errors
            .get(2)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|error| error.contains("unexpected"))
    );
    assert!(
        errors
            .get(3)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|error| error.contains("arguments must be an object"))
    );
    assert_eq!(
        result.pointer("/valid/result"),
        Some(&serde_json::json!("s:v:0:0:None::None"))
    );
    assert_eq!(CONTRACT_CALLS.load(Ordering::SeqCst), 1);
}

async fn assert_single_deserialization(client: &reqwest::Client, url: &str) {
    let before = DESERIALIZER_CALLS.load(Ordering::SeqCst);
    let response = require_some!(post_with_retry(client, url, &tool_call(9, "execute", serde_json::json!({"code":
        "return await myko.command('ContractProbe', {sceneId: 's', wireValue: 'v', values: [], config: {revision: 1, mode: 'fast'}});"
    }))).await, "custom deserializer response");
    assert_ne!(
        response.pointer("/result/isError"),
        Some(&serde_json::json!(true))
    );
    assert_eq!(
        DESERIALIZER_CALLS.load(Ordering::SeqCst),
        before.saturating_add(1)
    );
}

async fn assert_schema_preflight(client: &reqwest::Client, url: &str) {
    let before = DESERIALIZER_CALLS.load(Ordering::SeqCst);
    let response = require_some!(post_with_retry(client, url, &tool_call(10, "execute", serde_json::json!({"code":
        "try { await myko.command('ContractProbe', {sceneId:'s', wireValue:'v', values:[], config:{revision:1, mode:'invalid'}}); return 'accepted'; } catch(e) { return e.message; }"
    }))).await, "preflight response");
    let text = require_some!(
        response
            .pointer("/result/content/0/text")
            .and_then(serde_json::Value::as_str),
        "preflight text"
    );
    assert!(text.contains("Invalid arguments for command_ContractProbe"));
    assert!(text.contains("/config/mode"));
    assert_eq!(DESERIALIZER_CALLS.load(Ordering::SeqCst), before);
}

async fn assert_flattened_input(client: &reqwest::Client, url: &str) {
    let _args = FlattenProbeArgs {
        extras: std::collections::BTreeMap::new(),
    };
    let response = require_some!(post_with_retry(client, url, &tool_call(12, "execute", serde_json::json!({"code":
        "const d=myko.describe('command','FlattenProbe'); const r=await myko.command('FlattenProbe',{free:'value'}); return {d,r};"
    }))).await, "flattened input response");
    let text = require_some!(
        response
            .pointer("/result/content/0/text")
            .and_then(serde_json::Value::as_str),
        "flattened input text"
    );
    let result = require_some!(
        serde_json::from_str::<serde_json::Value>(text).ok(),
        "flattened input JSON"
    );
    assert_eq!(result.pointer("/r/result"), Some(&serde_json::json!("1")));
    assert_eq!(
        result.pointer("/d/operation/args/0/flattened"),
        Some(&serde_json::json!(true))
    );
    assert!(result.pointer("/d/inputSchema/properties/extras").is_none());
}

async fn assert_container_naming(client: &reqwest::Client, url: &str) {
    let args = serde_json::from_value::<SnakeEchoArgs>(serde_json::json!({"scene_id": "s"}));
    assert!(args.is_ok());
    let strict_args = serde_json::from_value::<ContractProbeArgs>(serde_json::json!({
        "sceneId": "s", "wireValue": "v", "values": [], "unexpected": true
    }));
    assert!(strict_args.is_err());
    let response = require_some!(post_with_retry(client, url, &tool_call(8, "execute", serde_json::json!({"code":
        "const d = myko.describe('command', 'SnakeEcho'); const r = await myko.command('SnakeEcho', {scene_id: 's'}); return {d, r};"
    }))).await, "snake case response");
    let text = require_some!(
        response
            .pointer("/result/content/0/text")
            .and_then(serde_json::Value::as_str),
        "snake case text"
    );
    let result = require_some!(
        serde_json::from_str::<serde_json::Value>(text).ok(),
        "snake case JSON"
    );
    assert_eq!(
        result.pointer("/d/operation/args/0/name"),
        Some(&serde_json::json!("scene_id"))
    );
    assert_eq!(result.pointer("/r/result"), Some(&serde_json::json!("s")));
}

myko::register_domain_guide!(
    "computation-guide",
    "Computation vocabulary",
    "How computation records and references join",
    "Join references.entityId against records.id; unresolved references do not contribute membership."
);

async fn assert_domain_discovery(client: &reqwest::Client, url: &str) {
    let response = require_some!(post_with_retry(client, url, &tool_call(20, "execute", serde_json::json!({"code":
        "const entity = myko.describe('entity', 'ComputationRecord'); const guide = myko.describe('guide', 'computation-guide'); return {entity, guide};"
    }))).await, "domain response");
    let text = require_some!(
        response
            .pointer("/result/content/0/text")
            .and_then(serde_json::Value::as_str),
        "domain text"
    );
    let value = require_some!(
        serde_json::from_str::<serde_json::Value>(text).ok(),
        "domain JSON"
    );
    assert_eq!(
        value.pointer("/entity/id"),
        Some(&serde_json::json!("ComputationRecord"))
    );
    assert!(
        value
            .pointer("/entity/fields")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|fields| fields
                .iter()
                .any(|field| field.get("name") == Some(&serde_json::json!("payload"))))
    );
    assert!(
        value
            .pointer("/guide/content")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|text| text.contains("references.entityId"))
    );
    let resource = require_some!(post_with_retry(client, url, &serde_json::json!({"jsonrpc":"2.0","id":21,"method":"resources/read","params":{"uri":"myko://domain/entity/ComputationRecord"}})).await, "entity resource");
    let resource_text = require_some!(
        resource
            .pointer("/result/contents/0/text")
            .and_then(serde_json::Value::as_str),
        "resource text"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(resource_text)
            .ok()
            .as_ref(),
        value.get("entity")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_then_execute_round_trip_over_http() {
    let _contract_args_type_is_used = ContractProbeArgs {
        scene_id: "s".to_string(),
        input_value: "v".to_string(),
        values: vec![],
        retries: 0,
        optional_value: None,
        config: None,
        ignored: String::new(),
    };
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

    let handle = spawn_test_server(addr, |_| {});

    let client = reqwest::Client::new();
    let url = format!("http://{addr}/myko/mcp");

    assert_tool_discovery(&client, &url).await;

    assert_query_execution(&client, &url).await;

    assert_strict_commands(&client, &url).await;
    assert_operation_contracts(&client, &url).await;
    assert_container_naming(&client, &url).await;
    assert_domain_discovery(&client, &url).await;
    assert_single_deserialization(&client, &url).await;
    assert_schema_preflight(&client, &url).await;
    assert_flattened_input(&client, &url).await;

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn programmatic_membership_summary_over_http() {
    let probe = std::net::TcpListener::bind("127.0.0.1:0");
    assert!(probe.is_ok());
    let Ok(probe) = probe else {
        return;
    };
    let address = probe.local_addr();
    assert!(address.is_ok());
    let Ok(address) = address else {
        return;
    };
    drop(probe);
    let server = spawn_test_server(address, seed_computation_records);
    let client = reqwest::Client::new();
    let url = format!("http://{address}/myko/mcp");
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "a": {"type": "integer", "minimum": 0}, "b": {"type": "integer", "minimum": 0},
            "shared": {"type": "integer", "minimum": 0}, "onlyA": {"type": "integer", "minimum": 0},
            "onlyB": {"type": "integer", "minimum": 0}, "unresolved": {"type": "integer", "minimum": 0}
        },
        "required": ["a", "b", "shared", "onlyA", "onlyB", "unresolved"], "additionalProperties": false
    });
    let response = require_some!(post_with_retry(&client, &url, &tool_call(11, "execute", serde_json::json!({
        "outputSchema": schema, "code": r"
            const [entities, references] = await Promise.all([
                myko.query('GetAllComputationRecords', {}), myko.query('GetAllComputationReferences', {})
            ]);
            const byId = new Map(entities.items.map(row => [row.id, row]));
            const refs = references.items;
            const select = group => new Set(refs.filter(row => row.group === 'shared' || row.group === group)
                .filter(row => byId.has(row.entityId)).map(row => row.entityId));
            const a = select('a');
            const b = select('b');
            return {
                a: a.size, b: b.size,
                shared: [...a].filter(id => b.has(id)).length,
                onlyA: [...a].filter(id => !b.has(id)).length,
                onlyB: [...b].filter(id => !a.has(id)).length,
                unresolved: new Set(refs.filter(row => !byId.has(row.entityId)).map(row => row.entityId)).size
            };
        "
    }))).await, "computed summary response");
    assert_ne!(
        response.pointer("/result/isError"),
        Some(&serde_json::json!(true)),
        "{response}"
    );
    let text = require_some!(
        response
            .pointer("/result/content/0/text")
            .and_then(serde_json::Value::as_str),
        "summary text"
    );
    let result = require_some!(
        serde_json::from_str::<serde_json::Value>(text).ok(),
        "summary JSON"
    );
    assert_eq!(
        result,
        serde_json::json!({"a":29, "b":25, "shared":18, "onlyA":11, "onlyB":7, "unresolved":6})
    );
    assert!(text.len() < 256);
    assert!(!text.contains("payload"));
    server.abort();
}

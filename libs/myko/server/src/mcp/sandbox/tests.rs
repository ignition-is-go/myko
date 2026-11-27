use super::*;
use myko::prelude::*;
use myko::{
    hyphae::{Cell, Definite, MapQuery, Materialize},
    report::{ReportContext, ReportHandler},
    view::{ViewBuildArgs, ViewHandler},
};

#[myko::myko_item]
pub struct SandboxTransferRow {
    #[serde(serialize_with = "serialize_transfer_payload")]
    pub payload: String,
}

static REVOKE_ON_SERIALIZE: std::sync::Mutex<Option<myko_auth::AuthSession>> =
    std::sync::Mutex::new(None);
fn serialize_transfer_payload<S: serde::Serializer>(
    value: &str,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let session = REVOKE_ON_SERIALIZE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(session) = session {
        session.revoke();
    }
    value.serialize(serializer)
}

#[myko::myko_view(SandboxTransferRow)]
pub struct SandboxTransferView {}
impl ViewHandler for SandboxTransferView {
    fn build_cell(
        ctx: ViewBuildArgs<Self>,
    ) -> impl MapQuery<Key = Arc<str>, Value = Arc<SandboxTransferRow>> {
        ctx.view_context
            .query_map_by_str(GetAllSandboxTransferRows {})
    }
}

#[myko::myko_report(bool)]
pub struct SandboxTransferReport {}
impl ReportHandler for SandboxTransferReport {
    type Output = bool;
    fn compute(&self, _: ReportContext) -> impl Materialize<Arc<bool>, Definite> {
        Cell::new(Arc::new(true)).lock()
    }
}

#[tokio::test]
async fn revocation_during_row_conversion_discards_the_result() -> anyhow::Result<()> {
    use myko_auth::{
        AuthError, AuthSession, AuthenticatedIdentity, AuthorizationRequest, Authorizer, Principal,
        PrincipalKind,
    };
    struct Allow;
    impl Authorizer for Allow {
        fn authorize(
            &self,
            _: &AuthenticatedIdentity,
            _: AuthorizationRequest<'_>,
        ) -> Result<(), AuthError> {
            Ok(())
        }
    }
    let session = AuthSession::new(
        AuthenticatedIdentity {
            user_id: None,
            principal: Principal {
                issuer: "test".into(),
                subject: "transfer".into(),
                kind: PrincipalKind::Service,
            },
            client_id: None,
            scopes: Vec::new(),
            delegation: Vec::new(),
            claims: Value::Null,
            expires_at: std::time::SystemTime::now() + std::time::Duration::from_secs(30),
        },
        Arc::new(Allow),
    );
    let server = crate::MykoServer::builder()
        .with_default_persister(Arc::new(crate::BlackholePersister))
        .build();
    let source = Arc::new(server.ctx());
    source.set_transient(&SandboxTransferRow {
        id: "revoke-transfer".into(),
        payload: "private".into(),
    })?;
    *REVOKE_ON_SERIALIZE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(session.clone());
    let result = execute(
        "return await myko.query('GetAllSandboxTransferRows', {});",
        Arc::new(Executor::External {
            ctx: source,
            session,
        }),
        ClientFilters::allow_all(),
    )
    .await;
    anyhow::ensure!(
        result
            .as_ref()
            .is_err_and(|message| message.contains("revoked")),
        "{result:?}"
    );
    anyhow::ensure!(
        REVOKE_ON_SERIALIZE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn native_view_and_report_keep_canonical_contracts() -> anyhow::Result<()> {
    let server = crate::MykoServer::builder()
        .with_default_persister(Arc::new(crate::BlackholePersister))
        .build();
    let result = execute("return await Promise.all([myko.view('SandboxTransferView', {}), myko.report('SandboxTransferReport', {})]);", Arc::new(Executor::InProcess(Arc::new(server.ctx()))), ClientFilters::allow_all()).await;
    {
        let actual = result;
        let expected = Ok(serde_json::json!([
            myko::operation_index::VIEW_RESULT.wrap(
                "SandboxTransferView",
                "SandboxTransferRow",
                serde_json::json!([])
            ),
            myko::operation_index::REPORT_RESULT.wrap(
                "SandboxTransferReport",
                "bool",
                serde_json::json!(true)
            )
        ]));
        anyhow::ensure!(
            actual == expected,
            "actual={actual:?}, expected={expected:?}"
        );
    };
    Ok(())
}

static TRANSFER_COMMAND_CALLS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[myko::myko_command(String)]
struct SandboxTransferCommand {}
impl CommandHandler for SandboxTransferCommand {
    fn execute(self, _: CommandContext) -> Result<String, CommandError> {
        TRANSFER_COMMAND_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok("x".repeat(640 * 1024))
    }
}

#[tokio::test]
async fn command_conversion_failure_does_not_retry_the_effect() -> anyhow::Result<()> {
    let _: SandboxTransferCommandArgs = SandboxTransferCommandArgs {};
    TRANSFER_COMMAND_CALLS.store(0, std::sync::atomic::Ordering::SeqCst);
    let server = crate::MykoServer::builder()
        .with_default_persister(Arc::new(crate::BlackholePersister))
        .build();
    let result = super::execute_with_memory_limit(
        "let held = 'y'.repeat(400*1024); try { await myko.command('SandboxTransferCommand', {}); return false; } catch (e) { held = null; return e.message.includes('out of memory'); }",
        Arc::new(Executor::InProcess(Arc::new(server.ctx()))),
        ClientFilters::allow_all(),
        Arc::new(myko::operation_index::build_operation_index()),
        1024 * 1024,
    ).await;
    {
        let actual = result;
        let expected = Ok(serde_json::json!(true));
        anyhow::ensure!(
            actual == expected,
            "actual={actual:?}, expected={expected:?}"
        );
    };
    {
        let actual = TRANSFER_COMMAND_CALLS.load(std::sync::atomic::Ordering::SeqCst);
        let expected = 1;
        anyhow::ensure!(
            actual == expected,
            "actual={actual:?}, expected={expected:?}"
        );
    };
    Ok(())
}

#[tokio::test]
async fn failed_query_conversion_releases_rows_and_allows_same_runtime_retry() -> anyhow::Result<()>
{
    let server = crate::MykoServer::builder()
        .with_default_persister(Arc::new(crate::BlackholePersister))
        .build();
    let source = Arc::new(server.ctx());
    source.set_transient(&SandboxTransferRow {
        id: "retry-transfer".into(),
        payload: "x".repeat(640 * 1024),
    })?;
    let result = super::execute_with_memory_limit(
        "let held = 'y'.repeat(400*1024); let failed = false; try { await myko.query('GetAllSandboxTransferRows', {}); } catch (e) { failed = e.message.includes('out of memory'); } held = null; const retry = await myko.query('GetAllSandboxTransferRows', {}); return {failed,count:retry.count};",
        Arc::new(Executor::InProcess(source.clone())),
        ClientFilters::allow_all(),
        Arc::new(myko::operation_index::build_operation_index()),
        1024 * 1024,
    ).await;
    {
        let actual = result;
        let expected = Ok(serde_json::json!({"failed":true,"count":1}));
        anyhow::ensure!(
            actual == expected,
            "actual={actual:?}, expected={expected:?}"
        );
    };
    let rows = source.query_snapshot(
        GetAllSandboxTransferRows {},
        source.new_server_transaction(),
    );
    {
        let actual = rows.len();
        let expected = 1;
        anyhow::ensure!(
            actual == expected,
            "actual={actual:?}, expected={expected:?}"
        );
    };
    {
        let actual = rows
            .first()
            .ok_or_else(|| anyhow::anyhow!("missing row"))?
            .payload
            .len();
        let expected = 640 * 1024;
        anyhow::ensure!(
            actual == expected,
            "actual={actual:?}, expected={expected:?}"
        );
    };
    Ok(())
}

#[tokio::test]
async fn partial_vm_allocation_failure_drops_every_native_row_reference() -> anyhow::Result<()> {
    let server = crate::MykoServer::builder()
        .with_default_persister(Arc::new(crate::BlackholePersister))
        .build();
    let source = Arc::new(server.ctx());
    source.set_transient(&SandboxTransferRow {
        id: "drop-transfer".into(),
        payload: "x".repeat(640 * 1024),
    })?;
    let executor = Arc::new(Executor::InProcess(source));
    let pending = call_operation(
        &executor,
        &ClientFilters::allow_all(),
        &myko::operation_index::build_operation_index(),
        "query",
        "GetAllSandboxTransferRows",
        None,
    )
    .await
    .map_err(anyhow::Error::msg)?;
    let preserved = match &pending.output {
        OperationResult::Snapshot(SnapshotResult {
            rows: Rows::Native(rows),
            ..
        }) => rows.clone(),
        _ => anyhow::bail!("native query must retain shared rows"),
    };
    let before = preserved.iter().map(Arc::strong_count).collect::<Vec<_>>();
    let runtime = rquickjs::Runtime::new()?;
    runtime.set_memory_limit(1024 * 1024);
    let context = rquickjs::Context::full(&runtime)?;
    context.with(|ctx| -> anyhow::Result<()> {
        ctx.eval::<(), _>("globalThis.held = 'y'.repeat(400*1024)")?;
        let response = BridgeResponse {
            result: Ok(pending),
            executor,
            kind: "query".into(),
            id: "GetAllSandboxTransferRows".into(),
            deadline: None,
        };
        anyhow::ensure!(response.into_js(&ctx).is_err());
        anyhow::ensure!(ctx.has_exception());
        let _exception = ctx.catch();
        Ok(())
    })?;
    for (row, previous) in preserved.iter().zip(before) {
        {
            let actual = Arc::strong_count(row);
            let expected = previous
                .checked_sub(1)
                .ok_or_else(|| anyhow::anyhow!("missing response reference"))?;
            anyhow::ensure!(
                actual == expected,
                "actual={actual:?}, expected={expected:?}"
            );
        };
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_query_calls_keep_their_results_and_contracts() -> anyhow::Result<()> {
    let server = crate::MykoServer::builder()
        .with_default_persister(Arc::new(crate::BlackholePersister))
        .build();
    let source = Arc::new(server.ctx());
    source.set_transient(&SandboxTransferRow {
        id: "concurrent-transfer".into(),
        payload: "value".into(),
    })?;
    let result = execute(
        "const results = await Promise.all([myko.query('GetAllSandboxTransferRows', {}), myko.query('GetAllSandboxTransferRows', {})]); return results.map(r=>({count:r.count,id:r.query_id,type:r.item_type,keys:Object.keys(r)}));",
        Arc::new(Executor::InProcess(source)),
        ClientFilters::allow_all(),
    ).await;
    let expected = serde_json::json!({"count":1,"id":"GetAllSandboxTransferRows","type":"SandboxTransferRow","keys":["count","item_type","items","query_id"]});
    {
        let actual = result;
        let expected = Ok(serde_json::json!([expected, expected]));
        anyhow::ensure!(
            actual == expected,
            "actual={actual:?}, expected={expected:?}"
        );
    };
    Ok(())
}

#[tokio::test]
async fn conversion_defines_own_properties_without_invoking_inherited_setters() -> anyhow::Result<()>
{
    let server = crate::MykoServer::builder()
        .with_default_persister(Arc::new(crate::BlackholePersister))
        .build();
    let source = Arc::new(server.ctx());
    source.set_transient(&SandboxTransferRow {
        id: "own-properties".into(),
        payload: "present".into(),
    })?;
    let result = execute(
        "const pending = myko.query('GetAllSandboxTransferRows',{}); Object.defineProperty(Array.prototype,'0',{set(){throw new Error('array setter called')}}); Object.defineProperty(Object.prototype,'value',{set(){throw new Error('object setter called')}}); const result = await pending; return result.items[0].payload;",
        Arc::new(Executor::InProcess(source)), ClientFilters::allow_all(),
    ).await;
    {
        let actual = result;
        let expected = Ok(serde_json::json!("present"));
        anyhow::ensure!(
            actual == expected,
            "actual={actual:?}, expected={expected:?}"
        );
    };
    Ok(())
}

#[tokio::test]
async fn canceling_a_pending_client_query_releases_executor_ownership() -> anyhow::Result<()> {
    let executor = dummy_executor();
    let before = Arc::strong_count(&executor);
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(20),
        execute(
            "return await myko.query('GetAllSandboxTransferRows', {});",
            executor.clone(),
            ClientFilters::allow_all(),
        ),
    )
    .await;
    anyhow::ensure!(
        result.is_err(),
        "the disconnected client query should still be pending"
    );
    {
        let actual = Arc::strong_count(&executor);
        let expected = before;
        anyhow::ensure!(
            actual == expected,
            "actual={actual:?}, expected={expected:?}"
        );
    };
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_query_and_view_keep_empty_and_nonempty_snapshots() -> anyhow::Result<()> {
    let probe = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = probe.local_addr()?;
    drop(probe);
    let server = Arc::new(
        crate::MykoServer::builder()
            .with_bind_addr(address)
            .with_default_persister(Arc::new(crate::BlackholePersister))
            .build(),
    );
    let source = server.ctx();
    let task = tokio::spawn(async move { server.run_ws_loop().await });
    let client = Arc::new(myko::client::MykoClient::new_with_auto_reconnect(false));
    client.set_address(Some(format!("ws://{address}/myko")));
    let connected = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if matches!(
                client.connection_status_sync(),
                myko::client::ConnectionStatus::Connected(_)
            ) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    if connected.is_err() {
        client.disconnect();
        task.abort();
        anyhow::bail!("loopback client did not connect");
    }
    let executor = Arc::new(Executor::Client(client.clone()));
    let empty = tokio::time::timeout(std::time::Duration::from_secs(5), execute(
        "const q=await myko.query('GetAllSandboxTransferRows',{}); const v=await myko.view('SandboxTransferView',{}); return [q.count,v.count];", executor.clone(), ClientFilters::allow_all(),
    )).await;
    source.set_transient(&SandboxTransferRow {
        id: "client-transfer".into(),
        payload: "present".into(),
    })?;
    let populated = tokio::time::timeout(std::time::Duration::from_secs(5), execute(
        "const q=await myko.query('GetAllSandboxTransferRows',{}); const v=await myko.view('SandboxTransferView',{}); return [q.items[0].payload,v.items[0].payload];", executor, ClientFilters::allow_all(),
    )).await;
    client.disconnect();
    task.abort();
    {
        let actual = empty;
        let expected = Ok(Ok(serde_json::json!([0, 0])));
        anyhow::ensure!(
            actual == expected,
            "actual={actual:?}, expected={expected:?}"
        );
    };
    {
        let actual = populated;
        let expected = Ok(Ok(serde_json::json!(["present", "present"])));
        anyhow::ensure!(
            actual == expected,
            "actual={actual:?}, expected={expected:?}"
        );
    };
    Ok(())
}

#[tokio::test]
async fn query_summary_fits_without_duplicate_encoded_payload() -> anyhow::Result<()> {
    let server = crate::MykoServer::builder()
        .with_default_persister(Arc::new(crate::BlackholePersister))
        .build();
    let source = Arc::new(server.ctx());
    source.set_transient(&SandboxTransferRow {
        id: "bounded-transfer".into(),
        payload: "x".repeat(640 * 1024),
    })?;
    let result = super::execute_with_memory_limit(
        "return (await myko.query('GetAllSandboxTransferRows', {})).count;",
        Arc::new(Executor::InProcess(source)),
        ClientFilters::allow_all(),
        Arc::new(myko::operation_index::build_operation_index()),
        1024 * 1024,
    )
    .await;
    {
        let actual = result;
        let expected = Ok(serde_json::json!(1));
        anyhow::ensure!(
            actual == expected,
            "actual={actual:?}, expected={expected:?}"
        );
    };
    Ok(())
}

#[test]
fn success_envelope_preserves_all_operation_contracts() -> anyhow::Result<()> {
    use myko::operation_index::{COMMAND_RESULT, QUERY_RESULT, REPORT_RESULT, VIEW_RESULT};

    let payload = serde_json::json!({
        "nested": [null, true, 42, {"text": "quote\" newline\n Unicodeλ"}],
        "error": "a payload field, not a bridge error"
    });
    let values = [
        QUERY_RESULT.wrap("Query", "Item", serde_json::json!([payload.clone()])),
        VIEW_RESULT.wrap("View", "Item", serde_json::json!([payload.clone()])),
        REPORT_RESULT.wrap("Report", "Output", payload.clone()),
        COMMAND_RESULT.wrap("Command", "", payload),
        Value::Null,
        serde_json::json!({"operation":{"kind":"query", "id":"Query"}}),
    ];
    for value in values {
        let expected = serde_json::json!({"value": value}).to_string();
        {
            let actual = json_success(value);
            anyhow::ensure!(
                actual == expected,
                "actual={actual:?}, expected={expected:?}"
            );
        };
    }
    Ok(())
}

#[test]
fn error_envelope_remains_separate_from_success_payloads() -> anyhow::Result<()> {
    let message = "failed \"operation\"\nUnicodeλ";
    {
        let actual = serde_json::from_str::<Value>(&json_error(message))?;
        let expected = serde_json::json!({"error":message});
        anyhow::ensure!(
            actual == expected,
            "actual={actual:?}, expected={expected:?}"
        );
    };
    {
        let actual =
            serde_json::from_str::<Value>(&json_success(serde_json::json!({"error":message})))?;
        let expected = serde_json::json!({"value":{"error":message}});
        anyhow::ensure!(
            actual == expected,
            "actual={actual:?}, expected={expected:?}"
        );
    };
    Ok(())
}

async fn execute(
    code: &str,
    executor: Arc<Executor>,
    filter: ClientFilters,
) -> Result<Value, String> {
    super::execute(
        code,
        executor,
        filter,
        Arc::new(myko::operation_index::build_operation_index()),
    )
    .await
}

fn dummy_executor() -> Arc<Executor> {
    Arc::new(Executor::Client(Arc::new(myko::client::MykoClient::new())))
}

#[tokio::test]
async fn returns_the_scripts_return_value() {
    let result = execute(
        "return 1 + 2;",
        dummy_executor(),
        ClientFilters::allow_all(),
    )
    .await;
    assert_eq!(result, Ok(serde_json::json!(3)));
}

#[tokio::test]
async fn missing_return_yields_null() {
    let result = execute("const x = 1;", dummy_executor(), ClientFilters::allow_all()).await;
    assert_eq!(result, Ok(serde_json::Value::Null));
}

#[tokio::test]
async fn can_chain_multiple_myko_calls_in_one_script() {
    let result = execute(
        "const a = await myko.query('DoesNotExist', {}).catch(e => e.message);\n\
         const b = await myko.command('AlsoMissing', {}).catch(e => e.message);\n\
         return [a, b];",
        dummy_executor(),
        ClientFilters::allow_all(),
    )
    .await;
    assert!(
        result.is_ok(),
        "script should complete, errors are caught in JS"
    );
    let Ok(result) = result else {
        return;
    };
    assert!(result.is_array(), "array result");
    let Some(arr) = result.as_array() else {
        return;
    };
    assert_eq!(arr.len(), 2);
    assert!(
        arr.first()
            .and_then(serde_json::Value::as_str)
            .is_some_and(|message| message.contains("Query not found"))
    );
    assert!(
        arr.get(1)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|message| !message.is_empty()),
        "second call should also report an error"
    );
}

#[tokio::test]
async fn filter_denies_hidden_operation_before_dispatch() {
    let filter = ClientFilters::from_strings(None, Some("query_*"), None, None);
    let result = execute(
        "try { await myko.query('GetAllServers', {}); return 'no-throw'; } \
         catch (e) { return e.message; }",
        dummy_executor(),
        filter,
    )
    .await;
    assert!(result.is_ok(), "script should complete, error caught in JS");
    let Ok(result) = result else {
        return;
    };
    assert_eq!(
        result,
        serde_json::json!("Unknown operation: query_GetAllServers")
    );
}

#[tokio::test]
async fn describe_obeys_operation_visibility() {
    let result = execute(
        "try { return myko.describe('query', 'GetAllServers'); } catch (e) { return e.message; }",
        dummy_executor(),
        ClientFilters::from_strings(None, Some("query_*"), None, None),
    )
    .await;
    assert_eq!(
        result,
        Ok(serde_json::json!("Unknown operation: query_GetAllServers"))
    );
}

#[tokio::test]
async fn invalid_client_arguments_fail_before_connection() {
    let result = execute(
        "try { await myko.command('DeleteServer', []); return 'accepted'; } catch (e) { return e.message; }",
        dummy_executor(),
        ClientFilters::allow_all(),
    ).await;
    assert!(
        result
            .as_ref()
            .ok()
            .and_then(Value::as_str)
            .is_some_and(
                |error| error.contains("Invalid arguments for command_DeleteServer")
                    && error.contains("arguments must be an object")
            )
    );
}

#[tokio::test]
async fn client_defers_typed_argument_validation_to_remote_server() {
    let result = execute(
        "try { await myko.command('DeleteServer', {}); } catch(e) { return e.message; }",
        dummy_executor(),
        ClientFilters::allow_all(),
    )
    .await;
    assert!(
        result
            .as_ref()
            .ok()
            .and_then(Value::as_str)
            .is_some_and(|message| message.contains("Not connected to Myko server")
                && !message.contains("Invalid arguments"))
    );
}

#[tokio::test]
async fn infinite_loop_is_interrupted_by_script_deadline() {
    let start = Instant::now();
    let result = execute(
        "while (true) {}",
        dummy_executor(),
        ClientFilters::allow_all(),
    )
    .await;
    assert!(result.is_err(), "runaway loop must not succeed");
    assert!(
        start.elapsed() < std::time::Duration::from_secs(25),
        "interrupt handler should stop the loop well before the outer timeout"
    );
}

#[tokio::test]
async fn object_return_value_round_trips() {
    let result = execute(
        "return { ok: true, values: [1, 'two', null] };",
        dummy_executor(),
        ClientFilters::allow_all(),
    )
    .await;
    assert!(result.is_ok(), "object result should round-trip");
    let Ok(result) = result else {
        return;
    };
    assert_eq!(
        result,
        serde_json::json!({ "ok": true, "values": [1, "two", null] })
    );
}

//! Sandboxed JS execution for the MCP "Code Mode" `execute()` tool.
//!
//! Runs LLM-submitted JavaScript in an embedded `QuickJS` engine (via
//! `rquickjs`), bound to a small `myko.*` API surface that calls back into
//! the same [`Executor`] methods the old per-operation `tools/call` path
//! used. Every individual `myko.*` call re-checks [`ClientFilters`] exactly
//! as `handle_tools_call` did before — just per-operation-inside-the-script
//! instead of once per top-level MCP tool call.
//!
//! Two independent guards bound script execution, because they cover
//! different failure modes:
//! - [`EXECUTE_TIMEOUT`] wraps the whole call in `tokio::time::timeout`,
//!   covering native async hops (query/report/command round trips) — which
//!   already have their own per-op timeouts in `exec.rs`, so this is a
//!   safety net.
//! - [`SCRIPT_DEADLINE`] drives a `QuickJS` interrupt handler, which the VM
//!   polls periodically *during* synchronous bytecode execution. This is
//!   the one that actually stops a pure `while (true) {}` loop: such a loop
//!   never yields back to the tokio executor, so `tokio::time::timeout`
//!   alone would never get a chance to fire.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use rquickjs::{
    AsyncContext, AsyncRuntime, CatchResultExt, Ctx, Function,
    function::{Async, Opt},
};
use serde_json::Value;

use super::{exec::Executor, filter::ClientFilters};

/// Wall-clock budget for one `execute()` call, including native async hops.
const EXECUTE_TIMEOUT: Duration = Duration::from_secs(30);
/// `QuickJS` interrupt-handler deadline, bounding pure synchronous JS compute.
const SCRIPT_DEADLINE: Duration = Duration::from_secs(10);
/// Sandbox heap ceiling. Generous enough for JSON-shaped query results,
/// tight enough to bound a runaway allocation loop.
const MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;

const MYKO_SHIM: &str = r#"
const __mykoInvoke = async (kind, id, args) => {
  const response = JSON.parse(await __myko_call(kind, id, JSON.stringify(args ?? {})));
  if (response.error !== undefined) throw new Error(response.error);
  return response.value;
};
globalThis.myko = {
  query: async (id, args) => __mykoInvoke("query", id, args),
  view: async (id, args) => __mykoInvoke("view", id, args),
  report: async (id, args) => __mykoInvoke("report", id, args),
  command: async (id, args) => __mykoInvoke("command", id, args),
};
"#;

/// Run `code` — a JS function body that may `return` a JSON-serializable
/// value — against `executor`, enforcing `filter` on every `myko.*` call
/// the script makes.
/// # Errors
///
/// Returns an error when the script cannot be evaluated or an operation fails.
pub async fn execute(
    code: &str,
    executor: Arc<Executor>,
    filter: ClientFilters,
) -> Result<Value, String> {
    let rt = AsyncRuntime::new().map_err(|e| format!("Failed to start sandbox: {e}"))?;
    rt.set_memory_limit(MEMORY_LIMIT_BYTES).await;
    let now = Instant::now();
    let deadline = now.checked_add(SCRIPT_DEADLINE).unwrap_or(now);
    rt.set_interrupt_handler(Some(Box::new(move || Instant::now() >= deadline)))
        .await;

    let ctx = AsyncContext::full(&rt)
        .await
        .map_err(|e| format!("Failed to create sandbox context: {e}"))?;

    // `code` runs inside an async function body so `await myko.*(...)` works
    // anywhere in it (including a final `return`). The IIFE's Promise is
    // stashed on a global rather than relied on as `eval`'s completion
    // value — QuickJS's top-level-await eval doesn't surface a bare
    // expression's value the way a normal (non-await) `eval` does — so we
    // fetch it back out explicitly and drive it via `Promise::into_future`.
    let wrapped = format!(
        "globalThis.__result = (async () => {{\n\
           const __userFn = async () => {{\n{code}\n}};\n\
           const __r = await __userFn();\n\
           return JSON.stringify(__r === undefined ? null : __r);\n\
         }})();"
    );

    let run = ctx.async_with(async move |ctx| {
        install_myko_bindings(&ctx, executor, filter).map_err(|e| e.to_string())?;
        ctx.eval::<(), _>(wrapped.into_bytes())
            .map_err(|e| e.to_string())?;
        let promise: rquickjs::Promise =
            ctx.globals().get("__result").map_err(|e| e.to_string())?;
        promise
            .into_future::<std::string::String>()
            .await
            .catch(&ctx)
            .map_err(|e| e.to_string())
    });

    match tokio::time::timeout(EXECUTE_TIMEOUT, run).await {
        Ok(Ok(json_text)) => serde_json::from_str(&json_text)
            .map_err(|e| format!("Script returned invalid JSON: {e}")),
        Ok(Err(message)) => Err(format!("Script error: {message}")),
        Err(_) => Err(format!(
            "Script execution timed out after {}s",
            EXECUTE_TIMEOUT.as_secs()
        )),
    }
}

fn install_myko_bindings(
    ctx: &Ctx<'_>,
    executor: Arc<Executor>,
    filter: ClientFilters,
) -> rquickjs::Result<()> {
    let call = Function::new(
        ctx.clone(),
        Async(
            move |kind: std::string::String,
                  id: std::string::String,
                  args_json: Opt<std::string::String>| {
                let executor = executor.clone();
                let filter = filter.clone();
                async move { call_operation(&executor, &filter, &kind, &id, args_json.0).await }
            },
        ),
    )?
    .with_name("__myko_call")?;
    ctx.globals().set("__myko_call", call)?;
    ctx.eval::<(), _>(MYKO_SHIM)?;
    Ok(())
}

async fn call_operation(
    executor: &Executor,
    filter: &ClientFilters,
    kind: &str,
    id: &str,
    args_json: Option<std::string::String>,
) -> std::string::String {
    let name = format!("{kind}_{id}");

    if !filter.tool_visible(&name) {
        return json_error(format!("Unknown operation: {name}"));
    }

    let args_text = args_json.unwrap_or_else(|| "{}".to_string());
    let args: Value = match serde_json::from_str(&args_text) {
        Ok(v) => v,
        Err(e) => return json_error(format!("Invalid arguments for {name}: {e}")),
    };

    if let Err(message) = filter.tool_callable(&name, &args) {
        return json_error(message);
    }

    let result = match kind {
        "query" => executor.execute_query(id, args).await,
        "view" => executor.execute_view(id, args).await,
        "report" => executor.execute_report(id, args).await,
        "command" => executor.execute_command(id, args).await,
        _ => Err(format!("Unknown operation kind: {kind}")),
    };

    result.map_or_else(json_error, |value| {
        serde_json::json!({ "value": value }).to_string()
    })
}

fn json_error(message: impl Into<std::string::String>) -> std::string::String {
    serde_json::json!({ "error": message.into() }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // No registered operations exist in this test binary, so both calls
        // fail — but critically, the script drives *two* sequential
        // `myko.*` invocations from a single `execute()` round trip, which
        // is the whole point of Code Mode over one-call-per-tool-call.
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
    async fn infinite_loop_is_interrupted_by_script_deadline() {
        let start = Instant::now();
        let result = execute(
            "while (true) {}",
            dummy_executor(),
            ClientFilters::allow_all(),
        )
        .await;
        assert!(result.is_err(), "runaway loop must not succeed");
        // Should be bounded by SCRIPT_DEADLINE, not EXECUTE_TIMEOUT (30s) —
        // give generous headroom since this environment can be slow.
        assert!(
            start.elapsed() < Duration::from_secs(25),
            "interrupt handler should stop the loop well before the outer timeout"
        );
    }

    #[tokio::test]
    async fn invalid_json_return_value_is_impossible_by_construction() {
        // JSON.stringify always produces valid JSON for any JS value the
        // sandbox can construct (functions/symbols become `undefined`
        // inside arrays/objects, not thrown), so this exercises that a
        // non-trivial object round-trips correctly rather than testing the
        // (unreachable) invalid-JSON error path directly.
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
}

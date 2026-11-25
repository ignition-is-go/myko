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
//! - `MCP_EXECUTE_TIMEOUT` wraps the whole call in `tokio::time::timeout`,
//!   covering native async hops (query/report/command round trips) — which
//!   already have their own per-op timeouts in `exec.rs`, so this is a
//!   safety net.
//! - `MCP_SCRIPT_DEADLINE` drives a `QuickJS` interrupt handler, which the VM
//!   polls periodically *during* synchronous bytecode execution. This is
//!   the one that actually stops a pure `while (true) {}` loop: such a loop
//!   never yields back to the tokio executor, so `tokio::time::timeout`
//!   alone would never get a chance to fire.

use std::{sync::Arc, time::Instant};

use myko::operation_index::OperationSchema;
use rquickjs::{
    AsyncContext, AsyncRuntime, CatchResultExt, Ctx, Function, IntoJs,
    function::{Async, Opt},
};
use serde::Serialize;
use serde_json::Value;

use super::{
    exec::{Executor, Rows, SnapshotResult},
    filter::ClientFilters,
    json_vm::{ConversionError, Converter},
};
use crate::time_limits::{MCP_EXECUTE_TIMEOUT, MCP_SCRIPT_DEADLINE};

/// Sandbox heap ceiling. Generous enough for JSON-shaped query results,
/// tight enough to bound a runaway allocation loop.
const MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;

const MYKO_SHIM: &str = r#"
const __mykoInvoke = async (kind, id, args) => {
  const response = await __myko_call(kind, id, JSON.stringify(args ?? {}));
  if (response.error !== undefined) throw new Error(response.error);
  return response.value;
};
globalThis.myko = {
  query: async (id, args) => __mykoInvoke("query", id, args),
  view: async (id, args) => __mykoInvoke("view", id, args),
  report: async (id, args) => __mykoInvoke("report", id, args),
  command: async (id, args) => __mykoInvoke("command", id, args),
  describe: (kind, id) => {
    const response = JSON.parse(__myko_describe(kind, id));
    if (response.error !== undefined) throw new Error(response.error);
    return response.value;
  },
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
    index: Arc<Vec<OperationSchema>>,
) -> Result<Value, String> {
    execute_with_memory_limit(code, executor, filter, index, MEMORY_LIMIT_BYTES).await
}

async fn execute_with_memory_limit(
    code: &str,
    executor: Arc<Executor>,
    filter: ClientFilters,
    index: Arc<Vec<OperationSchema>>,
    memory_limit: usize,
) -> Result<Value, String> {
    let rt = AsyncRuntime::new().map_err(|e| format!("Failed to start sandbox: {e}"))?;
    rt.set_memory_limit(memory_limit).await;
    let now = Instant::now();
    let deadline = MCP_SCRIPT_DEADLINE
        .get()
        .map(|limit| now.checked_add(limit).unwrap_or(now));
    if let Some(deadline) = deadline {
        rt.set_interrupt_handler(Some(Box::new(move || Instant::now() >= deadline)))
            .await;
    }

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
        install_myko_bindings(&ctx, executor, filter, index, deadline)
            .map_err(|e| e.to_string())?;
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

    match MCP_EXECUTE_TIMEOUT.run(run).await {
        Ok(Ok(json_text)) => {
            super::schema::check_result_size(&json_text)?;
            serde_json::from_str(&json_text)
                .map_err(|e| format!("Script returned invalid JSON: {e}"))
        }
        Ok(Err(message)) => Err(format!("Script error: {message}")),
        Err(_) => Err(format!(
            "Script execution timed out after {}s",
            MCP_EXECUTE_TIMEOUT.get().map_or(0, |limit| limit.as_secs())
        )),
    }
}

fn install_myko_bindings(
    ctx: &Ctx<'_>,
    executor: Arc<Executor>,
    filter: ClientFilters,
    index: Arc<Vec<OperationSchema>>,
    deadline: Option<Instant>,
) -> rquickjs::Result<()> {
    let describe_filter = filter.clone();
    let describe_index = index.clone();
    let describe_executor = executor.clone();
    let describe = Function::new(ctx.clone(), move |kind: String, id: String| {
        let visible = if matches!(kind.as_str(), "entity" | "relationship" | "guide") {
            matches!(describe_executor.as_ref(), Executor::InProcess(_))
        } else {
            describe_executor.can_discover(&kind, &id)
        };
        if !visible {
            return json_error(format!("Unknown operation: {kind}_{id}"));
        }
        super::contract::describe(&describe_index, &describe_filter, &kind, &id)
            .map_or_else(json_error, json_success)
    })?;
    ctx.globals().set("__myko_describe", describe)?;
    let call = Function::new(
        ctx.clone(),
        Async(
            move |kind: std::string::String,
                  id: std::string::String,
                  args_json: Opt<std::string::String>| {
                let executor = executor.clone();
                let filter = filter.clone();
                let index = index.clone();
                async move {
                    let result =
                        call_operation(&executor, &filter, &index, &kind, &id, args_json.0).await;
                    BridgeResponse {
                        result,
                        executor,
                        kind,
                        id,
                        deadline,
                    }
                }
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
    index: &[OperationSchema],
    kind: &str,
    id: &str,
    args_json: Option<std::string::String>,
) -> Result<PendingResult, String> {
    let name = format!("{kind}_{id}");

    if !filter.tool_visible(&name) {
        return Err(format!("Unknown operation: {name}"));
    }

    let args_text = args_json.unwrap_or_else(|| "{}".to_string());
    let args: Value = match serde_json::from_str(&args_text) {
        Ok(v) => v,
        Err(e) => return Err(format!("Invalid arguments for {name}: {e}")),
    };

    filter.tool_callable(&name, &args)?;

    if !args.is_object() {
        return Err(format!(
            "Invalid arguments for {name}: arguments must be an object"
        ));
    }

    if matches!(executor, Executor::InProcess(_))
        && let Some(operation) = index
            .iter()
            .find(|operation| operation.kind == kind && operation.id == id)
    {
        let schema = match super::schema::compile(&operation.input_schema()) {
            Ok(schema) => schema,
            Err(error) => return Err(format!("Invalid input schema for {name}: {error}")),
        };
        if let Err(error) = super::schema::validate(&schema, &args) {
            return Err(format!(
                "Invalid arguments for {name}: {error}. Use myko.describe({kind:?}, {id:?}) for the contract."
            ));
        }
    }

    let result = match kind {
        "query" => executor
            .execute_query_snapshot(id, args.clone())
            .await
            .map(OperationResult::Snapshot),
        "view" => executor
            .execute_view_snapshot(id, args.clone())
            .await
            .map(OperationResult::Snapshot),
        "report" => executor
            .execute_report(id, args.clone())
            .await
            .map(OperationResult::Json),
        "command" => executor
            .execute_command(id, args.clone())
            .await
            .map(OperationResult::Json),
        _ => Err(format!("Unknown operation kind: {kind}")),
    };

    result
        .map_err(|message| {
            format!("{message}. Use myko.describe({kind:?}, {id:?}) for the contract.")
        })
        .map(|output| PendingResult {
            output,
            arguments: args,
        })
}

enum OperationResult {
    Snapshot(SnapshotResult),
    Json(Value),
}
struct PendingResult {
    output: OperationResult,
    arguments: Value,
}
struct BridgeResponse {
    result: Result<PendingResult, String>,
    executor: Arc<Executor>,
    kind: String,
    id: String,
    deadline: Option<Instant>,
}

impl<'js> IntoJs<'js> for BridgeResponse {
    fn into_js(self, ctx: &Ctx<'js>) -> rquickjs::Result<rquickjs::Value<'js>> {
        use rquickjs::{Array, Exception, Object, object::Property};
        let converter = Converter::new(ctx.clone(), self.deadline);
        converter
            .check()
            .map_err(|error| error.into_js_error(ctx))?;
        let response = Object::new(ctx.clone())?;
        let pending = match self.result {
            Ok(pending) => pending,
            Err(message) => {
                response.prop(
                    "error",
                    Property::from(message)
                        .writable()
                        .enumerable()
                        .configurable(),
                )?;
                return Ok(response.into_value());
            }
        };
        self.executor
            .authorize_result(&self.kind, &self.id, &pending.arguments)
            .map_err(|message| Exception::throw_message(ctx, &message))?;
        let result = match pending.output {
            OperationResult::Json(value) => value
                .serialize(converter.clone())
                .map_err(|error| error.into_js_error(ctx))?,
            OperationResult::Snapshot(snapshot) => {
                let result = snapshot
                    .metadata()
                    .serialize(converter.clone())
                    .map_err(|error| error.into_js_error(ctx))?;
                let items = Array::new(ctx.clone())?;
                match snapshot.rows {
                    Rows::Native(rows) => {
                        for (index, row) in rows.into_iter().enumerate() {
                            let value = match row.serialize(converter.clone()) {
                                Ok(value) => value,
                                Err(ConversionError::Unsupported(_)) if !ctx.has_exception() => {
                                    rquickjs::Value::new_null(ctx.clone())
                                }
                                Err(error) => {
                                    return Err(if ctx.has_exception() {
                                        rquickjs::Error::Exception
                                    } else {
                                        error.into_js_error(ctx)
                                    });
                                }
                            };
                            super::json_vm::array_item(&items, index, value)?;
                        }
                    }
                    Rows::Client(rows) => {
                        for (index, row) in rows.iter().enumerate() {
                            let value = row
                                .serialize(converter.clone())
                                .map_err(|error| error.into_js_error(ctx))?;
                            super::json_vm::array_item(&items, index, value)?;
                        }
                    }
                }
                let object = result
                    .as_object()
                    .ok_or_else(|| Exception::throw_message(ctx, "invalid result contract"))?;
                let count = items.len();
                object.prop(
                    "items",
                    Property::from(items).writable().enumerable().configurable(),
                )?;
                object.prop(
                    "count",
                    Property::from(count).writable().enumerable().configurable(),
                )?;
                result
            }
        };
        converter
            .check()
            .map_err(|error| error.into_js_error(ctx))?;
        self.executor
            .authorize_result(&self.kind, &self.id, &pending.arguments)
            .map_err(|message| Exception::throw_message(ctx, &message))?;
        response.prop(
            "value",
            Property::from(result)
                .writable()
                .enumerable()
                .configurable(),
        )?;
        Ok(response.into_value())
    }
}

fn json_success(value: Value) -> std::string::String {
    #[derive(serde::Serialize)]
    struct Success {
        value: Value,
    }

    serde_json::to_string(&Success { value })
        .unwrap_or_else(|error| json_error(format!("Failed to encode Myko result: {error}")))
}

fn json_error(message: impl Into<std::string::String>) -> std::string::String {
    serde_json::json!({ "error": message.into() }).to_string()
}

#[cfg(test)]
mod tests;

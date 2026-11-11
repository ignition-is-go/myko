//! Shared reflection metadata captured at macro-expansion time and embedded
//! directly in query/view/report/command registrations.
//!
//! This is deliberately *not* derived from ts-rs `.ts` binding files at
//! runtime: those are generated per-crate (each crate's `#[derive(TS)]`
//! writes to its own `bindings/` directory), so a single runtime bindings-dir
//! path can't resolve arguments for types defined in a different crate than
//! the one hosting the MCP server — and it depends on codegen having already
//! run. Capturing field names/types/optionality in the macro itself, where
//! the full field list is already available, means every crate's operations
//! carry this data automatically and it can never go stale relative to the
//! compiled binary.

/// One field of a query/view/report/command's argument struct, captured by
/// the defining macro directly from the struct's own field list.
#[derive(Debug, Clone, Copy)]
pub struct OperationArgField {
    pub name: &'static str,
    /// Rust type as written on the field (e.g. `"Option<String>"`).
    pub rust_type: &'static str,
    /// Whether Serde permits this field to be omitted.
    pub optional: bool,
    pub aliases: &'static [&'static str],
    /// The field is spread into the input object by Serde.
    pub flattened: bool,
}

/// The compiled Serde input contract for an operation. Kept separately from
/// handler registration so remote MCP clients can validate before dispatch.
pub struct OperationInputContract {
    pub kind: &'static str,
    pub id: &'static str,
    pub validate: fn(&serde_json::Value) -> Result<(), String>,
}

inventory::collect!(OperationInputContract);

/// Validate using the same deserializer as execution, including nested types,
/// custom deserializers, aliases, defaults, and unknown-field policies.
/// # Errors
/// Returns the failing field path and Serde's diagnostic.
pub fn validate_input<T: serde::de::DeserializeOwned>(
    input: &serde_json::Value,
) -> Result<(), String> {
    if !input.is_object() {
        return Err("$: expected an argument object".into());
    }
    serde_path_to_error::deserialize::<_, T>(input)
        .map(|_| ())
        .map_err(|error| format!("{}: {}", error.path(), error.inner()))
}

/// Resolve the input contract without contacting a server or running a handler.
/// # Errors
/// Returns a diagnostic for an unknown operation or malformed arguments.
pub fn validate_operation_input(
    kind: &str,
    id: &str,
    input: &serde_json::Value,
) -> Result<(), String> {
    let contract = inventory::iter::<OperationInputContract>
        .into_iter()
        .find(|contract| contract.kind == kind && contract.id == id)
        .ok_or_else(|| format!("Unknown operation: {kind}_{id}"))?;
    (contract.validate)(input)
        .map_err(|message| format!("Invalid arguments for {kind}_{id}: {message}"))
}

/// A downstream example for arguments whose valid values cannot be inferred
/// from their nominal Rust type (e.g. enums or custom deserializers).
pub struct OperationExample {
    pub kind: &'static str,
    pub id: &'static str,
    pub example: fn() -> serde_json::Value,
}

inventory::collect!(OperationExample);

/// Register example arguments; discovery checks them against Serde before
/// publishing them. The expression must return a `serde_json::Value`.
#[macro_export]
macro_rules! register_operation_example {
    ($kind:literal, $operation:ident, $example:expr) => {
        #[cfg(not(target_arch = "wasm32"))]
        $crate::inventory::submit! {
            $crate::reflection::OperationExample {
                kind: $kind,
                id: stringify!($operation),
                example: || $example,
            }
        }
    };
}

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
    pub aliases: &'static [&'static str],
    pub description: Option<&'static str>,
    pub flattened: bool,
    pub custom_deserializer: bool,
    pub presence: OperationArgPresence,
    pub input_schema: fn() -> serde_json::Value,
}

/// How Serde handles an omitted argument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationArgPresence {
    Required,
    Optional,
    Defaulted,
}

/// Deserialize operation input with a field path in the error diagnostic.
/// # Errors
///
/// Returns the field path and Serde diagnostic for invalid input.
pub fn deserialize_operation<T: serde::de::DeserializeOwned>(
    value: serde_json::Value,
) -> Result<T, serde_path_to_error::Error<serde_json::Error>> {
    serde_path_to_error::deserialize(value)
}

/// Reflect supported field types without requiring every downstream type to implement `JsonSchema`.
pub struct InputSchema<T>(std::marker::PhantomData<T>);

impl<T> Default for InputSchema<T> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

/// Field-schema discovery. Unsupported types remain unconstrained.
///
/// Macros use a double reference to a concrete field type. Autoderef selects
/// the fallback when that type has no `JsonSchema` implementation.
pub trait ReflectInputSchema {
    fn input_schema(self) -> serde_json::Value;
}

impl<T> ReflectInputSchema for &InputSchema<T> {
    fn input_schema(self) -> serde_json::Value {
        serde_json::json!({"x-myko-opaque": true, "x-rust-type": std::any::type_name::<T>()})
    }
}

impl<T: schemars::JsonSchema> ReflectInputSchema for &&InputSchema<T> {
    fn input_schema(self) -> serde_json::Value {
        schemars::generate::SchemaSettings::default()
            .for_deserialize()
            .into_generator()
            .into_root_schema_for::<T>()
            .to_value()
    }
}

/// A downstream example for arguments whose valid values cannot be inferred
/// from their nominal Rust type (e.g. enums or custom deserializers).
pub struct OperationExample {
    pub kind: &'static str,
    pub id: &'static str,
    pub example: fn() -> serde_json::Value,
}

inventory::collect!(OperationExample);

/// Register example arguments; MCP discovery checks structural constraints before
/// publishing them; Serde remains authoritative during execution. The expression must return a `serde_json::Value`.
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

/// Serialized entity fields and documentation captured from the Rust definition.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityField {
    pub name: &'static str,
    pub rust_type: &'static str,
    pub description: Option<&'static str>,
    pub flattened: bool,
}

pub struct EntityDescription {
    pub entity_type: &'static str,
    pub description: Option<&'static str>,
    pub fields: &'static [EntityField],
}
inventory::collect!(EntityDescription);

/// Application-owned vocabulary, invariants, or workflow guidance. Static documentation only.
pub struct DomainGuide {
    pub id: &'static str,
    pub title: &'static str,
    pub description: &'static str,
    pub content: &'static str,
}
inventory::collect!(DomainGuide);

/// Register application domain documentation for on-demand discovery.
#[macro_export]
macro_rules! register_domain_guide {
    ($id:literal, $title:literal, $description:literal, $content:expr) => {
        #[cfg(not(target_arch = "wasm32"))]
        $crate::inventory::submit! {
            $crate::reflection::DomainGuide {
                id: $id, title: $title, description: $description, content: $content,
            }
        }
    };
}

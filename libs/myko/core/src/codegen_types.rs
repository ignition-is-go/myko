//! Registration types for TypeScript codegen macros.
//!
//! These live outside the gated `codegen` module so the macros work on all platforms.
//! The actual codegen functions that consume these registrations remain in `codegen/mod.rs`.

/// Value types for TypeScript constant generation.
#[derive(Debug, PartialEq)]
pub enum TsConstValue {
    Str(&'static str),
    Int(i64),
    Float(f64),
    Bool(bool),
}

/// Registration for a Rust constant to be exported as a TypeScript constant.
pub struct TsConstRegistration {
    pub name: &'static str,
    pub value: TsConstValue,
    pub crate_name: &'static str,
}

inventory::collect!(TsConstRegistration);

/// Registration for ts-rs type exports.
/// Allows types with `#[derive(TS)]` to be exported without running cargo test.
pub struct TsExportRegistration {
    /// Name of the type being exported (for logging)
    pub type_name: &'static str,
    /// Function that calls `T::export()` for the registered type
    pub export_fn: fn() -> Result<(), ts_rs::ExportError>,
}

inventory::collect!(TsExportRegistration);

// Language-neutral public names for generated-typegen producers. The TypeScript
// names remain available for compatibility with the current renderer.
pub type TypegenConstValue = TsConstValue;
pub type TypegenConstRegistration = TsConstRegistration;
pub type TypegenTypeRegistration = TsExportRegistration;

/// Registration for a language-neutral generated typegen module.
pub struct TypegenModuleRegistration {
    /// Stable registration identifier, used in diagnostics.
    pub id: &'static str,
    /// Registering crate/module path, used to select the current typegen crate.
    pub crate_name: &'static str,
    /// Build the module IR from the registering crate's inventories.
    pub build: fn() -> crate::typegen_module::TypegenModule,
}

inventory::collect!(TypegenModuleRegistration);

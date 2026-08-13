//! TypeScript adapters attached to backend-neutral typegen registrations.

/// A `ts-rs` exporter associated with a neutral type registration id.
pub struct TypeExportRegistration {
    pub type_id: &'static str,
    pub type_name: &'static str,
    pub rust_type_id: fn() -> std::any::TypeId,
    pub generated_name: fn(&ts_rs::Config) -> String,
    pub output_path: fn() -> Option<std::path::PathBuf>,
    pub export_fn: fn() -> Result<(), ts_rs::ExportError>,
}

inventory::collect!(TypeExportRegistration);

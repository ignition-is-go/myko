//! TypeScript adapters attached to backend-neutral typegen registrations.

/// A `ts-rs` exporter associated with a neutral type registration id.
pub struct TypeExportRegistration {
    pub type_id: &'static str,
    pub type_name: &'static str,
    pub export_fn: fn() -> Result<(), ts_rs::ExportError>,
}

inventory::collect!(TypeExportRegistration);

//! Handler registry for the cell-based server.
//!
//! Collects all Item, Query, and Report registrations from inventory
//! and provides lookup by type/id.

use std::{collections::BTreeSet, sync::Arc};

#[cfg(not(target_arch = "wasm32"))]
mod contract;
mod scoped;
use scoped::ScopedHandlers;

use crate::{
    ServiceTypeId,
    command::CommandHandlerRegistration,
    graph::GraphQueryRegistration,
    item::{IngestBufferPolicy, IngestBufferRegistration, ItemParseFn, ItemRegistration},
    query::{QueryCellFactory, QueryParseFn, QueryRegistration, QueryWindowCellFactory},
    report::{ReportCellFactory, ReportParseFn, ReportRegistration},
    view::{RegisteredViewOutput, ViewCellFactory, ViewParseFn, ViewRegistration},
};

#[cfg(not(target_arch = "wasm32"))]
use crate::query::QueryAuthorityFactory;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone, Default)]
pub struct HandlerAuthority {
    /// Filled from the activated registration by `HandlerRegistry::handler_authority`.
    pub service_id: Option<ServiceTypeId>,
    pub source_node: Option<myko_federation::NodeId>,
    pub scope_id: Option<myko_federation::ScopeId>,
    pub resource_claims: Vec<myko_federation::ResourceClaim>,
    pub application_capabilities: Vec<myko_federation::CapabilityId>,
}

#[cfg(not(target_arch = "wasm32"))]
#[allow(clippy::unnecessary_wraps)]
fn default_query_authority(
    _value: serde_json::Value,
    _local_node: myko_federation::NodeId,
) -> Result<HandlerAuthority, String> {
    Ok(HandlerAuthority::default())
}

// AHash on the dispatch maps below: every wire-routed command/query/view/
// report/event hits one of these. Bench: ~3.1× faster std HashMap lookups
// vs default SipHash on Arc<str> keys.
type AMap<K, V> = std::collections::HashMap<K, V, ahash::RandomState>;

fn format_list<'a>(keys: impl Iterator<Item = &'a Arc<str>>) -> String {
    let mut items: Vec<&str> = keys.map(std::convert::AsRef::as_ref).collect();
    items.sort_unstable();
    if items.is_empty() {
        "(none)".to_string()
    } else {
        items.join(", ")
    }
}

fn format_str_list(items: &[&str]) -> String {
    if items.is_empty() {
        "(none)".to_string()
    } else {
        items.join(", ")
    }
}

/// Stored query registration data.
pub struct StoredQueryData {
    #[cfg(feature = "schema")]
    pub payload_schema: Option<fn() -> crate::schema::HandlerPayloadSchema>,
    pub service_id: Option<ServiceTypeId>,
    pub query_id: Arc<str>,
    pub query_item_type: Arc<str>,
    pub parse: QueryParseFn,
    pub cell_factory: QueryCellFactory,
    pub window_cell_factory: Option<QueryWindowCellFactory>,
    #[cfg(not(target_arch = "wasm32"))]
    pub authority: QueryAuthorityFactory,
}

/// Stored view registration data.
pub struct StoredViewData {
    #[cfg(feature = "schema")]
    pub payload_schema: Option<fn() -> crate::schema::HandlerPayloadSchema>,
    pub service_id: Option<ServiceTypeId>,
    pub view_id: Arc<str>,
    pub view_item_type: Arc<str>,
    pub parse: ViewParseFn,
    pub cell_factory: ViewCellFactory,
    #[cfg(not(target_arch = "wasm32"))]
    pub authority: crate::view::ViewAuthorityFactory,
}

/// Stored report registration data.
pub struct StoredReportData {
    #[cfg(feature = "schema")]
    pub payload_schema: Option<fn() -> crate::schema::HandlerPayloadSchema>,
    pub service_id: Option<ServiceTypeId>,
    pub report_id: Arc<str>,
    pub output_type: &'static str,
    pub parse: ReportParseFn,
    pub cell_factory: ReportCellFactory,
    #[cfg(not(target_arch = "wasm32"))]
    pub authority: crate::report::ReportAuthorityFactory,
}

/// Registry of all handlers for the cell-based server.
///
/// Collects registrations from inventory at construction time
/// and provides O(1) lookup by type/id.
pub struct HandlerRegistry {
    /// Item parse functions by entity type name
    item_parsers: AMap<Arc<str>, ItemParseFn>,
    /// Optional ingest buffering policy by entity type name
    item_buffer_policies: AMap<Arc<str>, IngestBufferPolicy>,
    /// Query data by query id
    query_data: ScopedHandlers<StoredQueryData>,
    /// View data by view id
    view_data: ScopedHandlers<StoredViewData>,
    /// Report data by report id
    report_data: ScopedHandlers<StoredReportData>,
    /// Command handlers admitted by the activated service set.
    commands: Vec<&'static CommandHandlerRegistration>,
}

impl HandlerRegistry {
    /// Create a new handler registry by collecting all registrations from inventory.
    #[must_use]
    pub fn new() -> Self {
        Self::collect(None)
    }

    /// Create a registry containing global handlers and handlers owned by an activated service.
    #[must_use]
    pub fn for_services(services: &BTreeSet<ServiceTypeId>) -> Self {
        Self::collect(Some(services))
    }

    // Registration families are collected together so service filtering is
    // applied consistently before the immutable registry becomes observable.
    #[allow(clippy::too_many_lines)]
    fn collect(services: Option<&BTreeSet<ServiceTypeId>>) -> Self {
        let mut item_parsers = AMap::default();
        let mut item_buffer_policies = AMap::default();
        let mut query_data = ScopedHandlers::default();
        let mut view_data = ScopedHandlers::default();
        let mut report_data = ScopedHandlers::default();

        // Collect item registrations
        for registration in inventory::iter::<ItemRegistration> {
            if services.is_some_and(|services| {
                registration
                    .service_id
                    .is_some_and(|service| !services.contains(&service))
            }) {
                continue;
            }
            tracing::trace!("Registered item parser: {}", registration.entity_type);
            item_parsers.insert(registration.entity_type.into(), registration.parse);
        }

        for registration in inventory::iter::<IngestBufferRegistration> {
            tracing::trace!(
                "Registered ingest buffer policy: {} -> {:?}",
                registration.entity_type,
                registration.policy
            );
            item_buffer_policies.insert(registration.entity_type.into(), registration.policy);
        }

        // Collect query registrations
        for registration in inventory::iter::<QueryRegistration> {
            if services.is_some_and(|services| {
                registration
                    .service_id
                    .is_some_and(|service| !services.contains(&service))
            }) {
                continue;
            }
            tracing::trace!("Registered query: {}", registration.query_id);
            let data = StoredQueryData {
                #[cfg(feature = "schema")]
                payload_schema: registration.payload_schema,
                service_id: registration.service_id,
                query_id: registration.query_id.into(),
                query_item_type: registration.query_item_type.into(),
                parse: registration.parse,
                cell_factory: registration.cell_factory,
                window_cell_factory: Some(registration.window_cell_factory),
                authority: registration.authority,
            };
            query_data.insert(data.service_id, data.query_id.clone(), data);
        }

        // Graph operations use the ordinary query wire/runtime but render
        // endpoint-aware bindings from the separate graph catalog.
        for registration in inventory::iter::<GraphQueryRegistration> {
            tracing::trace!("Registered graph query: {}", registration.query_id);
            let data = StoredQueryData {
                #[cfg(feature = "schema")]
                payload_schema: None,
                service_id: None,
                query_id: registration.query_id.into(),
                query_item_type: registration.edge_type.into(),
                parse: registration.parse,
                cell_factory: registration.cell_factory,
                window_cell_factory: Some(registration.window_cell_factory),
                authority: default_query_authority,
            };
            query_data.insert(data.service_id, data.query_id.clone(), data);
        }

        // Collect view registrations
        for registration in inventory::iter::<ViewRegistration> {
            if services.is_some_and(|services| {
                registration
                    .service_id
                    .is_some_and(|service| !services.contains(&service))
            }) {
                continue;
            }
            tracing::trace!("Registered view: {}", registration.view_id);
            let data = StoredViewData {
                #[cfg(feature = "schema")]
                payload_schema: registration.payload_schema,
                service_id: registration.service_id,
                view_id: registration.view_id.into(),
                view_item_type: registration.view_item_type.into(),
                parse: registration.parse,
                cell_factory: registration.cell_factory,
                #[cfg(not(target_arch = "wasm32"))]
                authority: registration.authority,
            };
            view_data.insert(data.service_id, data.view_id.clone(), data);
        }

        // Collect report registrations
        for registration in inventory::iter::<ReportRegistration> {
            if services.is_some_and(|services| {
                registration
                    .service_id
                    .is_some_and(|service| !services.contains(&service))
            }) {
                continue;
            }
            tracing::trace!("Registered report: {}", registration.report_id);
            let data = StoredReportData {
                output_type: registration.output_type,
                #[cfg(feature = "schema")]
                payload_schema: registration.payload_schema,
                service_id: registration.service_id,
                report_id: registration.report_id.into(),
                parse: registration.parse,
                cell_factory: registration.cell_factory,
                #[cfg(not(target_arch = "wasm32"))]
                authority: registration.authority,
            };
            report_data.insert(data.service_id, data.report_id.clone(), data);
        }

        let mut commands: Vec<_> = inventory::iter::<CommandHandlerRegistration>()
            .filter(|registration| {
                !services.is_some_and(|services| {
                    registration
                        .service_id
                        .is_some_and(|service| !services.contains(&service))
                })
            })
            .collect();
        commands.sort_by_key(|registration| registration.command_id);
        let command_ids: Vec<_> = commands
            .iter()
            .map(|registration| registration.command_id)
            .collect();

        tracing::trace!(
            "HandlerRegistry initialized:\n  Items ({}):\n    {}\n  Queries ({}):\n    {}\n  Views ({}):\n    {}\n  Reports ({}):\n    {}\n  Commands ({}):\n    {}",
            item_parsers.len(),
            format_list(item_parsers.keys()),
            query_data.len(),
            format_list(query_data.values().map(|data| &data.query_id)),
            view_data.len(),
            format_list(view_data.values().map(|data| &data.view_id)),
            report_data.len(),
            format_list(report_data.values().map(|data| &data.report_id)),
            command_ids.len(),
            format_str_list(&command_ids),
        );

        Self {
            item_parsers,
            item_buffer_policies,
            query_data,
            view_data,
            report_data,
            commands,
        }
    }

    /// Get an item parse function by entity type name.
    #[must_use]
    pub fn item_parser(&self, entity_type: &str) -> Option<ItemParseFn> {
        self.item_parsers.get(entity_type).copied()
    }

    /// Get the ingest buffering policy for an entity type.
    #[must_use]
    pub fn item_buffer_policy(&self, entity_type: &str) -> IngestBufferPolicy {
        self.item_buffer_policies
            .get(entity_type)
            .copied()
            .unwrap_or(IngestBufferPolicy::None)
    }

    /// Get query registration data by query id.
    #[must_use]
    pub fn query(&self, service: Option<&str>, query_id: &str) -> Option<&StoredQueryData> {
        self.query_data.get(service, query_id)
    }

    /// Parse and open one durable query through the retained registration.
    ///
    /// # Errors
    ///
    /// Returns an error when the handler is absent, its parameters are
    /// malformed, or its durable source cannot be established.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_federated_query(
        &self,
        service: Option<&str>,
        query_id: &str,
        mut params: serde_json::Value,
        request: Arc<crate::request::RequestContext>,
        server: Arc<crate::server::MykoServerContext>,
        source: crate::server::federated_source::FederatedRequest,
    ) -> Result<crate::query::QueryValue, String> {
        let registration = self.query_data.get(service, query_id).ok_or_else(|| {
            format!("query handler {query_id} is not registered for the requested service")
        })?;
        let object = params
            .as_object_mut()
            .ok_or_else(|| "query parameters must be an object".to_owned())?;
        object.insert(
            "tx".to_owned(),
            serde_json::Value::String(request.tx.to_string()),
        );
        let query = (registration.parse)(params).map_err(|error| error.to_string())?;
        (registration.cell_factory)(
            query,
            server.registry.clone(),
            request,
            Some(server),
            Some(source),
        )
    }

    /// Get report registration data by report id.
    #[must_use]
    pub fn report(&self, service: Option<&str>, report_id: &str) -> Option<&StoredReportData> {
        self.report_data.get(service, report_id)
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Resolve the typed authority declared by a registered handler.
    ///
    /// # Errors
    ///
    /// Returns an error when the handler is absent or its parameters cannot be
    /// decoded by the retained registration.
    pub fn handler_authority(
        &self,
        kind: myko_federation::HandlerKind,
        service: Option<&str>,
        handler_id: &str,
        params: serde_json::Value,
        local_node: myko_federation::NodeId,
    ) -> Result<HandlerAuthority, String> {
        let (service_id, mut authority) = match kind {
            myko_federation::HandlerKind::Query => {
                let registration = self.query_data.get(service, handler_id).ok_or_else(|| {
                    format!(
                        "query handler {handler_id} is not registered for the requested service"
                    )
                })?;
                (
                    registration.service_id,
                    (registration.authority)(params, local_node)?,
                )
            }
            myko_federation::HandlerKind::View => {
                let registration = self.view_data.get(service, handler_id).ok_or_else(|| {
                    format!("view handler {handler_id} is not registered for the requested service")
                })?;
                (
                    registration.service_id,
                    (registration.authority)(params, local_node)?,
                )
            }
            myko_federation::HandlerKind::Report => {
                let registration = self.report_data.get(service, handler_id).ok_or_else(|| {
                    format!(
                        "report handler {handler_id} is not registered for the requested service"
                    )
                })?;
                (
                    registration.service_id,
                    (registration.authority)(params, local_node)?,
                )
            }
            myko_federation::HandlerKind::Command => {
                return Err("commands are admitted through SubmitCommand".to_owned());
            }
        };
        authority.service_id = service_id;
        Ok(authority)
    }

    /// Parse and open one durable report through the retained registration.
    ///
    /// # Errors
    ///
    /// Returns an error when the handler is absent, its parameters are
    /// malformed, or its durable source cannot be established.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_federated_report(
        &self,
        service: Option<&str>,
        report_id: &str,
        mut params: serde_json::Value,
        request: Arc<crate::request::RequestContext>,
        server: Arc<crate::server::MykoServerContext>,
        source: crate::server::federated_source::FederatedRequest,
    ) -> Result<crate::report::ReportValue<dyn crate::report::AnyOutput>, String> {
        let registration = self.report_data.get(service, report_id).ok_or_else(|| {
            format!("report handler {report_id} is not registered for the requested service")
        })?;
        let object = params
            .as_object_mut()
            .ok_or_else(|| "report parameters must be an object".to_owned())?;
        object.insert(
            "tx".to_owned(),
            serde_json::Value::String(request.tx.to_string()),
        );
        let report = (registration.parse)(params).map_err(|error| error.to_string())?;
        (registration.cell_factory)(report, request, server, Some(source))
    }

    /// Get view registration data by view id.
    #[must_use]
    pub fn view(&self, service: Option<&str>, view_id: &str) -> Option<&StoredViewData> {
        self.view_data.get(service, view_id)
    }

    /// Parse and open one durable view through the retained registration.
    ///
    /// # Errors
    ///
    /// Returns an error when the handler is absent, its parameters are
    /// malformed, or its durable source cannot be established.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_federated_view(
        &self,
        service: Option<&str>,
        view_id: &str,
        mut params: serde_json::Value,
        request: Arc<crate::request::RequestContext>,
        server: Arc<crate::server::MykoServerContext>,
        source: crate::server::federated_source::FederatedRequest,
    ) -> Result<RegisteredViewOutput, String> {
        let registration = self.view_data.get(service, view_id).ok_or_else(|| {
            format!("view handler {view_id} is not registered for the requested service")
        })?;
        let object = params
            .as_object_mut()
            .ok_or_else(|| "view parameters must be an object".to_owned())?;
        object.insert(
            "tx".to_owned(),
            serde_json::Value::String(request.tx.to_string()),
        );
        let view = (registration.parse)(params).map_err(|error| error.to_string())?;
        (registration.cell_factory)(view, server.registry.clone(), request, server, Some(source))
    }

    /// Check if an entity type has a registered parser.
    #[must_use]
    pub fn has_item_parser(&self, entity_type: &str) -> bool {
        self.item_parsers.contains_key(entity_type)
    }

    /// Get all registered entity type names.
    pub fn entity_types(&self) -> impl Iterator<Item = &Arc<str>> {
        self.item_parsers.keys()
    }

    /// Iterate retained queries without discarding their service namespaces.
    pub fn queries(&self) -> impl Iterator<Item = &StoredQueryData> {
        self.query_data.values()
    }

    /// Iterate retained reports without discarding their service namespaces.
    pub fn reports(&self) -> impl Iterator<Item = &StoredReportData> {
        self.report_data.values()
    }

    /// Iterate retained views without discarding their service namespaces.
    pub fn views(&self) -> impl Iterator<Item = &StoredViewData> {
        self.view_data.values()
    }

    /// Return command IDs admitted by the activated service set.
    pub fn command_ids(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.commands
            .iter()
            .map(|registration| registration.command_id)
    }

    /// Retained command registrations used for metadata and application execution.
    pub fn commands(&self) -> impl Iterator<Item = &'static CommandHandlerRegistration> + '_ {
        self.commands.iter().copied()
    }
}

impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_creation() {
        // Just verify it doesn't panic - actual registrations depend on linked crates
        let registry = HandlerRegistry::new();
        // Registry should be created without error
        let _ = registry.entity_types().count();
    }
}

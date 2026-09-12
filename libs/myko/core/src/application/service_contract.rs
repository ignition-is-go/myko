//! Payload contracts from activated application code, not storage advertisements.

use std::{collections::BTreeMap, sync::Arc};

use crate::{
    ServiceTypeId,
    schema::{HandlerPayloadSchema, HandlerResultSchema, ItemSchema},
};

use super::MykoApplication;

/// Distinct application operation namespaces, including non-reactive commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ServiceHandlerKind {
    Query,
    Report,
    View,
    Command,
}

/// Generated payload contracts of one activated service.
///
/// This snapshot describes the retained executors. It is not evidence of current
/// assignment, scope readiness, authority, or semantic rollout compatibility.
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceContract {
    pub service_id: ServiceTypeId,
    pub items: BTreeMap<&'static str, ItemSchema>,
    pub handlers: BTreeMap<(ServiceHandlerKind, Arc<str>), HandlerPayloadSchema>,
}

/// Missing or contradictory evidence must not become an empty compatible contract.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ServiceContractError {
    #[error("service {service_id} is not activated")]
    InactiveService { service_id: ServiceTypeId },
    #[error("activated service {service_id} has no generated item schema evidence")]
    MissingItems { service_id: ServiceTypeId },
    #[error("service {service_id} includes item {item_type} owned by {owner}")]
    ForeignItem {
        service_id: ServiceTypeId,
        item_type: &'static str,
        owner: ServiceTypeId,
    },
    #[error("service {service_id} includes duplicate item {item_type}")]
    DuplicateItem {
        service_id: ServiceTypeId,
        item_type: &'static str,
    },
    #[error("service {service_id} has no schema for {kind:?} handler {id}")]
    MissingHandler {
        service_id: ServiceTypeId,
        kind: ServiceHandlerKind,
        id: Arc<str>,
    },
    #[error("service {service_id} includes duplicate {kind:?} handler {id}")]
    DuplicateHandler {
        service_id: ServiceTypeId,
        kind: ServiceHandlerKind,
        id: Arc<str>,
    },
    #[error("service {service_id} has the wrong result shape for {kind:?} handler {id}")]
    InvalidResult {
        service_id: ServiceTypeId,
        kind: ServiceHandlerKind,
        id: Arc<str>,
    },
}

impl MykoApplication {
    /// Assemble item and handler payload contracts from this application's activation.
    ///
    /// # Errors
    ///
    /// Returns an error for an inactive service, missing schema evidence, or
    /// contradictory registrations. Activating only an identity does not supply
    /// typed item evidence. No global inventory catalog fills that gap.
    pub fn service_contract(
        &self,
        service_id: ServiceTypeId,
    ) -> Result<ServiceContract, ServiceContractError> {
        if !self.services.contains(&service_id) {
            return Err(ServiceContractError::InactiveService { service_id });
        }
        let items = self
            .service_schemas
            .get(&service_id)
            .ok_or(ServiceContractError::MissingItems { service_id })?;
        let mut contract = ServiceContract {
            service_id,
            items: BTreeMap::new(),
            handlers: BTreeMap::new(),
        };
        for item in items {
            if item.service_id != service_id {
                return Err(ServiceContractError::ForeignItem {
                    service_id,
                    item_type: item.item_type,
                    owner: item.service_id,
                });
            }
            if contract
                .items
                .insert(item.item_type, item.clone())
                .is_some()
            {
                return Err(ServiceContractError::DuplicateItem {
                    service_id,
                    item_type: item.item_type,
                });
            }
        }
        for query in self.handlers.queries() {
            if query.service_id == Some(service_id) {
                contract.insert_handler(
                    ServiceHandlerKind::Query,
                    query.query_id.clone(),
                    query.payload_schema,
                )?;
            }
        }
        for report in self.handlers.reports() {
            if report.service_id == Some(service_id) {
                contract.insert_handler(
                    ServiceHandlerKind::Report,
                    report.report_id.clone(),
                    report.payload_schema,
                )?;
            }
        }
        for view in self.handlers.views() {
            if view.service_id == Some(service_id) {
                contract.insert_handler(
                    ServiceHandlerKind::View,
                    view.view_id.clone(),
                    view.payload_schema,
                )?;
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        for command in self.handlers.commands() {
            if command.service_id == Some(service_id) && command.durable_factory.is_some() {
                contract.insert_handler(
                    ServiceHandlerKind::Command,
                    command.command_id.into(),
                    command.payload_schema,
                )?;
            }
        }
        Ok(contract)
    }
}

impl ServiceContract {
    fn insert_handler(
        &mut self,
        kind: ServiceHandlerKind,
        id: Arc<str>,
        provider: Option<fn() -> HandlerPayloadSchema>,
    ) -> Result<(), ServiceContractError> {
        let schema = provider.ok_or_else(|| ServiceContractError::MissingHandler {
            service_id: self.service_id,
            kind,
            id: id.clone(),
        })?();
        let expects_rows = match kind {
            ServiceHandlerKind::Query | ServiceHandlerKind::View => true,
            ServiceHandlerKind::Report | ServiceHandlerKind::Command => false,
        };
        let returns_rows = match &schema.result {
            HandlerResultSchema::Rows(_) => true,
            HandlerResultSchema::Value(_) => false,
        };
        if expects_rows != returns_rows {
            return Err(ServiceContractError::InvalidResult {
                service_id: self.service_id,
                kind,
                id,
            });
        }
        if self.handlers.insert((kind, id.clone()), schema).is_some() {
            return Err(ServiceContractError::DuplicateHandler {
                service_id: self.service_id,
                kind,
                id,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

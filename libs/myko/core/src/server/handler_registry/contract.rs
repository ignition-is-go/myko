use myko_federation::NodeId;
use myko_wire::{HandlerContract, HandlerRequest};

use super::HandlerRegistry;

impl HandlerRegistry {
    pub(crate) fn handler_contract(
        &self,
        serving_node: NodeId,
        request: &HandlerRequest,
    ) -> Result<HandlerContract, String> {
        #[cfg(not(feature = "schema"))]
        {
            let _ = (self, serving_node, request);
            Err("application build has no generated handler schema evidence".to_owned())
        }
        #[cfg(feature = "schema")]
        {
            use myko_federation::{HandlerKind, ServiceId};
            use myko_wire::HandlerResultContract;

            let service = request.service_id.as_ref().map(ServiceId::as_str);
            let missing = || {
                format!(
                    "{} handler {} is not registered for the requested service",
                    request.kind.as_str(),
                    request.handler_id
                )
            };
            let (owner, provider) = match request.kind {
                HandlerKind::Query => {
                    let entry = self
                        .query(service, &request.handler_id)
                        .ok_or_else(missing)?;
                    (entry.service_id, entry.payload_schema)
                }
                HandlerKind::Report => {
                    let entry = self
                        .report(service, &request.handler_id)
                        .ok_or_else(missing)?;
                    (entry.service_id, entry.payload_schema)
                }
                HandlerKind::View => {
                    let entry = self
                        .view(service, &request.handler_id)
                        .ok_or_else(missing)?;
                    (entry.service_id, entry.payload_schema)
                }
                HandlerKind::Command => {
                    return Err("commands have no reactive handler contract".to_owned());
                }
            };
            let schema = provider.ok_or_else(|| {
                format!(
                    "handler {} has no generated schema evidence",
                    request.handler_id
                )
            })?();
            let result = match schema.result {
                crate::schema::HandlerResultSchema::Value(schema) => {
                    HandlerResultContract::Value(schema_pair(schema)?)
                }
                crate::schema::HandlerResultSchema::Rows(schema) => {
                    HandlerResultContract::Rows(schema_pair(schema)?)
                }
            };
            let contract = HandlerContract {
                serving_node,
                service_id: owner.map(|owner| ServiceId::new(owner.as_str())),
                kind: request.kind,
                handler_id: request.handler_id.clone(),
                arguments: schema_pair(schema.arguments)?,
                result,
            };
            contract.validate_for(request, Some(serving_node))?;
            Ok(contract)
        }
    }
}

#[cfg(feature = "schema")]
fn schema_pair(schema: crate::schema::TypeSchema) -> Result<myko_wire::TypeSchemaPair, String> {
    Ok(myko_wire::TypeSchemaPair {
        serialization: serde_json::Value::from(schema.serialization).try_into()?,
        deserialization: serde_json::Value::from(schema.deserialization).try_into()?,
    })
}

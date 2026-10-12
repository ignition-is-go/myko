//! Static domain metadata, separate from live entity values.
use myko::{
    graph::{EdgeRegistration, EndpointRequirement},
    reflection::{DomainGuide, EntityDescription},
    relationship::{Relation, iter_relations},
};
use serde_json::{Value, json};

use super::filter::ClientFilters;

fn entity_visible(filter: &ClientFilters, id: &str) -> bool {
    filter.tool_visible(&format!("entity_{id}"))
}

pub(super) fn search(
    kind: &str,
    query: Option<&str>,
    id: Option<&str>,
    details: bool,
    filter: &ClientFilters,
) -> Result<Value, String> {
    let query = query.map(str::to_lowercase);
    let matches = |value: &Value| {
        id.is_none_or(|id| value.get("id").and_then(Value::as_str) == Some(id))
            && query
                .as_ref()
                .is_none_or(|query| value.to_string().to_lowercase().contains(query))
    };
    let mut values = match kind {
        "entity" => myko::inventory::iter::<EntityDescription>
            .into_iter()
            .filter(|entry| entity_visible(filter, entry.entity_type))
            .map(|entry| {
                let mut value = json!({"id": entry.entity_type, "description": entry.description});
                if details && let Some(object) = value.as_object_mut() {
                    object.insert("fields".into(), json!(entry.fields));
                    object.insert(
                        "relationships".into(),
                        json!(
                            relationships(filter)
                                .into_iter()
                                .filter(|relation| references_entity(relation, entry.entity_type))
                                .collect::<Vec<_>>()
                        ),
                    );
                }
                value
            })
            .collect::<Vec<_>>(),
        "relationship" => relationships(filter),
        "guide" => myko::inventory::iter::<DomainGuide>
            .into_iter()
            .filter(|entry| filter.tool_visible(&format!("guide_{}", entry.id)))
            .map(|entry| {
                let mut value =
                    json!({"id":entry.id, "title":entry.title, "description":entry.description});
                if details && let Some(object) = value.as_object_mut() {
                    object.insert("content".into(), json!(entry.content));
                }
                value
            })
            .collect::<Vec<_>>(),
        _ => return Err(format!("Unknown domain kind: {kind}")),
    };
    values.retain(matches);
    values.sort_by_key(|value| {
        value
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    });
    if details {
        if id.is_none() {
            return Err("Domain details requires id".into());
        }
        return values
            .into_iter()
            .next()
            .ok_or_else(|| format!("Unknown {kind}: {}", id.unwrap_or_default()));
    }
    Ok(json!({"entries": values}))
}

fn requirement(value: EndpointRequirement, filter: &ClientFilters) -> Option<Value> {
    match value {
        EndpointRequirement::Concrete(entity) => {
            entity_visible(filter, entity).then(|| json!({"entity":entity}))
        }
        EndpointRequirement::OneOf(entities) => entities
            .iter()
            .all(|entity| entity_visible(filter, entity))
            .then(|| json!({"oneOf":entities})),
        EndpointRequirement::Category(category) => {
            let entities: Vec<_> = myko::inventory::iter::<myko::graph::ItemCategoryRegistration>
                .into_iter()
                .filter(|entry| entry.entity_category_id == category)
                .map(|entry| entry.item_type)
                .collect();
            entities
                .iter()
                .all(|entity| entity_visible(filter, entity))
                .then(|| json!({"category":category,"entities":entities}))
        }
        EndpointRequirement::AnyRegisteredItem => Some(json!({"anyVisibleEntity":true})),
    }
}

fn relationships(filter: &ClientFilters) -> Vec<Value> {
    let mut values = Vec::new();
    for entry in iter_relations() {
        match &entry.relation {
            Relation::BelongsTo {local_type,foreign_type,fk_field_json,exclude_from_tree,..} if entity_visible(filter,local_type) && entity_visible(filter,foreign_type) => values.push(json!({
                "id":format!("{local_type}.{fk_field_json}"),"kind":"belongsTo", "source":local_type,"target":foreign_type,"field":fk_field_json,
                "onTargetDelete":"cascadeSource", "excludedFromTree":exclude_from_tree,
            })),
            Relation::OwnsMany {local_type,foreign_type,exclude_from_tree,..} if entity_visible(filter,local_type) && entity_visible(filter,foreign_type) => values.push(json!({
                "id":format!("{local_type}.owns.{foreign_type}"),"kind":"ownsMany", "source":local_type,"target":foreign_type,
                "onSourceDelete":"cascadeTargets","onTargetDelete":"removeReference", "excludedFromTree":exclude_from_tree,
            })),
            Relation::EnsureFor {local_type,dependencies,exclude_from_tree,..} if entity_visible(filter,local_type) && dependencies.iter().all(|dependency|entity_visible(filter,dependency.foreign_type)) => values.push(json!({
                "id":format!("{local_type}.ensureFor"),"kind":"ensureFor","source":local_type,
                "dependencies":dependencies.iter().map(|dependency|dependency.foreign_type).collect::<Vec<_>>(), "excludedFromTree":exclude_from_tree,
            })),
            _ => {}
        }
    }
    for edge in myko::inventory::iter::<EdgeRegistration> {
        if !entity_visible(filter, edge.edge_type)
            || (edge.scope_type)().is_some_and(|scope| !entity_visible(filter, scope))
        {
            continue;
        }
        let endpoints: Option<Vec<_>> = edge
            .endpoints
            .iter()
            .map(|endpoint| requirement((endpoint.requirement)(), filter))
            .collect();
        if let Some(endpoints) = endpoints {
            values.push(json!({"id":edge.edge_type,"kind":"graphEdge", "endpoints":endpoints,"shape":edge.shape,
                "pairPolicy":edge.pair_policy,
                "pairProjection":edge.pair_projection, "scopeType":(edge.scope_type)(),"adjacency":edge.adjacency,"selfLoops":edge.self_loops,"onADelete":edge.a_delete,"onBDelete":edge.b_delete}));
        }
    }
    values
}

fn references_entity(value: &Value, entity: &str) -> bool {
    match value {
        Value::String(value) => value == entity,
        Value::Array(values) => values.iter().any(|value| references_entity(value, entity)),
        Value::Object(fields) => fields
            .values()
            .any(|value| references_entity(value, entity)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod group {
        use myko::prelude::*;
        /// A group of records with a shared lifecycle.
        #[myko::myko_item]
        pub struct DomainGroup {}
    }
    mod record {
        use myko::prelude::*;

        use super::group::{DomainGroup, DomainGroupId};
        /// A record owned by a group.
        #[myko::myko_item]
        pub struct DomainRecord {
            /// Group responsible for this record; deleting it cascades this record.
            #[belongs_to(DomainGroup)]
            pub group_id: DomainGroupId,
            /// Human-readable label.
            #[serde(rename = "displayLabel")]
            pub label: String,
        }
    }

    myko::register_domain_guide!(
        "record-workflow",
        "Record workflow",
        "How records belong to groups",
        "Query groups first, then join record.groupId against group.id. Deleting a group cascades its records."
    );

    #[test]
    fn describes_entity_docs_wire_fields_and_relationship_semantics() {
        let value = search(
            "entity",
            None,
            Some("DomainRecord"),
            true,
            &ClientFilters::allow_all(),
        );
        assert!(value.is_ok());
        let Ok(value) = value else {
            return;
        };
        assert_eq!(
            value.get("description"),
            Some(&json!("A record owned by a group."))
        );
        assert!(
            value
                .get("fields")
                .and_then(Value::as_array)
                .is_some_and(|fields| fields
                    .iter()
                    .any(|field| field.get("name") == Some(&json!("displayLabel"))
                        && field.get("description") == Some(&json!("Human-readable label."))))
        );
        assert!(
            value
                .get("relationships")
                .and_then(Value::as_array)
                .is_some_and(
                    |relations| relations.iter().any(|relation| relation.get("target")
                        == Some(&json!("DomainGroup"))
                        && relation.get("onTargetDelete") == Some(&json!("cascadeSource")))
                )
        );
    }

    #[test]
    fn hidden_entities_hide_their_relationships_and_details() {
        let filter = ClientFilters::from_strings(None, Some("entity_DomainGroup"), None, None);
        assert!(search("entity", None, Some("DomainGroup"), true, &filter).is_err());
        let value = search("relationship", None, None, false, &filter);
        assert!(
            value
                .as_ref()
                .is_ok_and(|value| !value.to_string().contains("DomainGroup"))
        );
    }

    #[test]
    fn guides_are_selective_and_visibility_filtered() {
        let brief = search(
            "guide",
            Some("workflow"),
            None,
            false,
            &ClientFilters::allow_all(),
        );
        assert!(
            brief
                .as_ref()
                .is_ok_and(|value| !value.to_string().contains("Query groups first"))
        );
        let detail = search(
            "guide",
            None,
            Some("record-workflow"),
            true,
            &ClientFilters::allow_all(),
        );
        assert!(
            detail
                .as_ref()
                .is_ok_and(|value| value.get("content").is_some())
        );
        let filter = ClientFilters::from_strings(None, Some("guide_*"), None, None);
        assert!(search("guide", None, Some("record-workflow"), true, &filter).is_err());
    }
}

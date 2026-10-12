use myko::operation_index::OperationSchema;
use serde_json::{Value, json};

use super::filter::ClientFilters;

pub(super) fn describe(
    index: &[OperationSchema],
    filter: &ClientFilters,
    kind: &str,
    id: &str,
) -> Result<Value, String> {
    if matches!(kind, "entity" | "relationship" | "guide") {
        return super::domain::search(kind, None, Some(id), true, filter);
    }
    let name = format!("{kind}_{id}");
    if !filter.tool_visible(&name) {
        return Err(format!("Unknown operation: {name}"));
    }
    let operation = index
        .iter()
        .find(|op| op.kind == kind && op.id == id)
        .ok_or_else(|| format!("Unknown operation: {name}"))?;
    let fields = operation
        .args
        .iter()
        .filter(|arg| !arg.flattened)
        .map(|arg| {
            format!(
                "{:?}{}: {}",
                arg.name,
                if arg.optional { "?" } else { "" },
                arg.ts_type
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    let mut input_type = format!("{{ {fields} }}");
    for arg in operation.args.iter().filter(|arg| arg.flattened) {
        input_type.push_str(" & (");
        input_type.push_str(&arg.ts_type);
        input_type.push(')');
    }
    let mut description = operation
        .describe()
        .as_object()
        .cloned()
        .ok_or("Operation description must be an object")?;
    let result_shape = description
        .get("outputType")
        .cloned()
        .unwrap_or(Value::Null);
    let result_type = result_shape
        .as_object()
        .map(|fields| {
            fields
                .iter()
                .map(|(name, ty)| format!("{name}: {}", ty.as_str().unwrap_or("unknown")))
                .collect::<Vec<_>>()
                .join("; ")
        })
        .unwrap_or_default();
    let result_type = format!("{{ {result_type} }}");
    if let Some(args) = description
        .get("exampleArgs")
        .filter(|args| !args.is_null())
    {
        let schema = super::schema::compile(&operation.input_schema())?;
        if super::schema::validate(&schema, args).is_err() {
            description.insert("exampleArgs".to_string(), Value::Null);
            description.insert("example".to_string(), Value::Null);
            description.insert(
                "exampleUnavailable".to_string(),
                json!("Registered example does not satisfy structural input constraints."),
            );
        }
    }
    description.insert("argumentShape".to_string(), json!("object"));
    description.insert("typescript".to_string(), json!(format!("declare const myko: {{ {kind}(id: {id:?}, args: {input_type}): Promise<{result_type}> }};")));
    description.insert("resultShape".to_string(), result_shape);
    description.insert("notes".to_string(), json!("JSON Schema constrains reflected types. Custom deserializers and unresolved types remain opaque. Examples satisfy known structural constraints; authoritative Serde validation runs once during execution and may enforce additional domain rules."));
    Ok(Value::Object(description))
}

#[cfg(test)]
mod tests {
    use myko::operation_index::OperationArg;

    use super::*;
    myko::register_operation_example!("command", BadExample, json!({"count":"wrong"}));

    #[test]
    fn suppresses_registered_examples_that_fail_structural_constraints() {
        let index = vec![OperationSchema {
            id: "BadExample".into(),
            kind: "command".into(),
            description: None,
            output_type: "number".into(),
            args: vec![OperationArg {
                name: "count".into(),
                ts_type: "number".into(),
                rust_type: "u32".into(),
                optional: false,
                aliases: vec![],
                description: None,
                flattened: false,
                defaulted: false,
                input_schema: json!({"type":"integer","minimum":0}),
            }],
        }];
        let description = describe(&index, &ClientFilters::allow_all(), "command", "BadExample");
        assert!(
            description
                .as_ref()
                .is_ok_and(|description| description.get("example") == Some(&Value::Null))
        );
    }
}

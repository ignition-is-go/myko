use serde_json::Value;

pub(super) const MAX_RESULT_BYTES: usize = 64 * 1024;

struct NoExternalReferences;

impl jsonschema::Retrieve for NoExternalReferences {
    fn retrieve(
        &self,
        _uri: &jsonschema::Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "MCP schemas must use local references",
        )
        .into())
    }
}

pub(super) fn compile(schema: &Value) -> Result<jsonschema::Validator, String> {
    jsonschema::options()
        .with_retriever(NoExternalReferences)
        .build(schema)
        .map_err(|error| format!("Invalid JSON Schema: {error}"))
}

pub(super) fn validate(schema: &jsonschema::Validator, value: &Value) -> Result<(), String> {
    schema.validate(value).map_err(|error| diagnostic(&error).1)
}

pub(super) fn check_result_size(text: &str) -> Result<(), String> {
    if text.len() > MAX_RESULT_BYTES {
        return Err(format!(
            "MCP result exceeds {MAX_RESULT_BYTES} bytes. Return selected fields or computed counts; narrow discovery searches."
        ));
    }
    Ok(())
}

pub(super) fn bounded_error(message: String) -> String {
    if message.len() > MAX_RESULT_BYTES {
        "MCP error exceeds the result limit; narrow the request.".to_string()
    } else {
        message
    }
}

pub(super) fn serialize_result(value: &Value) -> Result<String, String> {
    let text = serde_json::to_string(value).map_err(|error| error.to_string())?;
    check_result_size(&text)?;
    Ok(text)
}

fn diagnostic(error: &jsonschema::ValidationError<'_>) -> (usize, String) {
    if let jsonschema::error::ValidationErrorKind::AnyOf { context }
    | jsonschema::error::ValidationErrorKind::OneOfNotValid { context } = &error.kind
        && let Some(diagnostic) = context
            .iter()
            .flatten()
            .map(diagnostic)
            .max_by_key(|(depth, _)| *depth)
    {
        return diagnostic;
    }
    let path = error.instance_path.to_string();
    (
        path.split('/').count(),
        format!("{path}: {}", error.masked()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concrete_schema_reflection_handles_known_and_opaque_types() {
        use myko::reflection::{InputSchema, ReflectInputSchema};
        struct Opaque;
        let opaque = (&InputSchema::<Opaque>::default()).input_schema();
        assert_eq!(opaque.get("x-myko-opaque"), Some(&serde_json::json!(true)));
        let known = (&&InputSchema::<Vec<Option<String>>>::default()).input_schema();
        let validator = compile(&known);
        assert!(validator.is_ok());
        let Ok(validator) = validator else {
            return;
        };
        assert!(validate(&validator, &serde_json::json!(["value", null])).is_ok());
        assert!(validate(&validator, &serde_json::json!([123])).is_err());
    }

    #[test]
    fn rejects_external_schema_references_without_fetching() {
        let result = compile(&serde_json::json!({"$ref": "file:///does-not-exist.json"}));
        assert!(result.is_err());
    }

    #[test]
    fn reports_deepest_nested_constraint_without_raw_values() {
        let schema = compile(&serde_json::json!({"type":"object", "properties":{
            "config":{"anyOf":[{"type":"null"},{"type":"object","properties":{"mode":{"enum":["fast","precise"]}}}]}
        }}));
        assert!(schema.is_ok());
        let Ok(schema) = schema else {
            return;
        };
        let result = validate(
            &schema,
            &serde_json::json!({"config":{"mode":"private-invalid-value"}}),
        );
        assert!(result.as_ref().err().is_some_and(
            |error| error.contains("/config/mode") && !error.contains("private-invalid-value")
        ));
    }
}

#![cfg(feature = "schema")]

use std::error::Error;

use myko_items::{
    ItemScope, MykoItem, MykoService, myko_item, myko_service, myko_subtype, schema::TypeSchema,
};
use serde_json::{Value, json};

type TestResult = Result<(), Box<dyn Error>>;

#[myko_service(Collection, Entry)]
pub struct Catalog;

#[myko_item(service = Catalog, scope_root)]
pub struct Collection {
    pub display_name: String,
}

#[myko_subtype(derive(Eq))]
#[serde(tag = "kind", content = "value")]
pub enum Content {
    Text(String),
    Count(u16),
}

#[myko_item(service = Catalog, scoped_by = Collection)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    #[serde(rename = "contents")]
    pub values: Vec<Content>,
    pub note: Option<String>,
}

fn field<'a>(root: &'a Value, name: &str) -> Result<&'a Value, Box<dyn Error>> {
    let value = root
        .get("properties")
        .and_then(|properties| properties.get(name))
        .ok_or("missing generated field")?;
    if let Some(reference) = value.get("$ref").and_then(Value::as_str) {
        let pointer = reference.strip_prefix('#').ok_or("non-local schema ref")?;
        return root
            .pointer(pointer)
            .ok_or_else(|| "unresolved schema ref".into());
    }
    Ok(value)
}

#[test]
fn service_generation_includes_scope_metadata_and_transitive_payloads() -> TestResult {
    let schemas = Catalog::item_schemas().ok_or("missing generated item schemas")?;
    let [collection, entry] = schemas.as_slice() else {
        return Err("service schema omitted a registered item".into());
    };
    if collection.scope != ItemScope::Root || entry.scope != Entry::SCOPE {
        return Err("service schema lost placement metadata".into());
    }
    let accepted = serde_json::to_value(&entry.value.deserialization)?;
    if field(&accepted, "id")?.get("type") != Some(&json!("string"))
        || field(&accepted, "collectionId")?.get("type") != Some(&json!("string"))
        || field(&accepted, "contents")?.get("type") != Some(&json!("array"))
        || accepted.get("additionalProperties") != Some(&json!(false))
        || accepted.pointer("/properties/values").is_some()
    {
        return Err("generated payload schema disagreed with Serde or generated IDs".into());
    }
    let variants = accepted
        .pointer("/$defs/Content/oneOf")
        .and_then(Value::as_array)
        .ok_or("missing transitive enum schema")?;
    if variants.len() != 2
        || !variants.iter().any(|variant| {
            variant.pointer("/properties/kind/const") == Some(&json!("Count"))
                && variant.pointer("/properties/value/type") == Some(&json!("integer"))
        })
    {
        return Err("generated schema omitted tagged enum payloads".into());
    }
    let value = Entry {
        id: EntryId::from("entry"),
        collection_id: CollectionId::from("collection"),
        values: vec![Content::Count(12)],
        note: None,
    };
    let encoded = serde_json::to_value(value)?;
    if encoded.get("collectionId") != Some(&json!("collection"))
        || encoded.get("contents") != Some(&json!([{"kind": "Count", "value": 12}]))
    {
        return Err("fixture's actual serialized payload drifted".into());
    }
    Ok(())
}

#[test]
fn generated_query_inputs_use_the_generated_id_contract() -> TestResult {
    let one = serde_json::to_value(TypeSchema::of::<GetEntryById>().deserialization)?;
    if field(&one, "id")?.get("type") != Some(&json!("string")) {
        return Err("query schema did not include the generated ID".into());
    }
    let many = serde_json::to_value(TypeSchema::of::<GetEntrysByIds>().deserialization)?;
    if field(&many, "ids")?.get("type") != Some(&json!("array")) {
        return Err("multi-ID query schema did not include its array".into());
    }
    let all = serde_json::to_value(TypeSchema::of::<GetAllEntrys>().deserialization)?;
    if all.get("type") != Some(&json!("null")) {
        return Err("unit query input was not represented as null".into());
    }
    Ok(())
}

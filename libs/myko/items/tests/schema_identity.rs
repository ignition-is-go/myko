use std::error::Error;

use myko_items::{ItemMutation, MykoItem, myko_item, myko_service};

type TestResult = Result<(), Box<dyn Error>>;

// Separate modules model releases with the same service and item wire identities.
#[myko_service(original::Record)]
pub struct Records;

mod original {
    use super::{Records, myko_item};

    #[myko_item(service = Records)]
    pub struct Record {
        pub title: String,
    }
}

mod changed_type {
    use super::{Records, myko_item};

    #[myko_item(service = Records)]
    pub struct Record {
        pub title: u64,
    }
}

mod changed_wire_name {
    use super::{Records, myko_item};

    #[myko_item(service = Records)]
    pub struct Record {
        #[serde(rename = "body")]
        pub title: String,
    }
}

mod extended {
    use super::{Records, myko_item};

    #[myko_item(service = Records)]
    pub struct Record {
        pub title: String,
        #[serde(default)]
        pub category: String,
    }
}

#[test]
fn matching_item_identity_and_declared_version_do_not_prove_decoding_compatibility() -> TestResult {
    let old = original::Record {
        id: original::RecordId::from("record-1"),
        title: "a record".to_owned(),
    };
    let mutation = ItemMutation::set(&old)?;
    if !mutation.is::<changed_type::Record>() || !mutation.is::<changed_wire_name::Record>() {
        return Err("fixture did not retain the same declared identity and version".into());
    }
    if mutation.decode_set::<changed_type::Record>().is_ok()
        || mutation.decode_set::<changed_wire_name::Record>().is_ok()
    {
        return Err("incompatible type or wire rename unexpectedly decoded".into());
    }
    if original::Record::ITEM_TYPE != changed_type::Record::ITEM_TYPE
        || original::Record::SCHEMA_VERSION != changed_type::Record::SCHEMA_VERSION
    {
        return Err("fixture compared different declared schema versions".into());
    }
    Ok(())
}

#[test]
fn bidirectional_decoding_does_not_prove_safe_rolling_writes() -> TestResult {
    let newer = extended::Record {
        id: extended::RecordId::from("record-1"),
        title: "before".to_owned(),
        category: "must survive".to_owned(),
    };
    let initial = ItemMutation::set(&newer)?;
    let mut older_writer = initial.decode_set::<original::Record>()?;
    older_writer.title = "after".to_owned();
    let replacement = ItemMutation::set(&older_writer)?;
    let decoded = replacement.decode_set::<extended::Record>()?;
    if decoded.title != "after" || !decoded.category.is_empty() {
        return Err("fixture did not expose the older writer's dropped field".into());
    }
    if initial.decode_set::<extended::Record>()? != newer {
        return Err("fixture changed the retained original mutation".into());
    }
    Ok(())
}

#[cfg(feature = "schema")]
mod generated {
    use myko_items::{
        MykoService,
        schema::{ItemSchema, TypeSchema},
    };

    use super::{Records, TestResult, changed_type, changed_wire_name, extended, original};

    #[test]
    fn generated_schemas_distinguish_changes_hidden_by_declared_identity() -> TestResult {
        let original = ItemSchema::of::<original::Record>();
        for changed in [
            ItemSchema::of::<changed_type::Record>(),
            ItemSchema::of::<changed_wire_name::Record>(),
            ItemSchema::of::<extended::Record>(),
        ] {
            if original.service_id != changed.service_id
                || original.item_type != changed.item_type
                || original.declared_version != changed.declared_version
            {
                return Err("fixture no longer has identical declared identity".into());
            }
            if original.value.serialization == changed.value.serialization
                || original.value.deserialization == changed.value.deserialization
            {
                return Err("generated schema concealed a payload change".into());
            }
        }
        if Records::item_schemas() != Some(vec![original]) {
            return Err("service schema did not follow the declared item list".into());
        }
        Ok(())
    }

    #[test]
    fn a_defaulted_field_has_distinct_emitted_and_accepted_contracts() -> TestResult {
        let generated = TypeSchema::of::<extended::Record>();
        let emitted = serde_json::to_value(generated.serialization)?;
        let accepted = serde_json::to_value(generated.deserialization)?;
        let name = serde_json::json!("category");
        if !emitted
            .get("required")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|fields| fields.contains(&name))
            || accepted
                .get("required")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|fields| fields.contains(&name))
        {
            return Err("defaulted field lost its serialize/deserialize distinction".into());
        }
        Ok(())
    }
}

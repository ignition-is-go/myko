use std::cell::Cell;

use myko::{myko_item, myko_service};
use serde::{Deserialize as _, Deserializer};

use super::*;

thread_local! {
    static DECODES: Cell<usize> = const { Cell::new(0) };
}

fn count_decode<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    DECODES.with(|count| count.set(count.get().saturating_add(1)));
    bool::deserialize(deserializer)
}

#[myko_service(CountedRealm)]
pub struct CountingService;

#[myko_item(service = CountingService, scope_root)]
pub struct CountedRealm {
    #[serde(deserialize_with = "count_decode")]
    value: bool,
}

fn realm_item() -> CountedRealm {
    CountedRealm {
        id: "projection-test".into(),
        value: true,
    }
}

#[test]
fn certified_set_is_decoded_once_for_projection_and_realm_validation() -> Result<(), String> {
    let item = realm_item();
    let mutation = myko_federation::ItemMutation::set(&item).map_err(|error| error.to_string())?;
    let mut projection = ItemProjection::<CountedRealm>::default();
    let before = DECODES.with(Cell::get);
    project_mutation(
        &mut projection,
        &mut BTreeSet::new(),
        false,
        &mutation,
        &AuthorityRealmKey::new("projection-test"),
    )?;
    if projection.get(&item.id) != Some(&item) {
        return Err("certified mutation did not produce the original item".to_owned());
    }
    let count = DECODES.with(Cell::get).saturating_sub(before);
    if count != 1 {
        return Err(format!(
            "one certified set decoded its payload {count} times"
        ));
    }
    Ok(())
}

#[test]
fn malformed_sets_cannot_use_an_existing_valid_item_to_pass_validation() -> Result<(), String> {
    let item = realm_item();
    let valid = myko_federation::ItemMutation::set(&item).map_err(|error| error.to_string())?;
    let realm = AuthorityRealmKey::new("projection-test");
    let mut invalid = Vec::new();
    let mut mutation = valid.clone();
    mutation.service_id = "another-service".to_owned();
    invalid.push(("foreign service", mutation, realm.clone()));
    let mut mutation = valid.clone();
    mutation.schema_version = 0;
    invalid.push(("schema version", mutation, realm.clone()));
    let mut mutation = valid.clone();
    mutation.item_id = "another-id".to_owned();
    invalid.push(("identifier", mutation, realm.clone()));
    let mut mutation = valid.clone();
    mutation.roots_scope = false;
    invalid.push(("scope metadata", mutation, realm.clone()));
    let mut mutation = valid.clone();
    mutation.payload = Some(b"invalid-json".to_vec());
    invalid.push(("payload", mutation, realm));
    invalid.push((
        "realm",
        valid.clone(),
        AuthorityRealmKey::new("another-realm"),
    ));
    for (label, mutation, expected_realm) in invalid {
        let mut projection = ItemProjection::<CountedRealm>::default();
        projection
            .apply(&valid)
            .map_err(|error| error.to_string())?;
        if project_mutation(
            &mut projection,
            &mut BTreeSet::new(),
            false,
            &mutation,
            &expected_realm,
        )
        .is_ok()
        {
            return Err(format!("certified projection accepted invalid {label}"));
        }
    }
    Ok(())
}

#[test]
fn immutable_records_cannot_be_replaced_or_deleted() -> Result<(), String> {
    let item = realm_item();
    let realm = AuthorityRealmKey::new("projection-test");
    let set = myko_federation::ItemMutation::set(&item).map_err(|error| error.to_string())?;
    let delete = myko_federation::ItemMutation::delete::<CountedRealm>(&item.id);
    for next in [&set, &delete] {
        let mut projection = ItemProjection::<CountedRealm>::default();
        let mut seen = BTreeSet::new();
        project_mutation(&mut projection, &mut seen, true, &set, &realm)?;
        if project_mutation(&mut projection, &mut seen, true, next, &realm).is_ok() {
            return Err("certified projection replaced or deleted an immutable record".to_owned());
        }
    }
    let mut projection = ItemProjection::<CountedRealm>::default();
    let mut seen = BTreeSet::new();
    project_mutation(&mut projection, &mut seen, false, &set, &realm)?;
    let mut invalid_delete = delete.clone();
    invalid_delete.payload = Some(b"unexpected".to_vec());
    if project_mutation(&mut projection, &mut seen, false, &invalid_delete, &realm).is_ok()
        || projection.get(&item.id) != Some(&item)
    {
        return Err("malformed deletion was accepted or changed the projection".to_owned());
    }
    project_mutation(&mut projection, &mut seen, false, &delete, &realm)?;
    if projection.get(&item.id).is_some() {
        return Err("mutable deletion did not remove the record".to_owned());
    }
    Ok(())
}

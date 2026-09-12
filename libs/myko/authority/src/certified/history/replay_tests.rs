use std::cell::Cell;

use ed25519_dalek::SigningKey;

use super::*;

thread_local! {
    static REPLAYS: Cell<usize> = const { Cell::new(0) };
}

pub(super) fn record_replay() {
    REPLAYS.with(|count| count.set(count.get().saturating_add(1)));
}

#[test]
fn repeated_head_reads_reuse_facts_only_within_the_same_snapshot() -> Result<(), String> {
    let key = SigningKey::from_bytes(&[71; 32]);
    let head = ControlHead([72; 32]);
    let anchor = AuthorityAnchor::new(
        AuthorityRealmKey::new("historical-replay-reuse"),
        ControlEpochId([73; 32]),
        head,
        vec![ControllerId(key.verifying_key().to_bytes())],
    )?;
    let history = AuthorityHistory::from_events(Vec::new(), anchor)?;
    let before = REPLAYS.with(Cell::get);
    let first = history.selected_at(head)?;
    let repeated = history.selected_at(head)?;
    if first != repeated {
        return Err("repeated historical selection changed its facts".to_owned());
    }
    let count = REPLAYS.with(Cell::get).saturating_sub(before);
    if count != 1 {
        return Err(format!("the same snapshot and head replayed {count} times"));
    }
    let refreshed = history.refresh(Vec::new())?;
    if refreshed.selected_at(head)? != first {
        return Err("fresh snapshot changed equivalent historical facts".to_owned());
    }
    if REPLAYS.with(Cell::get).saturating_sub(before) != 2 {
        return Err("a refreshed snapshot reused another snapshot's replay".to_owned());
    }
    Ok(())
}

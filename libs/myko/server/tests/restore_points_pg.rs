//! Live-Postgres validation of the restore-point / undo history queries.
//!
//! Skips cleanly unless `MYKO_POSTGRES_URL` is set, so it is safe in CI. Run it against
//! a real event store with your server environment:
//!
//! ```bash
//! MYKO_POSTGRES_URL=... cargo test -p myko-server --test restore_points_pg -- --nocapture
//! ```
//!
//! It exercises the two SQL paths added for undo (`events_for_transaction` and
//! `transaction_undo_plan`) against the live schema and a real, recently-written
//! transaction.

use myko::server::{HandlerRegistry, HistoryReplayProvider};
use myko_server::postgres::{PostgresConfig, PostgresHistoryReplayProvider};

#[test]
fn tx_history_queries_against_live_db() {
    let Some(config) = PostgresConfig::from_env() else {
        eprintln!("SKIP: MYKO_POSTGRES_URL not set");
        return;
    };
    let url = config.url.clone();
    let table = config.table.clone();

    // Discover the most recent transaction (table name is a trusted config value).
    let mut client =
        postgres::Client::connect(&url, postgres::NoTls).expect("connect to MYKO_POSTGRES_URL");
    let discover = format!(
        "SELECT tx, COUNT(*) AS n FROM {table} GROUP BY tx ORDER BY MAX(id) DESC LIMIT 1"
    );
    let rows = client.query(&discover, &[]).expect("discover recent tx");
    if rows.is_empty() {
        eprintln!("SKIP: events table `{table}` is empty");
        return;
    }
    let tx: String = rows[0].get(0);
    let n: i64 = rows[0].get(1);
    eprintln!("most recent tx `{tx}` wrote {n} event(s)");

    let registry = HandlerRegistry::new();
    let provider = PostgresHistoryReplayProvider::new(config);

    // events_for_transaction — must return the transaction's events.
    let events = provider
        .events_for_transaction(&tx, &registry)
        .expect("events_for_transaction should execute");
    eprintln!("events_for_transaction -> {} event(s)", events.len());
    assert!(!events.is_empty(), "a real tx must return events");

    // transaction_undo_plan — one target per touched entity.
    let plan = provider
        .transaction_undo_plan(&tx, &registry)
        .expect("transaction_undo_plan should execute");
    eprintln!("transaction_undo_plan -> {} target(s)", plan.len());
    assert!(!plan.is_empty(), "a real tx must produce an undo plan");

    let distinct_entities: std::collections::HashSet<(String, String)> = events
        .iter()
        .map(|e| {
            let id = e
                .item
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            (e.item_type.clone(), id)
        })
        .collect();
    assert_eq!(
        plan.len(),
        distinct_entities.len(),
        "one undo target per distinct entity touched by the tx"
    );

    for t in &plan {
        eprintln!(
            "  {}:{} prior={} conflict={}",
            t.item_type,
            t.item_id,
            t.prior.is_some(),
            t.conflict
        );
        assert!(
            distinct_entities.contains(&(t.item_type.to_string(), t.item_id.to_string())),
            "every undo target must be an entity the tx touched"
        );
        // in_tx_value must carry the entity's id so a DEL payload is well-formed.
        assert_eq!(
            t.in_tx_value.get("id").and_then(|v| v.as_str()),
            Some(t.item_id.as_ref()),
            "in_tx_value must carry the entity id"
        );
    }

    // A bogus tx proves the SQL executes cleanly and filters correctly.
    let none = provider
        .transaction_undo_plan("definitely-not-a-real-tx-id", &registry)
        .expect("undo plan for bogus tx should execute");
    assert!(none.is_empty(), "bogus tx must yield an empty plan");

    eprintln!("OK: live-DB validation passed");
}

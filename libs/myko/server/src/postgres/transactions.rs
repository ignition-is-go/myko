//! Indexed transaction selection and repeatable prior-state lookup.
use super::{PostgresHistoryStore, connect_pg_client, qi};
use myko::server::{EventRecord, HistoryEntityKey, TransactionChange, TransactionHistory};
use std::sync::Arc;

pub(super) const fn boundary_filter() -> &'static str {
    "item_type = 'UndoHistoryBoundary' AND change_type = 'SET' AND event->'provenance'->>'commandId' = 'myko:undo-history-boundary' AND event->'provenance'->>'actorUserId' IS NULL AND event->'provenance'->>'requestTx' = tx"
}
fn predates_boundary(
    snapshot: &mut postgres::Transaction<'_>,
    table: &str,
    tx: &str,
) -> Result<bool, String> {
    snapshot.query_one(&format!(
        "SELECT COALESCE((SELECT MIN(id) FROM {table} WHERE tx = $1) <= (SELECT MAX(id) FROM {table} WHERE {}), false)", boundary_filter()), &[&tx])
        .map(|row| row.get(0)).map_err(|error| error.to_string())
}
fn decode_records(rows: Vec<postgres::Row>) -> Result<Vec<EventRecord>, String> {
    rows.into_iter()
        .map(|row| {
            serde_json::from_str(&row.get::<_, String>(0)).map_err(|error| error.to_string())
        })
        .collect()
}
fn blocking_record_query(table: &str, completions: &str) -> String {
    format!(
        "
        SELECT later.event::text AS event FROM {table} later
        WHERE later.item_type = b.item_type AND later.item_id = b.item_id
        AND later.id > b.last_id AND later.tx <> ALL($2::text[])
        AND NOT EXISTS (
            SELECT 1 FROM {table} receipt JOIN {completions} finished
            ON finished.tx = receipt.event->'item'->>'undoTransactionId'
            WHERE finished.last_id = (SELECT MAX(id) FROM {table} WHERE tx = finished.tx)
            AND receipt.item_type = 'TransactionUndo' AND receipt.change_type = 'SET'
            AND receipt.event->'provenance'->>'undoOfTx' = receipt.event->'item'->>'transactionId'
            AND receipt.event->'provenance'->>'requestTx' = receipt.tx
            AND receipt.event->'item'->>'undoTransactionId' = receipt.tx
            AND receipt.event->'provenance'->>'actorUserId' = finished.user_id
            AND (receipt.event->'item'->>'transactionId' = later.tx
                OR receipt.event->'item'->>'undoTransactionId' = later.tx)
        ) ORDER BY later.id DESC LIMIT 1"
    )
}
impl PostgresHistoryStore {
    /// # Errors
    /// Returns errors reading the durable migration boundary.
    pub fn undo_boundary_exists(&self, id: &str) -> Result<bool, String> {
        let mut client = connect_pg_client(&self.config, "undo_boundary_exists")?;
        client
            .query_one(
                &format!(
                    "SELECT EXISTS (SELECT 1 FROM {} WHERE {} AND item_id = $1)",
                    qi(&self.config.table),
                    boundary_filter()
                ),
                &[&id],
            )
            .map(|row| row.get(0))
            .map_err(|error| error.to_string())
    }

    /// # Errors
    /// Returns an error when durable transaction existence cannot be checked.
    pub fn transaction_exists(&self, tx: &str) -> Result<bool, String> {
        let mut client = connect_pg_client(&self.config, "transaction_exists")?;
        client
            .query_one(
                &format!(
                    "SELECT EXISTS (SELECT 1 FROM {} WHERE tx = $1)",
                    qi(&self.config.table)
                ),
                &[&tx],
            )
            .map(|row| row.get(0))
            .map_err(|error| error.to_string())
    }

    /// # Errors
    /// Returns an error for unreadable, absent or malformed durable history.
    pub fn transaction_history(&self, tx: &str) -> Result<TransactionHistory, String> {
        self.transaction_history_excluding(tx, &[])
    }

    /// # Errors
    /// Returns snapshot/history errors while excluding a trusted newer undo range.
    pub fn transaction_history_excluding(
        &self,
        tx: &str,
        ignored: &[Arc<str>],
    ) -> Result<TransactionHistory, String> {
        let mut client = connect_pg_client(&self.config, "transaction_history")?;
        let mut snapshot = client.transaction().map_err(|error| error.to_string())?;
        snapshot
            .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .map_err(|error| error.to_string())?;
        let table = qi(&self.config.table);
        let completions = qi(&format!("{}_transactions", self.config.table));
        let rows = snapshot
            .query(
                &format!("SELECT event::text FROM {table} WHERE tx = $1 ORDER BY id"),
                &[&tx],
            )
            .map_err(|error| error.to_string())?;
        let records = decode_records(rows)?;
        if records.is_empty() {
            return Err("transaction was not found".into());
        }
        let ignored: Vec<&str> = ignored.iter().map(AsRef::as_ref).collect();
        let conflict = blocking_record_query(&table, &completions);
        let rows = snapshot.query(&format!("
            WITH bounds AS (
                SELECT item_type, item_id, MIN(id) first_id, MAX(id) last_id
                FROM {table} WHERE tx = $1 GROUP BY item_type, item_id
            )
            SELECT b.item_type, b.item_id, b.last_id, previous.event::text, final.event::text, latest.id,
                blocking.event IS NOT NULL, blocking.event
            FROM bounds b
            JOIN {table} final ON final.id = b.last_id
            LEFT JOIN LATERAL (
                SELECT event FROM {table} e WHERE e.item_type = b.item_type
                AND e.item_id = b.item_id AND e.id < b.first_id ORDER BY e.id DESC LIMIT 1
            ) previous ON TRUE
            JOIN LATERAL (
                SELECT id FROM {table} e WHERE e.item_type = b.item_type
                AND e.item_id = b.item_id ORDER BY id DESC LIMIT 1
            ) latest ON TRUE
            LEFT JOIN LATERAL ({conflict}) blocking ON TRUE
            ORDER BY b.item_type, b.item_id"), &[&tx, &ignored])
            .map_err(|error| error.to_string())?;
        let mut changes = Vec::with_capacity(rows.len());
        for row in rows {
            let before: Option<String> = row.get(3);
            let last: String = row.get(4);
            changes.push(TransactionChange {
                key: HistoryEntityKey::new(row.get::<_, String>(0), row.get::<_, String>(1)),
                before: before
                    .map(|value| serde_json::from_str(&value))
                    .transpose()
                    .map_err(|error| error.to_string())?,
                last: serde_json::from_str(&last).map_err(|error| error.to_string())?,
                last_id: row.get(2),
                latest_id: row.get(5),
                has_conflict: row.get(6),
                blocking_record: row
                    .get::<_, Option<String>>(7)
                    .map(|value| serde_json::from_str(&value))
                    .transpose()
                    .map_err(|error| error.to_string())?,
            });
        }
        let undone: bool = snapshot
            .query_one(
                &format!(
                    "SELECT EXISTS (
            SELECT 1 FROM {table} receipt JOIN {completions} finished
            ON finished.tx = receipt.event->'item'->>'undoTransactionId'
            WHERE receipt.item_type = 'TransactionUndo' AND receipt.change_type = 'SET'
                AND receipt.event->'provenance'->>'undoOfTx' = receipt.event->'item'->>'transactionId'
                AND receipt.event->'provenance'->>'requestTx' = receipt.tx
                AND receipt.event->'item'->>'undoTransactionId' = receipt.tx
                AND receipt.event->'provenance'->>'actorUserId' = finished.user_id
            AND receipt.event->'item'->>'transactionId' = $1
            AND finished.last_id = (SELECT MAX(id) FROM {table} WHERE tx = finished.tx))"
                ),
                &[&tx],
            )
            .map_err(|error| error.to_string())?
            .get(0);
        let complete: bool = snapshot.query_one(&format!("SELECT EXISTS (
            SELECT 1 FROM {completions} c WHERE c.tx = $1 AND c.last_id = (SELECT MAX(id) FROM {table} WHERE tx = $1))"), &[&tx])
            .map_err(|error| error.to_string())?.get(0);
        let predates_undo_boundary = predates_boundary(&mut snapshot, &table, tx)?;
        snapshot.commit().map_err(|error| error.to_string())?;
        Ok(TransactionHistory {
            transaction_id: tx.into(),
            records,
            changes,
            already_undone: undone,
            complete,
            predates_undo_boundary,
        })
    }

    /// # Errors
    /// Returns an error when user history cannot be read.
    pub fn latest_user_transaction(
        &self,
        user: &myko::identity::UserId,
    ) -> Result<Option<Arc<str>>, String> {
        Ok(self
            .user_transaction_page(user, None, 1)?
            .first()
            .map(|row| row.transaction_id.clone()))
    }

    /// # Errors
    /// Returns an error for invalid page bounds or unreadable history.
    pub fn user_transaction_page(
        &self,
        user: &myko::identity::UserId,
        before_first_id: Option<i64>,
        limit: usize,
    ) -> Result<Vec<myko::server::UserTransaction>, String> {
        if !(1..=200).contains(&limit) {
            return Err("transaction page size must be between 1 and 200".into());
        }
        let limit = i64::try_from(limit).map_err(|error| error.to_string())?;
        let mut client = connect_pg_client(&self.config, "user_transaction_page")?;
        let table = qi(&self.config.table);
        let completions = qi(&format!("{}_transactions", self.config.table));
        let boundary = boundary_filter();
        let rows = client
            .query(
                &format!(
                    "
            SELECT c.tx, c.first_id FROM {completions} c
            WHERE c.first_id > COALESCE((SELECT MAX(id) FROM {table} WHERE {boundary}), 0)
            AND c.user_id = $1 AND ($2::bigint IS NULL OR c.first_id < $2) AND c.last_id = (SELECT MAX(id) FROM {table} WHERE tx = c.tx)
            AND NOT EXISTS (SELECT 1 FROM {table} e WHERE e.tx = c.tx AND e.event->'provenance'->>'undoOfTx' IS NOT NULL)
            AND NOT EXISTS (SELECT 1 FROM {table} u
                JOIN {completions} finished ON finished.tx = u.event->'item'->>'undoTransactionId'
                WHERE u.item_type = 'TransactionUndo' AND u.change_type = 'SET'
                AND u.event->'provenance'->>'undoOfTx' = u.event->'item'->>'transactionId'
                AND u.event->'provenance'->>'requestTx' = u.tx
                AND u.event->'item'->>'undoTransactionId' = u.tx
                AND u.event->'provenance'->>'actorUserId' = finished.user_id
                AND u.event->'item'->>'transactionId' = c.tx
                AND finished.last_id = (SELECT MAX(id) FROM {table} WHERE tx = finished.tx))
            ORDER BY c.first_id DESC LIMIT $3"
                ),
                &[&user.as_ref(), &before_first_id, &limit],
            )
            .map_err(|error| error.to_string())?;
        Ok(rows
            .iter()
            .map(|row| myko::server::UserTransaction {
                transaction_id: Arc::from(row.get::<_, String>(0)),
                first_id: row.get(1),
            })
            .collect())
    }
    /// # Errors
    /// Returns an error for connection, query or historical payload failures.
    pub fn entity_event_at(
        &self,
        key: &HistoryEntityKey,
        row_id: i64,
    ) -> Result<Option<myko::wire::MEvent>, String> {
        let mut client = connect_pg_client(&self.config, "entity_event_at")?;
        let row = client.query_opt(&format!(
            "SELECT event::text FROM {} WHERE item_type = $1 AND item_id = $2 AND id <= $3 ORDER BY id DESC LIMIT 1",
            qi(&self.config.table)), &[&key.item_type.as_ref(), &key.item_id.as_ref(), &row_id])
            .map_err(|error| error.to_string())?;
        row.map(|row| {
            serde_json::from_str(&row.get::<_, String>(0)).map_err(|error| error.to_string())
        })
        .transpose()
    }
}

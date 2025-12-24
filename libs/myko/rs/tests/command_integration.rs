//! Integration tests for command flow.
//!
//! These tests spin up a real server and verify end-to-end command execution:
//! - Command execution with proper response
//! - Command errors are propagated correctly
//! - Auto-generated Delete commands work
//! - Query updates reflect command changes

#![cfg(feature = "bench")]

use futures::StreamExt;
use myko_rs::{
    actors::command::command_manager::CommandManagerMsg,
    bench_entities::{
        BenchItem, DeleteBenchItem, DeleteBenchItemResult, DeleteBenchItems,
        DeleteBenchItemsResult, GetAllBenchItems, GetAllBenchItemsArgs,
    },
    command::WrappedCommand,
    event::{MEvent, MEventType},
    item::Eventable,
    server::{MykoServer, MykoServerArgs},
};
use std::{sync::Arc, time::Duration};
use tokio::time::timeout;

/// Helper to create a test server (no WebSocket binding needed for direct tests)
async fn create_test_server() -> Arc<MykoServer> {
    let server = MykoServer::init(MykoServerArgs {
        bind_addr: "127.0.0.1".to_string(),
        bind_path: "/".to_string(),
        bind_port: 0,       // Don't bind
        kafka_config: None, // In-memory mode
        public_host_address: "127.0.0.1".to_string(),
    })
    .await
    .expect("Failed to create MykoServer");

    // Register benchmark entities
    BenchItem::register(&server).expect("Failed to register BenchItem");

    // Initialize
    server.init_modules().expect("Failed to init");

    // Small delay for actors to initialize
    tokio::time::sleep(Duration::from_millis(100)).await;

    server
}

/// Helper to emit a BenchItem to the server
async fn emit_bench_item(server: &MykoServer, item: &BenchItem) {
    let managers = server.get_managers();
    let event = MEvent::from_item(item, MEventType::SET, uuid::Uuid::new_v4().to_string());

    use myko_rs::actors::event::{
        common::{PersistEvent, ProcessEventData},
        event_manager::EventManagerMsg,
    };
    use myko_rs::prelude::AnyItem;

    let parsed_item: Arc<dyn AnyItem> = Arc::new(item.clone());
    managers
        .event_manager
        .send_message(EventManagerMsg::ProcessEvent(ProcessEventData {
            event,
            persist: PersistEvent::NoPersist,
            parsed_item: Some(parsed_item),
        }))
        .expect("Failed to emit event");

    // Let event propagate
    tokio::time::sleep(Duration::from_millis(50)).await;
}

/// Helper to execute a command directly via the CommandManager
async fn execute_command<T: serde::Serialize + myko_rs::command::CommandId>(
    server: &MykoServer,
    cmd: &T,
) -> Result<serde_json::Value, myko_rs::command::CommandError> {
    let managers = server.get_managers();

    let tx = uuid::Uuid::new_v4().to_string();

    // Serialize the command and add tx field
    let mut command_value = serde_json::to_value(cmd).expect("Failed to serialize command");
    if let Some(obj) = command_value.as_object_mut() {
        obj.insert("tx".to_string(), serde_json::Value::String(tx));
    }

    let wrapped = WrappedCommand {
        command: command_value,
        command_id: cmd.command_id(),
    };

    let client_id: Arc<str> = "test-client".into();

    ractor::call!(
        managers.command_manager,
        CommandManagerMsg::Execute,
        wrapped,
        client_id
    )
    .expect("Failed to call command manager")
}

#[tokio::test]
async fn test_delete_command_success() {
    let server = create_test_server().await;

    // Create an item
    let item = BenchItem {
        id: "test-delete-1".into(),
        hash: uuid::Uuid::new_v4().to_string().into(),
        name: "Test Item".to_string(),
        category: "test".to_string(),
        value: 42,
    };
    emit_bench_item(&server, &item).await;

    // Delete the item
    let delete_cmd = DeleteBenchItem {
        id: "test-delete-1".into(),
    };
    let result = execute_command(&server, &delete_cmd).await;

    assert!(result.is_ok(), "Delete command should succeed");
    let result: DeleteBenchItemResult =
        serde_json::from_value(result.unwrap()).expect("Failed to parse result");
    assert!(result.deleted, "Delete should return deleted=true");
}

#[tokio::test]
async fn test_delete_command_not_found() {
    let server = create_test_server().await;

    // Try to delete non-existent item
    let delete_cmd = DeleteBenchItem {
        id: "non-existent-id".into(),
    };

    let result = execute_command(&server, &delete_cmd).await;

    assert!(result.is_err(), "Delete of non-existent item should fail");
    let err = result.unwrap_err();
    assert!(
        err.message.contains("not found"),
        "Error should mention 'not found': {}",
        err.message
    );
}

#[tokio::test]
async fn test_delete_multiple_items() {
    let server = create_test_server().await;

    // Create multiple items
    for i in 0..3 {
        let item = BenchItem {
            id: format!("multi-delete-{}", i).into(),
            hash: uuid::Uuid::new_v4().to_string().into(),
            name: format!("Item {}", i),
            category: "batch".to_string(),
            value: i,
        };
        emit_bench_item(&server, &item).await;
    }

    // Delete multiple items
    let delete_cmd = DeleteBenchItems {
        ids: vec![
            "multi-delete-0".into(),
            "multi-delete-1".into(),
            "multi-delete-2".into(),
        ],
    };

    let result = execute_command(&server, &delete_cmd).await;

    assert!(result.is_ok(), "Bulk delete command should succeed");
    let result: DeleteBenchItemsResult =
        serde_json::from_value(result.unwrap()).expect("Failed to parse result");
    assert_eq!(result.deleted_count, 3, "Should delete all 3 items");
}

#[tokio::test]
async fn test_delete_partial_batch() {
    let server = create_test_server().await;

    // Create only one item
    let item = BenchItem {
        id: "partial-batch-exists".into(),
        hash: uuid::Uuid::new_v4().to_string().into(),
        name: "Exists".to_string(),
        category: "partial".to_string(),
        value: 1,
    };
    emit_bench_item(&server, &item).await;

    // Try to delete mix of existing and non-existing
    let delete_cmd = DeleteBenchItems {
        ids: vec![
            "partial-batch-exists".into(),
            "partial-batch-missing-1".into(),
            "partial-batch-missing-2".into(),
        ],
    };

    let result = execute_command(&server, &delete_cmd).await;

    assert!(result.is_ok(), "Partial batch delete should succeed");
    let result: DeleteBenchItemsResult =
        serde_json::from_value(result.unwrap()).expect("Failed to parse result");
    assert_eq!(
        result.deleted_count, 1,
        "Should only delete the 1 existing item"
    );
}

#[tokio::test]
async fn test_query_reflects_delete() {
    use futures_signals::signal_map::MapDiff;
    use myko_rs::{
        actors::query::query_manager::QueryManagerMsg, api::query::WrappedQuery,
        utils::signal_stream::SignalMapStream,
    };

    let server = create_test_server().await;
    let managers = server.get_managers();

    // Create items
    for i in 0..3 {
        let item = BenchItem {
            id: format!("query-delete-{}", i).into(),
            hash: uuid::Uuid::new_v4().to_string().into(),
            name: format!("Query Item {}", i),
            category: "query-test".to_string(),
            value: i,
        };
        emit_bench_item(&server, &item).await;
    }

    // Start watching query
    let query = GetAllBenchItems::new();
    let wrapped = WrappedQuery {
        query: serde_json::to_value(&query).unwrap(),
        query_id: "GetAllBenchItems".into(),
        query_item_type: "BenchItem".into(),
    };

    let signal = ractor::call!(managers.query_manager, QueryManagerMsg::StartQuery, wrapped)
        .expect("Failed to start query");

    let mut stream = SignalMapStream::new(signal);

    // Get initial state
    let initial = timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("Query timeout")
        .expect("No query result");

    let initial_count = match initial {
        MapDiff::Replace { entries } => entries
            .iter()
            .filter(|(_, item)| {
                let val = item.to_value();
                val.get("category")
                    .and_then(|c| c.as_str())
                    .map(|c| c == "query-test")
                    .unwrap_or(false)
            })
            .count(),
        _ => panic!("Expected Replace for initial state"),
    };
    assert_eq!(initial_count, 3, "Should have 3 query-test items initially");

    // Delete one item
    let delete_cmd = DeleteBenchItem {
        id: "query-delete-1".into(),
    };
    let result = execute_command(&server, &delete_cmd).await;
    assert!(result.is_ok(), "Delete should succeed");

    // Wait for query update (should be a Remove)
    let updated = timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("Query update timeout")
        .expect("No query update");

    match updated {
        MapDiff::Remove { key } => {
            assert_eq!(
                key.as_ref(),
                "query-delete-1",
                "Should remove the deleted item"
            );
        }
        other => panic!("Expected Remove, got {:?}", other),
    }
}

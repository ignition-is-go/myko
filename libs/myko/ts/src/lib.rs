//! Minimal NAPI bindings for MykoClient.
//!
//! This crate provides thin wrappers around the FFI-friendly APIs in myko-rs.
//! All business logic (query state management, etc.) lives in myko-rs.
//! JSON serialization happens here at the FFI boundary.

use myko_rs::api::query::WrappedQuery;
use myko_rs::client::MykoClient;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::{bindgen_prelude::*, JsFunction, Result};
use serde_json::Value;
use std::sync::Arc;

#[macro_use]
extern crate napi_derive;

/// The Myko client exposed to JavaScript.
#[napi(js_name = "MykoClient")]
pub struct JsMykoClient {
    client: Arc<MykoClient>,
}

#[napi]
impl JsMykoClient {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self {
            client: Arc::new(MykoClient::new()),
        }
    }

    /// Set the server address to connect to
    #[napi]
    pub fn set_address(&self, address: Option<String>) {
        self.client.set_address(address);
    }

    /// Get current connection status as JSON
    #[napi]
    pub async fn get_connection_status(&self) -> String {
        let status = self.client.get_connection_status().await;
        serde_json::to_string(&status).unwrap_or_else(|_| r#"{"type":"disconnected"}"#.to_string())
    }

    /// Watch connection status changes.
    /// Callback receives JSON: `{"type":"connected","data":"ws://..."}` or `{"type":"disconnected"}`
    #[napi(ts_args_type = "callback: (err: null | Error, statusJson: string) => void")]
    pub fn on_connection_status(&self, callback: JsFunction) -> Result<()> {
        let tsfn: ThreadsafeFunction<String, ErrorStrategy::CalleeHandled> =
            callback.create_threadsafe_function(0, |ctx| Ok(vec![ctx.value]))?;

        self.client.watch_connection_status_callback(move |json| {
            tsfn.call(Ok(json), ThreadsafeFunctionCallMode::Blocking);
        });

        Ok(())
    }

    /// Watch a query and receive updates via callback.
    ///
    /// - `query_json`: JSON with `query`, `queryId`, `queryItemType`. tx/createdAt added here.
    /// - `callback`: Receives JSON array of current items on each update.
    #[napi(ts_args_type = "queryJson: string, callback: (err: null | Error, itemsJson: string) => void")]
    pub fn watch_query(&self, query_json: String, callback: JsFunction) -> Result<()> {
        let tsfn: ThreadsafeFunction<String, ErrorStrategy::CalleeHandled> =
            callback.create_threadsafe_function(0, |ctx| Ok(vec![ctx.value]))?;

        // Parse and add tx/createdAt at the NAPI boundary
        let mut query_value: Value = serde_json::from_str(&query_json)
            .map_err(|e| Error::from_reason(format!("Failed to parse query: {}", e)))?;

        let tx = uuid::Uuid::new_v4().to_string();
        let created_at = chrono::Utc::now().to_rfc3339();

        if let Some(query_obj) = query_value.get_mut("query").and_then(|q| q.as_object_mut()) {
            query_obj.insert("tx".to_string(), Value::String(tx));
            query_obj.insert("createdAt".to_string(), Value::String(created_at));
        }

        let wrapped: WrappedQuery = serde_json::from_value(query_value)
            .map_err(|e| Error::from_reason(format!("Failed to parse wrapped query: {}", e)))?;

        // Call Rust client - it returns Vec<Value>, we serialize to JSON here
        let _ = self.client.watch_query_callback(wrapped, move |items| {
            if let Ok(json) = serde_json::to_string(&items) {
                tsfn.call(Ok(json), ThreadsafeFunctionCallMode::Blocking);
            }
        });

        Ok(())
    }

    /// Send an event to the server.
    /// Returns empty string on success, error message on failure.
    #[napi]
    pub async fn send_event(&self, event_json: String) -> String {
        self.client.send_event_json(event_json).await
    }
}

impl Default for JsMykoClient {
    fn default() -> Self {
        Self::new()
    }
}

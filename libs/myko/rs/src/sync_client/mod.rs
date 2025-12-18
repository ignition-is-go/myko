//! Sync Client for myko-rs
//!
//! Provides synchronized timing values across distributed systems.
//! Connects to a sync server via WebSocket and streams value updates.

use std::collections::HashMap;
use std::sync::Arc;

use autosocket::{AutoReconnectSocket, SocketConnectionStatus};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, RwLock};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::Stream;
use tungstenite::Message;

/// Rate at which value updates are sent (Hz/fps)
pub type TickRate = f64;
/// The synchronized value
pub type Value = f64;
/// Rate of value change per second
pub type Rate = f64;

// ============================================================================
// DTOs - Protocol messages matching sync server
// ============================================================================

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RunnerInfo {
    pub value: Value,
    pub rate: Rate,
    pub tick_rate: TickRate,
}

impl Default for RunnerInfo {
    fn default() -> Self {
        Self {
            value: 0.0,
            rate: 1.0,
            tick_rate: 60.0,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct SubscribeValue {
    key: String,
    init_context: Option<RunnerInfo>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct SetValue {
    key: String,
    value: f64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct SetRate {
    key: String,
    rate: f64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct SetTickRate {
    key: String,
    tick_rate: TickRate,
}

#[derive(Serialize, Deserialize, Debug)]
struct CancelSubscription {
    key: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct DestroyValue {
    key: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "sync:request")]
enum SyncRequest {
    SubscribeValue(SubscribeValue),
    SetValue(SetValue),
    SetRate(SetRate),
    SetTickRate(SetTickRate),
    CancelSubscription(CancelSubscription),
    DestroyValue(DestroyValue),
}

/// Value update received from sync server
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ValueUpdated {
    pub key: String,
    pub value: f64,
    pub current_rate: f64,
    pub last_update: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "sync:response")]
enum SyncResponse {
    ValueUpdated(ValueUpdated),
    Dummy,
}

// ============================================================================
// SyncClient
// ============================================================================

/// Client for connecting to a sync server and receiving synchronized value updates.
///
/// # Example
///
/// ```ignore
/// let client = SyncClient::connect("ws://localhost:5150").await?;
///
/// // Subscribe to a value
/// let mut stream = client.watch_value("playback-123", None).await?;
///
/// // Receive updates
/// while let Some(update) = stream.next().await {
///     println!("Value: {}, Rate: {}", update.value, update.current_rate);
/// }
///
/// // Control the value
/// client.set_rate("playback-123", 1000.0).await?;
/// client.set_tick_rate("playback-123", 60.0).await?;
/// ```
pub struct SyncClient {
    ws: AutoReconnectSocket,
    subscriptions: Arc<RwLock<HashMap<String, broadcast::Sender<ValueUpdated>>>>,
    _message_handler: tokio::task::JoinHandle<()>,
}

impl SyncClient {
    /// Connect to a sync server at the given address.
    ///
    /// The address can be a WebSocket URL (ws://host:port) or just host:port.
    pub async fn connect(addr: &str) -> anyhow::Result<Self> {
        let ws = AutoReconnectSocket::new();
        ws.set_addr(Some(addr.to_string()));

        // Wait for connection
        let mut status_rx = ws.subscribe_status();
        loop {
            let status = status_rx.borrow_and_update().clone();
            match status {
                SocketConnectionStatus::Connected(_) => break,
                SocketConnectionStatus::Disconnected => {
                    // Wait for status change
                    status_rx.changed().await?;
                }
                SocketConnectionStatus::Connecting(_) => {
                    status_rx.changed().await?;
                }
            }
        }

        let subscriptions: Arc<RwLock<HashMap<String, broadcast::Sender<ValueUpdated>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Spawn message handler
        let subs_clone = subscriptions.clone();
        let mut incoming = ws.incoming.subscribe();

        let message_handler = tokio::spawn(async move {
            while let Ok(msg) = incoming.recv().await {
                let text = match msg.into_text() {
                    Ok(t) => t,
                    Err(_) => continue,
                };

                let response: SyncResponse = match serde_json::from_str(&text) {
                    Ok(r) => r,
                    Err(e) => {
                        log::warn!("Failed to parse sync response: {e}");
                        continue;
                    }
                };

                match response {
                    SyncResponse::ValueUpdated(update) => {
                        let subs = subs_clone.read().await;
                        if let Some(sender) = subs.get(&update.key) {
                            let _ = sender.send(update);
                        }
                    }
                    SyncResponse::Dummy => {}
                }
            }
        });

        Ok(Self {
            ws,
            subscriptions,
            _message_handler: message_handler,
        })
    }

    /// Subscribe to value updates for a key.
    ///
    /// Returns a stream of `ValueUpdated` messages.
    /// If `init` is provided, the value runner will be initialized with those settings.
    pub async fn watch_value(
        &self,
        key: &str,
        init: Option<RunnerInfo>,
    ) -> anyhow::Result<impl Stream<Item = ValueUpdated>> {
        use tokio_stream::StreamExt;

        // Create or get subscription channel
        let receiver = {
            let mut subs = self.subscriptions.write().await;
            let sender = subs
                .entry(key.to_string())
                .or_insert_with(|| broadcast::channel(100).0);
            sender.subscribe()
        };

        // Send subscribe request
        let request = SyncRequest::SubscribeValue(SubscribeValue {
            key: key.to_string(),
            init_context: init,
        });
        self.send_request(&request)?;

        Ok(BroadcastStream::new(receiver).filter_map(|x| x.ok()))
    }

    /// Set the rate (value change per second) for a key.
    pub async fn set_rate(&self, key: &str, rate: f64) -> anyhow::Result<()> {
        let request = SyncRequest::SetRate(SetRate {
            key: key.to_string(),
            rate,
        });
        self.send_request(&request)
    }

    /// Set the tick rate (update frequency in Hz) for a key.
    pub async fn set_tick_rate(&self, key: &str, tick_rate: f64) -> anyhow::Result<()> {
        let request = SyncRequest::SetTickRate(SetTickRate {
            key: key.to_string(),
            tick_rate,
        });
        self.send_request(&request)
    }

    /// Set the current value for a key.
    pub async fn set_value(&self, key: &str, value: f64) -> anyhow::Result<()> {
        let request = SyncRequest::SetValue(SetValue {
            key: key.to_string(),
            value,
        });
        self.send_request(&request)
    }

    /// Cancel subscription to a key (stops receiving updates but doesn't destroy the value).
    pub async fn cancel_subscription(&self, key: &str) -> anyhow::Result<()> {
        // Remove from local subscriptions
        {
            let mut subs = self.subscriptions.write().await;
            subs.remove(key);
        }

        let request = SyncRequest::CancelSubscription(CancelSubscription {
            key: key.to_string(),
        });
        self.send_request(&request)
    }

    /// Destroy a value on the server (stops the runner entirely).
    pub async fn destroy_value(&self, key: &str) -> anyhow::Result<()> {
        // Remove from local subscriptions
        {
            let mut subs = self.subscriptions.write().await;
            subs.remove(key);
        }

        let request = SyncRequest::DestroyValue(DestroyValue {
            key: key.to_string(),
        });
        self.send_request(&request)
    }

    /// Check if connected to the sync server.
    pub fn is_connected(&self) -> bool {
        matches!(self.ws.get_status(), SocketConnectionStatus::Connected(_))
    }

    /// Get the current connection status.
    pub fn get_status(&self) -> SocketConnectionStatus {
        self.ws.get_status()
    }

    /// Close the connection to the sync server.
    pub fn close(&self) {
        self.ws.close();
    }

    fn send_request(&self, request: &SyncRequest) -> anyhow::Result<()> {
        let json = serde_json::to_string(request)?;
        self.ws
            .outgoing
            .send(Message::Text(json))
            .map_err(|e| anyhow::anyhow!("Failed to send request: {e}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_subscribe() {
        let req = SyncRequest::SubscribeValue(SubscribeValue {
            key: "test-key".to_string(),
            init_context: Some(RunnerInfo {
                value: 0.0,
                rate: 1000.0,
                tick_rate: 60.0,
            }),
        });
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("sync:request"));
        assert!(json.contains("SubscribeValue"));
        assert!(json.contains("test-key"));
    }

    #[test]
    fn test_deserialize_value_updated() {
        let json = r#"{"sync:response":"ValueUpdated","key":"test","value":123.5,"currentRate":1000.0,"lastUpdate":"2024-01-01T00:00:00Z"}"#;
        let response: SyncResponse = serde_json::from_str(json).unwrap();
        match response {
            SyncResponse::ValueUpdated(update) => {
                assert_eq!(update.key, "test");
                assert_eq!(update.value, 123.5);
                assert_eq!(update.current_rate, 1000.0);
            }
            _ => panic!("Expected ValueUpdated"),
        }
    }
}

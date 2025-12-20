//! Subscription stream for query subscriptions.
//!
//! Uses CancellationToken for cleanup - when the token fires, a background task
//! sends CancelQuery to stop the subscription. This works reliably even inside
//! async_stream generators (which don't properly drop locals).

use std::{collections::BTreeMap, pin::Pin, sync::Arc};

use futures::Stream;
use log::error;
use tokio_util::sync::CancellationToken;

use crate::{prelude::AnyItem, runtime::ActorRef};

use super::{common::QueryStreamUpdate, query_manager::QueryManagerMsg};

/// Create a query subscription stream that automatically cleans up when the token is cancelled.
///
/// When `cancel_token` is cancelled:
/// 1. A background task sends CancelQuery to the QueryManager
/// 2. The stream's select! loop detects cancellation and exits
///
/// This is the only cleanup mechanism - no RAII guards needed.
pub fn create_subscription_stream<T>(
    tx: Arc<str>,
    receiver: flume::Receiver<QueryStreamUpdate>,
    query_manager: ActorRef<QueryManagerMsg>,
    cancel_token: CancellationToken,
    tokio_handle: tokio::runtime::Handle,
) -> Pin<Box<dyn Stream<Item = Vec<T>> + Send>>
where
    T: serde::de::DeserializeOwned + Clone + Send + 'static,
{
    // Check if already cancelled - return empty stream immediately
    if cancel_token.is_cancelled() {
        log::debug!("Token already cancelled, skipping subscription tx={}", tx);
        return Box::pin(futures::stream::empty());
    }

    // Clone token for the stream
    let stream_token = cancel_token.clone();

    // Spawn background task to cancel subscription when token fires
    let tx_for_cancel = tx.clone();
    let qm_for_cancel = query_manager.clone();
    tokio_handle.spawn(async move {
        cancel_token.cancelled().await;
        log::debug!(
            "CancellationToken fired, cancelling subscription tx={}",
            tx_for_cancel
        );
        let _ = qm_for_cancel.send_message(QueryManagerMsg::CancelQuery(tx_for_cancel));
    });

    // Create the stream that processes updates
    let inner_stream = async_stream::stream! {
        let mut state: BTreeMap<Arc<str>, Arc<dyn AnyItem>> = BTreeMap::new();

        loop {
            tokio::select! {
                biased;

                _ = stream_token.cancelled() => {
                    log::debug!("Subscription stream cancelled via token, tx={}", tx);
                    break;
                }

                result = receiver.recv_async() => {
                    match result {
                        Ok(update) => {
                            match update {
                                QueryStreamUpdate::Initial(entries) => {
                                    state = entries;
                                }
                                QueryStreamUpdate::Upsert(key, value) => {
                                    state.insert(key, value);
                                }
                                QueryStreamUpdate::Remove(key) => {
                                    state.remove(&key);
                                }
                                QueryStreamUpdate::Error(msg) => {
                                    error!("Query subscription error: {}", msg);
                                    break;
                                }
                            }

                            // Convert state to Vec<T>
                            let items: Vec<T> = state
                                .values()
                                .filter_map(|item| serde_json::from_value(item.to_value()).ok())
                                .collect();

                            yield items;
                        }
                        Err(_) => {
                            // Channel closed
                            break;
                        }
                    }
                }
            }
        }
    };

    Box::pin(inner_stream)
}

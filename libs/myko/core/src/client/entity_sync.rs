use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use hyphae::{JoinExt, MapExt, Materialize, Signal, SubscriptionGuard, Watchable};
use serde::de::DeserializeOwned;
use tracing::{debug, error};

use crate::{
    client::MykoClient,
    common::with_id::WithId,
    core::item::Eventable,
    query::{QueryFactory, QueryHandler, QueryParams},
    server::MykoServerContext,
    wire::{MEvent, MEventType},
};

#[derive(Debug, Clone, Copy)]
pub struct EntityStoreSyncOptions {
    pub delete_stale_remote: bool,
}

impl Default for EntityStoreSyncOptions {
    fn default() -> Self {
        Self {
            delete_stale_remote: true,
        }
    }
}

type ItemComparator<T> = Arc<dyn Fn(&T, &T) -> bool + Send + Sync>;

pub struct EntityStoreSyncConfig<T, QLocal, QRemote>
where
    T: Eventable + WithId + Clone,
    QLocal: QueryFactory + QueryHandler + QueryParams<Item = T> + Clone + Send + Sync + 'static,
    QRemote: QueryParams<Item = T> + Clone + Send + Sync + 'static,
{
    pub client: MykoClient,
    pub local_ctx: Arc<MykoServerContext>,
    pub local_query: QLocal,
    pub remote_query: QRemote,
    pub options: EntityStoreSyncOptions,
    pub items_equal: ItemComparator<T>,
}

/// Generic reconciler that keeps a local authoritative entity set in sync with
/// the remote result of a query by emitting SET/DEL events through `MykoClient`.
pub struct EntityStoreSync<T>
where
    T: Eventable + WithId + Clone + Send + Sync + 'static,
{
    _sync_guard: SubscriptionGuard,
    _marker: PhantomData<T>,
}

impl<T> EntityStoreSync<T>
where
    T: Eventable + WithId + Clone + Send + Sync + 'static,
{
    fn index_items(items: &[Arc<T>]) -> HashMap<String, Arc<T>> {
        items
            .iter()
            .cloned()
            .map(|item| (item.id().to_string(), item))
            .collect()
    }

    fn sync_once(
        client: &MykoClient,
        local_items: &[Arc<T>],
        remote_items: &[Arc<T>],
        options: EntityStoreSyncOptions,
        items_equal: &(dyn Fn(&T, &T) -> bool + Send + Sync),
    ) {
        let local = Self::index_items(local_items);
        let remote = Self::index_items(remote_items);
        let events = Self::reconcile(&local, &remote, options, items_equal);
        if events.is_empty() {
            return;
        }

        debug!(
            "EntityStoreSync local={} remote={} events={}",
            local.len(),
            remote.len(),
            events.len()
        );
        if let Err(err) = client.send_event_batch(events) {
            error!("EntityStoreSync send failed: {}", err);
        }
    }

    pub fn reconcile(
        local: &HashMap<String, Arc<T>>,
        remote: &HashMap<String, Arc<T>>,
        options: EntityStoreSyncOptions,
        items_equal: &(dyn Fn(&T, &T) -> bool + Send + Sync),
    ) -> Vec<MEvent> {
        let mut events = Vec::new();

        for (id, local_item) in local {
            let should_set = remote.get(id).is_none_or(|remote_item| {
                !items_equal(local_item.as_ref(), remote_item.as_ref())
            });
            if should_set {
                events.push(MEvent::from_item(local_item.as_ref(), MEventType::SET, ""));
            }
        }

        if options.delete_stale_remote {
            for (id, remote_item) in remote {
                if !local.contains_key(id) {
                    events.push(MEvent::del(remote_item.as_ref(), ""));
                }
            }
        }

        events
    }

    pub fn new<QLocal, QRemote>(config: EntityStoreSyncConfig<T, QLocal, QRemote>) -> Arc<Self>
    where
        T: DeserializeOwned + std::fmt::Debug,
        QLocal: QueryFactory + QueryHandler + QueryParams<Item = T> + Clone + Send + Sync + 'static,
        QRemote: QueryParams<Item = T> + Clone + Send + Sync + 'static,
    {
        let EntityStoreSyncConfig {
            client,
            local_ctx,
            local_query,
            remote_query,
            options,
            items_equal,
        } = config;
        let local_cell = local_ctx
            .query_map_by_str(local_query, local_ctx.new_server_transaction())
            .entries()
            .materialize()
            .map(|entries: &Vec<(Arc<str>, Arc<T>)>| {
                entries
                    .iter()
                    .map(|(_, item)| item.clone())
                    .collect::<Vec<_>>()
            })
            .materialize();
        let remote_cell = client.watch_query(remote_query);
        let joined = local_cell.join(remote_cell).materialize();
        let sync_client = client;
        let options_for_join = options;
        let items_equal_for_join = items_equal.clone();
        let joined_guard = joined.subscribe(move |signal| {
            if let Signal::Value(value) = signal {
                let (local_items, remote_items) = &**value;
                Self::sync_once(
                    &sync_client,
                    local_items,
                    remote_items,
                    options_for_join,
                    items_equal_for_join.as_ref(),
                );
            }
        });

        Arc::new(Self {
            _sync_guard: joined_guard,
            _marker: PhantomData,
        })
    }
}

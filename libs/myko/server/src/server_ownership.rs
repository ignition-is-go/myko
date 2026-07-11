use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use myko::{
    entities::server::{GetAllServers, ServerId},
    relationship::iter_server_owned_registrations,
    server::{CellServerCtx, PersistError},
};

pub struct ServerOwnershipManager;

impl ServerOwnershipManager {
    /// Get IDs of all currently live servers.
    fn live_server_ids(ctx: &CellServerCtx) -> Vec<ServerId> {
        use hyphae::Gettable;
        let req = ctx.new_server_transaction();
        ctx.query_map(GetAllServers {}, req)
            .items()
            .get()
            .iter()
            .map(|s| s.id.clone())
            .collect()
    }

    /// Count how many server_owned items each server currently owns
    /// across ALL registered #[server_owned] types.
    fn count_distribution(ctx: &CellServerCtx) -> HashMap<Arc<str>, usize> {
        let mut counts: HashMap<Arc<str>, usize> = HashMap::new();

        for reg in iter_server_owned_registrations() {
            let store = ctx.registry.get_or_create(reg.entity_type);
            for (_, item) in store.snapshot() {
                if let Some(owner) = item.server_owner() {
                    *counts.entry(Arc::from(owner)).or_default() += 1;
                }
            }
        }
        counts
    }

    /// Pick the server with the lowest item count from the given set.
    fn least_loaded(live_ids: &[ServerId], counts: &HashMap<Arc<str>, usize>) -> Option<ServerId> {
        live_ids
            .iter()
            .min_by_key(|id| counts.get(id.0.as_ref()).copied().unwrap_or(0))
            .cloned()
    }

    /// Scan all server_owned items and reassign any referencing dead/empty servers.
    pub fn claim_orphaned(ctx: &CellServerCtx) -> Result<(), PersistError> {
        let live_ids = Self::live_server_ids(ctx);
        if live_ids.is_empty() {
            tracing::warn!("[ServerOwnership] No live servers found, skipping orphan claim");
            return Ok(());
        }

        let live_set: HashSet<&str> = live_ids.iter().map(|id| id.0.as_ref()).collect();
        let mut counts = Self::count_distribution(ctx);
        counts.retain(|k, _| live_set.contains(k.as_ref()));

        let mut reassigned = 0usize;

        for reg in iter_server_owned_registrations() {
            let store = ctx.registry.get_or_create(reg.entity_type);
            let items: Vec<_> = store.snapshot().into_iter().map(|(_, item)| item).collect();

            for item in &items {
                let current_owner = item.server_owner().unwrap_or("");

                if !current_owner.is_empty() && live_set.contains(current_owner) {
                    continue; // healthy
                }

                let Some(new_owner) = Self::least_loaded(&live_ids, &counts) else {
                    continue;
                };

                if let Some(patched) = item.bake_server_owner(&new_owner.0) {
                    // Server-ownership rebakes are Local (per the event-bus design):
                    // re-emit the item normally rather than suppressing relationships.
                    ctx.set_dyn(patched)?;
                    *counts.entry(new_owner.0.clone()).or_default() += 1;
                    reassigned += 1;
                }
            }
        }

        if reassigned > 0 {
            tracing::info!(
                "[ServerOwnership] Reassigned {} orphaned item(s)",
                reassigned
            );
        }
        Ok(())
    }

    /// Watch for Server entity removals and redistribute orphaned items.
    /// Returns a SubscriptionGuard that must be kept alive.
    pub fn watch_peer_deaths(ctx: &CellServerCtx) -> hyphae::SubscriptionGuard {
        use hyphae::{Gettable, Signal, Watchable};

        let req = ctx.new_server_transaction();
        let servers_cell = ctx.query_map(GetAllServers {}, req).items();

        let prev_ids: std::sync::Mutex<HashSet<Arc<str>>> =
            std::sync::Mutex::new(servers_cell.get().iter().map(|s| s.id.0.clone()).collect());

        let ctx = ctx.clone();
        servers_cell.subscribe(move |signal| {
            let Signal::Value(servers) = signal else {
                return;
            };

            let current_ids: HashSet<Arc<str>> = servers.iter().map(|s| s.id.0.clone()).collect();

            let mut prev = prev_ids.lock().unwrap();
            let removed: Vec<Arc<str>> = prev.difference(&current_ids).cloned().collect();
            *prev = current_ids;
            drop(prev);

            if removed.is_empty() {
                return;
            }

            for id in &removed {
                tracing::warn!(
                    "[ServerOwnership] Server {} left cluster, redistributing",
                    id
                );
            }

            if let Err(e) = ServerOwnershipManager::claim_orphaned(&ctx) {
                tracing::error!("[ServerOwnership] Failed to redistribute: {}", e);
            }
        })
    }
}

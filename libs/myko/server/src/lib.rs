//! Myko server runtime — WebSocket, durable event backends, peer federation.
//!
//! This crate contains the tokio-dependent parts of the Myko server:
//! - `CellServer` — server lifecycle (durable catch-up init, WS accept loop)
//! - `postgres` — PostgreSQL producer/consumer (event-table + LISTEN/NOTIFY)
//! - `ws_handler` — WebSocket connection handling
//! - `peer_registry` — federation with other servers
//! - `mcp` — Model Context Protocol server
//!
//! Tokio-free server types (CellServerCtx, HandlerRegistry, etc.) live in `myko::server`.

pub mod mcp;
pub mod peer_persister;
pub mod peer_registry;
pub mod postgres;
pub mod server_ownership;
pub mod ws_handler;
pub mod ws_timing;

// Re-export all tokio-free server types from myko
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures_util::StreamExt;
pub use myko::server::*;
use myko::{
    client::MykoClient, command::CommandContext, request::RequestContext, saga::SagaRegistration,
    search::SearchIndex, store::StoreRegistry, wire::MEvent,
};
pub use peer_persister::PeerPersister;
pub use server_ownership::ServerOwnershipManager;
use uuid::Uuid;

use crate::postgres::{
    CellPostgresConsumer, CellPostgresProducer, PostgresConfig, PostgresHistoryReplayProvider,
    PostgresHistoryStore, PostgresProducerHandle,
};

/// Cell-based Myko server configuration.
#[derive(Clone)]
pub struct CellServerConfig {
    /// Address to bind the WebSocket server
    pub bind_addr: SocketAddr,
    /// Optional Postgres configuration for event persistence/distribution
    pub postgres: Option<PostgresConfig>,
    /// Server host ID (auto-generated if not provided)
    pub host_id: Option<Uuid>,
    /// Optional peer registry configuration for federation
    pub peer_registry: Option<peer_registry::PeerRegistryConfig>,
    /// Default persister override
    pub default_persister: Option<Arc<dyn Persister>>,
    /// Per-entity persister overrides keyed by entity type name
    pub persister_overrides: HashMap<String, Arc<dyn Persister>>,
    /// Optional pre-constructed peer-client map. When provided, it will be
    /// used as-is (so any `PeerPersister` built against the same `Arc`
    /// shares the live map). If `None`, the server creates its own.
    pub peer_clients: Option<Arc<dashmap::DashMap<Arc<str>, Arc<MykoClient>>>>,
}

/// Builder for creating a CellServer.
#[derive(Default)]
pub struct CellServerBuilder {
    bind_addr: Option<SocketAddr>,
    host_id: Option<Uuid>,
    postgres: Option<PostgresConfig>,
    peer_registry: Option<peer_registry::PeerRegistryConfig>,
    default_persister: Option<Arc<dyn Persister>>,
    persister_overrides: HashMap<String, Arc<dyn Persister>>,
    /// Optional pre-constructed peer-client map — useful when a
    /// `PeerPersister` must reference the same map the server will use.
    /// Defaults to a fresh empty map if not provided.
    peer_clients: Option<Arc<dashmap::DashMap<Arc<str>, Arc<MykoClient>>>>,
    after_init: Option<AfterInitCallback>,
}

type AfterInitCallback = Box<dyn FnOnce(&CellServer) + Send>;

impl CellServerBuilder {
    /// Create a new server builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the WebSocket bind address.
    pub fn with_bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = Some(addr);
        self
    }

    /// Set the server host ID (auto-generated if not set).
    pub fn with_host_id(mut self, id: Uuid) -> Self {
        self.host_id = Some(id);
        self
    }

    /// Configure Postgres for event persistence/distribution.
    pub fn with_postgres(mut self, config: PostgresConfig) -> Self {
        self.postgres = Some(config);
        self
    }

    /// Configure peer registry for federation.
    pub fn with_peer_registry(mut self, config: peer_registry::PeerRegistryConfig) -> Self {
        self.peer_registry = Some(config);
        self
    }

    /// Set the default persister used for all entity types without explicit overrides.
    pub fn with_default_persister(mut self, persister: Arc<dyn Persister>) -> Self {
        self.default_persister = Some(persister);
        self
    }

    /// Override persister for a specific entity type (e.g. "Pulse").
    pub fn with_persister_override(
        mut self,
        entity_type: impl Into<String>,
        persister: Arc<dyn Persister>,
    ) -> Self {
        self.persister_overrides
            .insert(entity_type.into(), persister);
        self
    }

    /// Provide a pre-constructed peer-client map. The server's peer
    /// registry will populate it as peers connect. Pass the same `Arc`
    /// into `PeerPersister::new(...)` when you register a
    /// `with_persister_override(..., PeerPersister)` so the persister
    /// shares the live map.
    pub fn with_peer_clients(
        mut self,
        peer_clients: Arc<dashmap::DashMap<Arc<str>, Arc<MykoClient>>>,
    ) -> Self {
        self.peer_clients = Some(peer_clients);
        self
    }

    /// Register a callback to run after initialization and relation establishment,
    /// but before the WebSocket accept loop starts. Use this for starting subsystems
    /// that need entity data (e.g., scene engine).
    pub fn after_init(mut self, f: impl FnOnce(&CellServer) + Send + 'static) -> Self {
        self.after_init = Some(Box::new(f));
        self
    }

    /// Build the server.
    pub fn build(self) -> CellServer {
        let bind_addr = self
            .bind_addr
            .unwrap_or_else(|| "127.0.0.1:5155".parse().unwrap());

        let mut server = CellServer::new(CellServerConfig {
            bind_addr,
            postgres: self.postgres,
            host_id: self.host_id,
            peer_registry: self.peer_registry,
            default_persister: self.default_persister,
            persister_overrides: self.persister_overrides,
            peer_clients: self.peer_clients,
        });
        server.after_init = std::sync::Mutex::new(self.after_init);
        server
    }
}

/// Cell-based Myko server.
///
/// Uses hyphae cells for reactive queries and reports instead of actors.
pub struct CellServer {
    /// Central entity store registry
    pub registry: Arc<StoreRegistry>,
    /// Handler registry for items, queries, and reports
    pub handler_registry: Arc<HandlerRegistry>,
    /// Relationship manager for cascade operations
    pub relationship_manager: Arc<RelationshipManager>,
    /// Optional Postgres producer handle
    pub postgres_producer: Option<PostgresProducerHandle>,
    /// Full-text search index
    pub search_index: Arc<SearchIndex>,
    /// Persister routing (default + per-entity overrides)
    pub persisters: Arc<PersisterRouter>,
    /// Server host ID
    pub host_id: Uuid,
    /// Server configuration
    config: CellServerConfig,
    /// Postgres producer (kept alive)
    _postgres_producer_owner: Option<CellPostgresProducer>,
    /// Postgres consumer (kept alive)
    postgres_consumer: Option<CellPostgresConsumer>,
    /// Whether the server is ready to accept connections
    ready: Arc<AtomicBool>,
    /// Peer registry for federation (initialized after catch-up)
    peer_registry_instance: RwLock<Option<peer_registry::PeerRegistry>>,
    /// Live peer clients shared with report context.
    peer_clients: Arc<dashmap::DashMap<Arc<str>, Arc<MykoClient>>>,
    /// Callback to run after init (catch-up + relations) but before WS loop
    after_init: std::sync::Mutex<Option<AfterInitCallback>>,
    /// Sender for local+replicated event fan-out to saga runtime.
    saga_event_tx: flume::Sender<MEvent>,
    /// Receiver consumed when saga runtime starts.
    saga_event_rx: std::sync::Mutex<Option<flume::Receiver<MEvent>>>,
    /// Saga tasks kept alive for server lifetime.
    saga_tasks: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// Server ownership death-watch guard (kept alive for server lifetime).
    _server_ownership_guard: std::sync::Mutex<Option<hyphae::SubscriptionGuard>>,
    /// Hyphae cell inspector server (kept alive for the lifetime of the server)
    #[cfg(feature = "inspector")]
    _inspector: hyphae::server::InspectorServer,
}

impl CellServer {
    /// Create a new server builder.
    pub fn builder() -> CellServerBuilder {
        CellServerBuilder::new()
    }

    /// Create a new cell-based server.
    pub fn new(config: CellServerConfig) -> Self {
        let host_id = config.host_id.unwrap_or_else(Uuid::new_v4);
        let registry = Arc::new(StoreRegistry::new());
        let handler_registry = Arc::new(HandlerRegistry::new());
        let relationship_manager = Arc::new(RelationshipManager::new());

        // Initialize the client registry for WebSocket client message dispatch
        init_client_registry();

        let (saga_event_tx, saga_event_rx) = flume::unbounded::<MEvent>();
        let (postgres_producer_owner, postgres_producer, postgres_consumer) =
            if let Some(ref postgres_config) = config.postgres {
                match CellPostgresProducer::new(postgres_config, host_id) {
                    Ok(producer) => {
                        let handle = producer.handle();
                        let consumer = match CellPostgresConsumer::start(
                            postgres_config,
                            host_id,
                            handler_registry.clone(),
                            registry.clone(),
                        ) {
                            Ok(c) => Some(c),
                            Err(e) => {
                                log::error!("Failed to start Postgres consumer: {}", e);
                                None
                            }
                        };
                        (Some(producer), Some(handle), consumer)
                    }
                    Err(e) => {
                        log::error!("Failed to create Postgres producer: {}", e);
                        (None, None, None)
                    }
                }
            } else {
                (None, None, None)
            };

        // If no durable consumer, server is immediately ready
        let ready = Arc::new(AtomicBool::new(postgres_consumer.is_none()));

        // Initialize full-text search index
        let search_index = Arc::new(SearchIndex::new());

        // Build persister routing:
        // - explicit default from config if provided
        // - otherwise Postgres producer handle when available
        // - explicit per-entity overrides always win
        let mut persister_router = PersisterRouter::default();
        if let Some(default_persister) = config.default_persister.clone() {
            persister_router.set_default(Some(default_persister));
        } else if let Some(handle) = postgres_producer.clone() {
            persister_router.set_default(Some(Arc::new(handle) as Arc<dyn Persister>));
        }
        for (entity_type, persister) in &config.persister_overrides {
            persister_router.set_override(entity_type.clone(), persister.clone());
        }
        let persisters = Arc::new(persister_router);

        // Start the hyphae cell inspector server
        #[cfg(feature = "inspector")]
        let inspector = hyphae::server::start_server("myko");
        #[cfg(feature = "inspector")]
        log::info!("Hyphae inspector on port {}", inspector.port());

        let peer_clients = config
            .peer_clients
            .clone()
            .unwrap_or_else(|| Arc::new(dashmap::DashMap::new()));

        Self {
            registry,
            handler_registry,
            relationship_manager,
            postgres_producer,
            search_index,
            persisters,
            host_id,
            config,
            _postgres_producer_owner: postgres_producer_owner,
            postgres_consumer,
            ready,
            peer_registry_instance: RwLock::new(None),
            peer_clients,
            after_init: std::sync::Mutex::new(None),
            saga_event_tx,
            saga_event_rx: std::sync::Mutex::new(Some(saga_event_rx)),
            saga_tasks: std::sync::Mutex::new(Vec::new()),
            _server_ownership_guard: std::sync::Mutex::new(None),
            #[cfg(feature = "inspector")]
            _inspector: inspector,
        }
    }

    /// Start the peer registry for federation.
    pub fn start_peer_registry(&self, config: Option<peer_registry::PeerRegistryConfig>) {
        let peer_config = config.or_else(|| self.config.peer_registry.clone());

        if let Some(peer_config) = peer_config {
            log::info!("Starting peer registry");
            let pr = peer_registry::PeerRegistry::new(self.ctx(), peer_config);
            *self.peer_registry_instance.write().unwrap() = Some(pr);
        }
    }

    /// Check if peer registry is running.
    pub fn has_peer_registry(&self) -> bool {
        self.peer_registry_instance.read().unwrap().is_some()
    }

    /// Get the store registry.
    pub fn registry(&self) -> Arc<StoreRegistry> {
        self.registry.clone()
    }

    /// Get the handler registry.
    pub fn handler_registry(&self) -> Arc<HandlerRegistry> {
        self.handler_registry.clone()
    }

    /// Get a server context for module use.
    pub fn ctx(&self) -> CellServerCtx {
        let history_replay: Option<Arc<dyn myko::server::HistoryReplayProvider>> =
            self.config.postgres.as_ref().map(|pg| {
                Arc::new(PostgresHistoryReplayProvider::new(pg.clone()))
                    as Arc<dyn myko::server::HistoryReplayProvider>
            });
        CellServerCtx::new(
            self.host_id,
            self.registry.clone(),
            self.handler_registry.clone(),
            self.relationship_manager.clone(),
            self.persisters.clone(),
            self.search_index.clone(),
            self.peer_clients.clone(),
            Some(self.saga_event_tx.clone()),
            history_replay,
        )
    }

    fn start_saga_runtime(&self) {
        let registrations: Vec<_> = inventory::iter::<SagaRegistration>().collect();
        if registrations.is_empty() {
            return;
        }
        let Some(rx) = self
            .saga_event_rx
            .lock()
            .expect("saga_event_rx mutex poisoned")
            .take()
        else {
            return;
        };

        log::info!("Starting saga runtime with {} saga(s)", registrations.len());

        // NOTE(ts): One unbounded flume channel per saga, with dispatch-side filtering
        // so sagas only receive events matching their entity type and change type.
        struct SagaChannel {
            tx: flume::Sender<MEvent>,
            entity_type: &'static str,
            change_type: myko::event::MEventType,
        }
        let mut saga_channels: Vec<SagaChannel> = Vec::new();

        for registration in registrations {
            let saga = (registration.create)();
            let saga_name = saga.name().to_string();
            let (saga_tx, saga_rx) = flume::unbounded::<MEvent>();
            saga_channels.push(SagaChannel {
                tx: saga_tx,
                entity_type: registration.event_entity_type,
                change_type: registration.event_change_type,
            });
            let events: myko::saga::EventStream = Box::pin(futures_util::stream::unfold(
                saga_rx,
                move |saga_rx| async move {
                    saga_rx
                        .recv_async()
                        .await
                        .ok()
                        .map(|event| (event, saga_rx))
                },
            ));

            let saga_ctx = Arc::new(myko::saga::SagaContext::with_event_sink(
                self.host_id,
                self.registry.clone(),
                self.saga_event_tx.clone(),
            ));
            let mut command_stream = saga.build_boxed(events, saga_ctx);

            let host_id = self.host_id;
            let registry = self.registry.clone();
            let handler_registry = self.handler_registry.clone();
            let relationship_manager = self.relationship_manager.clone();
            let persisters = self.persisters.clone();
            let search_index = self.search_index.clone();
            let peer_clients = self.peer_clients.clone();
            let saga_event_tx = self.saga_event_tx.clone();

            let handle = tokio::spawn(async move {
                while let Some(command) = command_stream.next().await {
                    let command_name = command.command_name();
                    log::debug!("Saga {} executing command {}", saga_name, command_name);
                    let req = Arc::new(RequestContext::internal(
                        Arc::from(Uuid::new_v4().to_string()),
                        host_id,
                        &format!("saga:{saga_name}"),
                    ));

                    let cmd_ctx = CommandContext::new(
                        Arc::from(command_name),
                        req,
                        Arc::new(CellServerCtx::new(
                            host_id,
                            registry.clone(),
                            handler_registry.clone(),
                            relationship_manager.clone(),
                            persisters.clone(),
                            search_index.clone(),
                            peer_clients.clone(),
                            Some(saga_event_tx.clone()),
                            None,
                        )),
                    );

                    if let Err(err) = command.execute_boxed(cmd_ctx) {
                        log::error!(
                            "Saga {} command {} failed: {}",
                            saga_name,
                            command_name,
                            err.message
                        );
                    }
                }
            });

            self.saga_tasks
                .lock()
                .expect("saga_tasks mutex poisoned")
                .push(handle);
        }

        // NOTE(ts): Dispatcher fans out events to saga channels, filtering by
        // entity type and change type so each saga only receives relevant events.
        let dispatcher = tokio::spawn(async move {
            while let Ok(event) = rx.recv_async().await {
                for ch in &saga_channels {
                    if event.item_type == ch.entity_type && event.change_type == ch.change_type {
                        let _ = ch.tx.send(event.clone());
                    }
                }
            }
        });
        self.saga_tasks
            .lock()
            .expect("saga_tasks mutex poisoned")
            .push(dispatcher);
    }

    /// Create a Postgres-backed history store for replay/windback operations.
    pub fn postgres_history_store(&self) -> Result<Option<PostgresHistoryStore>, String> {
        self.config
            .postgres
            .clone()
            .map(PostgresHistoryStore::new)
            .transpose()
    }

    /// Initialize Postgres replay/listener and wait for catch-up.
    pub fn init_postgres_and_wait(&self, timeout: Duration) -> Result<(), String> {
        if self.config.postgres.is_some() && self.postgres_consumer.is_none() {
            return Err(
                "Postgres is configured but the Postgres consumer is not running".to_string(),
            );
        }

        if let Some(ref consumer) = self.postgres_consumer {
            consumer.wait_until_caught_up(timeout)?;
            self.ready.store(true, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Establish relationship invariants.
    pub fn establish_relations(&self) {
        if let Err(e) = self.relationship_manager.establish_relations(&self.ctx()) {
            log::error!("Failed to establish relations: {e}");
        }
    }

    /// Check if the server is ready to accept connections.
    pub fn is_ready(&self) -> bool {
        if let Some(ref consumer) = self.postgres_consumer {
            if consumer.is_caught_up() {
                self.ready.store(true, Ordering::SeqCst);
                return true;
            }
            return false;
        }
        true
    }

    /// Run the server with full initialization.
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use tokio::net::TcpListener;

        // Persisters can veto startup via startup healthchecks.
        let entity_types: Vec<&str> = self
            .handler_registry
            .entity_types()
            .map(|t| t.as_ref())
            .collect();
        self.persisters
            .startup_healthcheck(&entity_types)
            .map_err(|reason| format!("Persister startup healthcheck failed: {reason}"))?;

        if self.config.postgres.is_some() && self.postgres_consumer.is_none() {
            return Err("Postgres is configured but the Postgres consumer failed to start".into());
        }

        // Wait for Postgres catch-up if configured
        if self.postgres_consumer.is_some() {
            log::info!("Waiting for Postgres event consumer to catch up...");
            let timeout = std::time::Duration::from_secs(300);
            self.init_postgres_and_wait(timeout)
                .map_err(|reason| format!("Postgres startup catch-up failed: {reason}"))?;
            log::info!("Postgres caught up, ready to accept connections");
        }

        // Build search index from store data (after catch-up)
        log::info!("Building search index...");
        self.search_index.build_from_registry(&self.registry);

        // Establish relations (cleanup orphans, ensure required entities)
        log::info!("Establishing relations...");
        self.establish_relations();

        // Claim orphaned server-owned items and start death watch
        log::info!("Checking server-owned item ownership...");
        if let Err(e) = ServerOwnershipManager::claim_orphaned(&self.ctx()) {
            log::error!("Failed to claim orphaned server-owned items: {}", e);
        }
        let ownership_guard = ServerOwnershipManager::watch_peer_deaths(&self.ctx());
        *self
            ._server_ownership_guard
            .lock()
            .expect("server_ownership_guard mutex poisoned") = Some(ownership_guard);

        // Bind WebSocket listener first so peer publication only happens once
        // the gateway is actually available.
        let listener = TcpListener::bind(&self.config.bind_addr).await?;
        log::info!("CellServer listening on {}", self.config.bind_addr);
        log::info!(
            "WebSocket server listening on ws://{}/myko",
            self.config.bind_addr
        );

        // Start peer registry if configured
        if self.config.peer_registry.is_some() {
            self.start_peer_registry(None);
        }

        // Run after_init hook (e.g., scene engine startup)
        if let Some(hook) = self
            .after_init
            .lock()
            .expect("after_init mutex poisoned")
            .take()
        {
            hook(self);
        }

        self.start_saga_runtime();

        // WS message-throughput summary thread. Emits a single log line every
        // 250ms with inbound/outbound counts per message kind. Used for
        // diagnosing server-vs-client pacing during slow loads.
        crate::ws_timing::start_periodic_logger();

        // Report-cache hit/miss summary thread. Replaces the per-call debug
        // log spam that was dominating I/O during loads.
        myko::server::report_cache_stats::start_periodic_logger();

        log::info!("Server started");
        self.run_ws_accept_loop(listener).await
    }

    /// Run just the WebSocket accept loop.
    pub async fn run_ws_loop(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind(&self.config.bind_addr).await?;
        log::info!("CellServer listening on {}", self.config.bind_addr);
        log::info!(
            "WebSocket server listening on ws://{}/myko",
            self.config.bind_addr
        );
        self.run_ws_accept_loop(listener).await
    }

    async fn run_ws_accept_loop(
        &self,
        listener: tokio::net::TcpListener,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ready = self.ready.clone();

        loop {
            let (stream, addr) = listener.accept().await?;

            // Check if server is ready (durable backend caught up)
            if !ready.load(Ordering::SeqCst) {
                if self.is_ready() {
                    log::info!("Server is now ready to accept connections");
                } else {
                    log::warn!(
                        "Rejecting connection from {} - server not ready (durable backend catching up)",
                        addr
                    );
                    drop(stream);
                    continue;
                }
            }

            log::debug!("New connection from {}", addr);

            let ctx = self.ctx();

            tokio::spawn(async move {
                if let Err(e) =
                    ws_handler::WsHandler::handle_connection(stream, addr, Arc::new(ctx)).await
                {
                    log::error!("Connection error from {}: {}", addr, e);
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_creation() {
        let config = CellServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            postgres: None,
            host_id: None,
            peer_registry: None,
            default_persister: None,
            persister_overrides: HashMap::new(),
            peer_clients: None,
        };
        let server = CellServer::new(config);
        assert!(Arc::strong_count(&server.registry) >= 1);
    }

    #[test]
    fn test_server_with_host_id() {
        let host_id = Uuid::new_v4();
        let config = CellServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            postgres: None,
            host_id: Some(host_id),
            peer_registry: None,
            default_persister: None,
            persister_overrides: HashMap::new(),
            peer_clients: None,
        };
        let server = CellServer::new(config);
        assert_eq!(server.host_id, host_id);
    }
}

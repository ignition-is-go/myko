//! Myko server runtime — WebSocket, durable event backends, peer federation.
//!
//! This crate contains the tokio-dependent parts of the Myko server:
//! - `MykoServer` — server lifecycle (durable catch-up init, WS accept loop)
//! - `postgres` — `PostgreSQL` producer/consumer (event-table + LISTEN/NOTIFY)
//! - `ws_handler` — WebSocket connection handling
//! - `peer_registry` — federation with other servers
//! - `mcp` — Model Context Protocol server
//!
//! Tokio-free server types (`MykoServerContext`, `HandlerRegistry`, etc.) live in `myko::server`.

pub mod mcp;
pub mod peer_persister;
pub mod peer_registry;
pub mod postgres;
pub mod router;
pub mod server_ownership;
pub mod telemetry;
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

const CACHE_SWEEP_INTERVAL: Duration = Duration::from_mins(1);

struct SagaChannel {
    tx: flume::Sender<MEvent>,
    entity_type: &'static str,
    change_type: myko::event::MEventType,
}

use crate::postgres::{
    PostgresConfig, PostgresConsumer, PostgresHistoryReplayProvider, PostgresHistoryStore,
    PostgresProducer, PostgresProducerHandle,
};

/// Cell-based Myko server configuration.
#[derive(Clone)]
pub struct MykoServerConfig {
    /// Address to bind the WebSocket server
    pub bind_addr: SocketAddr,
    /// Disable Nagle's algorithm (set `TCP_NODELAY`) on accepted connections.
    /// Myko's traffic is small, frequent, latency-sensitive messages (e.g.
    /// ~60Hz pulses); with Nagle on, TCP coalesces successive small writes
    /// into fewer segments that arrive together, so an even send cadence is
    /// delivered as bursts. Defaults to `true`.
    pub tcp_nodelay: bool,
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

/// Builder for creating a `MykoServer`.
#[derive(Default)]
pub struct MykoServerBuilder {
    bind_addr: Option<SocketAddr>,
    tcp_nodelay: Option<bool>,
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
    /// Optional MCP `ServerInfo`. Defaults to `ServerInfo::default()` if not
    /// set; binaries override this to advertise their own name / version /
    /// instructions on the `/myko/mcp` endpoint.
    server_info: Option<mcp::dispatch::ServerInfo>,
}

type AfterInitCallback = Box<dyn FnOnce(&MykoServer) + Send>;

impl MykoServerBuilder {
    /// Create a new server builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the WebSocket bind address.
    #[must_use]
    pub const fn with_bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = Some(addr);
        self
    }

    /// Set whether to disable Nagle's algorithm (`TCP_NODELAY`) on accepted
    /// connections. Defaults to `true` (Nagle off) — recommended for myko's
    /// small, frequent, latency-sensitive messages so an even send cadence
    /// isn't delivered as coalesced bursts.
    #[must_use]
    pub const fn with_tcp_nodelay(mut self, enabled: bool) -> Self {
        self.tcp_nodelay = Some(enabled);
        self
    }

    /// Set the server host ID (auto-generated if not set).
    #[must_use]
    pub const fn with_host_id(mut self, id: Uuid) -> Self {
        self.host_id = Some(id);
        self
    }

    /// Configure Postgres for event persistence/distribution.
    #[must_use]
    pub fn with_postgres(mut self, config: PostgresConfig) -> Self {
        self.postgres = Some(config);
        self
    }

    /// Configure peer registry for federation.
    #[must_use]
    pub fn with_peer_registry(mut self, config: peer_registry::PeerRegistryConfig) -> Self {
        self.peer_registry = Some(config);
        self
    }

    /// Set the default persister used for all entity types without explicit overrides.
    #[must_use]
    pub fn with_default_persister(mut self, persister: Arc<dyn Persister>) -> Self {
        self.default_persister = Some(persister);
        self
    }

    /// Override persister for a specific entity type (e.g. "Pulse").
    #[must_use]
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
    #[must_use]
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
    #[must_use]
    pub fn after_init(mut self, f: impl FnOnce(&MykoServer) + Send + 'static) -> Self {
        self.after_init = Some(Box::new(f));
        self
    }

    /// Set the MCP `ServerInfo` advertised on the `/myko/mcp` `initialize`
    /// response. Defaults to `ServerInfo::default()` (`myko-mcp` /
    /// `CARGO_PKG_VERSION` / no instructions).
    #[must_use]
    pub fn with_server_info(mut self, info: mcp::dispatch::ServerInfo) -> Self {
        self.server_info = Some(info);
        self
    }

    /// Build the server.
    #[must_use]
    pub fn build(self) -> MykoServer {
        let bind_addr = self
            .bind_addr
            .unwrap_or_else(|| std::net::SocketAddr::from(([127, 0, 0, 1], 5155)));

        let server_info = Arc::new(self.server_info.unwrap_or_default());

        let mut server = MykoServer::new(MykoServerConfig {
            bind_addr,
            tcp_nodelay: self.tcp_nodelay.unwrap_or(true),
            postgres: self.postgres,
            host_id: self.host_id,
            peer_registry: self.peer_registry,
            default_persister: self.default_persister,
            persister_overrides: self.persister_overrides,
            peer_clients: self.peer_clients,
        });
        server.after_init = std::sync::Mutex::new(self.after_init);
        server.server_info = server_info;
        server
    }
}

/// Cell-based Myko server.
///
/// Uses hyphae cells for reactive queries and reports instead of actors.
pub struct MykoServer {
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
    config: MykoServerConfig,
    /// Postgres producer (kept alive)
    _postgres_producer_owner: Option<PostgresProducer>,
    /// Postgres consumer (kept alive)
    postgres_consumer: Option<PostgresConsumer>,
    history_replay: Option<Arc<PostgresHistoryReplayProvider>>,
    /// Whether the server is ready to accept connections
    ready: Arc<AtomicBool>,
    /// Peer registry for federation (initialized after catch-up)
    peer_registry_instance: RwLock<Option<peer_registry::PeerRegistry>>,
    /// Live peer clients shared with report context.
    peer_clients: Arc<dashmap::DashMap<Arc<str>, Arc<MykoClient>>>,
    /// Callback to run after init (catch-up + relations) but before WS loop
    after_init: std::sync::Mutex<Option<AfterInitCallback>>,
    /// MCP `ServerInfo` advertised on the `/myko/mcp` `initialize` response.
    /// Set via [`MykoServerBuilder::with_server_info`]; defaults to
    /// `ServerInfo::default()`.
    server_info: Arc<mcp::dispatch::ServerInfo>,
    /// Sender for local+replicated event fan-out to saga runtime.
    saga_event_tx: flume::Sender<MEvent>,
    /// Receiver consumed when saga runtime starts.
    saga_event_rx: std::sync::Mutex<Option<flume::Receiver<MEvent>>>,
    /// Saga tasks kept alive for server lifetime.
    saga_tasks: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// Server ownership death-watch guard (kept alive for server lifetime).
    server_ownership_guard: std::sync::Mutex<Option<hyphae::SubscriptionGuard>>,
    /// Memoized server context. Built once on first `ctx()` and shared by
    /// every caller — the caches (`query_cache`/`view_cache`/`report_cache`/
    /// `compute_gates`/`ingest_buffers`) and the `peer_clients_tick` cell are
    /// then genuinely process-wide, so N connections subscribing to the same
    /// query share one reactive cell graph instead of N copies, cache sweeps
    /// actually reach live entries, and every peer-death watcher observes the
    /// same tick. Every `MykoServerContext` field is an `Arc`/`Cell`/`Uuid`, so a
    /// clone is a handful of refcount bumps that share the underlying state.
    ctx_cache: std::sync::OnceLock<MykoServerContext>,
}

impl MykoServer {
    /// Create a new server builder.
    #[must_use]
    pub fn builder() -> MykoServerBuilder {
        MykoServerBuilder::new()
    }

    /// Create a new cell-based server.
    pub fn new(config: MykoServerConfig) -> Self {
        let host_id = config.host_id.unwrap_or_else(Uuid::new_v4);
        let registry = Arc::new(StoreRegistry::new());
        let handler_registry = Arc::new(HandlerRegistry::new());
        let relationship_manager = Arc::new(RelationshipManager::new());

        // Initialize the client registry for WebSocket client message dispatch
        init_client_registry();

        let (saga_event_tx, saga_event_rx) = flume::unbounded::<MEvent>();
        let (history_replay, postgres_producer_owner, postgres_producer, postgres_consumer) =
            config.postgres.as_ref().map_or_else(
                || (None, None, None, None),
                |postgres_config| {
                    let history_replay =
                        Arc::new(PostgresHistoryReplayProvider::new(postgres_config.clone()));
                    match PostgresProducer::new(postgres_config, host_id) {
                        Ok(producer) => {
                            let handle = producer.handle();
                            let consumer = match PostgresConsumer::start(
                                postgres_config,
                                host_id,
                                handler_registry.clone(),
                                registry.clone(),
                                history_replay.clone(),
                            ) {
                                Ok(c) => Some(c),
                                Err(e) => {
                                    tracing::error!("Failed to start Postgres consumer: {}", e);
                                    None
                                }
                            };
                            (Some(history_replay), Some(producer), Some(handle), consumer)
                        }
                        Err(e) => {
                            tracing::error!("Failed to create Postgres producer: {}", e);
                            (Some(history_replay), None, None, None)
                        }
                    }
                },
            );

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
            let persister: Arc<dyn Persister> = Arc::new(handle);
            persister_router.set_default(Some(persister));
        }
        for (entity_type, persister) in &config.persister_overrides {
            persister_router.set_override(entity_type.clone(), persister.clone());
        }
        let persisters = Arc::new(persister_router);

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
            history_replay,
            ready,
            peer_registry_instance: RwLock::new(None),
            peer_clients,
            after_init: std::sync::Mutex::new(None),
            server_info: Arc::new(mcp::dispatch::ServerInfo::default()),
            saga_event_tx,
            saga_event_rx: std::sync::Mutex::new(Some(saga_event_rx)),
            saga_tasks: std::sync::Mutex::new(Vec::new()),
            server_ownership_guard: std::sync::Mutex::new(None),
            ctx_cache: std::sync::OnceLock::new(),
        }
    }

    /// Start the peer registry for federation.
    pub fn start_peer_registry(&self, config: Option<peer_registry::PeerRegistryConfig>) {
        let peer_config = config.or_else(|| self.config.peer_registry.clone());

        if let Some(peer_config) = peer_config {
            tracing::info!("Starting peer registry");
            let pr = peer_registry::PeerRegistry::new(self.ctx(), peer_config);
            *self
                .peer_registry_instance
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(pr);
        }
    }

    /// Check if peer registry is running.
    pub fn has_peer_registry(&self) -> bool {
        self.peer_registry_instance
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }

    /// Get the store registry.
    pub fn registry(&self) -> Arc<StoreRegistry> {
        self.registry.clone()
    }

    /// Get the handler registry.
    pub fn handler_registry(&self) -> Arc<HandlerRegistry> {
        self.handler_registry.clone()
    }

    /// Get the MCP `ServerInfo` advertised on the `/myko/mcp` `initialize`
    /// response.
    pub fn server_info(&self) -> Arc<mcp::dispatch::ServerInfo> {
        self.server_info.clone()
    }

    /// Get a server context for module use.
    ///
    /// Returns a clone of the one memoized [`MykoServerContext`] (see `ctx_cache`)
    /// so all callers share the same caches and peer-tick cell. Building a
    /// fresh context per call — the old behavior — gave every connection its
    /// own caches: no cross-client cache sharing (an N× reactive-graph
    /// memory multiplier), sweeps that never reached connection caches, and a
    /// `peer_clients_tick` that register/unregister bumped on one context
    /// while a watcher built on another never observed.
    pub fn ctx(&self) -> MykoServerContext {
        self.ctx_cache
            .get_or_init(|| {
                let history_replay: Option<Arc<dyn myko::server::HistoryReplayProvider>> =
                    self.history_replay.clone().map(|provider| {
                        let provider: Arc<dyn myko::server::HistoryReplayProvider> = provider;
                        provider
                    });
                MykoServerContext::new(
                    self.host_id,
                    self.registry.clone(),
                    self.handler_registry.clone(),
                    self.relationship_manager.clone(),
                    self.persisters.clone(),
                    self.search_index.clone(),
                    myko::server::MykoServerRuntime {
                        peer_clients: self.peer_clients.clone(),
                        event_sink: Some(self.saga_event_tx.clone()),
                        history_replay,
                    },
                )
            })
            .clone()
    }

    fn start_saga_runtime(&self) {
        let registrations: Vec<_> = inventory::iter::<SagaRegistration>().collect();
        if registrations.is_empty() {
            return;
        }
        let Some(rx) = self
            .saga_event_rx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        else {
            return;
        };

        tracing::info!("Starting saga runtime with {} saga(s)", registrations.len());

        // NOTE(ts): One unbounded flume channel per saga, with dispatch-side filtering
        // so sagas only receive events matching their entity type and change type.
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

            let saga_ctx = Arc::new(myko::saga::SagaContext::new(
                self.host_id,
                self.registry.clone(),
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
                    tracing::debug!("Saga {} executing command {}", saga_name, command_name);
                    let req = Arc::new(RequestContext::internal(
                        Arc::from(Uuid::new_v4().to_string()),
                        host_id,
                        &format!("saga:{saga_name}"),
                    ));

                    let cmd_ctx = CommandContext::new(
                        Arc::from(command_name),
                        req,
                        Arc::new(MykoServerContext::new(
                            host_id,
                            registry.clone(),
                            handler_registry.clone(),
                            relationship_manager.clone(),
                            persisters.clone(),
                            search_index.clone(),
                            myko::server::MykoServerRuntime {
                                peer_clients: peer_clients.clone(),
                                event_sink: Some(saga_event_tx.clone()),
                                history_replay: None,
                            },
                        )),
                    );

                    if let Err(err) = command.execute_boxed(cmd_ctx) {
                        tracing::error!(
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
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(handle);
        }

        // NOTE(ts): Dispatcher fans out events to saga channels, filtering by
        // entity type and change type so each saga only receives relevant events.
        let dispatcher = tokio::spawn(async move {
            while let Ok(event) = rx.recv_async().await {
                for ch in &saga_channels {
                    if &*event.item_type == ch.entity_type && event.change_type == ch.change_type {
                        let _ = ch.tx.send(event.clone());
                    }
                }
            }
        });
        self.saga_tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(dispatcher);
    }

    /// Create a Postgres-backed history store for replay/windback operations.
    /// # Errors
    ///
    /// Returns an error when the `PostgreSQL` history store cannot be opened.
    pub fn postgres_history_store(&self) -> Result<Option<PostgresHistoryStore>, String> {
        self.config
            .postgres
            .clone()
            .map(PostgresHistoryStore::new)
            .transpose()
    }

    /// Initialize Postgres replay/listener and wait for catch-up.
    /// # Errors
    ///
    /// Returns an error when `PostgreSQL` initialization fails or times out.
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
            tracing::error!("Failed to establish relations: {e}");
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
    /// # Errors
    ///
    /// Returns an error when the server cannot initialize or accept connections.
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use tokio::net::TcpListener;

        // Persisters can veto startup via startup healthchecks.
        let entity_types: Vec<&str> = self
            .handler_registry
            .entity_types()
            .map(std::convert::AsRef::as_ref)
            .collect();
        self.persisters
            .startup_healthcheck(&entity_types)
            .map_err(|reason| format!("Persister startup healthcheck failed: {reason}"))?;

        if self.config.postgres.is_some() && self.postgres_consumer.is_none() {
            return Err("Postgres is configured but the Postgres consumer failed to start".into());
        }

        // Wait for Postgres catch-up if configured
        if self.postgres_consumer.is_some() {
            tracing::info!("Waiting for Postgres event consumer to catch up...");
            let timeout = std::time::Duration::from_mins(5);
            self.init_postgres_and_wait(timeout)
                .map_err(|reason| format!("Postgres startup catch-up failed: {reason}"))?;
            tracing::info!("Postgres caught up, ready to accept connections");
        }

        // Build search index from store data (after catch-up)
        tracing::info!("Building search index...");
        self.search_index.build_from_registry(&self.registry);

        // Establish relations (cleanup orphans, ensure required entities)
        tracing::info!("Establishing relations...");
        self.establish_relations();

        // Claim orphaned server-owned items and start death watch
        tracing::info!("Checking server-owned item ownership...");
        if let Err(e) = ServerOwnershipManager::claim_orphaned(&self.ctx()) {
            tracing::error!("Failed to claim orphaned server-owned items: {}", e);
        }
        let ownership_guard = ServerOwnershipManager::watch_peer_deaths(&self.ctx());
        *self
            .server_ownership_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ownership_guard);

        // Run after_init hook (e.g., scene engine startup) BEFORE binding the
        // listener. This hook runs synchronously and can be slow (e.g.
        // rship's scene-editor-view warmup, which materializes a view per
        // scene) — binding first and accepting later left a window where the
        // OS would complete TCP handshakes and queue them in the accept
        // backlog while nothing in the process was reading them yet. A
        // client connecting in that window would see an established TCP
        // connection that never got a WebSocket-upgrade response: not
        // rejected (so no clean retry trigger), not served (so no response
        // ever arrives) — stuck rather than cleanly failing. Binding late
        // means a connection attempt during startup gets a prompt
        // ECONNREFUSED instead, which every client/proxy already retries.
        let hook = self
            .after_init
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(hook) = hook {
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

        // Entity-SET summary thread. Replaces the per-`set` "[entity] SET ..."
        // debug spam (Pulse SETs dominate under pulse-heavy workloads).
        myko::server::entity_set_stats::start_periodic_logger();

        // Per-search summary thread. One log line per window listing each
        // search that completed (entity_type, result count, elapsed).
        myko::search::search_stats::start_periodic_logger();

        // Live per-entity-type item-count gauge, sampled by the OTLP metrics
        // exporter's own periodic reader (see telemetry::init_from_env) —
        // no-op when telemetry isn't configured.
        crate::telemetry::register_item_count_gauge(self.registry.clone());

        // Opt-in malloc_trim probe (MYKO_MALLOC_TRIM_INTERVAL_SECS): logs RSS
        // before/after returning free glibc arena pages, to tell allocator
        // page retention apart from real memory retention in deployments.
        crate::telemetry::start_malloc_trim_probe();

        // Bind WebSocket listener last, once all synchronous startup work
        // (including after_init) is done, so peer publication only happens
        // once the gateway is actually available to serve requests, not just
        // listening.
        let listener = TcpListener::bind(&self.config.bind_addr).await?;
        tracing::info!("MykoServer listening on {}", self.config.bind_addr);
        tracing::info!(
            "Myko gateway: ws://{}/myko | MCP: /myko/mcp (POST + WS + SSE)",
            self.config.bind_addr
        );

        // Start peer registry if configured
        if self.config.peer_registry.is_some() {
            self.start_peer_registry(None);
        }

        tracing::info!("Server started");
        self.run_ws_accept_loop(listener).await
    }

    /// Run just the accept loop (no Postgres / relations / saga startup).
    /// # Errors
    ///
    /// Returns an error when the WebSocket listener cannot accept connections.
    pub async fn run_ws_loop(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind(&self.config.bind_addr).await?;
        tracing::info!("MykoServer listening on {}", self.config.bind_addr);
        tracing::info!(
            "Myko gateway: ws://{}/myko | MCP: /myko/mcp (POST + WS + SSE)",
            self.config.bind_addr
        );
        self.run_ws_accept_loop(listener).await
    }

    async fn run_ws_accept_loop(
        &self,
        listener: tokio::net::TcpListener,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ready = self.ready.clone();
        let sweep_ctx = self.ctx();
        // Bound tombstone retention even while the listener is otherwise idle.
        let mut cache_sweep = tokio::time::interval(CACHE_SWEEP_INTERVAL);
        cache_sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            let accepted = tokio::select! {
                accepted = listener.accept() => Some(accepted),
                _ = cache_sweep.tick() => {
                    sweep_ctx.sweep_dead_cache_entries();
                    None
                }
            };
            let Some(accepted) = accepted else {
                continue;
            };
            let (stream, addr) = accepted?;

            // Disable Nagle (unless configured off): our writes are small,
            // frequent, latency-sensitive messages (e.g. ~60Hz pulses). With
            // Nagle on (tokio's default), TCP coalesces successive small writes
            // into fewer segments that land together, so an even 60Hz send
            // arrives at the client as ~15-20Hz bursts of 3-4 — which downstream
            // latest-wins consumers (e.g. the pulse-unreal transform apply) then
            // collapse to one update per burst, producing visibly choppy motion.
            // Ship each write promptly instead.
            if self.config.tcp_nodelay
                && let Err(e) = stream.set_nodelay(true)
            {
                tracing::warn!("failed to set TCP_NODELAY on connection from {addr}: {e}");
            }

            // Check if server is ready (durable backend caught up)
            if !ready.load(Ordering::SeqCst) {
                if self.is_ready() {
                    tracing::info!("Server is now ready to accept connections");
                } else {
                    tracing::warn!(
                        "Rejecting connection from {} - server not ready (durable backend catching up)",
                        addr
                    );
                    drop(stream);
                    continue;
                }
            }

            tracing::debug!("New connection from {}", addr);

            let ctx = Arc::new(self.ctx());
            let server_info = self.server_info.clone();

            tokio::spawn(async move {
                if let Err(e) = router::route_connection(stream, addr, ctx, server_info).await {
                    tracing::error!("Connection error from {}: {}", addr, e);
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
        let config = MykoServerConfig {
            bind_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            tcp_nodelay: true,
            postgres: None,
            host_id: None,
            peer_registry: None,
            default_persister: None,
            persister_overrides: HashMap::new(),
            peer_clients: None,
        };
        let server = MykoServer::new(config);
        assert!(Arc::strong_count(&server.registry) >= 1);
    }

    #[test]
    fn test_server_with_host_id() {
        let host_id = Uuid::new_v4();
        let config = MykoServerConfig {
            bind_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            tcp_nodelay: true,
            postgres: None,
            host_id: Some(host_id),
            peer_registry: None,
            default_persister: None,
            persister_overrides: HashMap::new(),
            peer_clients: None,
        };
        let server = MykoServer::new(config);
        assert_eq!(server.host_id, host_id);
    }

    #[test]
    fn ctx_is_memoized_and_shares_caches() {
        // Regression for lv-38b7: every `ctx()` must return the SAME shared
        // context, so a query cached through one call is visible through the
        // next. Before the fix, each `ctx()` allocated its own caches, so a
        // second call reported an empty query cache (and per-connection
        // caches leaked, never swept).
        let config = MykoServerConfig {
            bind_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            tcp_nodelay: true,
            postgres: None,
            host_id: None,
            peer_registry: None,
            default_persister: None,
            persister_overrides: HashMap::new(),
            peer_clients: None,
        };
        let server = MykoServer::new(config);
        let ctx1 = server.ctx();
        let ctx2 = server.ctx();

        let req = Arc::new(RequestContext::internal(
            Arc::from("test"),
            server.host_id,
            "ctx_sharing_test",
        ));
        // Hold the map so its cache entry (weak-referenced) stays live.
        let _held = ctx1.query_map(myko::entities::client::GetAllClients {}, req);

        assert!(
            ctx1.query_cache_len() >= 1,
            "querying through ctx1 should populate the shared query cache"
        );
        assert_eq!(
            ctx2.query_cache_len(),
            ctx1.query_cache_len(),
            "both ctx() calls must share one query cache"
        );
    }
}

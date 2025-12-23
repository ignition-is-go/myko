//! Cell-based Myko server
//!
//! A reactive server implementation using hypha cells instead of actors.
//! This replaces the ractor-based MykoServer with a simpler, more direct approach.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                           CellServer                                    │
//! │                                                                         │
//! │  WebSocket ──► WsHandler ──► CellServerCtx ──► StoreRegistry             │
//! │      │              │             │                   │                 │
//! │      │              │             │             CellMap<id, item>       │
//! │      │              ▼             │                   │                 │
//! │      │        KafkaProducer       │                   ▼                 │
//! │      │              │             │         Query/Report cells          │
//! │      │              ▼             │                   │                 │
//! │      │           Kafka ◄─────────────── KafkaConsumer                   │
//! │      │              │             │                                     │
//! │      │              └─────────────┘                                     │
//! │      │                                                                  │
//! │      ◄────────────────────── (subscription updates)                     │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```

mod context;
mod handler_registry;
pub mod kafka;
mod peer_registry;
mod relationship_manager;
mod ws_handler;

pub use context::CellServerCtx;
pub use handler_registry::HandlerRegistry;
pub use kafka::{
    CatchUpStatus, CellKafkaConsumer, CellKafkaProducer, KafkaConfig, KafkaProducerHandle,
};
pub use peer_registry::{PeerRegistry, PeerRegistryConfig, PeerStatus};
pub use relationship_manager::RelationshipManager;
pub use ws_handler::WsHandler;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use uuid::Uuid;

use crate::store::StoreRegistry;

/// Cell-based Myko server configuration.
#[derive(Debug, Clone)]
pub struct CellServerConfig {
    /// Address to bind the WebSocket server
    pub bind_addr: SocketAddr,
    /// Optional Kafka configuration for event persistence/distribution
    pub kafka: Option<KafkaConfig>,
    /// Server host ID (auto-generated if not provided)
    pub host_id: Option<Uuid>,
    /// Optional peer registry configuration for federation
    pub peer_registry: Option<PeerRegistryConfig>,
}

/// Builder for creating a CellServer.
#[derive(Default)]
pub struct CellServerBuilder {
    bind_addr: Option<SocketAddr>,
    host_id: Option<Uuid>,
    kafka: Option<KafkaConfig>,
    peer_registry: Option<PeerRegistryConfig>,
}

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

    /// Configure Kafka for event persistence/distribution.
    pub fn with_kafka(mut self, config: KafkaConfig) -> Self {
        self.kafka = Some(config);
        self
    }

    /// Configure peer registry for federation.
    pub fn with_peer_registry(mut self, config: PeerRegistryConfig) -> Self {
        self.peer_registry = Some(config);
        self
    }

    /// Build the server.
    pub fn build(self) -> CellServer {
        let bind_addr = self
            .bind_addr
            .unwrap_or_else(|| "127.0.0.1:5155".parse().unwrap());

        CellServer::new(CellServerConfig {
            bind_addr,
            kafka: self.kafka,
            host_id: self.host_id,
            peer_registry: self.peer_registry,
        })
    }
}

/// Cell-based Myko server.
///
/// Uses hypha cells for reactive queries and reports instead of actors.
pub struct CellServer {
    /// Central entity store registry
    pub registry: Arc<StoreRegistry>,
    /// Handler registry for items, queries, and reports
    pub handler_registry: Arc<HandlerRegistry>,
    /// Relationship manager for cascade operations
    pub relationship_manager: Arc<RelationshipManager>,
    /// Optional Kafka producer handle
    pub kafka_producer: Option<KafkaProducerHandle>,
    /// Server host ID
    pub host_id: Uuid,
    /// Server configuration
    config: CellServerConfig,
    /// Kafka producer (kept alive)
    _kafka_producer_owner: Option<CellKafkaProducer>,
    /// Kafka consumer (kept alive)
    kafka_consumer: Option<CellKafkaConsumer>,
    /// Whether the server is ready to accept connections
    ready: Arc<AtomicBool>,
    /// Peer registry for federation (initialized after Kafka catch-up)
    peer_registry: RwLock<Option<PeerRegistry>>,
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

        // Initialize Kafka if configured
        let (kafka_producer_owner, kafka_producer, kafka_consumer) =
            if let Some(ref kafka_config) = config.kafka {
                match CellKafkaProducer::new(kafka_config, host_id) {
                    Ok(producer) => {
                        let handle = producer.handle();

                        // Start consumer with handler registry and registry
                        let consumer = match CellKafkaConsumer::start(
                            kafka_config,
                            host_id,
                            handler_registry.clone(),
                            registry.clone(),
                        ) {
                            Ok(c) => Some(c),
                            Err(e) => {
                                log::error!("Failed to start Kafka consumer: {}", e);
                                None
                            }
                        };

                        (Some(producer), Some(handle), consumer)
                    }
                    Err(e) => {
                        log::error!("Failed to create Kafka producer: {}", e);
                        (None, None, None)
                    }
                }
            } else {
                (None, None, None)
            };

        // If no Kafka, server is immediately ready
        let ready = Arc::new(AtomicBool::new(kafka_consumer.is_none()));

        Self {
            registry,
            handler_registry,
            relationship_manager,
            kafka_producer,
            host_id,
            config,
            _kafka_producer_owner: kafka_producer_owner,
            kafka_consumer,
            ready,
            peer_registry: RwLock::new(None),
        }
    }

    /// Start the peer registry for federation.
    ///
    /// This should be called after Kafka catch-up is complete (if using Kafka),
    /// since the peer registry uses queries that need entity data.
    ///
    /// If peer registry config was provided in CellServerConfig, this uses that.
    /// Otherwise, uses the provided config.
    ///
    /// This method uses interior mutability and can be called on `&self`.
    pub fn start_peer_registry(&self, config: Option<PeerRegistryConfig>) {
        let peer_config = config.or_else(|| self.config.peer_registry.clone());

        if let Some(peer_config) = peer_config {
            log::info!("Starting peer registry");
            let peer_registry = PeerRegistry::new(self.ctx(), peer_config);
            *self.peer_registry.write().unwrap() = Some(peer_registry);
        }
    }

    /// Check if peer registry is running.
    pub fn has_peer_registry(&self) -> bool {
        self.peer_registry.read().unwrap().is_some()
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
    ///
    /// The context provides modules with:
    /// - Reactive query capabilities (e.g., `ctx.query(GetPeerServers {})`)
    /// - Event publishing (e.g., `ctx.set(server)`)
    /// - Access to host_id
    pub fn ctx(&self) -> CellServerCtx {
        CellServerCtx::new(
            self.host_id,
            self.registry.clone(),
            self.handler_registry.clone(),
            self.relationship_manager.clone(),
            self.kafka_producer.clone(),
        )
    }

    /// Get the Kafka producer handle (if Kafka is enabled).
    pub fn kafka_producer(&self) -> Option<KafkaProducerHandle> {
        self.kafka_producer.clone()
    }

    /// Register a topic with the Kafka consumer.
    ///
    /// Call this for each entity type to enable Kafka consumption.
    pub fn register_kafka_topic(&self, entity_type: &str) {
        if let Some(ref consumer) = self.kafka_consumer {
            consumer.register_topic(entity_type);
        }
    }

    /// Register multiple entity types with the Kafka consumer.
    pub fn register_kafka_topics(&self, entity_types: &[&str]) {
        if let Some(ref consumer) = self.kafka_consumer {
            consumer.register_topics(entity_types);
        }
    }

    /// Register all known entity types with the Kafka consumer.
    ///
    /// This registers all entity types that have been registered with
    /// the handler registry (via inventory).
    pub fn register_all_kafka_topics(&self) {
        if let Some(ref consumer) = self.kafka_consumer {
            for entity_type in self.handler_registry.entity_types() {
                consumer.register_topic(entity_type);
            }
        }
    }

    /// Signal that initial Kafka topic registration is complete.
    ///
    /// After calling this, the server will track when all registered topics
    /// have caught up to their high water marks.
    pub fn finish_kafka_registration(&self) {
        if let Some(ref consumer) = self.kafka_consumer {
            consumer.finish_initial_registration();
        } else {
            // No Kafka, already ready
            self.ready.store(true, Ordering::SeqCst);
        }
    }

    /// Initialize Kafka with all known entity types and wait for catch-up.
    ///
    /// This is a convenience method that:
    /// 1. Registers all entity types from the handler registry
    /// 2. Signals that initial registration is complete
    /// 3. Waits for all topics to catch up (with timeout)
    ///
    /// Returns true if caught up, false if timed out.
    pub fn init_kafka_and_wait(&self, timeout: Duration) -> bool {
        if self.kafka_consumer.is_none() {
            self.ready.store(true, Ordering::SeqCst);
            return true;
        }

        self.register_all_kafka_topics();
        self.finish_kafka_registration();
        self.wait_for_kafka_catchup(timeout)
    }

    /// Wait for Kafka to catch up to all registered topics.
    ///
    /// Returns true if caught up, false if timed out.
    /// If Kafka is not configured, returns true immediately.
    pub fn wait_for_kafka_catchup(&self, timeout: Duration) -> bool {
        if let Some(ref consumer) = self.kafka_consumer {
            let caught_up = consumer.wait_until_caught_up(timeout);
            if caught_up {
                self.ready.store(true, Ordering::SeqCst);
            }
            caught_up
        } else {
            true
        }
    }

    /// Establish relationship invariants.
    ///
    /// This should be called after Kafka catch-up is complete to:
    /// 1. Clean up orphaned entities (children without parents)
    /// 2. Ensure EnsureFor entities exist for all dependency combinations
    ///
    /// Call this before starting to accept client connections.
    pub fn establish_relations(&self) {
        self.relationship_manager.establish_relations(&self.ctx());
    }

    /// Check if the server is ready to accept connections.
    ///
    /// The server is ready when:
    /// - Kafka is not configured, OR
    /// - All initial Kafka topics have caught up
    pub fn is_ready(&self) -> bool {
        if let Some(ref consumer) = self.kafka_consumer {
            if consumer.is_caught_up() {
                self.ready.store(true, Ordering::SeqCst);
                true
            } else {
                false
            }
        } else {
            true
        }
    }

    /// Run the server with full initialization.
    ///
    /// This will:
    /// 1. Wait for Kafka catch-up (if configured)
    /// 2. Establish relations (cleanup orphans, ensure required entities)
    /// 3. Start the peer registry (if configured)
    /// 4. Run the WebSocket server (blocking)
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Wait for Kafka catch-up if configured
        if self.kafka_consumer.is_some() {
            log::info!("Waiting for Kafka to catch up...");
            let timeout = std::time::Duration::from_secs(300);
            if self.init_kafka_and_wait(timeout) {
                log::info!("Kafka caught up, ready to accept connections");
            } else {
                log::warn!(
                    "Kafka catch-up timed out, proceeding anyway (will reject connections until ready)"
                );
            }
        }

        // Establish relations (cleanup orphans, ensure required entities)
        log::info!("Establishing relations...");
        self.establish_relations();

        // Start peer registry if configured
        if self.config.peer_registry.is_some() {
            self.start_peer_registry(None);
        }

        log::info!("Server started");
        self.run_ws_loop().await
    }

    /// Run just the WebSocket accept loop.
    ///
    /// For most use cases, use `run()` instead which handles full initialization.
    /// This is useful if you need custom initialization logic.
    pub async fn run_ws_loop(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind(&self.config.bind_addr).await?;
        log::info!("CellServer listening on {}", self.config.bind_addr);

        let ready = self.ready.clone();

        loop {
            let (stream, addr) = listener.accept().await?;

            // Check if server is ready (Kafka caught up)
            if !ready.load(Ordering::SeqCst) {
                // Check and update ready status
                if self.is_ready() {
                    log::info!("Server is now ready to accept connections");
                } else {
                    log::warn!(
                        "Rejecting connection from {} - server not ready (Kafka catching up)",
                        addr
                    );
                    // Close the connection by dropping the stream
                    drop(stream);
                    continue;
                }
            }

            log::debug!("New connection from {}", addr);

            let host_id = self.host_id;
            let registry = self.registry.clone();
            let handler_registry = self.handler_registry.clone();
            let relationship_manager = self.relationship_manager.clone();
            let kafka_producer = self.kafka_producer.clone();

            tokio::spawn(async move {
                if let Err(e) = WsHandler::handle_connection(
                    stream,
                    addr,
                    host_id,
                    registry,
                    handler_registry,
                    relationship_manager,
                    kafka_producer,
                )
                .await
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
            kafka: None,
            host_id: None,
            peer_registry: None,
        };
        let server = CellServer::new(config);
        assert!(Arc::strong_count(&server.registry) >= 1);
    }

    #[test]
    fn test_server_with_host_id() {
        let host_id = Uuid::new_v4();
        let config = CellServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            kafka: None,
            host_id: Some(host_id),
            peer_registry: None,
        };
        let server = CellServer::new(config);
        assert_eq!(server.host_id, host_id);
    }
}

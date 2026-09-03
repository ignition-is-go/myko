use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

use myko_federation::NodeId;
use myko_node::{EndpointId, Node, SecretKey};
use tokio::runtime::{Builder, Runtime};

const IDENTITY_BYTES: usize = 32;
const WORKER_THREADS: usize = 2;

/// Framework-owned failure while preparing or accessing an embedded Myko node.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EmbeddedNodeError {
    /// A platform key store returned a malformed Iroh secret key.
    #[error("identity must contain exactly {IDENTITY_BYTES} bytes")]
    InvalidIdentity,
    /// The durable Myko node identity could not be restored.
    #[error("could not open the embedded Myko node: {0}")]
    Storage(String),
    /// The native asynchronous runtime could not be constructed.
    #[error("could not create the embedded Myko runtime: {0}")]
    Runtime(String),
    /// Another failure poisoned the embedded lifecycle lock.
    #[error("embedded Myko node state is unavailable")]
    StateUnavailable,
    /// An operation requires a foreground node which has not been started.
    #[error("embedded Myko node is not active")]
    Inactive,
}

/// Stable typed identity and foreground state for an embedded Myko node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddedNodeInfo {
    /// Stable Myko history identity stored in the node journal.
    pub node_id: NodeId,
    /// Authenticated native transport identity owned by the platform key store.
    pub endpoint_id: EndpointId,
    /// Whether the application-specific node runtime is currently installed.
    pub active: bool,
}

/// Platform-neutral lifecycle owner for a foreground embedded Myko node.
///
/// Myko owns the durable node identity, authenticated transport secret, Tokio
/// runtime, and serialized active state. The consuming application supplies
/// only the typed construction and shutdown of its application runtime.
pub struct EmbeddedNodeHost<Active> {
    data_dir: PathBuf,
    node_id: NodeId,
    identity: SecretKey,
    runtime: Runtime,
    active: Mutex<Option<Active>>,
}

impl<Active> EmbeddedNodeHost<Active> {
    /// Creates a fresh platform-owned transport identity and restores the
    /// durable Myko history identity below `data_dir`.
    ///
    /// # Errors
    ///
    /// Returns an error when durable storage or the native runtime cannot open.
    pub fn generate(data_dir: impl AsRef<Path>) -> Result<Self, EmbeddedNodeError> {
        Self::with_identity(data_dir, SecretKey::generate())
    }

    /// Restores a platform-owned transport identity and the durable Myko
    /// history identity below `data_dir`.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed identity bytes, unavailable durable
    /// storage, or failure to create the native runtime.
    pub fn from_identity_bytes(
        data_dir: impl AsRef<Path>,
        identity: &[u8],
    ) -> Result<Self, EmbeddedNodeError> {
        let bytes: [u8; IDENTITY_BYTES] = identity
            .try_into()
            .map_err(|_| EmbeddedNodeError::InvalidIdentity)?;
        Self::with_identity(data_dir, SecretKey::from_bytes(&bytes))
    }

    /// Restores an embedded node with an already parsed transport identity.
    ///
    /// # Errors
    ///
    /// Returns an error when durable storage or the native runtime cannot open.
    pub fn with_identity(
        data_dir: impl AsRef<Path>,
        identity: SecretKey,
    ) -> Result<Self, EmbeddedNodeError> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let node_id = Node::load_or_create_node_id(&data_dir)
            .map_err(|error| EmbeddedNodeError::Storage(error.to_string()))?;
        let runtime = Builder::new_multi_thread()
            .worker_threads(WORKER_THREADS)
            .enable_all()
            .thread_name("myko-embedded")
            .build()
            .map_err(|error| EmbeddedNodeError::Runtime(error.to_string()))?;
        Ok(Self {
            data_dir,
            node_id,
            identity,
            runtime,
            active: Mutex::new(None),
        })
    }

    /// Returns the transport secret for persistence in the platform key store.
    #[must_use]
    pub fn identity_bytes(&self) -> [u8; IDENTITY_BYTES] {
        self.identity.to_bytes()
    }

    /// Returns typed identity and best-effort foreground state without binding
    /// a transport.
    #[must_use]
    pub fn info(&self) -> EmbeddedNodeInfo {
        EmbeddedNodeInfo {
            node_id: self.node_id,
            endpoint_id: self.identity.public(),
            active: self.active.lock().is_ok_and(|active| active.is_some()),
        }
    }

    /// Installs the application-specific foreground runtime exactly once.
    ///
    /// The callback runs while the lifecycle lock is held so concurrent starts,
    /// stops, and node-bound operations cannot observe a partial transition.
    ///
    /// # Errors
    ///
    /// Returns an application-mapped framework error when lifecycle state is
    /// unavailable, or the callback's own application error.
    pub fn start_with<E>(
        &self,
        map_framework_error: impl Fn(EmbeddedNodeError) -> E,
        start: impl FnOnce(&Runtime, &Path, SecretKey) -> Result<Active, E>,
    ) -> Result<EmbeddedNodeInfo, E> {
        let mut active = self
            .active
            .lock()
            .map_err(|_| map_framework_error(EmbeddedNodeError::StateUnavailable))?;
        if active.is_none() {
            *active = Some(start(
                &self.runtime,
                self.data_dir.as_path(),
                self.identity.clone(),
            )?);
        }
        drop(active);
        Ok(EmbeddedNodeInfo {
            node_id: self.node_id,
            endpoint_id: self.identity.public(),
            active: true,
        })
    }

    /// Runs an operation against the active application runtime.
    ///
    /// # Errors
    ///
    /// Returns an application-mapped framework error while inactive or when
    /// lifecycle state is unavailable, or the callback's own application error.
    #[allow(clippy::significant_drop_tightening)] // The guard serializes operations with stop.
    pub fn with_active<T, E>(
        &self,
        map_framework_error: impl Fn(EmbeddedNodeError) -> E,
        operation: impl FnOnce(&Active, &Runtime) -> Result<T, E>,
    ) -> Result<T, E> {
        let active = self
            .active
            .lock()
            .map_err(|_| map_framework_error(EmbeddedNodeError::StateUnavailable))?;
        let active = active
            .as_ref()
            .ok_or_else(|| map_framework_error(EmbeddedNodeError::Inactive))?;
        operation(active, &self.runtime)
    }

    /// Removes and shuts down the current application runtime.
    ///
    /// # Errors
    ///
    /// Returns an application-mapped framework error when lifecycle state is
    /// unavailable, or the callback's own application error.
    pub fn stop_with<E>(
        &self,
        map_framework_error: impl Fn(EmbeddedNodeError) -> E,
        stop: impl FnOnce(Active, &Runtime) -> Result<(), E>,
    ) -> Result<(), E> {
        let active = self
            .active
            .lock()
            .map_err(|_| map_framework_error(EmbeddedNodeError::StateUnavailable))?
            .take();
        active.map_or_else(|| Ok(()), |active| stop(active, &self.runtime))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn platform_identity_and_application_lifecycle_are_independent() {
        let directory = tempfile::tempdir();
        assert!(directory.is_ok());
        if let Ok(directory) = directory {
            let host = EmbeddedNodeHost::<EndpointId>::generate(directory.path());
            assert!(host.is_ok());
            if let Ok(host) = host {
                let initial = host.info();
                assert!(!initial.active);
                let started = host.start_with(
                    |error| error,
                    |_runtime, path, identity| {
                        assert_eq!(path, directory.path());
                        Ok(identity.public())
                    },
                );
                assert!(started.is_ok_and(|info| info.active));
                let active = host.with_active(|error| error, |endpoint, _runtime| Ok(*endpoint));
                assert_eq!(active, Ok(initial.endpoint_id));
                assert_eq!(
                    host.stop_with(|error| error, |_active, _runtime| Ok(())),
                    Ok(())
                );
                assert!(!host.info().active);

                let restored = EmbeddedNodeHost::<()>::from_identity_bytes(
                    directory.path(),
                    &host.identity_bytes(),
                );
                assert!(restored.is_ok_and(|restored| {
                    let info = restored.info();
                    info.node_id == initial.node_id && info.endpoint_id == initial.endpoint_id
                }));
            }
        }
    }
}

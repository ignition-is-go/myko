use super::*;
/// ALPN used for framed Myko history pull and live-follow streams over Iroh.
pub const MYKO_REPLICATION_ALPN: &[u8] = b"myko/federation/7";
pub const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
const NATIVE_NODE_DESCRIPTOR_VERSION: u32 = 1;
pub const MAX_LIVE_TOPICS: usize = 256;
pub const MAX_LIVE_TOPIC_BYTES: usize = 512;
pub const MAX_SCOPE_CATALOG_PAGE: usize = 1_024;

/// Maps an authenticated Iroh endpoint to its transport-neutral principal ID.
#[must_use]
pub fn endpoint_principal_id(endpoint_id: EndpointId) -> PrincipalId {
    PrincipalId::new(format!("iroh:{endpoint_id}"))
}

/// Loads or creates one persistent Iroh transport identity.
///
/// The key is JSON encoded for compatibility with Iroh's serde contract. New
/// files are created with owner-only permissions on Unix and synchronized
/// before the identity is returned. Applications can use this for durable
/// short-lived client identities without adopting Myko's Redb node runtime.
///
/// # Errors
///
/// Returns an error if the parent directory or key file cannot be accessed, or
/// if an existing key is malformed.
pub fn load_or_create_secret_key(
    path: impl AsRef<Path>,
) -> Result<SecretKey, IrohReplicationError> {
    let path = path.as_ref();
    match fs::read(path) {
        Ok(encoded) => serde_json::from_slice(&encoded).map_err(IrohReplicationError::from),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => create_secret_key(path),
        Err(error) => Err(IrohReplicationError::Identity(error.to_string())),
    }
}

fn create_secret_key(path: &Path) -> Result<SecretKey, IrohReplicationError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .map_err(|error| IrohReplicationError::Identity(error.to_string()))?;
    }
    let secret = SecretKey::generate();
    let encoded = serde_json::to_vec(&secret)?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return load_or_create_secret_key(path);
        }
        Err(error) => return Err(IrohReplicationError::Identity(error.to_string())),
    };
    let result = file
        .write_all(&encoded)
        .and_then(|()| file.sync_all())
        .map_err(|error| IrohReplicationError::Identity(error.to_string()));
    if let Err(error) = result {
        drop(file);
        let _cleanup = fs::remove_file(path);
        return Err(error);
    }
    drop(file);
    Ok(secret)
}

/// Errors produced by the Iroh replication adapter.
#[derive(Debug, Error)]
pub enum IrohReplicationError {
    #[error("Iroh endpoint error: {0}")]
    Endpoint(String),
    #[error("Iroh stream error: {0}")]
    Stream(String),
    #[error("replication encoding error: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("replication ingest error: {0}")]
    Ingest(#[from] myko_federation::NodeError),
    #[error("replication cursor error: {0}")]
    Cursor(String),
    #[error("replication supervisor error: {0}")]
    Supervisor(String),
    #[error("Iroh identity error: {0}")]
    Identity(String),
    #[error("access denied: {message}")]
    Authorization {
        decision: Box<myko_federation::AuthorizationDecision>,
        message: String,
    },
}

#[must_use]
pub fn authorization_error(
    decision: Box<myko_federation::AuthorizationDecision>,
) -> IrohReplicationError {
    let message = decision.public_message();
    IrohReplicationError::Authorization { decision, message }
}

/// Pairing descriptor binding an Iroh endpoint to one Myko source history.
///
/// Endpoint identity authenticates the native transport. `node_id` identifies
/// the immutable Myko log expected behind it. Pairing and discovery layers can
/// choose any outer ticket encoding while preserving this distinction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeNodeDescriptor {
    pub version: u32,
    pub node_id: NodeId,
    pub endpoint: EndpointAddr,
}

impl NativeNodeDescriptor {
    /// Creates the current descriptor representation.
    #[must_use]
    pub const fn new(node_id: NodeId, endpoint: EndpointAddr) -> Self {
        Self {
            version: NATIVE_NODE_DESCRIPTOR_VERSION,
            node_id,
            endpoint,
        }
    }

    /// Validates this descriptor's version.
    ///
    /// # Errors
    ///
    /// Returns an error when the representation is not supported.
    pub fn validate(&self) -> Result<(), String> {
        if self.version != NATIVE_NODE_DESCRIPTOR_VERSION {
            return Err(format!(
                "unsupported native node descriptor version {}",
                self.version
            ));
        }
        Ok(())
    }
}

/// Versioned native bootstrap input for one identity-pinned peer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NativePeerReference(NativeNodeDescriptor);

impl NativePeerReference {
    /// Returns the authenticated Iroh endpoint to contact.
    #[must_use]
    pub const fn endpoint(&self) -> &EndpointAddr {
        &self.0.endpoint
    }

    /// Returns the pinned descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> &NativeNodeDescriptor {
        &self.0
    }

    /// Consumes this reference into its pinned descriptor.
    #[must_use]
    pub fn into_descriptor(self) -> NativeNodeDescriptor {
        self.0
    }
}

impl From<NativeNodeDescriptor> for NativePeerReference {
    fn from(descriptor: NativeNodeDescriptor) -> Self {
        Self(descriptor)
    }
}

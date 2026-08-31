use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use hmac::{Hmac, Mac};
use iroh::{
    Endpoint, EndpointId,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use tokio::{sync::watch, time::timeout};
use uuid::Uuid;

use crate::{IrohReplicationError, NativeNodeDescriptor};

/// Separate ALPN for bounded, one-use native pairing control.
pub const MYKO_PAIRING_ALPN: &[u8] = b"myko/pairing/1";

const PAIRING_VERSION: u32 = 1;
const PAIRING_SECRET_BYTES: usize = 32;
const PAIRING_NONCE_BYTES: usize = 16;
const PAIRING_PROOF_BYTES: usize = 32;
const MAX_PAIRING_FRAME_BYTES: usize = 64 * 1024;
const MAX_ACTIVE_INVITATIONS: usize = 256;
const MAX_PENDING_RECEIPTS: usize = 256;
const MIN_INVITATION_TTL: Duration = Duration::from_millis(1);
const MAX_INVITATION_TTL: Duration = Duration::from_hours(24);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

type HmacSha256 = Hmac<Sha256>;

/// Expiring one-use bearer ticket for an identity-pinned native node.
///
/// The secret is serialized for QR/file transport but deliberately redacted
/// from `Debug`. Servers retain only its SHA-256 verifier.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairingInvitation {
    pub version: u32,
    pub invitation_id: Uuid,
    pub server: NativeNodeDescriptor,
    pub expires_at_unix_ms: u64,
    secret_hex: String,
}

impl fmt::Debug for PairingInvitation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PairingInvitation")
            .field("version", &self.version)
            .field("invitation_id", &self.invitation_id)
            .field("server", &self.server)
            .field("expires_at_unix_ms", &self.expires_at_unix_ms)
            .field("secret_hex", &"[redacted]")
            .finish()
    }
}

impl PairingInvitation {
    /// Validates the version, descriptor, secret encoding, and wall-clock
    /// expiration before a network connection is attempted.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported, malformed, or expired invitations.
    pub fn validate(&self) -> Result<(), String> {
        if self.version != PAIRING_VERSION {
            return Err(format!(
                "unsupported pairing invitation version {}",
                self.version
            ));
        }
        self.server.validate()?;
        let secret = decode_exact_hex::<PAIRING_SECRET_BYTES>(&self.secret_hex, "pairing secret")?;
        if secret.iter().all(|byte| *byte == 0) {
            return Err("pairing secret cannot be all zeroes".to_owned());
        }
        if unix_time_ms().map_err(|error| error.to_string())? >= self.expires_at_unix_ms {
            return Err("pairing invitation has expired".to_owned());
        }
        Ok(())
    }
}

/// Mutually identity-bound result of redeeming one invitation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairingReceipt {
    pub version: u32,
    pub invitation_id: Uuid,
    pub server: NativeNodeDescriptor,
    pub client: NativeNodeDescriptor,
    pub comparison_code: String,
}

impl PairingReceipt {
    /// Validates the receipt schema and both bound descriptors.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported versions, malformed descriptors, a
    /// self-pairing endpoint, or an invalid comparison code.
    pub fn validate(&self) -> Result<(), String> {
        if self.version != PAIRING_VERSION {
            return Err(format!(
                "unsupported pairing receipt version {}",
                self.version
            ));
        }
        self.server.validate()?;
        self.client.validate()?;
        if self.server.endpoint.id == self.client.endpoint.id {
            return Err("pairing receipt binds one endpoint to itself".to_owned());
        }
        if !valid_comparison_code(&self.comparison_code) {
            return Err("pairing comparison code must contain six digits".to_owned());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PairingRequest {
    version: u32,
    invitation_id: Uuid,
    client: NativeNodeDescriptor,
    nonce_hex: String,
    proof_hex: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum PairingResponse {
    Accepted { receipt: PairingReceipt },
    Rejected { message: String },
}

#[derive(Debug, Serialize)]
struct PairingTranscript<'a> {
    version: u32,
    invitation_id: Uuid,
    server_node_id: String,
    server_endpoint_id: String,
    client_node_id: String,
    client_endpoint_id: String,
    nonce_hex: &'a str,
}

#[derive(Debug, Clone)]
struct InvitationRecord {
    server: NativeNodeDescriptor,
    verifier: [u8; PAIRING_SECRET_BYTES],
    expires_at: Instant,
}

#[derive(Debug, Default)]
struct PairingState {
    invitations: HashMap<Uuid, InvitationRecord>,
    receipts: VecDeque<PairingReceipt>,
}

#[derive(Debug, Clone)]
pub struct PairingRegistry {
    state: Arc<Mutex<PairingState>>,
    revision: watch::Sender<u64>,
}

impl PairingRegistry {
    pub(crate) fn new() -> Self {
        let (revision, _) = watch::channel(0_u64);
        Self {
            state: Arc::new(Mutex::new(PairingState::default())),
            revision,
        }
    }

    pub(crate) fn issue(
        &self,
        server: NativeNodeDescriptor,
        ttl: Duration,
    ) -> Result<PairingInvitation, IrohReplicationError> {
        server.validate().map_err(IrohReplicationError::Identity)?;
        if ttl < MIN_INVITATION_TTL || ttl > MAX_INVITATION_TTL {
            return Err(IrohReplicationError::Identity(format!(
                "pairing invitation TTL must be between {MIN_INVITATION_TTL:?} and {MAX_INVITATION_TTL:?}"
            )));
        }
        let expires_at = Instant::now().checked_add(ttl).ok_or_else(|| {
            IrohReplicationError::Identity("pairing invitation TTL overflowed".to_owned())
        })?;
        let ttl_ms = u64::try_from(ttl.as_millis()).map_err(|error| {
            IrohReplicationError::Identity(format!("pairing invitation TTL is invalid: {error}"))
        })?;
        let expires_at_unix_ms = unix_time_ms()?.checked_add(ttl_ms).ok_or_else(|| {
            IrohReplicationError::Identity("pairing invitation expiry overflowed".to_owned())
        })?;
        let mut secret = [0_u8; PAIRING_SECRET_BYTES];
        getrandom::fill(&mut secret).map_err(|error| {
            IrohReplicationError::Identity(format!("failed to create pairing secret: {error}"))
        })?;
        let verifier: [u8; PAIRING_SECRET_BYTES] = Sha256::digest(secret).into();
        let invitation_id = Uuid::new_v4();
        let mut state = self
            .state
            .lock()
            .map_err(|_| IrohReplicationError::Identity("pairing state is poisoned".to_owned()))?;
        let now = Instant::now();
        state
            .invitations
            .retain(|_, invitation| invitation.expires_at > now);
        if state.invitations.len() >= MAX_ACTIVE_INVITATIONS {
            return Err(IrohReplicationError::Identity(format!(
                "pairing invitation limit {MAX_ACTIVE_INVITATIONS} reached"
            )));
        }
        state.invitations.insert(
            invitation_id,
            InvitationRecord {
                server: server.clone(),
                verifier,
                expires_at,
            },
        );
        drop(state);
        Ok(PairingInvitation {
            version: PAIRING_VERSION,
            invitation_id,
            server,
            expires_at_unix_ms,
            secret_hex: hex::encode(secret),
        })
    }

    pub(crate) fn take_receipts(&self) -> Result<Vec<PairingReceipt>, IrohReplicationError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| IrohReplicationError::Identity("pairing state is poisoned".to_owned()))?;
        Ok(state.receipts.drain(..).collect())
    }

    pub(crate) fn subscribe(&self) -> PairingReceiptSubscription {
        PairingReceiptSubscription {
            registry: self.clone(),
            revision: self.revision.subscribe(),
        }
    }

    fn redeem(
        &self,
        remote_id: EndpointId,
        request: PairingRequest,
    ) -> Result<PairingReceipt, String> {
        if request.version != PAIRING_VERSION {
            return Err(format!(
                "unsupported pairing request version {}",
                request.version
            ));
        }
        request.client.validate()?;
        if request.client.endpoint.id != remote_id {
            return Err(
                "pairing client descriptor does not match authenticated endpoint".to_owned(),
            );
        }
        let nonce = decode_exact_hex::<PAIRING_NONCE_BYTES>(&request.nonce_hex, "pairing nonce")?;
        if nonce.iter().all(|byte| *byte == 0) {
            return Err("pairing nonce cannot be all zeroes".to_owned());
        }
        let proof = decode_exact_hex::<PAIRING_PROOF_BYTES>(&request.proof_hex, "pairing proof")?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| "pairing state is poisoned".to_owned())?;
        let record = state
            .invitations
            .get(&request.invitation_id)
            .cloned()
            .ok_or_else(|| "pairing invitation is unknown or already used".to_owned())?;
        if record.expires_at <= Instant::now() {
            state.invitations.remove(&request.invitation_id);
            return Err("pairing invitation has expired".to_owned());
        }
        let transcript = pairing_transcript(
            request.invitation_id,
            &record.server,
            &request.client,
            &request.nonce_hex,
        )?;
        let mut authenticator = HmacSha256::new_from_slice(&record.verifier)
            .map_err(|error| format!("pairing verifier is invalid: {error}"))?;
        authenticator.update(&transcript);
        authenticator
            .verify_slice(&proof)
            .map_err(|_| "pairing proof did not verify".to_owned())?;
        if state.receipts.len() >= MAX_PENDING_RECEIPTS {
            return Err(format!(
                "pending pairing receipt limit {MAX_PENDING_RECEIPTS} reached"
            ));
        }
        let comparison_code = comparison_code(&record.verifier, &transcript)?;
        let receipt = PairingReceipt {
            version: PAIRING_VERSION,
            invitation_id: request.invitation_id,
            server: record.server,
            client: request.client,
            comparison_code,
        };
        state.invitations.remove(&request.invitation_id);
        state.receipts.push_back(receipt.clone());
        drop(state);
        let next = self.revision.borrow().wrapping_add(1);
        self.revision.send_replace(next);
        Ok(receipt)
    }
}

/// Lossless wake-up plus bounded pending-receipt drain for a pairing operator.
#[derive(Debug)]
pub struct PairingReceiptSubscription {
    registry: PairingRegistry,
    revision: watch::Receiver<u64>,
}

impl PairingReceiptSubscription {
    /// Waits for and drains one or more successfully authenticated receipts.
    ///
    /// # Errors
    ///
    /// Returns an error if shared state is poisoned or the endpoint closes.
    pub async fn recv(&mut self) -> Result<Vec<PairingReceipt>, IrohReplicationError> {
        loop {
            let receipts = self.registry.take_receipts()?;
            if !receipts.is_empty() {
                return Ok(receipts);
            }
            self.revision.changed().await.map_err(|_| {
                IrohReplicationError::Identity("pairing receipt stream closed".to_owned())
            })?;
        }
    }
}

#[derive(Debug, Clone)]
pub struct PairingProtocol {
    registry: PairingRegistry,
}

impl PairingProtocol {
    pub(crate) const fn new(registry: PairingRegistry) -> Self {
        Self { registry }
    }

    async fn handle(&self, connection: Connection) -> Result<(), AcceptError> {
        let remote_id = connection.remote_id();
        let (mut send, mut receive) = connection.accept_bi().await?;
        let request = read_bounded_json::<PairingRequest>(&mut receive)
            .await
            .map_err(AcceptError::from_err)?;
        let response = match self.registry.redeem(remote_id, request) {
            Ok(receipt) => PairingResponse::Accepted { receipt },
            Err(message) => PairingResponse::Rejected { message },
        };
        write_bounded_json(&mut send, &response)
            .await
            .map_err(AcceptError::from_err)?;
        send.finish().map_err(AcceptError::from_err)?;
        connection.closed().await;
        Ok(())
    }
}

impl ProtocolHandler for PairingProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        timeout(HANDSHAKE_TIMEOUT, self.handle(connection))
            .await
            .map_err(|_| AcceptError::from_err(std::io::Error::other("pairing timed out")))?
    }
}

pub async fn redeem_pairing(
    endpoint: &Endpoint,
    client: NativeNodeDescriptor,
    invitation: &PairingInvitation,
) -> Result<PairingReceipt, IrohReplicationError> {
    invitation
        .validate()
        .map_err(IrohReplicationError::Identity)?;
    client.validate().map_err(IrohReplicationError::Identity)?;
    let secret = decode_exact_hex::<PAIRING_SECRET_BYTES>(&invitation.secret_hex, "pairing secret")
        .map_err(IrohReplicationError::Identity)?;
    let verifier: [u8; PAIRING_SECRET_BYTES] = Sha256::digest(secret).into();
    let mut nonce = [0_u8; PAIRING_NONCE_BYTES];
    getrandom::fill(&mut nonce).map_err(|error| {
        IrohReplicationError::Identity(format!("failed to create pairing nonce: {error}"))
    })?;
    let nonce_hex = hex::encode(nonce);
    let transcript = pairing_transcript(
        invitation.invitation_id,
        &invitation.server,
        &client,
        &nonce_hex,
    )
    .map_err(IrohReplicationError::Identity)?;
    let mut authenticator = HmacSha256::new_from_slice(&verifier).map_err(|error| {
        IrohReplicationError::Identity(format!("pairing verifier is invalid: {error}"))
    })?;
    authenticator.update(&transcript);
    let request = PairingRequest {
        version: PAIRING_VERSION,
        invitation_id: invitation.invitation_id,
        client: client.clone(),
        nonce_hex,
        proof_hex: hex::encode(authenticator.finalize().into_bytes()),
    };
    let receipt = timeout(HANDSHAKE_TIMEOUT, async {
        let connection = endpoint
            .connect(invitation.server.endpoint.clone(), MYKO_PAIRING_ALPN)
            .await
            .map_err(|error| IrohReplicationError::Endpoint(error.to_string()))?;
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .map_err(|error| IrohReplicationError::Stream(error.to_string()))?;
        write_bounded_json(&mut send, &request).await?;
        send.finish()
            .map_err(|error| IrohReplicationError::Stream(error.to_string()))?;
        let response = read_bounded_json::<PairingResponse>(&mut receive).await?;
        connection.close(0u32.into(), b"pairing complete");
        match response {
            PairingResponse::Accepted { receipt } => Ok(receipt),
            PairingResponse::Rejected { message } => Err(IrohReplicationError::Identity(message)),
        }
    })
    .await
    .map_err(|_| IrohReplicationError::Identity("pairing timed out".to_owned()))??;
    receipt.validate().map_err(IrohReplicationError::Identity)?;
    if receipt.invitation_id != invitation.invitation_id
        || receipt.server != invitation.server
        || receipt.client != client
    {
        return Err(IrohReplicationError::Identity(
            "pairing receipt did not match the invitation transcript".to_owned(),
        ));
    }
    Ok(receipt)
}

fn pairing_transcript(
    invitation_id: Uuid,
    server: &NativeNodeDescriptor,
    client: &NativeNodeDescriptor,
    nonce_hex: &str,
) -> Result<Vec<u8>, String> {
    serde_json::to_vec(&PairingTranscript {
        version: PAIRING_VERSION,
        invitation_id,
        server_node_id: server.node_id.to_string(),
        server_endpoint_id: server.endpoint.id.to_string(),
        client_node_id: client.node_id.to_string(),
        client_endpoint_id: client.endpoint.id.to_string(),
        nonce_hex,
    })
    .map_err(|error| error.to_string())
}

fn comparison_code(key: &[u8], transcript: &[u8]) -> Result<String, String> {
    let mut authenticator = HmacSha256::new_from_slice(key)
        .map_err(|error| format!("pairing verifier is invalid: {error}"))?;
    authenticator.update(b"myko-pairing-comparison");
    authenticator.update(transcript);
    let output = authenticator.finalize().into_bytes();
    let prefix: [u8; 4] = output
        .get(..4)
        .ok_or_else(|| "pairing comparison output is truncated".to_owned())?
        .try_into()
        .map_err(|error| format!("pairing comparison prefix is invalid: {error}"))?;
    let value = u32::from_be_bytes(prefix)
        .checked_rem(1_000_000)
        .ok_or_else(|| "pairing comparison modulus is invalid".to_owned())?;
    Ok(format!("{value:06}"))
}

fn valid_comparison_code(code: &str) -> bool {
    code.len() == 6 && code.bytes().all(|byte| byte.is_ascii_digit())
}

fn decode_exact_hex<const N: usize>(encoded: &str, name: &str) -> Result<[u8; N], String> {
    let decoded = hex::decode(encoded).map_err(|error| format!("invalid {name}: {error}"))?;
    decoded
        .try_into()
        .map_err(|decoded: Vec<u8>| format!("{name} must contain {N} bytes, got {}", decoded.len()))
}

fn unix_time_ms() -> Result<u64, IrohReplicationError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            IrohReplicationError::Identity(format!("system clock is before Unix epoch: {error}"))
        })?;
    u64::try_from(duration.as_millis()).map_err(|error| {
        IrohReplicationError::Identity(format!("system time is out of range: {error}"))
    })
}

async fn write_bounded_json<T: Serialize + Sync>(
    send: &mut iroh::endpoint::SendStream,
    value: &T,
) -> Result<(), IrohReplicationError> {
    let encoded = serde_json::to_vec(value)?;
    if encoded.len() > MAX_PAIRING_FRAME_BYTES {
        return Err(IrohReplicationError::Stream(format!(
            "pairing frame exceeds {MAX_PAIRING_FRAME_BYTES} bytes"
        )));
    }
    send.write_all(&encoded)
        .await
        .map_err(|error| IrohReplicationError::Stream(error.to_string()))
}

async fn read_bounded_json<T: DeserializeOwned>(
    receive: &mut iroh::endpoint::RecvStream,
) -> Result<T, IrohReplicationError> {
    let encoded = receive
        .read_to_end(MAX_PAIRING_FRAME_BYTES)
        .await
        .map_err(|error| IrohReplicationError::Stream(error.to_string()))?;
    serde_json::from_slice(&encoded).map_err(IrohReplicationError::from)
}

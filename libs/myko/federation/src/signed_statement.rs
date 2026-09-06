use serde::{Deserialize, Serialize};

use crate::RetainedHistoryStatement;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum SignedStatementFormat {
    #[serde(rename = "ed25519_statement_v1")]
    Ed25519StatementV1,
}

/// Transport-neutral Ed25519 signed retained-history statement bytes.
///
/// Construction and deserialization enforce the encoding shape, not signature
/// validity, signer authority, persistence, or custody.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedRetainedHistoryStatement {
    format: SignedStatementFormat,
    statement: RetainedHistoryStatement,
    signer: [u8; 32],
    #[serde(with = "signature_bytes")]
    signature: [u8; 64],
}

impl SignedRetainedHistoryStatement {
    /// Construct a container from unverified signer and signature bytes.
    ///
    /// Callers must verify the signature and every authority or persistence
    /// precondition before relying on this value.
    #[must_use]
    pub const fn from_signature(
        statement: RetainedHistoryStatement,
        signer: [u8; 32],
        signature: [u8; 64],
    ) -> Self {
        Self {
            format: SignedStatementFormat::Ed25519StatementV1,
            statement,
            signer,
            signature,
        }
    }

    #[must_use]
    pub const fn statement(&self) -> &RetainedHistoryStatement {
        &self.statement
    }

    #[must_use]
    pub const fn signer(&self) -> &[u8; 32] {
        &self.signer
    }

    #[must_use]
    pub const fn signature(&self) -> &[u8; 64] {
        &self.signature
    }
}

pub mod signature_bytes {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    pub fn serialize<S>(signature: &[u8; 64], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(signature)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[u8; 64], D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<u8>::deserialize(deserializer)?
            .try_into()
            .map_err(|bytes: Vec<u8>| {
                D::Error::custom(format!(
                    "Ed25519 signature must be 64 bytes, received {}",
                    bytes.len()
                ))
            })
    }
}

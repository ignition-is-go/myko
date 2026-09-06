use iroh::Signature;
use myko_federation::{RetainedHistoryStatement, SignedRetainedHistoryStatement};
use thiserror::Error;

use crate::{NativeNodeDescriptor, SecretKey};

#[derive(Debug, Error)]
pub enum RetainedHistorySignatureError {
    #[error("retained-history statement encoding failed: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("the expected node descriptor has an unsupported version")]
    InvalidDescriptor,
    #[error("the retained-history signer does not match the trusted endpoint")]
    UnexpectedSigner,
    #[error("the retained-history holder does not match the trusted node")]
    UnexpectedHolder,
    #[error("the signed history or obligation does not match the expected statement")]
    UnexpectedStatement,
    #[error("the retained-history signature is invalid")]
    InvalidSignature,
}

/// Sign an assertion with the existing native transport key.
///
/// This low-level operation does not read a journal or authorize custody.
/// Its result must not be advertised as a durable receipt merely because
/// the signature verifies.
///
/// # Errors
///
/// Returns an error if the statement cannot be encoded.
pub fn sign_retained_history_statement(
    statement: RetainedHistoryStatement,
    secret: &SecretKey,
) -> Result<SignedRetainedHistoryStatement, RetainedHistorySignatureError> {
    let signature = secret.sign(&statement.signing_bytes()?);
    Ok(SignedRetainedHistoryStatement::from_signature(
        statement,
        *secret.public().as_bytes(),
        signature.to_bytes(),
    ))
}

/// Verify against independently trusted identity and obligation inputs.
///
/// Obtain the descriptor from an authenticated node/key binding, and derive
/// the expected statement from the intended obligation and history. Do not
/// use fields from this message as their own trust source. This verifies an
/// assertion only, not membership, persistence, availability, or safe leave.
///
/// # Errors
///
/// Rejects mismatched identities or statement context, unsupported descriptor
/// versions, malformed encodings, and invalid Ed25519 signatures.
pub fn verify_retained_history_statement(
    signed: &SignedRetainedHistoryStatement,
    expected_holder: &NativeNodeDescriptor,
    expected_statement: &RetainedHistoryStatement,
) -> Result<(), RetainedHistorySignatureError> {
    expected_holder
        .validate()
        .map_err(|_| RetainedHistorySignatureError::InvalidDescriptor)?;
    if signed.signer() != expected_holder.endpoint.id.as_bytes() {
        return Err(RetainedHistorySignatureError::UnexpectedSigner);
    }
    if signed.statement().holder() != expected_holder.node_id {
        return Err(RetainedHistorySignatureError::UnexpectedHolder);
    }
    if signed.statement() != expected_statement {
        return Err(RetainedHistorySignatureError::UnexpectedStatement);
    }
    expected_holder
        .endpoint
        .id
        .verify(
            &signed.statement().signing_bytes()?,
            &Signature::from_bytes(signed.signature()),
        )
        .map_err(|_| RetainedHistorySignatureError::InvalidSignature)
}

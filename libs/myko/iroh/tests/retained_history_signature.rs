use std::{error::Error, sync::Arc};

use myko_federation::{
    AllowAllAccessPolicy, AuthorityPresentation, CommandId, CommandRequest, EventId, LogPosition,
    Node, NodeId, PrincipalId, RetainedHistoryStatement, ScopeId, ScopeSelection,
    SelectedHistorySnapshot, ServiceId, SignedRetainedHistoryStatement, StorageIncarnationId,
};
use myko_iroh::{
    EndpointAddr, NativeNodeDescriptor, RetainedHistorySignatureError, SecretKey,
    sign_retained_history_statement, verify_retained_history_statement,
};

type TestResult = Result<(), Box<dyn Error>>;

fn statement() -> Result<RetainedHistoryStatement, Box<dyn Error>> {
    let node = Node::in_memory();
    let policy = Arc::new(AllowAllAccessPolicy);
    node.set_command_access_policy(policy.clone())?;
    let scope = ScopeId::new("statement:scope");
    let principal = PrincipalId::new("test:statement");
    node.admit(CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new("statement"),
        scope_id: scope.clone(),
        principal_id: principal.clone(),
        authority: AuthorityPresentation::direct_node(principal),
        resource_claims: Vec::new(),
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "statement.record".to_owned(),
        payload: vec![1, 2, 3],
    })?;
    let manifest = SelectedHistorySnapshot::current(&node)?
        .retained_manifest(&ScopeSelection::Exact(scope))?;
    if manifest.events().is_empty() {
        return Err("statement fixture did not retain any history".into());
    }
    let statement = RetainedHistoryStatement::new(
        node.node_id(),
        StorageIncarnationId::new(),
        EventId::new(NodeId::new(), LogPosition::new(1)),
        &manifest,
    )?;
    drop(policy);
    Ok(statement)
}

fn descriptor(statement: &RetainedHistoryStatement, key: &SecretKey) -> NativeNodeDescriptor {
    NativeNodeDescriptor::new(statement.holder(), EndpointAddr::new(key.public()))
}

#[test]
fn signature_roundtrip_requires_independently_expected_identity_and_statement() -> TestResult {
    let statement = statement()?;
    let key = SecretKey::generate();
    let trusted = descriptor(&statement, &key);
    let signed = sign_retained_history_statement(statement.clone(), &key)?;
    let decoded: SignedRetainedHistoryStatement =
        serde_json::from_slice(&serde_json::to_vec(&signed)?)?;
    verify_retained_history_statement(&decoded, &trusted, &statement)?;

    let stranger = SecretKey::generate();
    let impostor = sign_retained_history_statement(statement.clone(), &stranger)?;
    if !matches!(
        verify_retained_history_statement(&impostor, &trusted, &statement),
        Err(RetainedHistorySignatureError::UnexpectedSigner)
    ) {
        return Err("a valid signature from an untrusted endpoint was accepted".into());
    }
    let other_holder = NativeNodeDescriptor::new(NodeId::new(), trusted.endpoint.clone());
    if !matches!(
        verify_retained_history_statement(&decoded, &other_holder, &statement),
        Err(RetainedHistorySignatureError::UnexpectedHolder)
    ) {
        return Err("a trusted key was allowed to substitute another node identity".into());
    }
    let invalid_descriptor = NativeNodeDescriptor {
        version: 0,
        ..trusted
    };
    if !matches!(
        verify_retained_history_statement(&decoded, &invalid_descriptor, &statement),
        Err(RetainedHistorySignatureError::InvalidDescriptor)
    ) {
        return Err("an unsupported descriptor was accepted".into());
    }
    Ok(())
}

#[test]
fn every_signed_context_field_rejects_tampering() -> TestResult {
    let statement = statement()?;
    let key = SecretKey::generate();
    let trusted = descriptor(&statement, &key);
    let signed = sign_retained_history_statement(statement.clone(), &key)?;
    let mut changed_digest = serde_json::to_value(statement.commitment())?;
    changed_digest
        .as_object_mut()
        .ok_or("commitment is not an object")?
        .insert("digest".to_owned(), serde_json::to_value([0_u8; 32])?);
    let mut changed_count = serde_json::to_value(statement.commitment())?;
    changed_count
        .as_object_mut()
        .ok_or("commitment is not an object")?
        .insert("event_count".to_owned(), serde_json::json!(0));
    let replacements = [
        ("holder", serde_json::to_value(NodeId::new())?),
        (
            "storage_incarnation",
            serde_json::to_value(StorageIncarnationId::new())?,
        ),
        (
            "obligation",
            serde_json::to_value(EventId::new(NodeId::new(), LogPosition::new(2)))?,
        ),
        (
            "selection",
            serde_json::to_value(ScopeSelection::Exact(ScopeId::new("other:scope")))?,
        ),
        ("commitment", changed_digest),
        ("commitment", changed_count),
    ];
    for (field, replacement) in replacements {
        let mut encoded = serde_json::to_value(&signed)?;
        let payload = encoded
            .get_mut("statement")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or("signed statement is not an object")?;
        payload.insert(field.to_owned(), replacement);
        let tampered: SignedRetainedHistoryStatement = serde_json::from_value(encoded)?;
        if verify_retained_history_statement(&tampered, &trusted, &statement).is_ok() {
            return Err(
                format!("altering {field} bypassed expected-statement verification").into(),
            );
        }
        let alleged = tampered.statement();
        let alleged_holder = descriptor(alleged, &key);
        if !matches!(
            verify_retained_history_statement(&tampered, &alleged_holder, alleged),
            Err(RetainedHistorySignatureError::InvalidSignature)
        ) {
            return Err(format!("altering {field} was not bound by the signature").into());
        }
    }
    Ok(())
}

#[test]
fn signature_from_another_statement_cannot_be_transplanted() -> TestResult {
    let original = statement()?;
    let different = statement()?;
    let key = SecretKey::generate();
    let signed = sign_retained_history_statement(original.clone(), &key)?;
    let other = sign_retained_history_statement(different, &key)?;
    let mut encoded = serde_json::to_value(&signed)?;
    let replacement = serde_json::to_value(&other)?
        .get("signature")
        .cloned()
        .ok_or("signature missing from encoded statement")?;
    encoded
        .as_object_mut()
        .ok_or("signed statement is not an object")?
        .insert("signature".to_owned(), replacement);
    let tampered: SignedRetainedHistoryStatement = serde_json::from_value(encoded)?;
    if !matches!(
        verify_retained_history_statement(&tampered, &descriptor(&original, &key), &original),
        Err(RetainedHistorySignatureError::InvalidSignature)
    ) {
        return Err("signature transplantation was accepted".into());
    }
    Ok(())
}

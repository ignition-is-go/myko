use std::{error::Error, sync::Arc};

use myko_federation::*;

type TestResult = Result<(), Box<dyn Error>>;

fn allow(node: &Node) -> Result<Arc<dyn AccessPolicy>, NodeError> {
    let policy: Arc<dyn AccessPolicy> = Arc::new(AllowAllAccessPolicy);
    node.set_command_access_policy(policy.clone())?;
    Ok(policy)
}

fn request(scope: &ScopeId, claims: Vec<ResourceClaim>) -> CommandRequest {
    let principal = PrincipalId::new("test:manifest");
    CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new("manifest"),
        scope_id: scope.clone(),
        principal_id: principal.clone(),
        authority: AuthorityPresentation::direct_node(principal),
        resource_claims: claims,
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "manifest.change".to_owned(),
        payload: Vec::new(),
    }
}

fn mutation(scope: &ScopeId, operation: MutationOperation) -> ItemMutation {
    ItemMutation {
        service_id: "manifest".to_owned(),
        item_type: "Record".to_owned(),
        item_id: "record".to_owned(),
        schema_version: 1,
        roots_scope: false,
        belongs_to: None,
        scope_id: Some(scope.as_str().to_owned()),
        operation,
        payload: (operation == MutationOperation::Set).then(|| b"{}".to_vec()),
    }
}

fn commit(
    node: &Node,
    scope: &ScopeId,
    changes: Vec<ItemMutation>,
    extra_parent: Option<EventId>,
    claims: Vec<ResourceClaim>,
) -> Result<EventId, NodeError> {
    let request = request(scope, claims);
    let admission = node.admit(request.clone())?;
    let mut parents = vec![admission.snapshot().updated_at];
    parents.extend(extra_parent);
    Ok(node
        .commit(
            request.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: request.id,
                service_id: request.service_id,
                scope_id: scope.clone(),
                causal_parents: parents,
                changes,
            },
            b"result".to_vec(),
        )?
        .updated_at)
}

#[test]
fn fixed_cut_keeps_lifecycle_and_replaced_deleted_history() -> TestResult {
    let node = Node::in_memory();
    let _policy = allow(&node)?;
    let scope = ScopeId::new("manifest:one");
    commit(
        &node,
        &scope,
        vec![mutation(&scope, MutationOperation::Set)],
        None,
        Vec::new(),
    )?;
    let cut = node.local_history_cut()?;
    let original = node.events_after(None)?;
    commit(
        &node,
        &scope,
        vec![mutation(&scope, MutationOperation::Delete)],
        None,
        Vec::new(),
    )?;
    let selection = ScopeSelection::Exact(scope);
    let old = SelectedHistorySnapshot::at(&node, cut)?.retained_manifest(&selection)?;
    if old.through() != cut
        || old.events().len() != original.len()
        || original.iter().any(|event| !old.events().contains(event))
        || !old
            .events()
            .iter()
            .any(|event| matches!(event.event, NodeEvent::CommandLifecycle(_)))
        || old.events().iter().any(|event| {
            matches!(&event.event,
            NodeEvent::CommandCommitted { batch, .. }
            if batch.changes.iter().any(|change| change.operation == MutationOperation::Delete))
        })
    {
        return Err("fixed-cut manifest lost lifecycle history or included a later delete".into());
    }
    let current = SelectedHistorySnapshot::current(&node)?.retained_manifest(&selection)?;
    let accepted = node.events_after(None)?;
    if current.events().len() != accepted.len()
        || accepted
            .iter()
            .any(|event| !current.events().contains(event))
    {
        return Err(
            "current manifest lost accepted bodies from replaced or deleted history".into(),
        );
    }
    if !current.events().iter().any(|event| {
        matches!(&event.event,
        NodeEvent::CommandCommitted { batch, .. }
        if batch.changes.iter().any(|change| change.operation == MutationOperation::Delete))
    }) {
        return Err("current manifest omitted retained deletion history".into());
    }
    Ok(())
}

#[test]
fn pending_and_external_dependencies_are_never_silently_omitted() -> TestResult {
    let source = Node::in_memory();
    let _source_policy = allow(&source)?;
    let selected = ScopeId::new("manifest:selected");
    source.admit(request(&selected, Vec::new()))?;
    let dependency = source
        .events_after(None)?
        .into_iter()
        .next()
        .ok_or("dependency source emitted no event")?;
    let node = Node::in_memory();
    let _policy = allow(&node)?;
    commit(
        &node,
        &selected,
        vec![mutation(&selected, MutationOperation::Set)],
        Some(dependency.origin),
        Vec::new(),
    )?;
    let selection = ScopeSelection::Exact(selected);
    if !matches!(
        SelectedHistorySnapshot::current(&node)?.retained_manifest(&selection),
        Err(SelectedHistoryManifestError::PendingHistory(_))
    ) {
        return Err("selected pending history produced a manifest".into());
    }
    node.ingest(dependency)?;
    if SelectedHistorySnapshot::current(&node)?
        .retained_manifest(&selection)
        .is_err()
    {
        return Err("same-scope dependency did not release the manifest".into());
    }

    let empty = Node::in_memory();
    let _empty_policy = allow(&empty)?;
    let manifest = SelectedHistorySnapshot::current(&empty)?.retained_manifest(&selection)?;
    if manifest.through().is_some() || !manifest.events().is_empty() {
        return Err("empty node did not yield an empty None-cut manifest".into());
    }
    Ok(())
}

#[test]
fn external_dependency_and_unrelated_pending_history_are_distinguished() -> TestResult {
    let dependency_source = Node::in_memory();
    let _source_policy = allow(&dependency_source)?;
    let outside = ScopeId::new("manifest:outside");
    dependency_source.admit(request(&outside, Vec::new()))?;
    let dependency = dependency_source
        .events_after(None)?
        .into_iter()
        .next()
        .ok_or("external dependency source emitted no event")?;

    let node = Node::in_memory();
    let _policy = allow(&node)?;
    let selected = ScopeId::new("manifest:selected");
    commit(
        &node,
        &selected,
        vec![mutation(&selected, MutationOperation::Set)],
        Some(dependency.origin),
        Vec::new(),
    )?;
    node.ingest(dependency)?;
    let selection = ScopeSelection::Exact(selected);
    if !matches!(
        SelectedHistorySnapshot::current(&node)?.retained_manifest(&selection),
        Err(SelectedHistoryManifestError::MissingDependency { .. })
    ) {
        return Err("external causal dependency was silently omitted".into());
    }

    let independent = Node::in_memory();
    let _independent_policy = allow(&independent)?;
    commit(
        &independent,
        selection.root(),
        vec![mutation(selection.root(), MutationOperation::Set)],
        None,
        Vec::new(),
    )?;
    commit(
        &independent,
        &outside,
        vec![mutation(&outside, MutationOperation::Set)],
        Some(EventId::new(NodeId::new(), LogPosition::new(91))),
        Vec::new(),
    )?;
    SelectedHistorySnapshot::current(&independent)?.retained_manifest(&selection)?;
    Ok(())
}

#[test]
fn subtree_blocks_ordinary_pending_history_with_unknown_topology() -> TestResult {
    let node = Node::in_memory();
    let _policy = allow(&node)?;
    let child = ScopeId::new("manifest:unknown-child");
    commit(
        &node,
        &child,
        vec![mutation(&child, MutationOperation::Set)],
        Some(EventId::new(NodeId::new(), LogPosition::new(77))),
        Vec::new(),
    )?;
    if !matches!(
        SelectedHistorySnapshot::current(&node)?
            .retained_manifest(&ScopeSelection::Subtree(ScopeId::new("manifest:root"))),
        Err(SelectedHistoryManifestError::PendingHistory(_))
    ) {
        return Err("subtree manifest guessed that unknown pending topology was unrelated".into());
    }
    Ok(())
}

#[test]
fn manifest_retains_the_implicit_same_author_predecessor() -> TestResult {
    let node = Node::in_memory();
    let _policy = allow(&node)?;
    let scope = ScopeId::new("manifest:author-stream");
    let predecessor = commit(
        &node,
        &scope,
        vec![mutation(&scope, MutationOperation::Set)],
        None,
        Vec::new(),
    )?;
    let successor = commit(
        &node,
        &scope,
        vec![mutation(&scope, MutationOperation::Set)],
        None,
        Vec::new(),
    )?;
    let manifest = SelectedHistorySnapshot::current(&node)?
        .retained_manifest(&ScopeSelection::Exact(scope))?;
    let commits = manifest
        .events()
        .iter()
        .filter_map(|event| match &event.event {
            NodeEvent::CommandCommitted { batch, .. } => Some((event.origin, batch)),
            NodeEvent::CommandLifecycle(_) | NodeEvent::FrameworkControl(_) => None,
        })
        .collect::<Vec<_>>();
    let predecessor_index = commits
        .iter()
        .position(|(origin, _)| *origin == predecessor);
    let successor_index = commits.iter().position(|(origin, _)| *origin == successor);
    let successor_declares_predecessor = commits
        .iter()
        .any(|(origin, batch)| *origin == successor && batch.causal_parents.contains(&predecessor));
    if predecessor_index.is_none()
        || successor_index.is_none()
        || predecessor_index >= successor_index
        || successor_declares_predecessor
    {
        return Err("manifest did not retain the inferred author predecessor".into());
    }
    Ok(())
}

#[test]
fn exact_child_rejects_parent_spanning_establishment_but_parent_subtree_accepts_it() -> TestResult {
    let node = Node::in_memory();
    let _policy = allow(&node)?;
    let parent_entity = EntityRef::new("manifest", "Parent", "parent");
    let parent = ScopeId::for_entity(&parent_entity);
    let child = ScopeId::for_parts("manifest", "Root", "child");
    let command = request(
        &child,
        vec![ResourceClaim::scope(
            parent.clone(),
            ResourceClaimKind::Affected,
        )],
    );
    let admission = node.admit(command.clone())?;
    node.commit(
        command.id,
        ChangeBatch {
            id: BatchId::new(),
            command_id: command.id,
            service_id: command.service_id,
            scope_id: child.clone(),
            causal_parents: vec![admission.snapshot().updated_at],
            changes: vec![ItemMutation {
                service_id: "manifest".to_owned(),
                item_type: "Root".to_owned(),
                item_id: "child".to_owned(),
                schema_version: 1,
                roots_scope: true,
                belongs_to: Some(parent_entity),
                scope_id: Some(child.as_str().to_owned()),
                operation: MutationOperation::Set,
                payload: Some(b"{}".to_vec()),
            }],
        },
        Vec::new(),
    )?;
    let snapshot = SelectedHistorySnapshot::current(&node)?;
    if !matches!(
        snapshot.retained_manifest(&ScopeSelection::Exact(child)),
        Err(SelectedHistoryManifestError::OutsideSelection { scope, .. }) if scope == parent
    ) {
        return Err("exact child manifest omitted its establishment parent".into());
    }
    snapshot.retained_manifest(&ScopeSelection::Subtree(parent))?;
    Ok(())
}

#[test]
fn retained_scopes_ignore_read_claims_and_reject_atomic_write_crossings() -> TestResult {
    let node = Node::in_memory();
    let _policy = allow(&node)?;
    let selected = ScopeId::new("manifest:selected");
    let outside = ScopeId::new("manifest:outside");
    commit(
        &node,
        &outside,
        vec![mutation(&outside, MutationOperation::Set)],
        None,
        vec![ResourceClaim::scope(
            selected.clone(),
            ResourceClaimKind::Referenced,
        )],
    )?;
    let selection = ScopeSelection::Exact(selected.clone());
    if !SelectedHistorySnapshot::current(&node)?
        .retained_manifest(&selection)?
        .events()
        .is_empty()
    {
        return Err("read claim manufactured retained selected history".into());
    }
    commit(
        &node,
        &selected,
        vec![mutation(&selected, MutationOperation::Set)],
        None,
        vec![ResourceClaim::scope(
            outside.clone(),
            ResourceClaimKind::Referenced,
        )],
    )?;
    SelectedHistorySnapshot::current(&node)?.retained_manifest(&selection)?;
    commit(
        &node,
        &selected,
        vec![
            mutation(&selected, MutationOperation::Set),
            mutation(&outside, MutationOperation::Set),
        ],
        None,
        Vec::new(),
    )?;
    if !matches!(
        SelectedHistorySnapshot::current(&node)?.retained_manifest(&selection),
        Err(SelectedHistoryManifestError::OutsideSelection { scope, .. }) if scope == outside
    ) {
        return Err("atomic cross-scope write produced a partial manifest".into());
    }
    Ok(())
}

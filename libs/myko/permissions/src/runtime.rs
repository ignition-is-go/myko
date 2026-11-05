use crate::{
    commands::PermissionAdminOperation,
    engine,
    error::{PermissionError, PermissionResult},
    model::{
        AUTHORITY_ID, PermissionAuditActor, PermissionAuthority, PermissionAuthorityId,
        PermissionChange, PermissionChangeMode, PermissionChangeResult, PermissionGroup,
        PermissionMembership, PermissionOverride, PermissionPrincipal, PermissionRole,
        PermissionRoleBinding,
    },
    storage::PermissionStorage,
};
use myko::{
    common::with_id::WithId,
    item::{AnyItem, Eventable},
    request::RequestContext,
    server::MykoServerContext,
    store::StoreRegistry,
};
use myko_auth::{
    AuthenticatedIdentity, AuthorizationRequest, Authorizer, EntityScope, OperationKind,
    ScopeDecision, ScopePolicy, ScopedAuthorizer,
};
use serde_json::Value;
use std::{
    sync::{
        Arc, Mutex, OnceLock, RwLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

/// Select explicitly whether OAuth scopes constrain the local role grants.
#[derive(Clone, Copy, Debug)]
pub enum CredentialScopeMode {
    LocalGrants,
    TokenScopes,
}

#[derive(Clone)]
pub struct Permissions(Arc<Inner>);
struct Inner {
    storage: Arc<dyn PermissionStorage>,
    mode: CredentialScopeMode,
    gate: Mutex<()>,
    state: RwLock<PolicyState>,
    context: Mutex<Option<Arc<MykoServerContext>>>,
    watcher_started: AtomicBool,
}
#[derive(Default)]
struct PolicyState {
    snapshot: Option<Arc<PermissionAuthority>>,
    error: Option<PermissionError>,
    epoch: u64,
}
type AttachedRuntime = (Weak<StoreRegistry>, Weak<Inner>);
fn runtimes() -> &'static Mutex<Vec<AttachedRuntime>> {
    static RUNTIMES: OnceLock<Mutex<Vec<AttachedRuntime>>> = OnceLock::new();
    RUNTIMES.get_or_init(|| Mutex::new(Vec::new()))
}

pub struct PermissionEdit {
    pub expected_revision: u64,
    pub changes: Vec<PermissionChange>,
    pub reason: String,
}
impl Permissions {
    pub fn new(storage: Arc<dyn PermissionStorage>, mode: CredentialScopeMode) -> Self {
        Self(Arc::new(Inner {
            storage,
            mode,
            gate: Mutex::new(()),
            state: RwLock::new(PolicyState::default()),
            context: Mutex::new(None),
            watcher_started: AtomicBool::new(false),
        }))
    }
    /// Attach after server initialization and before accepting connections.
    /// # Errors
    /// Returns an error for invalid storage or a different policy already attached to this registry.
    pub fn attach(&self, context: Arc<MykoServerContext>) -> PermissionResult<()> {
        let _gate = self.0.gate.try_lock().map_err(|_| PermissionError::Busy)?;
        let mut attached = self
            .0
            .context
            .lock()
            .map_err(|_| PermissionError::Detached)?;
        if let Some(existing) = &*attached {
            if !Arc::ptr_eq(&existing.registry, &context.registry) {
                return Err(PermissionError::Invalid(
                    "a permission runtime cannot serve two registries".into(),
                ));
            }
        } else {
            let mut entries = runtimes().lock().map_err(|_| PermissionError::Detached)?;
            entries.retain(|(registry, runtime)| {
                registry.strong_count() != 0 && runtime.strong_count() != 0
            });
            if entries.iter().any(|(registry, _)| {
                registry
                    .upgrade()
                    .is_some_and(|registry| Arc::ptr_eq(&registry, &context.registry))
            }) {
                return Err(PermissionError::Invalid(
                    "a permission runtime is already attached".into(),
                ));
            }
            entries.push((Arc::downgrade(&context.registry), Arc::downgrade(&self.0)));
            drop(entries);
            *attached = Some(context);
        }
        drop(attached);
        let initial = self.refresh_locked();
        if self.0.watcher_started.swap(true, Ordering::AcqRel) {
            return initial;
        }
        let weak = Arc::downgrade(&self.0);
        let spawn = std::thread::Builder::new()
            .name("myko-permissions".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(Duration::from_millis(100));
                    let Some(inner) = weak.upgrade() else {
                        break;
                    };
                    let _ = Self(inner).refresh();
                }
            });
        if let Err(error) = spawn {
            self.0.watcher_started.store(false, Ordering::Release);
            let error = PermissionError::Storage(error.to_string());
            self.fail(error.clone());
            return Err(error);
        }
        initial
    }

    /// Refresh local grants after a commit made by another process or operator.
    /// # Errors
    /// Returns an error when storage or the persisted document is invalid. Authorization then denies access.
    pub fn refresh(&self) -> PermissionResult<()> {
        let Ok(_gate) = self.0.gate.try_lock() else {
            return Ok(());
        };
        self.refresh_locked()
    }
    fn refresh_locked(&self) -> PermissionResult<()> {
        let result = self.0.storage.load().and_then(|snapshot| {
            if let Some(snapshot) = &snapshot {
                engine::validate(&snapshot.document, false)?;
            }
            Ok(snapshot)
        });
        match result {
            Ok(snapshot) => {
                let changed = {
                    let state = self.0.state.read().map_err(|_| PermissionError::Detached)?;
                    if let (Some(old), Some(new)) = (&state.snapshot, &snapshot) {
                        if new.revision < old.revision
                            || (new.revision == old.revision && old.as_ref() != new)
                        {
                            drop(state);
                            let error = PermissionError::Invalid("permission history moved backwards or changed an accepted revision".into());
                            self.fail(error.clone());
                            return Err(error);
                        }
                    } else if state.snapshot.is_some() && snapshot.is_none() {
                        drop(state);
                        let error = PermissionError::Invalid(
                            "initialized permission history disappeared".into(),
                        );
                        self.fail(error.clone());
                        return Err(error);
                    }
                    state.error.is_some() || state.snapshot.as_deref() != snapshot.as_ref()
                };
                if changed {
                    self.publish(snapshot)?;
                }
                Ok(())
            }
            Err(error) => {
                self.fail(error.clone());
                Err(error)
            }
        }
    }
    fn fail(&self, error: PermissionError) {
        let mut state = self
            .0
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.error.as_ref() != Some(&error) {
            tracing::error!(%error, "Permission authority unavailable; authorization denied");
            state.error = Some(error);
            state.epoch = state.epoch.wrapping_add(1);
        }
    }
    fn publish(&self, snapshot: Option<PermissionAuthority>) -> PermissionResult<()> {
        let context = self
            .0
            .context
            .lock()
            .map_err(|_| PermissionError::Detached)?
            .clone()
            .ok_or(PermissionError::Detached)?;
        let snapshot = snapshot.map(Arc::new);
        {
            let mut state = self
                .0
                .state
                .write()
                .map_err(|_| PermissionError::Detached)?;
            state.snapshot.clone_from(&snapshot);
            state.error = None;
            state.epoch = state.epoch.wrapping_add(1);
        }
        hyphae::batch(|| {
            let document = snapshot.as_ref().map(|snapshot| &snapshot.document);
            project::<PermissionPrincipal>(
                &context,
                document.map_or(&[], |document| &document.principals),
            );
            project::<PermissionGroup>(&context, document.map_or(&[], |document| &document.groups));
            project::<PermissionMembership>(
                &context,
                document.map_or(&[], |document| &document.memberships),
            );
            project::<PermissionRole>(&context, document.map_or(&[], |document| &document.roles));
            project::<PermissionRoleBinding>(
                &context,
                document.map_or(&[], |document| &document.bindings),
            );
            project::<PermissionOverride>(
                &context,
                document.map_or(&[], |document| &document.overrides),
            );
            let authority = context
                .registry
                .get_or_create(PermissionAuthority::ENTITY_NAME_STATIC);
            authority.replace_all(
                snapshot
                    .iter()
                    .map(|item| {
                        let erased: Arc<dyn AnyItem> = item.clone();
                        (item.id(), erased)
                    })
                    .collect(),
            );
        });
        Ok(())
    }
    pub(crate) fn permits_administration(&self, request: &RequestContext, operation: &str) -> bool {
        let Some(session) = &request.auth else {
            return false;
        };
        let input = AuthorizationRequest::Discover {
            kind: OperationKind::Command,
            operation,
        };
        if session.authorize(input).is_err() {
            return false;
        }
        let Ok(Some(snapshot)) = self.snapshot() else {
            return false;
        };
        ScopedAuthorizer::new(Arc::new(SnapshotPolicy {
            snapshot,
            mode: self.0.mode,
        }))
        .authorize(session.identity(), input)
        .is_ok()
    }

    pub(crate) fn authorize_inspection(
        &self,
        request: &RequestContext,
        operation: &str,
        arguments: &Value,
    ) -> PermissionResult<Arc<PermissionAuthority>> {
        let session = request.auth.as_ref().ok_or(PermissionError::Unauthorized)?;
        let input = AuthorizationRequest::Execute {
            kind: OperationKind::Report,
            operation,
            arguments,
        };
        session
            .authorize(input)
            .map_err(|_| PermissionError::Unauthorized)?;
        let snapshot = self.snapshot()?.ok_or(PermissionError::Unauthorized)?;
        ScopedAuthorizer::new(Arc::new(SnapshotPolicy {
            snapshot: snapshot.clone(),
            mode: self.0.mode,
        }))
        .authorize(session.identity(), input)
        .map_err(|_| PermissionError::Unauthorized)?;
        Ok(snapshot)
    }

    /// The accepted local snapshot. Projected row stores are not an authority source.
    /// # Errors
    /// Returns an error while storage is unavailable or invalid.
    pub fn snapshot(&self) -> PermissionResult<Option<Arc<PermissionAuthority>>> {
        let state = self.0.state.read().map_err(|_| PermissionError::Detached)?;
        if let Some(error) = &state.error {
            return Err(error.clone());
        }
        Ok(state.snapshot.clone())
    }
    pub(crate) fn for_context(context: &MykoServerContext) -> PermissionResult<Self> {
        runtimes()
            .lock()
            .map_err(|_| PermissionError::Detached)?
            .iter()
            .find_map(|(registry, runtime)| {
                registry
                    .upgrade()
                    .filter(|registry| Arc::ptr_eq(registry, &context.registry))
                    .and_then(|_| runtime.upgrade())
                    .map(Self)
            })
            .ok_or(PermissionError::Detached)
    }
    pub(crate) fn apply(
        &self,
        request: &RequestContext,
        operation: &str,
        arguments: &Value,
        edit: PermissionEdit,
    ) -> PermissionResult<PermissionChangeResult> {
        let _gate = self.0.gate.try_lock().map_err(|_| PermissionError::Busy)?;
        self.refresh_locked()?;
        let current = self.snapshot()?.ok_or(PermissionError::Unauthorized)?;
        if current.revision != edit.expected_revision {
            return Err(PermissionError::Conflict {
                expected: Some(edit.expected_revision),
                actual: Some(current.revision),
            });
        }
        if edit.changes.is_empty()
            || edit.changes.len() > 1000
            || edit.reason.trim().is_empty()
            || edit.reason.len() > 4096
        {
            return Err(PermissionError::Invalid(
                "provide 1..1000 changes and a nonempty reason of at most 4096 bytes".into(),
            ));
        }
        let session = request.auth.as_ref().ok_or(PermissionError::Unauthorized)?;
        session.check().map_err(|_| PermissionError::Unauthorized)?;
        let identity = session.identity();
        let policy = SnapshotPolicy {
            snapshot: current.clone(),
            mode: self.0.mode,
        };
        let authorizer = ScopedAuthorizer::new(Arc::new(policy.clone()));
        let input = AuthorizationRequest::Execute {
            kind: OperationKind::Command,
            operation,
            arguments,
        };
        authorizer
            .authorize(identity, input)
            .map_err(|_| PermissionError::Unauthorized)?;
        for change in &edit.changes {
            myko_auth::PermissionCatalog::registered()
                .authorize_scope(engine::required_scope(change), |scope| {
                    policy.decision(identity, scope, input)
                })
                .map_err(|_| PermissionError::Unauthorized)?;
        }
        let mut document = current.document.clone();
        engine::apply_changes(&mut document, &edit.changes)?;
        engine::validate(&document, true)?;
        if !engine::has_administrator(&document) {
            return Err(PermissionError::LockedOut);
        }
        let next = PermissionAuthority {
            id: PermissionAuthorityId::from(AUTHORITY_ID),
            revision: current
                .revision
                .checked_add(1)
                .ok_or_else(|| PermissionError::Invalid("permission revision exhausted".into()))?,
            document,
            change_id: uuid::Uuid::new_v4().to_string(),
            actor: PermissionAuditActor::Principal {
                issuer: identity.principal.issuer.clone(),
                subject: identity.principal.subject.clone(),
            },
            reason: edit.reason,
            mode: PermissionChangeMode::Administration,
        };
        if let Err(error) = self
            .0
            .storage
            .compare_and_swap(Some(current.revision), &next)
        {
            self.fail(error.clone());
            return Err(error);
        }
        let result = PermissionChangeResult {
            revision: next.revision,
            change_id: next.change_id.clone(),
        };
        self.publish(Some(next))?;
        Ok(result)
    }
}

fn project<T: Eventable>(context: &MykoServerContext, items: &[T]) {
    context
        .registry
        .get_or_create(T::ENTITY_NAME_STATIC)
        .replace_all(
            items
                .iter()
                .map(|item| {
                    let erased: Arc<dyn AnyItem> = Arc::new(item.clone());
                    (item.id(), erased)
                })
                .collect(),
        );
}

#[derive(Clone)]
struct SnapshotPolicy {
    snapshot: Arc<PermissionAuthority>,
    mode: CredentialScopeMode,
}
impl ScopePolicy for SnapshotPolicy {
    fn decision(
        &self,
        identity: &AuthenticatedIdentity,
        scope: EntityScope,
        request: AuthorizationRequest<'_>,
    ) -> ScopeDecision {
        if !identity.delegation.is_empty() {
            return ScopeDecision::Deny;
        }
        let protected = engine::protected_entities();
        match request {
            AuthorizationRequest::ClientEvent { entity_type, .. }
                if protected.contains(&entity_type) =>
            {
                return ScopeDecision::Deny;
            }
            AuthorizationRequest::Execute {
                kind: OperationKind::Command,
                operation,
                ..
            }
            | AuthorizationRequest::Discover {
                kind: OperationKind::Command,
                operation,
            } if protected.contains(&scope.entity_type)
                && !inventory::iter::<PermissionAdminOperation>
                    .into_iter()
                    .any(|registration| registration.operation == operation) =>
            {
                return ScopeDecision::Deny;
            }
            _ => {}
        }
        let decision = engine::explicit_decision(&self.snapshot.document, identity, scope);
        if decision == ScopeDecision::Allow
            && matches!(self.mode, CredentialScopeMode::TokenScopes)
            && !identity
                .scopes
                .iter()
                .any(|candidate| candidate == &scope.to_string())
        {
            ScopeDecision::Unset
        } else {
            decision
        }
    }
}
impl ScopePolicy for Permissions {
    fn decision(
        &self,
        identity: &AuthenticatedIdentity,
        scope: EntityScope,
        request: AuthorizationRequest<'_>,
    ) -> ScopeDecision {
        self.snapshot()
            .ok()
            .flatten()
            .map_or(ScopeDecision::Deny, |snapshot| {
                SnapshotPolicy {
                    snapshot,
                    mode: self.0.mode,
                }
                .decision(identity, scope, request)
            })
    }
    fn revision(&self) -> u64 {
        self.0.state.read().map_or(u64::MAX, |state| state.epoch)
    }
}

//! Permission policy backed by ordinary Myko entities, queries, and mutations.
use super::{PermissionStorage, storage_error, validate_next};
use crate::{
    PermissionError, PermissionResult,
    model::{
        AUTHORITY_ID, GetAllPermissionAuthoritys, GetAllPermissionGroups,
        GetAllPermissionMemberships, GetAllPermissionOverrides, GetAllPermissionPrincipals,
        GetAllPermissionRoleBindings, GetAllPermissionRoles, PermissionAuthority,
        PermissionDocument, PermissionGroup, PermissionMembership, PermissionOverride,
        PermissionPrincipal, PermissionRole, PermissionRoleBinding,
    },
};
use hyphae::{
    Cell, CellImmutable, CellMap, Gettable, JoinExt, MapExt, Materialize, SubscriptionGuard,
    Watchable,
};
use myko::{
    common::with_id::WithId, item::AnyItem, request::RequestContext, server::MykoServerContext,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

type Rows<T> = CellMap<Arc<str>, Arc<T>, CellImmutable>;
struct Queries {
    context: Arc<MykoServerContext>,
    _authorities: Rows<PermissionAuthority>,
    principals: Rows<PermissionPrincipal>,
    groups: Rows<PermissionGroup>,
    memberships: Rows<PermissionMembership>,
    roles: Rows<PermissionRole>,
    bindings: Rows<PermissionRoleBinding>,
    overrides: Rows<PermissionOverride>,
    snapshot: Cell<PermissionResult<Option<Arc<PermissionAuthority>>>, CellImmutable>,
    _subscriptions: Vec<SubscriptionGuard>,
}

#[derive(Default)]
pub struct EntityPermissionStorage {
    queries: Mutex<Option<Arc<Queries>>>,
    commit: Mutex<()>,
    revision: Arc<AtomicU64>,
}
fn rows<T: Clone + WithId>(entries: &[(Arc<str>, Arc<T>)]) -> Vec<T> {
    let mut values = entries
        .iter()
        .map(|(_, row)| row.as_ref().clone())
        .collect::<Vec<_>>();
    values.sort_by_key(WithId::id);
    values
}

impl PermissionStorage for EntityPermissionStorage {
    fn attach(&self, context: Arc<MykoServerContext>) -> PermissionResult<()> {
        let mut queries = self.queries.lock().map_err(storage_error)?;
        if let Some(existing) = queries.as_ref() {
            return if Arc::ptr_eq(&existing.context.registry, &context.registry) {
                Ok(())
            } else {
                Err(PermissionError::Invalid(
                    "permission queries already attached to another registry".into(),
                ))
            };
        }
        let request = Arc::new(RequestContext::internal(
            "permission-policy".into(),
            context.host_id,
            "permissions",
        ));
        let mut subscriptions = Vec::new();
        macro_rules! query {
            ($query:ident) => {{
                let rows = context.query_map_by_str($query {}, request.clone());
                let revision = self.revision.clone();
                subscriptions.push(rows.subscribe_diffs(move |_| {
                    revision.fetch_add(1, Ordering::AcqRel);
                }));
                rows
            }};
        }
        let authorities = query!(GetAllPermissionAuthoritys);
        let principals = query!(GetAllPermissionPrincipals);
        let groups = query!(GetAllPermissionGroups);
        let memberships = query!(GetAllPermissionMemberships);
        let roles = query!(GetAllPermissionRoles);
        let bindings = query!(GetAllPermissionRoleBindings);
        let overrides = query!(GetAllPermissionOverrides);
        let snapshot = authorities
            .entries()
            .materialize()
            .join(principals.entries().materialize())
            .materialize()
            .join(groups.entries().materialize())
            .materialize()
            .join(memberships.entries().materialize())
            .materialize()
            .join(roles.entries().materialize())
            .materialize()
            .join(bindings.entries().materialize())
            .materialize()
            .join(overrides.entries().materialize())
            .materialize()
            .map(
                |(
                    (((((authorities, principals), groups), memberships), roles), bindings),
                    overrides,
                )| {
                    let document = PermissionDocument {
                        principals: rows(principals),
                        groups: rows(groups),
                        memberships: rows(memberships),
                        roles: rows(roles),
                        bindings: rows(bindings),
                        overrides: rows(overrides),
                    };
                    let Some((_, authority)) = authorities
                        .iter()
                        .find(|(id, _)| id.as_ref() == AUTHORITY_ID)
                    else {
                        if document.principals.is_empty()
                            && document.groups.is_empty()
                            && document.memberships.is_empty()
                            && document.roles.is_empty()
                            && document.bindings.is_empty()
                            && document.overrides.is_empty()
                        {
                            return Ok(None);
                        }
                        return Err(PermissionError::Invalid(
                            "permission entities have no authority record".into(),
                        ));
                    };
                    crate::engine::validate(&document, false)?;
                    let mut authority = authority.as_ref().clone();
                    authority.document = document;
                    Ok(Some(Arc::new(authority)))
                },
            )
            .materialize();
        subscriptions.push(snapshot.subscribe(|_| {}));
        *queries = Some(Arc::new(Queries {
            context,
            _authorities: authorities,
            principals,
            groups,
            memberships,
            roles,
            bindings,
            overrides,
            snapshot,
            _subscriptions: subscriptions,
        }));
        drop(queries);
        Ok(())
    }
    fn reactive_revision(&self) -> Option<u64> {
        Some(self.revision.load(Ordering::Acquire))
    }
    fn load(&self) -> PermissionResult<Option<PermissionAuthority>> {
        self.load_shared()
            .map(|snapshot| snapshot.map(|value| value.as_ref().clone()))
    }
    fn load_shared(&self) -> PermissionResult<Option<Arc<PermissionAuthority>>> {
        let queries = self
            .queries
            .lock()
            .map_err(storage_error)?
            .clone()
            .ok_or(PermissionError::Detached)?;
        queries.snapshot.get()
    }
    fn compare_and_swap(
        &self,
        expected: Option<u64>,
        next: &PermissionAuthority,
    ) -> PermissionResult<()> {
        let _commit = self.commit.lock().map_err(storage_error)?;
        let queries = self
            .queries
            .lock()
            .map_err(storage_error)?
            .clone()
            .ok_or(PermissionError::Detached)?;
        let current = queries.snapshot.get()?;
        if current.as_deref() == Some(next) {
            return Ok(());
        }
        validate_next(expected, current.as_deref(), next)?;
        let mut upserts: Vec<Arc<dyn AnyItem>> = vec![Arc::new(next.clone())];
        let mut deletes: Vec<Arc<dyn AnyItem>> = Vec::new();
        macro_rules! changes {
            ($field:ident) => {
                for row in &next.document.$field {
                    let current = queries.$field.get_value(&row.id());
                    if current.as_deref() != Some(row) {
                        upserts.push(Arc::new(row.clone()));
                    }
                }
                for row in queries.$field.items_snapshot() {
                    if !next
                        .document
                        .$field
                        .iter()
                        .any(|next| next.id() == row.id())
                    {
                        deletes.push(row);
                    }
                }
            };
        }
        changes!(principals);
        changes!(groups);
        changes!(memberships);
        changes!(roles);
        changes!(bindings);
        changes!(overrides);
        // One normal mutation batch keeps reactive observers on the final state and uses the configured persister.
        queries
            .context
            .replace_batch_any(upserts, deletes)
            .map_err(storage_error)
    }
}

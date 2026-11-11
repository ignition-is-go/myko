use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use leptos::prelude::*;
use myko_permissions::{
    GetPermissionStructure, InspectPermissionPrincipal, PERMISSION_STRUCTURE_VERSION,
    PermissionChangeResult, PermissionInspectionStatus, PermissionPrincipalId,
    PermissionPrincipalInspection, PermissionStructure, RemovePermissionMembership,
    UpsertPermissionMembership, UpsertPermissionRole,
};

use crate::{MembershipEdit, RoleEdit};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PermissionMutationState {
    Idle,
    Pending,
    Saved { revision: u64, change_id: String },
    Error(String),
    OutcomeUnknown,
}

/// A connection to the permission contract using the current Myko client context.
#[derive(Clone, Copy)]
pub struct PermissionConnection {
    pub structure: Signal<Option<PermissionStructure>>,
    pub selected_principal: RwSignal<Option<PermissionPrincipalId>>,
    pub inspection: Signal<Option<PermissionPrincipalInspection>>,
    pub mutation: RwSignal<PermissionMutationState>,
    pub save_role: Callback<RoleEdit>,
    pub save_membership: Callback<MembershipEdit>,
}

fn unavailable(message: String) -> PermissionStructure {
    let lower = message.to_lowercase();
    let status = if lower.contains("forbidden")
        || lower.contains("denied")
        || lower.contains("unauthorized")
    {
        PermissionInspectionStatus::Denied
    } else if lower.contains("unknown report") || lower.contains("not found") {
        PermissionInspectionStatus::Unsupported
    } else {
        PermissionInspectionStatus::Unavailable
    };
    PermissionStructure {
        version: PERMISSION_STRUCTURE_VERSION,
        status,
        message,
        revision: None,
        entities: vec![],
        operations: vec![],
        document: None,
        commands: vec![],
    }
}
fn completed(
    mutation: RwSignal<PermissionMutationState>,
    epoch: &AtomicU64,
    request: u64,
    result: Result<PermissionChangeResult, String>,
) {
    if epoch.load(Ordering::Acquire) != request {
        return;
    }
    let _ = mutation.try_set(match result {
        Ok(result) => PermissionMutationState::Saved {
            revision: result.revision,
            change_id: result.change_id,
        },
        Err(error) => PermissionMutationState::Error(error),
    });
}

fn inspected_principal(
    response: Option<Result<PermissionPrincipalInspection, String>>,
    principal_id: PermissionPrincipalId,
) -> Option<PermissionPrincipalInspection> {
    response.and_then(|result| match result {
        Ok(value) if value.principal_id != principal_id => None,
        Ok(value) => Some(value),
        Err(message) => Some(PermissionPrincipalInspection {
            version: PERMISSION_STRUCTURE_VERSION,
            status: PermissionInspectionStatus::Unavailable,
            message,
            revision: None,
            principal_id,
            scopes: vec![],
        }),
    })
}

/// Connect after providing a configured `MykoClient` in Leptos context.
#[must_use]
pub fn use_permissions() -> PermissionConnection {
    let connected = myko_leptos::use_connection_status();
    let response =
        myko_leptos::live_report_result::<_, PermissionStructure>(|| GetPermissionStructure {});
    let selected_principal = RwSignal::new(None::<PermissionPrincipalId>);
    let principal_response =
        myko_leptos::live_report_result::<_, PermissionPrincipalInspection>(move || {
            InspectPermissionPrincipal {
                principal_id: selected_principal
                    .get()
                    .unwrap_or_else(|| PermissionPrincipalId::from("")),
            }
        });
    let structure = Signal::derive(move || {
        if !connected.get() {
            return Some(unavailable(
                "Disconnected. Reconnect to load current permissions.".into(),
            ));
        }
        response
            .get()
            .map(|result| result.unwrap_or_else(unavailable))
    });
    let inspection = Signal::derive(move || {
        let principal_id = selected_principal.get()?;
        if !connected.get() {
            return None;
        }
        inspected_principal(principal_response.get(), principal_id)
    });
    let mutation = RwSignal::new(PermissionMutationState::Idle);
    let epoch = Arc::new(AtomicU64::new(0));
    let disconnected_epoch = epoch.clone();
    Effect::new(move || {
        if !connected.get() && mutation.get() == PermissionMutationState::Pending {
            disconnected_epoch.fetch_add(1, Ordering::AcqRel);
            mutation.set(PermissionMutationState::OutcomeUnknown);
        }
    });
    let role_sink = myko_leptos::use_command_sink();
    let role_epoch = epoch.clone();
    let save_role = Callback::new(move |edit: RoleEdit| {
        if mutation.get_untracked() == PermissionMutationState::Pending {
            return;
        }
        mutation.set(PermissionMutationState::Pending);
        let request = role_epoch.fetch_add(1, Ordering::AcqRel).wrapping_add(1);
        let completion_epoch = role_epoch.clone();
        role_sink.dispatch::<_, PermissionChangeResult>(
            UpsertPermissionRole {
                expected_revision: edit.expected_revision,
                role: edit.role,
                reason: edit.reason,
            },
            move |result| completed(mutation, &completion_epoch, request, result),
        );
    });
    let membership_sink = myko_leptos::use_command_sink();
    let save_membership = Callback::new(move |edit: MembershipEdit| {
        if mutation.get_untracked() == PermissionMutationState::Pending {
            return;
        }
        mutation.set(PermissionMutationState::Pending);
        let request = epoch.fetch_add(1, Ordering::AcqRel).wrapping_add(1);
        let completion_epoch = epoch.clone();
        if edit.member {
            membership_sink.dispatch::<_, PermissionChangeResult>(
                UpsertPermissionMembership {
                    expected_revision: edit.expected_revision,
                    membership: edit.membership,
                    reason: edit.reason,
                },
                move |result| completed(mutation, &completion_epoch, request, result),
            );
        } else {
            membership_sink.dispatch::<_, PermissionChangeResult>(
                RemovePermissionMembership {
                    expected_revision: edit.expected_revision,
                    id: edit.membership.id,
                    reason: edit.reason,
                },
                move |result| completed(mutation, &completion_epoch, request, result),
            );
        }
    });
    PermissionConnection {
        structure,
        selected_principal,
        inspection,
        mutation,
        save_role,
        save_membership,
    }
}

#[cfg(test)]
mod tests {
    use super::{PermissionMutationState, completed};
    use leptos::prelude::*;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn late_completion_cannot_replace_newer_state_or_write_to_a_disposed_owner() {
        let owner = Owner::new();
        let mutation = owner.with(|| RwSignal::new(PermissionMutationState::Pending));
        let epoch = AtomicU64::new(2);
        completed(mutation, &epoch, 1, Err("old request".into()));
        assert_eq!(mutation.get_untracked(), PermissionMutationState::Pending);
        owner.cleanup();
        completed(mutation, &epoch, 2, Err("late request".into()));
        assert!(mutation.try_get_untracked().is_none());
    }
}

use leptos::prelude::*;
use myko_permissions::{
    PERMISSION_STRUCTURE_VERSION, PermissionGroupId, PermissionInspectionStatus,
    PermissionMembership, PermissionMembershipId, PermissionPrincipal, PermissionPrincipalId,
    PermissionRole, PermissionRoleId, PermissionRule, PermissionStructure,
};

use crate::{
    MembershipEdit, PermissionMatrixRow, RoleEdit, RoleRuleChoice, permission_matrix, set_role_rule,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PermissionEditorStatus {
    Loading,
    Ready,
    ReadOnly,
    Stale,
    Denied,
    Unsupported(String),
    Unavailable(String),
    Missing(String),
}
impl PermissionEditorStatus {
    #[must_use]
    pub const fn is_editable(&self) -> bool {
        matches!(self, Self::Ready)
    }
    #[must_use]
    pub const fn is_visible(&self) -> bool {
        matches!(self, Self::Ready | Self::ReadOnly | Self::Stale)
    }
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::Loading => "Loading permissions…".into(),
            Self::Ready => "Changes are checked by the server before saving.".into(),
            Self::ReadOnly => "Read-only. Editing requires permission.".into(),
            Self::Stale => "Permissions changed. Reload the draft before editing or saving.".into(),
            Self::Denied => "Permission inspection denied".into(),
            Self::Unsupported(message) | Self::Unavailable(message) | Self::Missing(message) => {
                message.clone()
            }
        }
    }
}

pub fn structure_status(value: &PermissionStructure) -> PermissionEditorStatus {
    if value.version != PERMISSION_STRUCTURE_VERSION {
        return PermissionEditorStatus::Unsupported(format!(
            "Unsupported permission contract version {}",
            value.version
        ));
    }
    match value.status {
        PermissionInspectionStatus::Ready
            if value.revision.is_some() && value.document.is_some() =>
        {
            PermissionEditorStatus::Ready
        }
        PermissionInspectionStatus::Ready => {
            PermissionEditorStatus::Unavailable("Permission snapshot is incomplete.".into())
        }
        PermissionInspectionStatus::Denied => PermissionEditorStatus::Denied,
        PermissionInspectionStatus::Unsupported => PermissionEditorStatus::Unsupported(
            "This server does not support permission inspection".into(),
        ),
        PermissionInspectionStatus::Unavailable => PermissionEditorStatus::Unavailable(format!(
            "Permission information unavailable. {}",
            value.message
        )),
    }
}
fn command_allowed(value: &PermissionStructure, name: &str) -> bool {
    value
        .commands
        .iter()
        .any(|command| command.operation == name && command.allowed)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PermissionEditError {
    NotEditable(PermissionEditorStatus),
    ReasonRequired,
    UnknownScope,
    MissingPrincipal,
}
impl std::fmt::Display for PermissionEditError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&match self {
            Self::NotEditable(status) => status.message(),
            Self::ReasonRequired => "A reason is required.".into(),
            Self::UnknownScope => {
                "This scope is not advertised. Obsolete rules can only be removed.".into()
            }
            Self::MissingPrincipal => "This principal no longer exists.".into(),
        })
    }
}
impl std::error::Error for PermissionEditError {}

#[derive(Clone, Debug)]
struct RoleDraft {
    revision: u64,
    original: PermissionRole,
    edited: PermissionRole,
}

/// A DOM-free role editor. Consumers own every input, button, and container.
#[derive(Clone, Copy)]
pub struct RoleEditorController {
    structure: Signal<Option<PermissionStructure>>,
    role_id: Signal<PermissionRoleId>,
    draft: RwSignal<Option<RoleDraft>>,
    pub reason: RwSignal<String>,
    reload_epoch: RwSignal<u64>,
    on_save: Option<Callback<RoleEdit>>,
}
#[must_use]
pub fn use_role_editor(
    structure: Signal<Option<PermissionStructure>>,
    role_id: Signal<PermissionRoleId>,
    on_save: Option<Callback<RoleEdit>>,
) -> RoleEditorController {
    RoleEditorController {
        structure,
        role_id,
        draft: RwSignal::new(None),
        reason: RwSignal::new(String::new()),
        reload_epoch: RwSignal::new(0),
        on_save,
    }
}
impl RoleEditorController {
    fn invalidate(self) {
        if self.draft.get_untracked().is_some() {
            self.draft.set(None);
            self.reason.set(String::new());
        }
    }
    #[must_use]
    pub fn status(self) -> PermissionEditorStatus {
        let _ = self.reload_epoch.get();
        let Some(value) = self.structure.get() else {
            self.invalidate();
            return PermissionEditorStatus::Loading;
        };
        let status = structure_status(&value);
        if status != PermissionEditorStatus::Ready {
            self.invalidate();
            return status;
        }
        let selected = self.role_id.get();
        let current = value
            .document
            .as_ref()
            .and_then(|document| document.roles.iter().find(|role| role.id == selected));
        let Some(current) = current else {
            self.invalidate();
            return PermissionEditorStatus::Missing(
                "This role no longer exists. Reload the current role selection.".into(),
            );
        };
        let Some(revision) = value.revision else {
            return PermissionEditorStatus::Unavailable("Permission revision missing.".into());
        };
        let draft = self.draft.get_untracked();
        if let Some(draft) = draft {
            if draft.original.id != current.id {
                self.invalidate();
                self.draft.set(Some(RoleDraft {
                    revision,
                    original: current.clone(),
                    edited: current.clone(),
                }));
            } else if draft.revision != revision || draft.original != *current {
                return PermissionEditorStatus::Stale;
            }
        } else {
            self.draft.set(Some(RoleDraft {
                revision,
                original: current.clone(),
                edited: current.clone(),
            }));
        }
        if self.on_save.is_none() || !command_allowed(&value, "UpsertPermissionRole") {
            PermissionEditorStatus::ReadOnly
        } else {
            PermissionEditorStatus::Ready
        }
    }
    #[must_use]
    pub fn role(self) -> Option<PermissionRole> {
        self.status()
            .is_visible()
            .then(|| self.draft.get().map(|draft| draft.edited))
            .flatten()
    }
    #[must_use]
    pub fn rows(self) -> Vec<PermissionMatrixRow<String>> {
        if !self.status().is_visible() {
            return vec![];
        }
        self.structure
            .get()
            .map_or_else(Vec::new, |value| permission_matrix(&value.entities))
    }
    #[must_use]
    pub fn rule(self, scope: &str) -> Option<RoleRuleChoice> {
        let role = self.role()?;
        if let Some(rule) = role.rules.iter().find(|rule| rule.scope == scope) {
            return Some(match rule.effect {
                myko_permissions::PermissionEffect::Allow => RoleRuleChoice::Allow,
                myko_permissions::PermissionEffect::Deny => RoleRuleChoice::Deny,
            });
        }
        self.rows()
            .into_iter()
            .any(|row| {
                [row.read, row.write, row.delete]
                    .into_iter()
                    .flatten()
                    .any(|item| item == scope)
            })
            .then_some(RoleRuleChoice::Inherit)
    }
    #[must_use]
    pub fn obsolete_rules(self) -> Vec<PermissionRule> {
        let advertised: Vec<_> = self
            .rows()
            .into_iter()
            .flat_map(|row| [row.read, row.write, row.delete].into_iter().flatten())
            .collect();
        self.role().map_or_else(Vec::new, |role| {
            role.rules
                .into_iter()
                .filter(|rule| !advertised.contains(&rule.scope))
                .collect()
        })
    }
    /// # Errors
    /// Rejects stale, hidden, read-only, and unadvertised edits.
    pub fn set_rule(self, scope: &str, choice: RoleRuleChoice) -> Result<(), PermissionEditError> {
        self.require_editable()?;
        let advertised = self.rows().into_iter().any(|row| {
            [row.read, row.write, row.delete]
                .into_iter()
                .flatten()
                .any(|item| item == scope)
        });
        if !advertised {
            return Err(PermissionEditError::UnknownScope);
        }
        self.draft.update(|draft| {
            if let Some(draft) = draft {
                set_role_rule(&mut draft.edited, scope, choice);
            }
        });
        Ok(())
    }
    /// # Errors
    /// Only existing obsolete rules can be removed, and the draft must be editable.
    pub fn remove_obsolete(self, scope: &str) -> Result<(), PermissionEditError> {
        self.require_editable()?;
        if !self.obsolete_rules().iter().any(|rule| rule.scope == scope) {
            return Err(PermissionEditError::UnknownScope);
        }
        self.draft.update(|draft| {
            if let Some(draft) = draft {
                set_role_rule(&mut draft.edited, scope, RoleRuleChoice::Inherit);
            }
        });
        Ok(())
    }
    fn require_editable(self) -> Result<(), PermissionEditError> {
        let status = self.status();
        if status.is_editable() {
            Ok(())
        } else {
            Err(PermissionEditError::NotEditable(status))
        }
    }
    /// Discard the draft and load the currently selected role.
    pub fn reload(self) {
        self.draft.set(None);
        self.reason.set(String::new());
        self.reload_epoch
            .update(|epoch| *epoch = epoch.wrapping_add(1));
        let _ = self.status();
    }
    #[must_use]
    pub fn can_submit(self) -> bool {
        self.status().is_editable() && !self.reason.get().trim().is_empty()
    }
    /// # Errors
    /// Rechecks revision, current role, capability, and reason before invoking the callback.
    pub fn submit(self) -> Result<(), PermissionEditError> {
        self.require_editable()?;
        let reason = self.reason.get_untracked();
        if reason.trim().is_empty() {
            return Err(PermissionEditError::ReasonRequired);
        }
        if let (Some(draft), Some(callback)) = (self.draft.get_untracked(), self.on_save) {
            callback.run(RoleEdit {
                expected_revision: draft.revision,
                role: draft.edited,
                reason,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct MembershipEditorRow {
    pub principal: PermissionPrincipal,
    pub membership: Option<PermissionMembership>,
    pub editable: bool,
}
#[derive(Clone, Debug)]
struct GroupBaseline {
    revision: u64,
    group_id: PermissionGroupId,
}

#[derive(Clone, Copy)]
pub struct GroupMembershipController {
    structure: Signal<Option<PermissionStructure>>,
    group_id: Signal<PermissionGroupId>,
    baseline: RwSignal<Option<GroupBaseline>>,
    pub reason: RwSignal<String>,
    reload_epoch: RwSignal<u64>,
    on_save: Option<Callback<MembershipEdit>>,
}
#[must_use]
pub fn use_group_membership_editor(
    structure: Signal<Option<PermissionStructure>>,
    group_id: Signal<PermissionGroupId>,
    on_save: Option<Callback<MembershipEdit>>,
) -> GroupMembershipController {
    GroupMembershipController {
        structure,
        group_id,
        baseline: RwSignal::new(None),
        reason: RwSignal::new(String::new()),
        reload_epoch: RwSignal::new(0),
        on_save,
    }
}
impl GroupMembershipController {
    fn invalidate(self) {
        if self.baseline.get_untracked().is_some() {
            self.baseline.set(None);
            self.reason.set(String::new());
        }
    }
    #[must_use]
    pub fn status(self) -> PermissionEditorStatus {
        let _ = self.reload_epoch.get();
        let Some(value) = self.structure.get() else {
            self.invalidate();
            return PermissionEditorStatus::Loading;
        };
        let status = structure_status(&value);
        if status != PermissionEditorStatus::Ready {
            self.invalidate();
            return status;
        }
        let group_id = self.group_id.get();
        if !value
            .document
            .as_ref()
            .is_some_and(|document| document.groups.iter().any(|group| group.id == group_id))
        {
            self.invalidate();
            return PermissionEditorStatus::Missing(
                "This group no longer exists. Reload the current group selection.".into(),
            );
        }
        let Some(revision) = value.revision else {
            return PermissionEditorStatus::Unavailable("Permission revision missing.".into());
        };
        if let Some(baseline) = self.baseline.get_untracked() {
            if baseline.group_id != group_id {
                self.invalidate();
                self.baseline
                    .set(Some(GroupBaseline { revision, group_id }));
            } else if baseline.revision != revision {
                return PermissionEditorStatus::Stale;
            }
        } else {
            self.baseline
                .set(Some(GroupBaseline { revision, group_id }));
        }
        if self.on_save.is_none()
            || !["UpsertPermissionMembership", "RemovePermissionMembership"]
                .iter()
                .any(|operation| command_allowed(&value, operation))
        {
            PermissionEditorStatus::ReadOnly
        } else {
            PermissionEditorStatus::Ready
        }
    }
    #[must_use]
    pub fn group_name(self) -> Option<String> {
        if !self.status().is_visible() {
            return None;
        }
        let group_id = self.group_id.get();
        self.structure
            .get()
            .and_then(|value| value.document)
            .and_then(|document| {
                document
                    .groups
                    .into_iter()
                    .find(|group| group.id == group_id)
                    .map(|group| group.name)
            })
    }
    #[must_use]
    pub fn rows(self) -> Vec<MembershipEditorRow> {
        if !self.status().is_visible() {
            return vec![];
        }
        let Some(value) = self.structure.get() else {
            return vec![];
        };
        let Some(document) = value.document.as_ref() else {
            return vec![];
        };
        let group_id = self.group_id.get();
        document
            .principals
            .iter()
            .map(|principal| {
                let membership = document
                    .memberships
                    .iter()
                    .find(|entry| entry.group_id == group_id && entry.principal_id == principal.id)
                    .cloned();
                let operation = if membership.is_some() {
                    "RemovePermissionMembership"
                } else {
                    "UpsertPermissionMembership"
                };
                MembershipEditorRow {
                    principal: principal.clone(),
                    membership,
                    editable: self.status().is_editable() && command_allowed(&value, operation),
                }
            })
            .collect()
    }
    pub fn reload(self) {
        self.baseline.set(None);
        self.reason.set(String::new());
        self.reload_epoch
            .update(|epoch| *epoch = epoch.wrapping_add(1));
        let _ = self.status();
    }
    /// # Errors
    /// Rechecks the selected group, principal, revision, operation capability, and reason.
    pub fn set_member(
        self,
        principal_id: &PermissionPrincipalId,
        member: bool,
    ) -> Result<(), PermissionEditError> {
        let status = self.status();
        if !status.is_editable() {
            return Err(PermissionEditError::NotEditable(status));
        }
        let reason = self.reason.get_untracked();
        if reason.trim().is_empty() {
            return Err(PermissionEditError::ReasonRequired);
        }
        let row = self
            .rows()
            .into_iter()
            .find(|row| row.principal.id == *principal_id)
            .ok_or(PermissionEditError::MissingPrincipal)?;
        if row.membership.is_some() == member {
            return Ok(());
        }
        if !row.editable {
            return Err(PermissionEditError::NotEditable(
                PermissionEditorStatus::ReadOnly,
            ));
        }
        let group_id = self.group_id.get_untracked();
        let membership = row.membership.unwrap_or_else(|| PermissionMembership {
            id: PermissionMembershipId::from(uuid::Uuid::new_v4().to_string()),
            authority_id: row.principal.authority_id.clone(),
            principal_id: row.principal.id,
            group_id,
        });
        if let (Some(baseline), Some(callback)) = (self.baseline.get_untracked(), self.on_save) {
            callback.run(MembershipEdit {
                expected_revision: baseline.revision,
                membership,
                member,
                reason,
            });
        }
        Ok(())
    }
}

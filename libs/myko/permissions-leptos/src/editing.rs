use myko_permissions::{PermissionEffect, PermissionMembership, PermissionRole, PermissionRule};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoleRuleChoice {
    Inherit,
    Allow,
    Deny,
}
impl RoleRuleChoice {
    #[must_use]
    pub fn from_value(value: &str) -> Self {
        match value {
            "allow" => Self::Allow,
            "deny" => Self::Deny,
            _ => Self::Inherit,
        }
    }
}

/// Inherit removes this role's rule; other roles and overrides still apply.
pub fn set_role_rule(role: &mut PermissionRole, scope: &str, choice: RoleRuleChoice) {
    role.rules.retain(|rule| rule.scope != scope);
    let effect = match choice {
        RoleRuleChoice::Inherit => return,
        RoleRuleChoice::Allow => PermissionEffect::Allow,
        RoleRuleChoice::Deny => PermissionEffect::Deny,
    };
    role.rules.push(PermissionRule {
        scope: scope.into(),
        effect,
    });
    role.rules
        .sort_by(|left, right| left.scope.cmp(&right.scope));
}

#[derive(Clone, Debug)]
pub struct RoleEdit {
    pub expected_revision: u64,
    pub role: PermissionRole,
    pub reason: String,
}
#[derive(Clone, Debug)]
pub struct MembershipEdit {
    pub expected_revision: u64,
    pub membership: PermissionMembership,
    pub member: bool,
    pub reason: String,
}

#[cfg(test)]
mod tests {
    use super::{RoleRuleChoice, set_role_rule};
    use myko_permissions::{
        PermissionAuthorityId, PermissionEffect, PermissionRole, PermissionRoleId,
    };

    #[test]
    fn inherit_removes_only_the_selected_rule() {
        let mut role = PermissionRole {
            id: PermissionRoleId::from("role"),
            authority_id: PermissionAuthorityId::from("permissions"),
            name: "Operators".into(),
            enabled: true,
            rules: vec![],
        };
        set_role_rule(&mut role, "UnknownCamera:write", RoleRuleChoice::Allow);
        set_role_rule(&mut role, "UnknownProject:read", RoleRuleChoice::Deny);
        set_role_rule(&mut role, "UnknownCamera:write", RoleRuleChoice::Deny);
        assert_eq!(role.rules.len(), 2);
        assert!(role.rules.iter().any(
            |rule| rule.scope == "UnknownCamera:write" && rule.effect == PermissionEffect::Deny
        ));
        set_role_rule(&mut role, "UnknownCamera:write", RoleRuleChoice::Inherit);
        assert_eq!(role.rules.len(), 1);
        assert!(
            role.rules
                .iter()
                .all(|rule| rule.scope == "UnknownProject:read")
        );
    }
}

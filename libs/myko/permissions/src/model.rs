//! Permission records and the authoritative policy document.

pub const AUTHORITY_ID: &str = "permissions";

#[myko::myko_subtype(derive(Copy, Eq, Ord, PartialOrd, Hash))]
pub enum PermissionPrincipalKind {
    User,
    Service,
    Agent,
}

#[myko::myko_subtype(derive(Copy, Eq, Ord, PartialOrd, Hash))]
pub enum PermissionEffect {
    Allow,
    Deny,
}

#[myko::myko_subtype(derive(Copy, Eq, Ord, PartialOrd, Hash))]
pub enum PermissionChangeMode {
    Bootstrap,
    Administration,
    Recovery,
}

#[myko::myko_subtype(derive(Eq, Ord, PartialOrd, Hash))]
pub struct PermissionRule {
    pub scope: String,
    pub effect: PermissionEffect,
}

#[myko::myko_subtype(derive(Eq, Ord, PartialOrd, Hash))]
pub enum PermissionAuditActor {
    Principal { issuer: String, subject: String },
    Operator { identity: String },
}

#[myko::myko_subtype(derive(Eq, Ord, PartialOrd, Hash))]
pub enum PermissionGrantee {
    Principal(PermissionPrincipalId),
    Group(PermissionGroupId),
}

#[myko::myko_subtype(derive(Default))]
pub struct PermissionDocument {
    pub principals: Vec<PermissionPrincipal>,
    pub groups: Vec<PermissionGroup>,
    pub memberships: Vec<PermissionMembership>,
    pub roles: Vec<PermissionRole>,
    pub bindings: Vec<PermissionRoleBinding>,
    pub overrides: Vec<PermissionOverride>,
}

mod authority {
    use super::{PermissionAuditActor, PermissionChangeMode, PermissionDocument};
    use myko::prelude::*;

    #[myko::myko_item]
    pub struct PermissionAuthority {
        pub revision: u64,
        pub document: PermissionDocument,
        pub change_id: String,
        pub actor: PermissionAuditActor,
        pub reason: String,
        pub mode: PermissionChangeMode,
    }
}
pub use authority::*;

mod principal {
    use super::{PermissionAuthority, PermissionAuthorityId, PermissionPrincipalKind};
    use myko::prelude::*;

    #[myko::myko_item]
    pub struct PermissionPrincipal {
        #[belongs_to(PermissionAuthority)]
        pub authority_id: PermissionAuthorityId,
        pub issuer: String,
        pub subject: String,
        pub kind: PermissionPrincipalKind,
        pub enabled: bool,
        pub name: String,
    }
}
pub use principal::*;

mod group {
    use super::{PermissionAuthority, PermissionAuthorityId};
    use myko::prelude::*;

    #[myko::myko_item]
    pub struct PermissionGroup {
        #[belongs_to(PermissionAuthority)]
        pub authority_id: PermissionAuthorityId,
        pub name: String,
        pub enabled: bool,
    }
}
pub use group::*;

mod membership {
    use super::{
        PermissionAuthority, PermissionAuthorityId, PermissionGroupId, PermissionPrincipalId,
    };
    use myko::prelude::*;

    #[myko::myko_item]
    pub struct PermissionMembership {
        #[belongs_to(PermissionAuthority)]
        pub authority_id: PermissionAuthorityId,
        pub principal_id: PermissionPrincipalId,
        pub group_id: PermissionGroupId,
    }
}
pub use membership::*;

mod role {
    use super::{PermissionAuthority, PermissionAuthorityId, PermissionRule};
    use myko::prelude::*;

    #[myko::myko_item]
    pub struct PermissionRole {
        #[belongs_to(PermissionAuthority)]
        pub authority_id: PermissionAuthorityId,
        pub name: String,
        pub enabled: bool,
        pub rules: Vec<PermissionRule>,
    }
}
pub use role::*;

mod binding {
    use super::{PermissionAuthority, PermissionAuthorityId, PermissionGrantee, PermissionRoleId};
    use myko::prelude::*;

    #[myko::myko_item]
    pub struct PermissionRoleBinding {
        #[belongs_to(PermissionAuthority)]
        pub authority_id: PermissionAuthorityId,
        pub role_id: PermissionRoleId,
        pub grantee: PermissionGrantee,
    }
}
pub use binding::*;

mod permission_override {
    use super::{PermissionAuthority, PermissionAuthorityId, PermissionGrantee, PermissionRule};
    use myko::prelude::*;

    #[myko::myko_item]
    pub struct PermissionOverride {
        #[belongs_to(PermissionAuthority)]
        pub authority_id: PermissionAuthorityId,
        pub grantee: PermissionGrantee,
        pub rule: PermissionRule,
    }
}
pub use permission_override::*;

#[myko::myko_subtype]
pub enum PermissionChange {
    UpsertPrincipal(PermissionPrincipal),
    DeletePrincipal(PermissionPrincipalId),
    UpsertGroup(PermissionGroup),
    DeleteGroup(PermissionGroupId),
    UpsertMembership(PermissionMembership),
    DeleteMembership(PermissionMembershipId),
    UpsertRole(PermissionRole),
    DeleteRole(PermissionRoleId),
    UpsertRoleBinding(PermissionRoleBinding),
    DeleteRoleBinding(PermissionRoleBindingId),
    UpsertOverride(PermissionOverride),
    DeleteOverride(PermissionOverrideId),
}

#[myko::myko_subtype(derive(Eq))]
pub struct PermissionChangeResult {
    pub revision: u64,
    pub change_id: String,
}

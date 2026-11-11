use crate::model::{PermissionDocument, PermissionPrincipalId};

pub const PERMISSION_STRUCTURE_VERSION: u32 = 1;

#[myko::myko_subtype(derive(Eq))]
pub enum PermissionInspectionStatus {
    Ready,
    Denied,
    Unsupported,
    Unavailable,
}

#[myko::myko_subtype(derive(Eq))]
pub struct PermissionEntityDescription {
    pub entity_type: String,
    pub parents: Vec<String>,
    pub scopes: Vec<String>,
}

#[myko::myko_subtype(derive(Eq))]
pub struct PermissionOperationDescription {
    pub kind: String,
    pub operation: String,
    pub required_scopes: Vec<String>,
    pub mapped: bool,
}

#[myko::myko_subtype(derive(Eq))]
pub struct PermissionCommandCapability {
    pub operation: String,
    /// Policy discovery permission. Transport ceilings and argument-specific execution checks still apply.
    pub allowed: bool,
}

#[myko::myko_report_output]
pub struct PermissionStructure {
    pub version: u32,
    pub status: PermissionInspectionStatus,
    pub message: String,
    pub revision: Option<u64>,
    pub entities: Vec<PermissionEntityDescription>,
    pub operations: Vec<PermissionOperationDescription>,
    pub document: Option<PermissionDocument>,
    pub commands: Vec<PermissionCommandCapability>,
}

#[myko::myko_subtype(derive(Eq))]
pub enum PermissionRuleSource {
    PrincipalOverride(String),
    GroupOverride(String),
    Role(String),
}

#[myko::myko_subtype(derive(Eq))]
pub struct PermissionDecisionStep {
    pub scope: String,
    pub decision: String,
    pub distance: usize,
    pub sources: Vec<PermissionRuleSource>,
}

#[myko::myko_subtype(derive(Eq))]
pub struct PermissionScopeExplanation {
    pub scope: String,
    pub allowed: bool,
    pub steps: Vec<PermissionDecisionStep>,
}

#[myko::myko_report_output]
pub struct PermissionPrincipalInspection {
    pub version: u32,
    pub status: PermissionInspectionStatus,
    pub message: String,
    pub revision: Option<u64>,
    pub principal_id: PermissionPrincipalId,
    pub scopes: Vec<PermissionScopeExplanation>,
}

#[myko::myko_report(PermissionStructure)]
#[myko_scope(crate::model::PermissionAuthority, read)]
#[myko_scope(crate::model::PermissionPrincipal, read)]
#[myko_scope(crate::model::PermissionGroup, read)]
#[myko_scope(crate::model::PermissionMembership, read)]
#[myko_scope(crate::model::PermissionRole, read)]
#[myko_scope(crate::model::PermissionRoleBinding, read)]
#[myko_scope(crate::model::PermissionOverride, read)]
pub struct GetPermissionStructure {}

#[myko::myko_report(PermissionPrincipalInspection)]
#[myko_scope(crate::model::PermissionAuthority, read)]
#[myko_scope(crate::model::PermissionPrincipal, read)]
#[myko_scope(crate::model::PermissionGroup, read)]
#[myko_scope(crate::model::PermissionMembership, read)]
#[myko_scope(crate::model::PermissionRole, read)]
#[myko_scope(crate::model::PermissionRoleBinding, read)]
#[myko_scope(crate::model::PermissionOverride, read)]
pub struct InspectPermissionPrincipal {
    pub principal_id: PermissionPrincipalId,
}

#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
#[path = "inspection/server.rs"]
mod server;

#[cfg(not(all(feature = "server", not(target_arch = "wasm32"))))]
impl myko::report::ReportHandler for GetPermissionStructure {
    type Output = PermissionStructure;
    fn request_cache_scope() -> myko::request::RequestCacheScope {
        myko::request::RequestCacheScope::PerClient
    }
}
#[cfg(not(all(feature = "server", not(target_arch = "wasm32"))))]
impl myko::report::ReportHandler for InspectPermissionPrincipal {
    type Output = PermissionPrincipalInspection;
    fn request_cache_scope() -> myko::request::RequestCacheScope {
        myko::request::RequestCacheScope::PerClient
    }
}

use std::{collections::HashSet, fmt, sync::Arc};

use myko::query::QueryRegistration;

/// Restricts the operations accepted by a Myko server.
///
/// Servers remain unrestricted unless they explicitly opt into
/// [`MykoIngressPolicy::read_only_queries`].
#[derive(Clone, Debug, Default)]
pub enum MykoIngressPolicy {
    #[default]
    Unrestricted,
    ReadOnlyQueries(Arc<HashSet<String>>),
}

impl MykoIngressPolicy {
    /// Accept only subscriptions to the named queries.
    pub fn read_only_queries<I, S>(query_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::ReadOnlyQueries(Arc::new(query_ids.into_iter().map(Into::into).collect()))
    }

    pub(crate) fn allows_query(&self, query_id: &str) -> bool {
        match self {
            Self::Unrestricted => true,
            Self::ReadOnlyQueries(query_ids) => query_ids.contains(query_id),
        }
    }

    pub(crate) fn allows_operation(&self, kind: &str, operation_id: &str) -> bool {
        matches!(self, Self::Unrestricted) || kind == "query" && self.allows_query(operation_id)
    }

    pub(crate) const fn is_read_only(&self) -> bool {
        matches!(self, Self::ReadOnlyQueries(_))
    }

    pub(crate) fn validate(&self) -> Result<(), IngressPolicyError> {
        let Self::ReadOnlyQueries(query_ids) = self else {
            return Ok(());
        };
        let registered = inventory::iter::<QueryRegistration>
            .into_iter()
            .map(|registration| registration.query_id)
            .collect::<HashSet<_>>();
        let mut unknown = query_ids
            .iter()
            .filter(|query_id| !registered.contains(query_id.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        unknown.sort_unstable();
        if unknown.is_empty() {
            Ok(())
        } else {
            Err(IngressPolicyError { unknown })
        }
    }
}

/// A read-only query allowlist named queries absent from the linked binary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngressPolicyError {
    unknown: Vec<String>,
}

impl fmt::Display for IngressPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "read-only query allowlist contains unregistered queries: {}",
            self.unknown.join(", ")
        )
    }
}

impl std::error::Error for IngressPolicyError {}

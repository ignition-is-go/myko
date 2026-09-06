use std::{fmt, future::Future, pin::Pin};

use crate::{AuthorityUnavailable, AuthorizationDecision};

type DecisionResult = Result<AuthorizationDecision, AuthorityUnavailable>;

/// One policy decision, either immediate or requiring asynchronous coordination.
/// Coordinated work must remain lazy until the returned future is polled.
#[must_use]
pub enum PolicyDecision<'a> {
    Immediate(Box<DecisionResult>),
    Coordinated(Pin<Box<dyn Future<Output = DecisionResult> + Send + 'a>>),
}

impl<'a> PolicyDecision<'a> {
    pub fn coordinated(future: impl Future<Output = DecisionResult> + Send + 'a) -> Self {
        Self::Coordinated(Box::pin(future))
    }

    /// Resolve at an asynchronous access boundary without holding policy locks.
    ///
    /// # Errors
    /// Preserves unavailable authority separately from permit, deny, and challenge.
    pub async fn resolve(self) -> DecisionResult {
        match self {
            Self::Immediate(result) => *result,
            Self::Coordinated(future) => future.await,
        }
    }

    /// Resolve a local synchronous operation without polling coordinated work.
    ///
    /// # Errors
    /// Reports coordination unavailable when the policy requires asynchronous work.
    pub fn into_immediate(self) -> DecisionResult {
        match self {
            Self::Immediate(result) => *result,
            Self::Coordinated(_) => Err(AuthorityUnavailable::CoordinationUnavailable),
        }
    }
}

impl From<DecisionResult> for PolicyDecision<'_> {
    fn from(result: DecisionResult) -> Self {
        Self::Immediate(Box::new(result))
    }
}

impl fmt::Debug for PolicyDecision<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Immediate(result) => formatter.debug_tuple("Immediate").field(result).finish(),
            Self::Coordinated(_) => formatter.write_str("Coordinated(..)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[test]
    fn synchronous_access_never_polls_coordinated_work() {
        let polled = AtomicBool::new(false);
        let decision = PolicyDecision::coordinated(async {
            polled.store(true, Ordering::SeqCst);
            Err(AuthorityUnavailable::HistoryUnavailable)
        });
        assert_eq!(
            decision.into_immediate(),
            Err(AuthorityUnavailable::CoordinationUnavailable)
        );
        assert!(!polled.load(Ordering::SeqCst));
    }

    #[test]
    fn asynchronous_access_preserves_unavailable_reason() {
        let decision =
            PolicyDecision::coordinated(async { Err(AuthorityUnavailable::HistoryUnavailable) });
        let mut future = std::pin::pin!(decision.resolve());
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert_eq!(
            future.as_mut().poll(&mut context),
            std::task::Poll::Ready(Err(AuthorityUnavailable::HistoryUnavailable))
        );
    }
}

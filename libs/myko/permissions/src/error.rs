use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionError {
    Storage(String),
    Conflict {
        expected: Option<u64>,
        actual: Option<u64>,
    },
    Invalid(String),
    LockedOut,
    Unauthorized,
    Detached,
    Busy,
}
pub type PermissionResult<T> = Result<T, PermissionError>;
impl fmt::Display for PermissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(message) => write!(formatter, "permission storage failed: {message}"),
            Self::Conflict { expected, actual } => write!(
                formatter,
                "permission revision conflict: expected {expected:?}, actual {actual:?}"
            ),
            Self::Invalid(message) => write!(formatter, "invalid permission policy: {message}"),
            Self::LockedOut => formatter
                .write_str("change would remove the last effective permissions administrator"),
            Self::Unauthorized => formatter.write_str("permission administration denied"),
            Self::Busy => formatter
                .write_str("permission administration is busy; retry with the current revision"),
            Self::Detached => {
                formatter.write_str("permission policy is not attached to this runtime")
            }
        }
    }
}
impl std::error::Error for PermissionError {}

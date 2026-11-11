use myko::wire::MykoMessage;

/// Inbound capabilities for one gateway connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayAccess {
    /// Existing trusted native gateway behavior.
    Full,
    /// Queries, views, reports and their controls only. Forbidden input closes
    /// the connection before dispatch. MCP transports are unavailable.
    ReadOnly,
}

impl GatewayAccess {
    pub(crate) fn allows(self, message: &MykoMessage) -> bool {
        self == Self::Full
            || matches!(
                message,
                MykoMessage::Query(_)
                    | MykoMessage::QueryCancel(_)
                    | MykoMessage::QueryWindow(_)
                    | MykoMessage::QueryCursorWindow(_)
                    | MykoMessage::View(_)
                    | MykoMessage::ViewCancel(_)
                    | MykoMessage::ViewWindow(_)
                    | MykoMessage::ViewSampleRate(_)
                    | MykoMessage::Report(_)
                    | MykoMessage::ReportCancel(_)
                    | MykoMessage::Ping(_)
            )
    }
}

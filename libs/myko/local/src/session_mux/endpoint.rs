use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::sync::{Notify, mpsc, watch};

use super::supervisor::LocalSessionMux;
use crate::{AuthorizationDecision, HandlerClientError, PeerFrame, ReconnectPolicy};

#[derive(Debug)]
pub struct LocalMultiplexedSession {
    socket_path: PathBuf,
    reconnect_policy: ReconnectPolicy,
    mux: tokio::sync::OnceCell<Arc<LocalSessionMux>>,
}

impl LocalMultiplexedSession {
    pub fn new(socket_path: PathBuf, reconnect_policy: ReconnectPolicy) -> Self {
        Self {
            socket_path,
            reconnect_policy,
            mux: tokio::sync::OnceCell::new(),
        }
    }

    pub async fn mux(&self) -> Arc<LocalSessionMux> {
        self.mux
            .get_or_init(|| async {
                Arc::new(LocalSessionMux::spawn(
                    self.socket_path.clone(),
                    self.reconnect_policy,
                ))
            })
            .await
            .clone()
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub const fn reconnect_policy(&self) -> ReconnectPolicy {
        self.reconnect_policy
    }
}

#[derive(Debug)]
pub(super) struct LogicalLeaseState {
    pub(super) cancelled: AtomicBool,
}

impl LogicalLeaseState {
    pub(super) const fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
        }
    }
}

#[derive(Debug)]
pub struct MuxSubscription {
    _mux: Arc<LocalSessionMux>,
    lease: Arc<LogicalLeaseState>,
    cancel_wake: Arc<Notify>,
    events: mpsc::Receiver<MuxRouteEvent>,
    terminal: watch::Receiver<Option<RouteTerminal>>,
}

#[derive(Debug)]
pub enum MuxRouteEvent {
    Frame(PeerFrame),
    Reconnecting { reason: Arc<str> },
}

impl MuxSubscription {
    pub(super) const fn new(
        mux: Arc<LocalSessionMux>,
        lease: Arc<LogicalLeaseState>,
        cancel_wake: Arc<Notify>,
        events: mpsc::Receiver<MuxRouteEvent>,
        terminal: watch::Receiver<Option<RouteTerminal>>,
    ) -> Self {
        Self {
            _mux: mux,
            lease,
            cancel_wake,
            events,
            terminal,
        }
    }

    pub async fn recv_authorized(&mut self) -> Result<PeerFrame, HandlerClientError> {
        loop {
            match self.recv_authorized_event().await? {
                MuxRouteEvent::Frame(frame) => return Ok(frame),
                MuxRouteEvent::Reconnecting { .. } => {}
            }
        }
    }

    pub async fn recv_authorized_event(&mut self) -> Result<MuxRouteEvent, HandlerClientError> {
        loop {
            match self.recv_event().await? {
                MuxRouteEvent::Frame(PeerFrame::Authorization { decision })
                    if matches!(decision.as_ref(), AuthorizationDecision::Permit(_)) =>
                {
                    tracing::debug!("local multiplexed Myko request authorized");
                }
                MuxRouteEvent::Frame(PeerFrame::Authorization { decision }) => {
                    tracing::warn!(decision = ?decision, "local multiplexed Myko request authorization failed");
                    return Err(HandlerClientError::Protocol(decision.public_message()));
                }
                event => return Ok(event),
            }
        }
    }

    pub async fn recv_frame(&mut self) -> Result<PeerFrame, HandlerClientError> {
        loop {
            match self.recv_event().await? {
                MuxRouteEvent::Frame(frame) => return Ok(frame),
                MuxRouteEvent::Reconnecting { .. } => {}
            }
        }
    }

    pub(super) async fn recv_event(&mut self) -> Result<MuxRouteEvent, HandlerClientError> {
        loop {
            let terminal = self.terminal.borrow().clone();
            if let Some(terminal) = terminal {
                if !terminal.follows_queued_data() || self.events.is_empty() {
                    return Err(terminal.into_error());
                }
                if let Ok(event) = self.events.try_recv() {
                    return Ok(event);
                }
            }
            tokio::select! {
                biased;
                changed = self.terminal.changed() => {
                    if changed.is_err() && self.events.is_empty() {
                        return Err(HandlerClientError::Transport(
                            "local session route closed".to_owned(),
                        ));
                    }
                }
                event = self.events.recv() => {
                    if let Some(event) = event {
                        return Ok(event);
                    }
                    let terminal = self.terminal.borrow().clone();
                    if let Some(terminal) = terminal {
                        return Err(terminal.into_error());
                    }
                    return Err(HandlerClientError::Transport(
                        "local session route closed".to_owned(),
                    ));
                }
            }
        }
    }
}

impl Drop for MuxSubscription {
    fn drop(&mut self) {
        self.lease.cancelled.store(true, Ordering::Release);
        self.cancel_wake.notify_one();
    }
}

#[derive(Clone, Debug)]
pub(super) enum RouteTerminal {
    Completed(Arc<str>),
    Transport(Arc<str>),
    Protocol(Arc<str>),
}

impl RouteTerminal {
    const fn follows_queued_data(&self) -> bool {
        matches!(self, Self::Completed(_))
    }

    pub(super) fn into_error(self) -> HandlerClientError {
        match self {
            Self::Completed(message) | Self::Transport(message) => {
                HandlerClientError::Transport(message.to_string())
            }
            Self::Protocol(message) => HandlerClientError::Protocol(message.to_string()),
        }
    }
}

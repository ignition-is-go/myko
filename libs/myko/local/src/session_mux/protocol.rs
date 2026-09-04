use serde::{Deserialize, Serialize};

use crate::{NodeRequestEnvelope, PeerFrame};

pub(super) const LOCAL_SESSION_MUX_VERSION: u16 = 1;
pub(super) const MAX_LOGICAL_STREAMS: usize = 1024;
pub(super) const CONTROL_CAPACITY: usize = 256;
pub(super) const PER_STREAM_FRAME_CAPACITY: usize = 256;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct StreamId(pub(super) u64);

impl StreamId {
    pub(super) const fn is_valid(self) -> bool {
        self.0 != 0
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "local_transport", rename_all = "snake_case")]
pub enum LocalConnectionHello {
    SessionMux { version: u16 },
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum LocalInitialBody {
    Mux(LocalConnectionHello),
    Single(Box<NodeRequestEnvelope>),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum ClientMuxFrame {
    Open {
        stream_id: StreamId,
        request: Box<NodeRequestEnvelope>,
    },
    Cancel {
        stream_id: StreamId,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum ServerMuxFrame {
    Ready {
        version: u16,
        max_streams: u32,
    },
    Opened {
        stream_id: StreamId,
    },
    Rejected {
        stream_id: StreamId,
        reason: OpenReject,
    },
    Data {
        stream_id: StreamId,
        frame: PeerFrame,
    },
    Closed {
        stream_id: StreamId,
        reason: CloseReason,
    },
}

impl ServerMuxFrame {
    pub(super) const fn stream_id(&self) -> Option<StreamId> {
        match self {
            Self::Ready { .. } => None,
            Self::Opened { stream_id }
            | Self::Rejected { stream_id, .. }
            | Self::Data { stream_id, .. }
            | Self::Closed { stream_id, .. } => Some(*stream_id),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "code", content = "message", rename_all = "snake_case")]
pub(super) enum OpenReject {
    Capacity,
    DuplicateId,
    InvalidRequest(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "code", content = "message", rename_all = "snake_case")]
pub(super) enum CloseReason {
    Completed,
    Cancelled,
    ServerShutdown,
    Protocol(String),
}

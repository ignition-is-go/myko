//! State that follows the socket's connection transitions.
//!
//! Each status delivery starts a new connection generation. Readiness and
//! errors are tagged with the generation that produced them and derived by
//! joining against the current one, so a transition hides them in the same
//! ticket. A callback that cleared them would write a later ticket that the
//! status writer never waits for.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use hyphae::{
    Cell, CellImmutable, CellMutable, Gettable, JoinExt, MapExt, Materialize, Mutable as _, ScanExt,
};

use super::ConnectionStatus;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Connection {
    pub(super) generation: u64,
    pub(super) status: ConnectionStatus,
}

#[derive(Clone)]
pub(super) struct Connections {
    cell: Cell<Connection, CellImmutable>,
    current: Arc<AtomicU64>,
}

impl Connections {
    pub(super) fn new(status: Cell<ConnectionStatus, CellImmutable>) -> Self {
        let current = Arc::new(AtomicU64::new(0));
        let published = current.clone();
        let initial = Connection {
            generation: 0,
            status: ConnectionStatus::Idle,
        };
        // The store runs at this node's fire, before any subscriber of
        // `cell` sends a request for the new generation, so a response
        // handler always reads the generation its request was sent under.
        let cell = status
            .scan(
                initial,
                move |previous: &Connection, status: &ConnectionStatus| {
                    let next = if previous.status == *status && previous.generation != 0 {
                        previous.clone()
                    } else {
                        Connection {
                            generation: previous.generation.wrapping_add(1),
                            status: status.clone(),
                        }
                    };
                    published.fetch_max(next.generation, Ordering::AcqRel);
                    next
                },
            )
            .materialize();
        Self { cell, current }
    }

    pub(super) fn cell(&self) -> Cell<Connection, CellImmutable> {
        self.cell.clone()
    }

    /// The generation of the newest connection transition that has fired.
    pub(super) fn current(&self) -> u64 {
        self.current.load(Ordering::Acquire)
    }

    /// `true` while `answered` names the current generation.
    pub(super) fn ready(
        &self,
        answered: &Cell<Option<u64>, CellMutable>,
    ) -> Cell<bool, CellImmutable> {
        self.cell
            .clone()
            .join(answered.clone())
            .map(|(connection, answered): &(Connection, Option<u64>)| {
                *answered == Some(connection.generation)
            })
            .materialize()
    }

    /// The value in `tagged` while its generation is the current one.
    pub(super) fn current_only<T>(
        &self,
        tagged: &Cell<Option<(u64, T)>, CellMutable>,
    ) -> Cell<Option<T>, CellImmutable>
    where
        T: hyphae::CellValue,
    {
        self.cell
            .clone()
            .join(tagged.clone())
            .map(|(connection, tagged): &(Connection, Option<(u64, T)>)| {
                tagged
                    .as_ref()
                    .filter(|(generation, _)| *generation == connection.generation)
                    .map(|(_, value)| value.clone())
            })
            .materialize()
    }
}

/// Mark `answered` for the generation current when the response arrived.
pub(super) fn mark_answered(answered: &Cell<Option<u64>, CellMutable>, generation: u64) {
    if answered.get() != Some(generation) {
        answered.set(Some(generation));
    }
}

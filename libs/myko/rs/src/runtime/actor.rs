//! Single-threaded actor primitive.
//!
//! An Actor runs on a dedicated OS thread and processes messages sequentially.
//! This provides total ordering of all messages and simple state management.

use std::thread::{self, JoinHandle};

use crossbeam::channel::{self, Receiver, Sender};

use super::error::SendError;
use super::sink::Sink;

/// Trait for actor implementations.
///
/// Actors are stateful message processors that run on a dedicated thread.
pub trait Actor: Send + 'static {
    /// The message type this actor handles.
    type Msg: Send + 'static;

    /// Handle a single message.
    ///
    /// This is called sequentially for each message, so no synchronization
    /// is needed for actor state.
    fn handle(&mut self, msg: Self::Msg);

    /// Called when the actor is shutting down.
    ///
    /// Override this to perform cleanup. Default implementation does nothing.
    fn on_shutdown(&mut self) {}
}

/// Handle to a running actor, used to send messages.
pub struct ActorRef<M> {
    pub(crate) tx: Sender<M>,
}

// Manual Clone impl - Sender is always Clone regardless of M
impl<M> Clone for ActorRef<M> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

// Manual Debug impl - doesn't depend on M implementing Debug
impl<M> std::fmt::Debug for ActorRef<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ActorRef")
    }
}

impl<M: Send + 'static> ActorRef<M> {
    /// Send a message to the actor.
    pub fn send(&self, msg: M) -> Result<(), SendError<M>> {
        self.tx.send(msg).map_err(SendError::from)
    }

    /// Send a message to the actor (ractor API compatibility).
    ///
    /// This is an alias for `send()` that matches ractor's `send_message()` API.
    pub fn send_message(&self, msg: M) -> Result<(), SendError<M>> {
        self.send(msg)
    }

    /// Make a synchronous call and wait for a response.
    ///
    /// The message constructor receives a reply channel sender.
    pub fn call<R, F>(&self, msg_fn: F) -> Result<R, CallError>
    where
        R: Send + 'static,
        F: FnOnce(Sender<R>) -> M,
    {
        let (reply_tx, reply_rx) = channel::bounded(1);
        let msg = msg_fn(reply_tx);
        self.tx.send(msg).map_err(|_| CallError::ActorStopped)?;
        reply_rx.recv().map_err(|_| CallError::NoResponse)
    }
}

/// Error from a call operation.
#[derive(Debug)]
pub enum CallError {
    /// The actor has stopped and isn't accepting messages.
    ActorStopped,
    /// The actor received the message but didn't send a response.
    NoResponse,
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::ActorStopped => write!(f, "actor stopped"),
            CallError::NoResponse => write!(f, "no response from actor"),
        }
    }
}

impl std::error::Error for CallError {}

impl<M: Send + 'static> Sink<M> for ActorRef<M> {
    fn send(&self, msg: M) -> Result<(), SendError<M>> {
        ActorRef::send(self, msg)
    }
}

/// Handle to a running actor.
///
/// Returns the actor reference for sending messages and provides lifecycle management.
pub struct ActorHandle<M> {
    actor_ref: ActorRef<M>,
    thread: Option<JoinHandle<()>>,
}

impl<M> ActorHandle<M> {
    /// Get a cloneable reference for sending messages.
    ///
    /// The returned reference can be cloned and shared across threads.
    pub fn actor_ref(&self) -> ActorRef<M> {
        self.actor_ref.clone()
    }

    /// Shutdown the actor and wait for it to finish.
    ///
    /// This drops the internal sender, signals the actor to stop,
    /// and waits for the actor thread to complete.
    pub fn shutdown(mut self) -> thread::Result<()> {
        // Drop our sender to signal shutdown
        drop(std::mem::replace(
            &mut self.actor_ref,
            ActorRef {
                tx: crossbeam::channel::unbounded().0,
            },
        ));
        if let Some(handle) = self.thread.take() {
            handle.join()
        } else {
            Ok(())
        }
    }

    /// Wait for the actor to finish, assuming all external senders have been dropped.
    ///
    /// Note: This will block forever if any ActorRef clones are still alive.
    /// Prefer using `shutdown()` which handles this automatically.
    pub fn join(mut self) -> thread::Result<()> {
        if let Some(handle) = self.thread.take() {
            handle.join()
        } else {
            Ok(())
        }
    }
}

/// Spawn an actor on a dedicated thread.
pub fn spawn<A: Actor>(mut actor: A) -> ActorHandle<A::Msg> {
    let (tx, rx): (Sender<A::Msg>, Receiver<A::Msg>) = channel::unbounded();

    let thread = thread::spawn(move || {
        while let Ok(msg) = rx.recv() {
            actor.handle(msg);
        }
        actor.on_shutdown();
    });

    ActorHandle {
        actor_ref: ActorRef { tx },
        thread: Some(thread),
    }
}

/// Spawn an actor using a closure as the handler.
///
/// This is a convenience for simple actors that don't need the full trait.
pub fn spawn_fn<M, F>(mut handler: F) -> ActorHandle<M>
where
    M: Send + 'static,
    F: FnMut(M) + Send + 'static,
{
    let (tx, rx): (Sender<M>, Receiver<M>) = channel::unbounded();

    let thread = thread::spawn(move || {
        while let Ok(msg) = rx.recv() {
            handler(msg);
        }
    });

    ActorHandle {
        actor_ref: ActorRef { tx },
        thread: Some(thread),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_actor_processes_messages() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let handle = spawn_fn(move |n: usize| {
            counter_clone.fetch_add(n, Ordering::SeqCst);
        });

        let actor_ref = handle.actor_ref();
        actor_ref.send(1).expect("send failed");
        actor_ref.send(2).expect("send failed");
        actor_ref.send(3).expect("send failed");

        // Drop our ref, then shutdown (which drops the handle's ref too)
        drop(actor_ref);
        handle.shutdown().expect("shutdown failed");

        assert_eq!(counter.load(Ordering::SeqCst), 6);
    }

    #[test]
    fn test_actor_call_response() {
        struct Doubler;

        impl Actor for Doubler {
            type Msg = (i32, Sender<i32>);

            fn handle(&mut self, (n, reply): Self::Msg) {
                let _ = reply.send(n * 2);
            }
        }

        let handle = spawn(Doubler);

        let result = handle.actor_ref.call(|reply| (21, reply));
        assert_eq!(result.expect("call failed"), 42);
    }
}

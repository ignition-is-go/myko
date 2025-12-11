pub mod actors;
pub mod api;
pub mod client;
pub mod command;
pub mod common;
pub mod entities;
pub mod event;
pub mod item;
pub mod message;
pub mod parsers;
pub mod query;
pub mod report;
pub mod server;
pub mod utils;
//
pub mod prelude;
pub mod type_gen;
pub use inventory::submit;
pub use ts_rs::TS;

/// Helper macro for submitting message event registrations
#[macro_export]
macro_rules! submit_message_event {
    ($variant:ident, $event:expr) => {
        inventory::submit!($crate::message::MessageEventRegistration {
            variant_name: stringify!($variant),
            event_value: $event,
        });
    };
}

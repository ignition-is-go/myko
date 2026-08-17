use gpui::App;
use myko::{
    item::Eventable,
    wire::{MEvent, MEventType},
};

use crate::myko;

/// Sends a typed raw SET event through the application-global client.
///
/// This is the low-latency path for ephemeral, server-normalized state such as
/// cursor presence. Durable domain mutations should continue to use commands.
pub fn send_set_event(item: &impl Eventable, source_id: &str, cx: &App) -> Result<(), String> {
    myko(cx)
        .client()
        .send_event(MEvent::from_item(item, MEventType::SET, source_id))
}

/// Sends a typed raw DEL event through the application-global client.
pub fn send_delete_event(item: &impl Eventable, source_id: &str, cx: &App) -> Result<(), String> {
    myko(cx).client().send_event(MEvent::del(item, source_id))
}

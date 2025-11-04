use crate::message::MykoMessage;

pub struct WebSocketConnection;

pub enum WebSocketConnectionMsg {
    Transmit(MykoMessage<()>),
}

pub struct WebSocketConnectionState {}

pub struct WebSocketConnectionArgs {}

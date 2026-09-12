mod endpoint;
mod protocol;
mod server;
mod supervisor;

pub use endpoint::MuxRouteEvent;
pub use endpoint::{LocalMultiplexedSession, MuxSubscription};
pub use protocol::LocalInitialBody;
pub use server::serve_session_mux;

#[cfg(test)]
pub async fn interrupt_submission(
    listener: tokio::net::UnixListener,
    command_id: myko_federation::CommandId,
) -> Result<(), crate::LocalPeerError> {
    use protocol::{
        ClientMuxFrame, LOCAL_SESSION_MUX_VERSION, LocalConnectionHello, MAX_LOGICAL_STREAMS,
        ServerMuxFrame,
    };

    use crate::{
        Envelope, LocalPeerError, PeerRequest,
        transport::{read_frame, write_frame},
    };

    let (mut stream, _) = listener.accept().await?;
    let _: Envelope<LocalConnectionHello> = read_frame(&mut stream).await?;
    write_frame(
        &mut stream,
        &Envelope::new(ServerMuxFrame::Ready {
            version: LOCAL_SESSION_MUX_VERSION,
            max_streams: u32::try_from(MAX_LOGICAL_STREAMS)
                .map_err(|error| LocalPeerError::Protocol(error.to_string()))?,
        }),
    )
    .await?;
    let open: Envelope<ClientMuxFrame> = read_frame(&mut stream).await?;
    let ClientMuxFrame::Open { stream_id, request } = open.body else {
        return Err(LocalPeerError::Protocol(
            "expected submission open".to_owned(),
        ));
    };
    if !matches!(request.request, PeerRequest::Submit { command } if command.id == command_id) {
        return Err(LocalPeerError::Protocol(
            "unexpected submission identity".to_owned(),
        ));
    }
    write_frame(
        &mut stream,
        &Envelope::new(ServerMuxFrame::Opened { stream_id }),
    )
    .await
}

use std::net::ToSocketAddrs;

use bytes::BytesMut;
use futures::channel::mpsc;
use futures::future::TryFutureExt;
use futures::prelude::*;
use futures::sink::SinkExt;
use futures::stream::TryStreamExt;

use tokio::codec::{Decoder, Encoder, Framed};
use tokio::net::TcpStream;
use tokio::runtime::current_thread::spawn;

type ClientTransport = Framed<TcpStream, ClientCodec>;

use crate::frame;
use crate::{ClientMsg, Message, Result, ServerMsg};

/// Connect to a STOMP server via TCP, including the connection handshake.
/// If successful, returns a tuple of a message stream and a sender,
/// which may be used to receive and send messages respectively.
pub async fn connect<T: Into<Message<ClientMsg>> + 'static>(
    address: String,
    login: Option<String>,
    passcode: Option<String>,
) -> Result<(
    impl Stream<Item = Result<Message<ServerMsg>>>,
    mpsc::UnboundedSender<T>,
)> {
    let (tx, rx) = mpsc::unbounded::<T>();
    let addr = address.as_str().to_socket_addrs().unwrap().next().unwrap();
    let tcp = TcpStream::connect(&addr).await?;
    let mut transport = ClientCodec.framed(tcp);
    client_handshake(&mut transport, address, login, passcode).await?;
    let (sink, stream) = transport.split();

    let fut_sink = sink
        .send_all(
            rx.map_ok(|m| m.into())
                .map_err(|()| format_err!("Channel closed")),
        )
        .map(|res| match res {
            Ok(_) => println!("Sink closed"),
            Err(e) => eprintln!("{}", e),
        });

    spawn(fut_sink);

    Ok((stream, tx))
}

async fn client_handshake(
    transport: &mut ClientTransport,
    host: String,
    login: Option<String>,
    passcode: Option<String>,
) -> Result<()> {
    let connect = Message {
        content: ClientMsg::Connect {
            accept_version: "1.1,1.2".into(),
            host: host,
            login: login,
            passcode: passcode,
            heartbeat: None,
        },
        extra_headers: vec![],
    };
    // Send the message
    transport.send(connect).await?;
    // Receive reply
    let msg = transport.next().await.transpose()?;
    if let Some(ServerMsg::Connected { .. }) = msg.map(|m| m.content) {
        Ok(())
    } else {
        Err(format_err!("unexpected reply"))
    }
}

struct ClientCodec;

impl Decoder for ClientCodec {
    type Item = Message<ServerMsg>;
    type Error = failure::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        let (item, offset) = match frame::parse_frame(&src) {
            Ok((remain, frame)) => (
                Message::<ServerMsg>::from_frame(frame),
                remain.as_ptr() as usize - src.as_ptr() as usize,
            ),
            Err(nom::Err::Incomplete(_)) => return Ok(None),
            Err(e) => bail!("Parse failed: {:?}", e),
        };
        src.split_to(offset);
        item.map(|v| Some(v))
    }
}

impl Encoder for ClientCodec {
    type Item = Message<ClientMsg>;
    type Error = failure::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<()> {
        item.to_frame().serialize(dst);
        Ok(())
    }
}

use std::net::ToSocketAddrs;

use bytes::BytesMut;
use futures::prelude::*;
use futures::sync::mpsc;
use futures::future::{err as ferr, ok as fok};

use tokio::executor::current_thread::spawn;
use tokio::codec::{Decoder, Encoder, Framed};
use tokio::net::TcpStream;

type ClientTransport = Framed<TcpStream, ClientCodec>;

use {ClientMsg, ServerMsg, Message, Result};
use frame;

/// Connect to a STOMP server via TCP, including the connection handshake.
/// If successful, returns a tuple of a message stream and a sender,
/// which may be used to receive and send messages respectively.
pub fn connect<T: Into<Message<ClientMsg>> + 'static>(
    address: String,
    login: Option<String>,
    passcode: Option<String>,
) -> Result<(StompStream, mpsc::UnboundedSender<T>)> {
    let (tx, rx) = mpsc::unbounded::<T>();
    let addr = address.as_str().to_socket_addrs().unwrap().next().unwrap();
    let inner = TcpStream::connect(&addr)
        .map_err(|e| e.into())
        .and_then(|tcp| {
            let transport = ClientCodec.framed(tcp);
            client_handshake(transport, address, login, passcode)
        })
        .and_then(|tcp| {
            let (sink, stream) = tcp.split();
            let fsink = sink
                .send_all(
                    rx.map(|m| m.into())
                        .map_err(|()| format_err!("Channel closed")),
                )
                .map(|_| println!("Sink closed"))
                .map_err(|e| eprintln!("{}", e));
            spawn(fsink);
            fok(stream)
        })
        .flatten_stream();
    Ok((
        StompStream {
            inner: Box::new(inner),
        },
        tx,
    ))
}

/// Structure representing a stream of STOMP messages
pub struct StompStream {
    inner: Box<Stream<Item = Message<ServerMsg>, Error = failure::Error>>,
}

impl Stream for StompStream {
    type Item = Message<ServerMsg>;
    type Error = failure::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.inner.poll()
    }
}

fn client_handshake(
    transport: ClientTransport,
    host: String,
    login: Option<String>,
    passcode: Option<String>,
) -> Box<Future<Item = ClientTransport, Error = failure::Error>> {
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

    let fut = transport
        .send(connect)
        .and_then(|transport| transport.into_future().map_err(|(e, _)| e.into()))
        .and_then(|(msg, stream)| {
            if let Some(ServerMsg::Connected { .. }) = msg.map(|m| m.content) {
                fok(stream)
            } else {
                ferr(format_err!("unexpected reply"))
            }
        });
    Box::new(fut)
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

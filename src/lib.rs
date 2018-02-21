//! tokio-stomp - A library for asynchronous streaming of STOMP messages

extern crate bytes;
#[macro_use]
extern crate failure;
extern crate futures;
#[macro_use]
extern crate nom;
extern crate tokio;
extern crate tokio_io;

use bytes::{BufMut, BytesMut};
use futures::prelude::*;
use futures::sync::mpsc;
use futures::future::{err as ferr, join_all, ok as fok};
use tokio::net::{TcpListener, TcpStream};
use tokio::executor::current_thread::spawn;
use tokio_io::codec::{Decoder, Encoder, Framed};
use tokio_io::AsyncRead;

use std::net::{SocketAddr, ToSocketAddrs};
use std::borrow::Cow;
use std::collections::HashMap;
use std::rc::Rc;
use std::cell::RefCell;

type Result<T> = std::result::Result<T, failure::Error>;
type ClientTransport = Framed<TcpStream, ClientCodec>;
type ServerTransport = Framed<TcpStream, ServerCodec>;
type TxS = mpsc::UnboundedSender<Rc<Message<ServerMsg>>>;
type RxS = mpsc::UnboundedReceiver<Rc<Message<ServerMsg>>>;
type TxC = mpsc::UnboundedSender<Message<ClientMsg>>;
type RxC = mpsc::UnboundedReceiver<Message<ClientMsg>>;
type Connections = HashMap<SocketAddr, TxS>;
type State = Rc<RefCell<ServerState>>;

#[derive(Debug)]
struct Frame<'a> {
    command: &'a [u8],
    // TODO use ArrayVec to keep headers on the stack
    headers: Vec<(&'a [u8], Cow<'a, [u8]>)>,
    body: Option<&'a [u8]>,
}

impl<'a> Frame<'a> {
    fn new(
        command: &'a [u8],
        headers: &[(&'a [u8], Option<Cow<'a, [u8]>>)],
        body: Option<&'a [u8]>,
    ) -> Frame<'a> {
        let headers = headers
            .iter()
            // filter out headers with None value
            .filter_map(|&(k, ref v)| v.as_ref().map(|i| (k, (&*i).clone())))
            .collect();
        Frame {
            command,
            headers,
            body,
        }
    }

    fn serialize(&self, buffer: &mut BytesMut) {
        fn write_escaped(b: u8, buffer: &mut BytesMut) {
            match b {
                b'\r' => {
                    buffer.put(b'\\');
                    buffer.put(b'r')
                }
                b'\n' => {
                    buffer.put(b'\\');
                    buffer.put(b'n')
                }
                b':' => {
                    buffer.put(b'\\');
                    buffer.put(b'c')
                }
                b'\\' => {
                    buffer.put(b'\\');
                    buffer.put(b'\\')
                }
                b => buffer.put(b),
            }
        }
        let requires =
            self.command.len() + self.body.map(|b| b.len() + 20).unwrap_or(0)
                + self.headers
                    .iter()
                    .fold(0, |acc, &(ref k, ref v)| acc + k.len() + v.len()) + 30;
        if buffer.remaining_mut() < requires {
            buffer.reserve(requires);
        }
        buffer.put_slice(self.command);
        buffer.put(b'\n');
        self.headers.iter().for_each(|&(key, ref val)| {
            for byte in key {
                write_escaped(*byte, buffer);
            }
            buffer.put(b':');
            for byte in val.iter() {
                write_escaped(*byte, buffer);
            }
            buffer.put(b'\n');
        });
        if let Some(body) = self.body {
            buffer.put_slice(&get_content_length_header(&body));
            buffer.put(b'\n');
            buffer.put_slice(body);
        } else {
            buffer.put(b'\n');
        }
        buffer.put(b'\x00');
    }
}

named!(eol, preceded!(opt!(tag!("\r")), tag!("\n")));

named!(
    parse_header<(&[u8], Cow<[u8]>)>,
    pair!(
        take_until_either!(":\n"),
        preceded!(
            tag!(":"),
            map!(take_until_and_consume1!("\n"), |bytes| Cow::Borrowed(
                strip_cr(bytes)
            ))
        )
    )
);

fn get_content_length(headers: &[(&[u8], Cow<[u8]>)]) -> Option<u32> {
    for h in headers {
        if h.0 == b"content-length" {
            return std::str::from_utf8(&*h.1)
                .ok()
                .and_then(|v| v.parse::<u32>().ok());
        }
    }
    None
}

fn is_empty_slice(s: &[u8]) -> Option<&[u8]> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

named!(
    parse_frame<Frame>,
    do_parse!(
        many0!(eol) >> command: map!(take_until_and_consume!("\n"), strip_cr)
            >> headers: many0!(parse_header) >> eol
            >> body:
                switch!(value!(get_content_length(&*headers)),
            Some(v) => map!(take!(v), Some) |
            None => map!(take_until!("\x00"), is_empty_slice)
        ) >> tag!("\x00") >> many0!(complete!(eol)) >> (Frame {
            command,
            headers,
            body,
        })
    )
);

fn strip_cr(buf: &[u8]) -> &[u8] {
    if let Some(&b'\r') = buf.last() {
        &buf[..buf.len() - 1]
    } else {
        buf
    }
}

fn fetch_header<'a>(headers: &'a [(&'a [u8], Cow<'a, [u8]>)], key: &'a str) -> Option<String> {
    let kk = key.as_bytes();
    for &(k, ref v) in headers {
        if &*k == kk {
            return String::from_utf8(v.to_vec()).ok();
        }
    }
    None
}

fn expect_header<'a>(headers: &'a [(&'a [u8], Cow<'a, [u8]>)], key: &'a str) -> Result<String> {
    fetch_header(headers, key).ok_or_else(|| format_err!("Expected header {} missing", key))
}

impl<'a> Frame<'a> {
    #[allow(dead_code)]
    fn to_client_msg(&'a self) -> Result<Message<ClientMsg>> {
        use ClientMsg::*;
        use expect_header as eh;
        use fetch_header as fh;
        let h = &self.headers;
        let expect_keys: &[&[u8]];
        let content = match self.command {
            b"STOMP" | b"CONNECT" => {
                expect_keys = &[
                    b"accept-version",
                    b"host",
                    b"login",
                    b"passcode",
                    b"heart-beat",
                ];
                let heartbeat = if let Some(hb) = fh(h, "heart-beat") {
                    Some(parse_heartbeat(&hb)?)
                } else {
                    None
                };
                Connect {
                    accept_version: eh(h, "accept-version")?,
                    host: eh(h, "host")?,
                    login: fh(h, "login"),
                    passcode: fh(h, "passcode"),
                    heartbeat,
                }
            }
            b"DISCONNECT" => {
                expect_keys = &[b"receipt"];
                Disconnect {
                    receipt: fh(h, "receipt"),
                }
            }
            b"SEND" => {
                expect_keys = &[b"destination", b"transaction"];
                Send {
                    destination: eh(h, "destination")?,
                    transaction: fh(h, "transaction"),
                    body: self.body.map(|v| v.to_vec()),
                }
            }
            b"SUBSCRIBE" => {
                expect_keys = &[b"destination", b"id", b"ack"];
                Subscribe {
                    destination: eh(h, "destination")?,
                    id: eh(h, "id")?,
                    ack: match fh(h, "ack").as_ref().map(|s| s.as_str()) {
                        Some("auto") => Some(AckMode::Auto),
                        Some("client") => Some(AckMode::Client),
                        Some("client-individual") => Some(AckMode::ClientIndividual),
                        Some(other) => bail!("Invalid ack mode: {}", other),
                        None => None,
                    },
                }
            }
            b"UNSUBSCRIBE" => {
                expect_keys = &[b"id"];
                Unsubscribe { id: eh(h, "id")? }
            }
            b"ACK" => {
                expect_keys = &[b"id", b"transaction"];
                Ack {
                    id: eh(h, "id")?,
                    transaction: fh(h, "transaction"),
                }
            }
            b"NACK" => {
                expect_keys = &[b"id", b"transaction"];
                Nack {
                    id: eh(h, "id")?,
                    transaction: fh(h, "transaction"),
                }
            }
            b"BEGIN" => {
                expect_keys = &[b"transaction"];
                Begin {
                    transaction: eh(h, "transaction")?,
                }
            }
            b"COMMIT" => {
                expect_keys = &[b"transaction"];
                Commit {
                    transaction: eh(h, "transaction")?,
                }
            }
            b"ABORT" => {
                expect_keys = &[b"transaction"];
                Abort {
                    transaction: eh(h, "transaction")?,
                }
            }
            other => bail!("Frame not recognized: {:?}", String::from_utf8_lossy(other)),
        };
        let extra_headers = h.iter()
            .filter_map(|&(k, ref v)| {
                if !expect_keys.contains(&k) {
                    Some((k.to_vec(), (&*v).to_vec()))
                } else {
                    None
                }
            })
            .collect();
        Ok(Message {
            content,
            extra_headers,
        })
    }

    fn to_server_msg(&'a self) -> Result<Message<ServerMsg>> {
        use ServerMsg::{Connected, Error, Message as Msg, Receipt};
        use expect_header as eh;
        use fetch_header as fh;
        let h = &self.headers;
        let expect_keys: &[&[u8]];
        let content = match self.command {
            b"CONNECTED" => {
                expect_keys = &[b"version", b"session", b"server", b"heart-beat"];
                Connected {
                    version: eh(h, "version")?,
                    session: fh(h, "session"),
                    server: fh(h, "server"),
                    heartbeat: fh(h, "heart-beat"),
                }
            }
            b"MESSAGE" => {
                expect_keys = &[b"destination", b"message-id", b"subscription"];
                Msg {
                    destination: eh(h, "destination")?,
                    message_id: eh(h, "message-id")?,
                    subscription: eh(h, "subscription")?,
                    body: self.body.map(|v| v.to_vec()),
                }
            }
            b"RECEIPT" => {
                expect_keys = &[b"receipt-id"];
                Receipt {
                    receipt_id: eh(h, "receipt-id")?,
                }
            }
            b"ERROR" => {
                expect_keys = &[b"message"];
                Error {
                    message: fh(h, "message"),
                    body: self.body.map(|v| v.to_vec()),
                }
            }
            other => bail!("Frame not recognized: {:?}", String::from_utf8_lossy(other)),
        };
        let extra_headers = h.iter()
            .filter_map(|&(k, ref v)| {
                if !expect_keys.contains(&k) {
                    Some((k.to_vec(), (&*v).to_vec()))
                } else {
                    None
                }
            })
            .collect();
        Ok(Message {
            content,
            extra_headers,
        })
    }
}

/// A STOMP message sent from the server
/// See the [Spec](https://stomp.github.io/stomp-specification-1.2.html) for more information
#[derive(Debug, Clone)]
pub enum ServerMsg {
    #[doc(hidden)]
    Connected {
        version: String,
        session: Option<String>,
        server: Option<String>,
        heartbeat: Option<String>,
    },
    /// Conveys messages from subscriptions to the client
    Message {
        destination: String,
        message_id: String,
        subscription: String,
        body: Option<Vec<u8>>,
    },
    /// Sent from the server to the client once a server has successfully processed a client frame that requests a receipt
    Receipt { receipt_id: String },
    /// Something went wrong. After sending an Error, the server will close the connection
    Error {
        message: Option<String>,
        body: Option<Vec<u8>>,
    },
}

/// A representation of a STOMP frame
#[derive(Debug)]
pub struct Message<T> {
    /// The message content
    pub content: T,
    /// Headers present in the frame which were not required by the content
    pub extra_headers: Vec<(Vec<u8>, Vec<u8>)>,
}

// TODO tidy this lot up with traits?
impl Message<ServerMsg> {
    // fn to_frame<'a>(&'a self) -> Frame<'a> {
    //     unimplemented!()
    // }

    // TODO make this undead
    fn from_frame<'a>(frame: Frame<'a>) -> Result<Message<ServerMsg>> {
        frame.to_server_msg()
    }
}

impl Message<ClientMsg> {
    fn to_frame<'a>(&'a self) -> Frame<'a> {
        self.content.to_frame()
    }

    #[allow(dead_code)]
    fn from_frame<'a>(frame: Frame<'a>) -> Result<Message<ClientMsg>> {
        frame.to_client_msg()
    }
}

impl From<ClientMsg> for Message<ClientMsg> {
    fn from(content: ClientMsg) -> Message<ClientMsg> {
        Message {
            content,
            extra_headers: vec![],
        }
    }
}

/// A STOMP message sent by the client.
/// See the [Spec](https://stomp.github.io/stomp-specification-1.2.html) for more information
#[derive(Debug, Clone)]
pub enum ClientMsg {
    #[doc(hidden)]
    Connect {
        accept_version: String,
        host: String,
        login: Option<String>,
        passcode: Option<String>,
        heartbeat: Option<(u32, u32)>,
    },
    /// Send a message to a destination in the messaging system
    Send {
        destination: String,
        transaction: Option<String>,
        body: Option<Vec<u8>>,
    },
    /// Register to listen to a given destination
    Subscribe {
        destination: String,
        id: String,
        ack: Option<AckMode>,
    },
    /// Remove an existing subscription
    Unsubscribe { id: String },
    /// Acknowledge consumption of a message from a subscription using
    /// 'client' or 'client-individual' acknowledgment.
    Ack {
        // TODO ack and nack should be automatic?
        id: String,
        transaction: Option<String>,
    },
    /// Notify the server that the client did not consume the message
    Nack {
        id: String,
        transaction: Option<String>,
    },
    /// Start a transaction
    Begin { transaction: String },
    /// Commit an in-progress transaction
    Commit { transaction: String },
    /// Roll back an in-progress transaction
    Abort { transaction: String },
    /// Gracefully disconnect from the server
    /// Clients MUST NOT send any more frames after the DISCONNECT frame is sent.
    Disconnect { receipt: Option<String> },
}

#[derive(Debug, Clone, Copy)]
pub enum AckMode {
    Auto,
    Client,
    ClientIndividual,
}

fn opt_str_to_bytes<'a>(s: &'a Option<String>) -> Option<Cow<'a, [u8]>> {
    s.as_ref().map(|v| Cow::Borrowed(v.as_bytes()))
}

fn get_content_length_header(body: &[u8]) -> Vec<u8> {
    format!("content-length:{}\n", body.len()).into()
}

fn parse_heartbeat(hb: &str) -> Result<(u32, u32)> {
    let mut split = hb.splitn(1, ',');
    let left = split.next().ok_or_else(|| format_err!("Bad heartbeat"))?;
    let right = split.next().ok_or_else(|| format_err!("Bad heartbeat"))?;
    Ok((left.parse()?, right.parse()?))
}

impl ClientMsg {
    fn to_frame<'a>(&'a self) -> Frame<'a> {
        use opt_str_to_bytes as sb;
        use ClientMsg::*;
        use Cow::*;
        match *self {
            Connect {
                ref accept_version,
                ref host,
                ref login,
                ref passcode,
                ref heartbeat,
            } => Frame::new(
                b"CONNECT",
                &[
                    (b"accept-version", Some(Borrowed(accept_version.as_bytes()))),
                    (b"host", Some(Borrowed(host.as_bytes()))),
                    (b"login", sb(login)),
                    (b"passcode", sb(passcode)),
                    (
                        b"heart-beat",
                        heartbeat.map(|(v1, v2)| Owned(format!("{},{}", v1, v2).into())),
                    ),
                ],
                None,
            ),
            Disconnect { ref receipt } => {
                Frame::new(b"DISCONNECT", &[(b"receipt", sb(&receipt))], None)
            }
            Subscribe {
                ref destination,
                ref id,
                ref ack,
            } => Frame::new(
                b"SUBSCRIBE",
                &[
                    (b"destination", Some(Borrowed(destination.as_bytes()))),
                    (b"id", Some(Borrowed(id.as_bytes()))),
                    (
                        b"ack",
                        ack.map(|ack| match ack {
                            AckMode::Auto => Borrowed(&b"auto"[..]),
                            AckMode::Client => Borrowed(&b"client"[..]),
                            AckMode::ClientIndividual => Borrowed(&b"client-individual"[..]),
                        }),
                    ),
                ],
                None,
            ),
            Unsubscribe { ref id } => Frame::new(
                b"UNSUBSCRIBE",
                &[(b"id", Some(Borrowed(id.as_bytes())))],
                None,
            ),
            Send {
                ref destination,
                ref transaction,
                ref body,
            } => Frame::new(
                b"SEND",
                &[
                    (b"destination", Some(Borrowed(destination.as_bytes()))),
                    (b"id", sb(transaction)),
                ],
                body.as_ref().map(|v| v.as_ref()),
            ),
            Ack {
                ref id,
                ref transaction,
            } => Frame::new(
                b"ACK",
                &[
                    (b"id", Some(Borrowed(id.as_bytes()))),
                    (b"transaction", sb(transaction)),
                ],
                None,
            ),
            Nack {
                ref id,
                ref transaction,
            } => Frame::new(
                b"NACK",
                &[
                    (b"id", Some(Borrowed(id.as_bytes()))),
                    (b"transaction", sb(transaction)),
                ],
                None,
            ),
            Begin { ref transaction } => Frame::new(
                b"BEGIN",
                &[(b"transaction", Some(Borrowed(transaction.as_bytes())))],
                None,
            ),
            Commit { ref transaction } => Frame::new(
                b"COMMIT",
                &[(b"transaction", Some(Borrowed(transaction.as_bytes())))],
                None,
            ),
            Abort { ref transaction } => Frame::new(
                b"ABORT",
                &[(b"transaction", Some(Borrowed(transaction.as_bytes())))],
                None,
            ),
        }
    }
}

struct ClientCodec;

impl Decoder for ClientCodec {
    type Item = Message<ServerMsg>;
    type Error = failure::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        let (item, offset) = match parse_frame(&src) {
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

struct ServerCodec;

impl Decoder for ServerCodec {
    type Item = Message<ClientMsg>;
    type Error = failure::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        unimplemented!()
        // let (item, offset) = match parse_frame(&src) {
        //     Ok((remain, frame)) => {
        //         (Message::<ServerMsg>::from_frame(frame),
        //          remain.as_ptr() as usize - src.as_ptr() as usize)
        //     }
        //     Err(nom::Err::Incomplete(_)) => return Ok(None),
        //     Err(e) => bail!("Parse failed: {:?}", e),
        // };
        // src.split_to(offset);
        // item.map(|v| Some(v))
    }
}

impl<'a> Encoder for ServerCodec {
    type Item = Rc<Message<ServerMsg>>;
    type Error = failure::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<()> {
        unimplemented!()
        // item.to_frame().serialize(dst);
        // Ok(())
    }
}

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
            let transport = tcp.framed(ClientCodec);
            handshake(transport, address, login, passcode)
        })
        .and_then(|tcp| {
            let (sink, stream) = tcp.split();
            let fsink = sink.send_all(
                rx.map(|m| m.into())
                    .map_err(|()| format_err!("Channel closed")),
            ).map(|_| println!("Sink closed"))
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

fn handshake(
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

pub struct Server {
    inner: Box<Future<Item = (), Error = failure::Error>>,
}

#[derive(Default)]
struct ServerState {
    connections: Connections,
    subs_ip_lookup: HashMap<String, Vec<SocketAddr>>,
    ip_subs_lookup: HashMap<SocketAddr, Vec<String>>,
}

fn state_handler(rx: RxC) -> Box<Future<Item = (), Error = failure::Error>> {
    use ClientMsg::*;
    let mut state = ServerState::default();
    Box::new(rx.for_each(move |msg| {
        match msg.content {
            Connect {
                accept_version,
                host,
                login,
                passcode,
                heartbeat,
            } => {
                // Reply with connected
                unimplemented!()
            }
            Send {
                destination, body, ..
            } => route_message(&state, destination, body),
            Subscribe {
                destination,
                id,
                ack,
            } => {
                // add subscription
                unimplemented!()
            }
            Unsubscribe { id } => {
                // remove subscription
                unimplemented!()
            }
            Disconnect { receipt } => {
                // remove subscription
                unimplemented!()
            }
            _ => unimplemented!(),
        }
    }).map_err(|()| format_err!("")))
}

fn route_message(
    state: &ServerState,
    destination: String,
    body: Option<Vec<u8>>,
) -> Box<Future<Item = (), Error = ()>> {
    let servermsg = Rc::new(Message {
        content: ServerMsg::Message {
            destination,
            message_id: "hello".into(),
            subscription: "hi".into(),
            body,
        },
        extra_headers: vec![],
    });
    let results: Vec<_> = state
        .connections
        .values()
        .map(move |sink| {
            let servermsg = servermsg.clone();
            sink.unbounded_send(servermsg)
                .map_err(|_| format_err!("Send failed"))
        })
        .collect();
    Box::new(
        join_all(results)
            .map(|_| ())
            .map_err(|e| eprintln!("{}", e)),
    )
}

impl Server {
    pub fn new<T: ToSocketAddrs>(addr: T) -> Result<Server> {
        let state: State = Default::default();
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        let listener = TcpListener::bind(&addr)?;

        let (txc, rxc) = mpsc::unbounded();
        spawn(state_handler(rxc).map_err(|e| eprintln!("{}", e)));

        let fut = listener.incoming().for_each(move |conn| {
            let addr = conn.peer_addr().unwrap();
            let transport = conn.framed(ServerCodec);
            let (sink, source) = transport.split();

            let (txs, rxs) = mpsc::unbounded();

            //state.borrow_mut().connections.insert(addr, txs).unwrap();

            spawn(
                rxs.map_err(|()| format_err!("Channel closed"))
                    .forward(sink)
                    .map(|_| ())
                    .map_err(|e| eprintln!("{}", e)),
            );
            spawn(
                source
                    .forward(txc.clone())
                    .map(|_| ())
                    .map_err(|e| eprintln!("{}", e)),
            );

            fok(())
        });
        Ok(Server {
            inner: Box::new(fut.map_err(|e| e.into())),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_serialize_connect() {
        let data = b"CONNECT
accept-version:1.2
host:datafeeds.here.co.uk
login:user
passcode:password\n\n\x00"
            .to_vec();
        let (_, frame) = parse_frame(&data).unwrap();
        assert_eq!(frame.command, b"CONNECT");
        let headers_expect: Vec<(&[u8], &[u8])> = vec![
            (&b"accept-version"[..], &b"1.2"[..]),
            (b"host", b"datafeeds.here.co.uk"),
            (b"login", b"user"),
            (b"passcode", b"password"),
        ];
        let fh: Vec<_> = frame.headers.iter().map(|&(k, ref v)| (k, &**v)).collect();
        assert_eq!(fh, headers_expect);
        assert_eq!(frame.body, None);
        let stomp = frame.to_client_msg().unwrap();
        let mut buffer = BytesMut::new();
        stomp.to_frame().serialize(&mut buffer);
        assert_eq!(&*buffer, &*data);
    }

    #[test]
    fn parse_and_serialize_message() {
        let mut data = b"\nMESSAGE
destination:datafeeds.here.co.uk
message-id:12345
subscription:some-id
"
            .to_vec();
        let body = "this body contains \x00 nulls \n and \r\n newlines \x00 OK?";
        let rest = format!("content-length:{}\n\n{}\x00", body.len(), body);
        data.extend_from_slice(rest.as_bytes());
        let (_, frame) = parse_frame(&data).unwrap();
        assert_eq!(frame.command, b"MESSAGE");
        let headers_expect: Vec<(&[u8], &[u8])> = vec![
            (&b"destination"[..], &b"datafeeds.here.co.uk"[..]),
            (b"message-id", b"12345"),
            (b"subscription", b"some-id"),
            (b"content-length", b"50"),
        ];
        let fh: Vec<_> = frame.headers.iter().map(|&(k, ref v)| (k, &**v)).collect();
        assert_eq!(fh, headers_expect);
        assert_eq!(frame.body, Some(body.as_bytes()));
        frame.to_server_msg().unwrap();
        // TODO to_frame for ServerMsg
        // let roundtrip = stomp.to_frame().serialize();
        // assert_eq!(roundtrip, data);
    }
}

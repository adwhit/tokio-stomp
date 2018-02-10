#![feature(conservative_impl_trait)]

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
use futures::future::{err as ferr, ok as fok};
use tokio::net::TcpStream;
use tokio::executor::current_thread::spawn;
use tokio_io::codec::{Decoder, Encoder, Framed};
use tokio_io::AsyncRead;

use std::net::ToSocketAddrs;
use std::borrow::Cow;

type Result<T> = std::result::Result<T, failure::Error>;
type Transport = Framed<TcpStream, StompCodec>;

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

    // TODO make this an "impl Write"
    fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(
            self.command.len()  // TODO uhhhh
                           + self.body.map(|b| b.len()).unwrap_or(0) + 100,
        );
        buffer.extend(self.command);
        buffer.push(b'\n');
        // TODO escaping. use bytes crate?
        self.headers.iter().for_each(|&(k, ref v)| {
            buffer.extend(k);
            buffer.push(b':');
            buffer.extend(&**v);
            buffer.push(b'\n');
        });
        if let Some(body) = self.body {
            buffer.extend(&get_content_length_header(&body));
            buffer.push(b'\n');
            buffer.extend(body);
        } else {
            buffer.push(b'\n');
        }
        buffer.push(b'\x00');
        buffer
    }
}

named!(eol, preceded!(opt!(tag!("\r")), tag!("\n")));

named!(
    parse_header<(&[u8], Cow<[u8]>)>,
    pair!(
        take_until_either!(":\n"),
        preceded!(tag!(":"), map!(take_until_and_consume1!("\n"), |bytes| Cow::Borrowed(strip_cr(bytes))))
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
        many0!(eol) >>
        command: map!(take_until_and_consume!("\n"), strip_cr) >> headers: many0!(parse_header) >> eol
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

fn expect_header<'a>(headers: &'a [(&'a [u8], Cow<'a,[u8]>)], key: &'a str) -> Result<String> {
    fetch_header(headers, key).ok_or_else(|| format_err!("Expected header {} missing", key))
}

impl<'a> Frame<'a> {
    fn to_client_msg(&'a self) -> Result<Message<ClientMsg>> {
        use ClientMsg::*;
        use expect_header as eh;
        use fetch_header as fh;
        let h = &self.headers;
        let expect_keys: &[&[u8]];
        let content = match self.command {
            b"STOMP" | b"CONNECT" => {
                expect_keys = &[b"accept-version", b"host", b"login", b"passcode", b"heart-beat"];
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
                    heartbeat
                }
            },
            b"DISCONNECT" => {
                expect_keys = &[b"receipt"];
                Disconnect {
                    receipt: fh(h, "receipt"),
                }
            },
            b"SEND" => {
                expect_keys = &[b"destination", b"transaction"];
                Send {
                    destination: eh(h, "destination")?,
                    transaction: fh(h, "transaction"),
                    body: self.body.map(|v| v.to_vec()),
                }
            },
            b"SUBSCRIBE" => {
                expect_keys = &[b"destination", b"id", b"ack"];
                Subscribe {
                    destination: eh(h, "destination")?,
                    id: eh(h, "id")?,
                    ack: fh(h, "ack"),
                }
            },
            b"UNSUBSCRIBE" => {
                expect_keys = &[b"id"];
                Unsubscribe { id: eh(h, "id")? }
            },
            b"ACK" => {
                expect_keys = &[b"id", b"transaction"];
                Ack {
                    id: eh(h, "id")?,
                    transaction: fh(h, "transaction"),
                }
            },
            b"NACK" => {
                expect_keys = &[b"id", b"transaction"];
                Nack {
                    id: eh(h, "id")?,
                    transaction: fh(h, "transaction"),
                }
            },
            b"BEGIN" => {
                expect_keys = &[b"transaction"];
                Begin {
                    transaction: eh(h, "transaction")?,
                }
            },
            b"COMMIT" => {
                expect_keys = &[b"transaction"];
                Commit {
                    transaction: eh(h, "transaction")?,
                }
            },
            b"ABORT" => {
                expect_keys = &[b"transaction"];
                Abort {
                    transaction: eh(h, "transaction")?,
                }
            },
            other => bail!("Frame not recognized: {:?}", String::from_utf8_lossy(other)),
        };
        let extra_headers = h.iter().filter_map(
            |&(k, ref v)| if !expect_keys.contains(&k) {
                Some((k.to_vec(), (&*v).to_vec()))
            } else {
                None
            }).collect();
        Ok(Message {
            content,
            extra_headers
        })
    }

    fn to_server_msg(&'a self) -> Result<Message<ServerMsg>> {
        use ServerMsg::{Connected, Receipt, Error, Message as Msg};
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
            },
            b"MESSAGE" => {
                expect_keys = &[b"destination", b"message-id", b"subscription"];
                Msg {
                    destination: eh(h, "destination")?,
                    message_id: eh(h, "message-id")?,
                    subscription: eh(h, "subscription")?,
                    body: self.body.map(|v| v.to_vec()),
                }
            },
            b"RECEIPT" => {
                expect_keys = &[b"receipt-id"];
                Receipt {
                    receipt_id: eh(h, "receipt-id")?,
                }
            },
            b"ERROR" => {
                expect_keys = &[b"message"];
                Error {
                    message: fh(h, "message"),
                    body: self.body.map(|v| v.to_vec()),
                }
            },
            other => bail!("Frame not recognized: {:?}", String::from_utf8_lossy(other)),
        };
        let extra_headers = h.iter().filter_map(
            |&(k, ref v)| if !expect_keys.contains(&k) {
                Some((k.to_vec(), (&*v).to_vec()))
            } else {
                None
            }).collect();
        Ok(Message {
            content,
            extra_headers
        })
    }
}

#[derive(Debug, Clone)]
pub enum ServerMsg {
    Connected {
        version: String,
        session: Option<String>,
        server: Option<String>,
        heartbeat: Option<String>,
    },
    Message {
        destination: String,
        message_id: String,
        subscription: String,
        body: Option<Vec<u8>>,
    },
    Receipt {
        receipt_id: String,
    },
    Error {
        message: Option<String>,
        body: Option<Vec<u8>>,
    },
}

#[derive(Debug)]
pub struct Message<T> {
    pub content: T,
    pub extra_headers: Vec<(Vec<u8>, Vec<u8>)>,
}

// TODO tidy this lot up with traits?
impl Message<ServerMsg> {
    // fn to_frame<'a>(&'a self) -> Frame<'a> {
    //     unimplemented!()
    // }

    fn from_frame<'a>(frame: Frame<'a>) -> Result<Message<ServerMsg>> {
        frame.to_server_msg()
    }
}

impl Message<ClientMsg> {
    fn to_frame<'a>(&'a self) -> Frame<'a> {
        self.content.to_frame()
    }

    fn from_frame<'a>(frame: Frame<'a>) -> Result<Message<ClientMsg>> {
        frame.to_client_msg()
    }
}

impl From<ClientMsg> for Message<ClientMsg> {
    fn from(content: ClientMsg) -> Message<ClientMsg> {
        Message {
            content,
            extra_headers: vec![]
        }
    }
}

#[derive(Debug, Clone)]
pub enum ClientMsg {
    // TODO remove Connect from ClientMsg
    Connect {
        accept_version: String,
        host: String,
        login: Option<String>,
        passcode: Option<String>,
        heartbeat: Option<(u32, u32)>,
    },
    Send {
        destination: String,
        transaction: Option<String>,
        body: Option<Vec<u8>>,
    },
    Subscribe {
        destination: String,
        id: String,
        // TODO Ack should be enum
        ack: Option<String>,
    },
    Unsubscribe {
        id: String,
    },
    Ack {
        id: String,
        transaction: Option<String>,
    },
    Nack {
        id: String,
        transaction: Option<String>,
    },
    Begin {
        transaction: String,
    },
    Commit {
        transaction: String,
    },
    Abort {
        transaction: String,
    },
    Disconnect {
        receipt: Option<String>,
    },
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
                    (b"heart-beat", heartbeat.map(|(v1, v2)| Owned(format!("{},{}", v1, v2).into())))
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
                    (b"ack", sb(ack)),
                ],
                None,
            ),
            Unsubscribe { ref id } => {
                Frame::new(b"UNSUBSCRIBE", &[(b"id", Some(Borrowed(id.as_bytes())))], None)
            }
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

struct StompCodec;

impl Decoder for StompCodec {
    type Item = Message<ServerMsg>;
    type Error = failure::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        let (item, offset) = match parse_frame(&src) {
            Ok((remain, frame)) => {
                (Message::<ServerMsg>::from_frame(frame),
                remain.as_ptr() as usize - src.as_ptr() as usize)
            }
            Err(nom::Err::Incomplete(_)) => return Ok(None),
            Err(e) => bail!("Parse failed: {:?}", e),
        };
        src.split_to(offset);
        item.map(|v| Some(v))
    }
}

impl Encoder for StompCodec {
    type Item = Message<ClientMsg>;
    type Error = failure::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<()> {
        let buf = item.to_frame().serialize();
        dst.reserve(buf.len());
        dst.put(&buf);
        Ok(())
    }
}

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
            let transport = tcp.framed(StompCodec);
            handshake(transport, address, login, passcode)
        })
        .and_then(|tcp| {
            let (sink, stream) = tcp.split();
            let fsink = sink.send_all(
                rx.map(|m| m.into())
                    .map_err(|()| format_err!("Channel closed")))
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
    transport: Transport,
    host: String,
    login: Option<String>,
    passcode: Option<String>,
) -> impl Future<Item = Transport, Error = failure::Error> {
    let connect = Message {
        content: ClientMsg::Connect {
            accept_version: "1.1,1.2".into(),
            host: host,
            login: login,
            passcode: passcode,
            heartbeat: None,
        },
        extra_headers: vec![]
    };

    transport
        .send(connect)
        .and_then(|transport| transport.into_future().map_err(|(e, _)| e.into()))
        .and_then(|(msg, stream)| {
            if let Some(ServerMsg::Connected { .. }) = msg.map(|m| m.content) {
                fok(stream)
            } else {
                ferr(format_err!("unexpected reply"))
            }
        })
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
        let roundtrip = stomp.to_frame().serialize();
        assert_eq!(roundtrip, data);
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

#![feature(conservative_impl_trait)]

extern crate bytes;
#[macro_use]
extern crate failure;
extern crate futures;
#[macro_use]
extern crate nom;
extern crate tokio;
extern crate tokio_io;

use tokio_io::codec::{Decoder, Encoder, Framed};
use tokio_io::AsyncRead;
use bytes::{BufMut, BytesMut};
use futures::prelude::*;
use futures::sync::mpsc;
use futures::future::{err as ferr, ok as fok};
use tokio::net::TcpStream;
use tokio::executor::current_thread::spawn;

type Result<T> = std::result::Result<T, failure::Error>;
type Transport = Framed<TcpStream, StompCodec>;

#[derive(Debug)]
struct Frame<'a> {
    command: &'a [u8],
    // TODO use ArrayVec to keep headers on the stack
    headers: Vec<(&'a [u8], &'a [u8])>,
    body: Option<&'a [u8]>,
}

impl<'a> Frame<'a> {
    fn new(
        command: &'a [u8],
        headers: &[(&'a [u8], Option<&'a [u8]>)],
        body: Option<&'a [u8]>,
    ) -> Frame<'a> {
        let headers = headers
            .iter()
            .filter_map(|&(k, v)| v.map(|i| (k, i)))
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
        self.headers.iter().for_each(|&(k, v)| {
            buffer.extend(k);
            buffer.push(b':');
            buffer.extend(v);
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
    header<(&[u8], &[u8])>,
    pair!(
        take_until_either!(":\n"),
        preceded!(tag!(":"), map!(take_until_and_consume1!("\n"), strip_cr))
    )
);

fn get_content_length(headers: &[(&[u8], &[u8])]) -> Option<u32> {
    for h in headers {
        if h.0 == b"content-length" {
            return std::str::from_utf8(h.1).ok()
                .and_then(|v| v.parse::<u32>().ok())
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
        command: map!(take_until_and_consume!("\n"), strip_cr) >>
        headers: many0!(header) >> eol >>
        body: switch!(value!(get_content_length(&headers)),
            Some(v) => map!(take!(v), Some) |
            None => map!(take_until!("\x00"), is_empty_slice)
        ) >> tag!("\x00") >> many0!(complete!(eol)) >>
        (Frame {command, headers, body})
    )
);

fn strip_cr(buf: &[u8]) -> &[u8] {
    if let Some(&b'\r') = buf.last() {
        &buf[..buf.len() - 1]
    } else {
        buf
    }
}

fn fetch_header<'a>(headers: &'a [(&'a [u8], &'a [u8])], key: &'a str) -> Option<String> {
    let kk = key.as_bytes();
    for &(k, v) in headers {
        if k == kk {
            return String::from_utf8(v.to_vec()).ok();
        }
    }
    None
}

fn expect_header<'a>(headers: &'a [(&'a [u8], &'a [u8])], key: &'a str) -> Result<String> {
    fetch_header(headers, key).ok_or_else(|| format_err!("Expected header {} missing", key))
}

impl<'a> Frame<'a> {
    fn to_client_stomp(&'a self) -> Result<ClientStomp> {
        use ClientStomp::*;
        use expect_header as eh;
        use fetch_header as fh;
        let h = &self.headers;
        let out = match self.command {
            b"STOMP" | b"CONNECT" => Connect {
                accept_version: eh(h, "accept-version")?,
                host: eh(h, "host")?,
                login: fh(h, "login"),
                passcode: fh(h, "passcode"),
                heartbeat: fh(h, "heart-beat"),
            },
            b"DISCONNECT" => Disconnect {
                receipt: fh(h, "receipt"),
            },
            b"SEND" => Send {
                destination: eh(h, "destination")?,
                transaction: fh(h, "transaction"),
                body: self.body.map(|v| v.to_vec()),
            },
            b"SUBSCRIBE" => Subscribe {
                destination: eh(h, "destination")?,
                id: eh(h, "id")?,
                ack: fh(h, "ack"),
            },
            b"UNSUBSCRIBE" => Unsubscribe { id: eh(h, "id")? },
            b"ACK" => Ack {
                id: eh(h, "id")?,
                transaction: fh(h, "transaction"),
            },
            b"NACK" => Nack {
                id: eh(h, "id")?,
                transaction: fh(h, "transaction"),
            },
            b"BEGIN" => Begin {
                transaction: eh(h, "transaction")?,
            },
            b"COMMIT" => Commit {
                transaction: eh(h, "transaction")?,
            },
            b"ABORT" => Abort {
                transaction: eh(h, "transaction")?,
            },
            other => bail!("Frame not recognized: {:?}", String::from_utf8_lossy(other)),
        };
        Ok(out)
    }

    fn to_server_stomp(&'a self) -> Result<ServerStomp> {
        use ServerStomp::*;
        use expect_header as eh;
        use fetch_header as fh;
        let h = &self.headers;
        let out = match self.command {
            b"CONNECTED" => Connected {
                version: eh(h, "version")?,
                session: fh(h, "session"),
                server: fh(h, "server"),
                heartbeat: fh(h, "heart-beat"),
            },
            b"MESSAGE" => Message {
                destination: eh(h, "destination")?,
                message_id: eh(h, "message-id")?,
                subscription: eh(h, "subscription")?,
                body: self.body.map(|v| v.to_vec()),
            },
            b"RECEIPT" => Receipt {
                receipt_id: eh(h, "receipt-id")?,
            },
            b"ERROR" => Error {
                message: fh(h, "message-id"),
                body: self.body.map(|v| v.to_vec()),
            },
            other => bail!("Frame not recognized: {:?}", String::from_utf8_lossy(other)),
        };
        Ok(out)
    }
}

#[derive(Debug, Clone)]
pub enum ServerStomp {
    Connected {
        version: String,
        session: Option<String>,
        server: Option<String>,
        // TODO heartbeat is two ints
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

#[derive(Debug, Clone)]
pub enum ClientStomp {
    // TODO remove Connect from ClientStomp
    Connect {
        accept_version: String,
        host: String,
        login: Option<String>,
        passcode: Option<String>,
        // TODO heartbeat is two ints
        heartbeat: Option<String>,
    },
    Send {
        destination: String,
        transaction: Option<String>,
        body: Option<Vec<u8>>,
        // TODO: content-length
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

fn opt_str_to_bytes<'a>(s: &'a Option<String>) -> Option<&'a [u8]> {
    s.as_ref().map(|v| v.as_bytes())
}

fn get_content_length_header(body: &[u8]) -> Vec<u8> {
    format!("content-length:{}\n", body.len()).into()
}

impl ClientStomp {
    fn to_frame<'a>(&'a self) -> Frame<'a> {
        use opt_str_to_bytes as b;
        use ClientStomp::*;
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
                    (b"accept-version", Some(accept_version.as_bytes())),
                    (b"host", Some(host.as_bytes())),
                    (b"login", b(login)),
                    (b"passcode", b(passcode)),
                    (b"heart-beat", b(heartbeat)),
                ],
                None,
            ),
            Disconnect { ref receipt } => {
                Frame::new(b"DISCONNECT", &[(b"receipt", b(&receipt))], None)
            }
            Subscribe {
                ref destination,
                ref id,
                ref ack,
            } => Frame::new(
                b"SUBSCRIBE",
                &[
                    (b"destination", Some(destination.as_bytes())),
                    (b"id", Some(id.as_bytes())),
                    (b"ack", b(ack)),
                ],
                None,
            ),
            Unsubscribe { ref id } => {
                Frame::new(b"UNSUBSCRIBE", &[(b"id", Some(id.as_bytes()))], None)
            }
            Send {
                ref destination,
                ref transaction,
                ref body,
            } => Frame::new(
                b"SEND",
                &[
                    (b"destination", Some(destination.as_bytes())),
                    (b"id", b(transaction)),
                ],
                body.as_ref().map(|v| v.as_ref()),
            ),
            Ack {
                ref id,
                ref transaction,
            } => Frame::new(
                b"ACK",
                &[
                    (b"id", Some(id.as_bytes())),
                    (b"transaction", b(transaction)),
                ],
                None,
            ),
            Nack {
                ref id,
                ref transaction,
            } => Frame::new(
                b"NACK",
                &[
                    (b"id", Some(id.as_bytes())),
                    (b"transaction", b(transaction)),
                ],
                None,
            ),
            Begin {
                ref transaction,
            } => Frame::new(
                b"BEGIN",
                &[
                    (b"transaction", Some(transaction.as_bytes())),
                ],
                None,
            ),
            Commit {
                ref transaction,
            } => Frame::new(
                b"COMMIT",
                &[
                    (b"transaction", Some(transaction.as_bytes())),
                ],
                None,
            ),
            Abort {
                ref transaction,
            } => Frame::new(
                b"ABORT",
                &[
                    (b"transaction", Some(transaction.as_bytes())),
                ],
                None,
            ),
        }
    }
}

struct StompCodec;

impl Decoder for StompCodec {
    type Item = ServerStomp;
    type Error = failure::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        println!("{:?}", String::from_utf8_lossy(&src));
        let offset = match parse_frame(&src) {
            Ok((remain, _)) => remain.as_ptr() as usize - src.as_ptr() as usize,
            Err(nom::Err::Incomplete(_)) => return Ok(None),
            Err(e) => bail!("Parse failed: {:?}", e),
        };
        let bytes = src.split_to(offset);
        let item = parse_frame(&bytes).unwrap().1.to_server_stomp();
        item.map(|v| Some(v))
    }
}

impl Encoder for StompCodec {
    type Item = ClientStomp;
    type Error = failure::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<()> {
        let buf = item.to_frame().serialize();
        dst.reserve(buf.len());
        dst.put(&buf);
        Ok(())
    }
}

pub fn connect(address: &str) -> Result<(StompStream, mpsc::UnboundedSender<ClientStomp>)> {
    let (tx, rx) = mpsc::unbounded();
    let addr = address.parse()?;
    let inner = TcpStream::connect(&addr)
        .map_err(|e| e.into())
        .and_then(|tcp| {
            let transport = tcp.framed(StompCodec);
            handshake(transport)
        })
        .and_then(|tcp| {
            let (sink, stream) = tcp.split();
            let fsink = sink.send_all(rx.map_err(|()| format_err!("Channel closed")))
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
    inner: Box<Stream<Item = ServerStomp, Error = failure::Error>>,
}

impl Stream for StompStream {
    type Item = ServerStomp;
    type Error = failure::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.inner.poll()
    }
}

fn handshake(transport: Transport) -> impl Future<Item = Transport, Error = failure::Error> {
    let msg = ClientStomp::Connect {
        accept_version: "1.1,1.2".into(),
        host: "0.0.0.0".into(),
        login: None,
        passcode: None,
        heartbeat: None,
    };
    transport
        .send(msg)
        .and_then(|transport| transport.into_future().map_err(|(e, _)| e.into()))
        .and_then(|(msg, stream)| {
            println!("{:?}", msg);
            if let Some(ServerStomp::Connected { .. }) = msg {
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
        assert_eq!(frame.headers, headers_expect);
        assert_eq!(frame.body, None);
        let stomp = frame.to_client_stomp().unwrap();
        let roundtrip = stomp.to_frame().serialize();
        assert_eq!(roundtrip, data);
    }

    #[test]
    fn parse_and_serialize_message() {
        let mut data = b"MESSAGE
destination:datafeeds.here.co.uk
message-id:12345
subscription:some-id
".to_vec();
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
        assert_eq!(frame.headers, headers_expect);
        assert_eq!(frame.body, Some(body.as_bytes()));
        frame.to_server_stomp().unwrap();
        // TODO to_frame for ServerStomp
        // let roundtrip = stomp.to_frame().serialize();
        // assert_eq!(roundtrip, data);
    }
}

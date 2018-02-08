#![feature(conservative_impl_trait)]

#[macro_use]
extern crate failure;
extern crate hex;
#[macro_use]
extern crate nom;
extern crate tokio;
extern crate tokio_io;
extern crate bytes;
extern crate futures;

use tokio_io::codec::{Encoder, Decoder, Framed};
use tokio_io::AsyncRead;
use bytes::{BytesMut, BufMut};
use futures::prelude::*;
use futures::sync::mpsc;
use futures::future::{ok as fok, err as ferr};
use tokio::net::TcpStream;
use tokio::executor::current_thread::spawn;

type Result<T> = std::result::Result<T, failure::Error>;
type Transport = Framed<TcpStream, StompCodec>;

pub struct StompCodec;

impl Decoder for StompCodec {
    type Item = ServerStomp;
    type Error = failure::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        let offset = match parse_frame(&src) {
            Ok((remain, _)) => {
                remain.as_ptr() as usize - src.as_ptr() as usize
            }
            Err(nom::Err::Incomplete(_)) => return Ok(None),
            Err(e) => bail!("Parse failed: {:?}", e)
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

#[derive(Debug)]
pub struct Frame<'a> {
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

    fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(self.command.len()  // TODO uhhhh
                           + self.body.map(|b| b.len()).unwrap_or(0) + 100);
        buffer.extend(self.command);
        buffer.push(b'\n');
        // TODO escaping. use bytes crate?
        self.headers.iter().for_each(|&(k, v)| {
            buffer.extend(k);
            buffer.push(b':');
            buffer.extend(v);
            buffer.push(b'\n');
        });
        buffer.push(b'\n');
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

// TODO Strategy is to parse as normal and check header for content-length
// If content-length exists, take that quantity
// If not, take body until NULL
named!(
    pub parse_frame<Frame>,
    do_parse!(
        command: map!(take_until_and_consume!("\n"), strip_cr) >>
        headers: many0!(header) >> eol >>
        body: map!(take_until_and_consume!("\x00"), |v| if v.is_empty() {
            None
        } else {
            Some(v)
        }) >> (Frame {command, headers, body,})
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
            return String::from_utf8(v.to_vec()).ok()
        }
    }
    None
}

fn expect_header<'a>(headers: &'a [(&'a [u8], &'a [u8])], key: &'a str) -> Result<String> {
    fetch_header(headers, key).ok_or_else(|| format_err!("Expected header {} missing", key))
}

impl<'a> Frame<'a> {
    pub fn to_client_stomp(&'a self) -> Result<ClientStomp> {
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
            // b"SUBSCRIBE" => Subscribe {
            //     destination: eh(h, "destination")?,
            //     id: eh(h, "id")?,
            //     ack: fh(h, "ack"),
            // },
            // b"UNSUBSCRIBE" => Unsubscribe { id: eh(h, "id")? },
            // b"ACK" => Ack {
            //     id: eh(h, "id")?,
            //     transaction: fh(h, "transaction"),
            // },
            // b"NACK" => Nack {
            //     id: eh(h, "id")?,
            //     transaction: fh(h, "transaction"),
            // },
            // b"BEGIN" => Begin {
            //     transaction: eh(h, "transaction")?,
            // },
            // b"COMMIT" => Commit {
            //     transaction: eh(h, "transaction")?,
            // },
            // b"ABORT" => Abort {
            //     transaction: eh(h, "transaction")?,
            // },
            other => bail!("Frame not recognized: {}", String::from_utf8_lossy(other)),
        };
        Ok(out)
    }

    pub fn to_server_stomp(&'a self) -> Result<ServerStomp> {
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
            // b"RECEIPT" => Receipt {
            //     receipt_id: eh(h, "receipt-id")?,
            // },
            b"ERROR" => Error {
                message: fh(h, "message-id"),
                body: self.body.map(|v| v.to_vec()),
            },
            other => bail!("Frame not recognized: {}", String::from_utf8_lossy(other)),
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
        heartbeat: Option<String>,
    },
    Message {
        destination: String,
        message_id: String,
        subscription: String,
        body: Option<Vec<u8>>
    },
    // Receipt {
    //     receipt_id: &'a [u8],
    // },
    Error {
        message: Option<String>,
        body: Option<Vec<u8>>,
    },
}

#[derive(Debug, Clone)]
pub enum ClientStomp {
    Connect {
        accept_version: String,
        host: String,
        login: Option<String>,
        passcode: Option<String>,
        heartbeat: Option<String>,
    },
    Send {
        destination: String,
        transaction: Option<String>,
        body: Option<Vec<u8>>
    },
    // Subscribe {
    //     destination: &'a [u8],
    //     id: &'a [u8],
    //     ack: Option<&'a [u8]>,
    // },
    // Unsubscribe {
    //     id: &'a [u8],
    // },
    // Ack {
    //     id: &'a [u8],
    //     transaction: Option<&'a [u8]>,
    // },
    // Nack {
    //     id: &'a [u8],
    //     transaction: Option<&'a [u8]>,
    // },
    // Begin {
    //     transaction: &'a [u8],
    // },
    // Commit {
    //     transaction: &'a [u8],
    // },
    // Abort {
    //     transaction: &'a [u8],
    // },
    Disconnect {
        receipt: Option<String>,
    },
}

fn opt_str_to_bytes<'a>(s: &'a Option<String>) -> Option<&'a [u8]> {
    s.as_ref().map(|v| v.as_bytes())
}

impl ClientStomp {
    pub fn to_frame<'a>(&'a self) -> Frame<'a> {
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
            Disconnect {ref receipt} => Frame::new(
                b"DISCONNECT", &[(b"receipt", b(&receipt))], None
            ),
            _ => unimplemented!(),
        }
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
            let fsink = sink.send_all(rx.map_err(|()| format_err!("Channel closed"))).map(|_| ()).map_err(|e| eprintln!("{}", e));
            spawn(fsink);
            fok(stream)
        })
        .flatten_stream();
    Ok((StompStream {inner: Box::new(inner)}, tx))
}

pub struct StompStream {
    inner: Box<Stream<Item=ServerStomp, Error=failure::Error>>
}

impl Stream for StompStream {
    type Item = ServerStomp;
    type Error = failure::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.inner.poll()
    }
}

fn handshake(transport: Transport) -> impl Future<Item=Transport, Error=failure::Error> {
    let msg = ClientStomp::Connect {
        accept_version: "1.1,1.2".into(),
        host: "0.0.0.0".into(),
        login: None,
        passcode: None,
        heartbeat: None
    };
    transport
        .send(msg)
        .and_then(|transport| transport.into_future()
                  .map_err(|(e, _)| e.into()))
        .and_then(|(msg, stream)| {
            if let Some(ServerStomp::Connected {..}) = msg {
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
        let data =
b"CONNECT
accept-version:1.2
host:datafeeds.nationalrail.co.uk
login:user
passcode:password\n\n\x00".to_vec();
        let (_, frame) = parse_frame(&data).unwrap();
        assert_eq!(frame.command, b"CONNECT");
        let headers_expect: Vec<(&[u8], &[u8])> = vec![
            (&b"accept-version"[..], &b"1.2"[..]),
            (b"host", b"datafeeds.nationalrail.co.uk"),
            (b"login", b"user"),
            (b"passcode", b"password"),
        ];
        assert_eq!(frame.headers, headers_expect);
        assert_eq!(frame.body, None);
        let stomp = frame.to_client_stomp().unwrap();
        let roundtrip = stomp.to_frame().serialize();
        assert_eq!(roundtrip, data);
    }
}

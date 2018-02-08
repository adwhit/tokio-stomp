#[macro_use]
extern crate failure;
extern crate hex;
#[macro_use]
extern crate nom;
extern crate tokio_io;
extern crate bytes;
extern crate futures;

use nom::newline;
use tokio_io::codec;
use bytes::{Bytes, BytesMut, BufMut};
use futures::Stream;

use std::collections::HashMap;

type Result<T> = std::result::Result<T, failure::Error>;

pub struct StompCodec;

impl codec::Decoder for StompCodec {
    type Item = Bytes;
    type Error = failure::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        let offset = match parse_frame(&src) {
            Ok((remain, _)) => {
                remain.as_ptr() as usize - src.as_ptr() as usize
            }
            Err(nom::Err::Incomplete(_)) => return Ok(None),
            Err(e) => bail!("Parse failed")
        };
        let bytes = src.split_to(offset);
        Ok(Some(bytes.into()))
    }
}

impl codec::Encoder for StompCodec {
    type Item = Frame<'static>;
    type Error = failure::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<()> {
        let buf = item.serialize();
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

fn fetch_header<'a>(headers: &'a [(&'a [u8], &'a [u8])], key: &'a str) -> Option<&'a [u8]> {
    let kk = key.as_bytes();
    for &(k, v) in headers {
        if k == kk {
            return Some(v);
        }
    }
    None
}

fn expect_header<'a>(headers: &'a [(&'a [u8], &'a [u8])], key: &'a str) -> Result<&'a [u8]> {
    fetch_header(headers, key).ok_or_else(|| format_err!("Expected header {} missing", key))
}

impl<'a> Frame<'a> {
    pub fn to_stomp(&'a self) -> Result<Stomp> {
        use Stomp::*;
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
            b"CONNECTED" => Connected {
                version: eh(h, "version")?,
                session: fh(h, "session"),
                server: fh(h, "server"),
                heartbeat: fh(h, "heart-beat"),
            },
            b"SEND" => Send {
                destination: eh(h, "destination")?,
                transaction: fh(h, "transaction"),
                body: self.body,
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
            b"DISCONNECT" => Disconnect {
                receipt: fh(h, "receipt"),
            },
            b"MESSAGE" => Message {
                destination: eh(h, "destination")?,
                message_id: eh(h, "message-id")?,
                subscription: eh(h, "subscription")?,
                body: self.body,
            },
            b"RECEIPT" => Receipt {
                receipt_id: eh(h, "receipt-id")?,
            },
            b"ERROR" => Error {
                message: fh(h, "message-id"),
                body: self.body,
            },
            other => bail!("Frame not recognized: {}", String::from_utf8_lossy(other)),
        };
        Ok(out)
    }
}

fn protocol<'a>(msg: &'a Stomp<'a>) -> Option<Stomp<'static>> {
    use Stomp::*;
    match *msg {
        Connected {..} => unimplemented!(),
        _ => unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub enum Stomp<'a> {
    Connect {
        accept_version: &'a [u8],
        host: &'a [u8],
        login: Option<&'a [u8]>,
        passcode: Option<&'a [u8]>,
        heartbeat: Option<&'a [u8]>,
    },
    Connected {
        version: &'a [u8],
        session: Option<&'a [u8]>,
        server: Option<&'a [u8]>,
        heartbeat: Option<&'a [u8]>,
    },
    Send {
        destination: &'a [u8],
        transaction: Option<&'a [u8]>,
        body: Option<&'a [u8]>,
    },
    Subscribe {
        destination: &'a [u8],
        id: &'a [u8],
        ack: Option<&'a [u8]>,
    },
    Unsubscribe {
        id: &'a [u8],
    },
    Ack {
        id: &'a [u8],
        transaction: Option<&'a [u8]>,
    },
    Nack {
        id: &'a [u8],
        transaction: Option<&'a [u8]>,
    },
    Begin {
        transaction: &'a [u8],
    },
    Commit {
        transaction: &'a [u8],
    },
    Abort {
        transaction: &'a [u8],
    },
    Disconnect {
        receipt: Option<&'a [u8]>,
    },
    Message {
        destination: &'a [u8],
        message_id: &'a [u8],
        subscription: &'a [u8],
        body: Option<&'a [u8]>,
    },
    Receipt {
        receipt_id: &'a [u8],
    },
    Error {
        message: Option<&'a [u8]>,
        body: Option<&'a [u8]>,
    },
}

impl<'a> Stomp<'a> {
    pub fn to_frame(&'a self) -> Frame<'a> {
        use Stomp::*;
        match *self {
            Connect {
                accept_version,
                host,
                login,
                passcode,
                heartbeat,
            } => Frame::new(
                b"CONNECT",
                &[
                    (b"accept-version", Some(accept_version)),
                    (b"host", Some(host)),
                    (b"login", login),
                    (b"passcode", passcode),
                    (b"heart-beat", heartbeat),
                ],
                None,
            ),
            Disconnect {receipt} => Frame::new(
                b"DISCONNECT", &[(b"receipt", receipt)], None
            ),
            _ => unimplemented!(),
        }
    }
}

use std::net;
use std::io::prelude::*;

pub fn connect(conn: &mut net::TcpStream) -> Result<()> {
    let msg = Stomp::Connect {
        accept_version: b"1.1,1.2",
        host: b"0.0.0.0",
        login: None,
        passcode: None,
        heartbeat: None
    };
    let buffer = msg.to_frame().serialize();
    conn.write_all(&buffer)?;
    Ok(())
}

pub fn disconnect(conn: &mut net::TcpStream) -> Result<()> {
    let msg = Stomp::Disconnect {
        receipt: None
    };
    let buffer = msg.to_frame().serialize();
    conn.write_all(&buffer)?;
    Ok(())
}

// /// A `Stream` that represents a connection to a STOMP server
// #[must_use = "Streams are lazy and do nothing unless polled"]
// pub struct StompStream {
//     buf: Vec<u8>,
//     handle: Handle,
//     address: Option<String>,
//     body: Option<Body>,
// }

// impl StompStream {
//     fn new(handle: &Handle, request: Request) -> StompStream {
//         StompStream {
//             buf: vec![],
//             handle: handle.clone(),
//             request: Some(request),
//             response: None,
//             body: None,
//         }
//     }
// }

// impl Stream for StompStream {
// }

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
        let stomp = frame.to_stomp().unwrap();
        let roundtrip = stomp.to_frame().serialize();
        assert_eq!(roundtrip, data);
    }
}

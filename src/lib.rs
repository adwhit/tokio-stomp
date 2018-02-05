extern crate hex;
#[macro_use]
extern crate nom;
#[macro_use]
extern crate failure;

use nom::newline;

use std::collections::HashMap;

type Result<T> = std::result::Result<T, failure::Error>;

#[derive(Debug)]
struct Frame<'a> {
    command: &'a [u8],
    // TODO use ArrayVec to keep headers on the stack
    headers: Vec<(&'a [u8], &'a [u8])>,
    body: Option<&'a [u8]>
}

named!(eol, preceded!(opt!(tag!("\r")), tag!("\n")));

named!(header<(&[u8], &[u8])>,
       pair!(
           take_until_either!(":\n"),
           preceded!(tag!(":"),
                     map!(take_until_and_consume1!("\n"), strip_cr))));

named!(parse_frame<Frame>, do_parse!(
    command: map!(take_until_and_consume!("\n"), strip_cr) >>
    headers: many0!(header) >> eol >>
    body: map!(take_until_and_consume!("\x00"),
               |v| if v.is_empty() { None } else { Some(v) }) >>
    (Frame { command, headers, body })
));

fn strip_cr(buf: &[u8]) -> &[u8] {
    let l = buf.len();
    if buf[l - 1] == b'\r' {
        &buf[..l - 1]
    } else {
        buf
    }
}

fn fetch_header<'a>(headers: &'a [(&'a [u8], &'a [u8])], key: &'a str) -> Option<&'a [u8]> {
    let kk = key.as_bytes();
    for &(k, v) in headers {
        if k == kk {
            return Some(v)
        }
    }
    None
}

fn expect_header<'a>(headers: &'a [(&'a [u8], &'a [u8])], key: &'a str) -> Result<&'a [u8]> {
    fetch_header(headers, key).ok_or_else(|| format_err!("Expected header {} missing", key))
}

impl<'a> Frame<'a> {
    fn to_stomp(&'a self) -> Result<Stomp> {
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
                body: self.body
            },
            b"SUBSCRIBE" => Subscribe {
                destination: eh(h, "destination")?,
                id: eh(h, "id")?,
                ack: fh(h, "ack"),
            },
            b"UNSUBSCRIBE" => Unsubscribe {
                id: eh(h, "id")?,
            },
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
                body: self.body
            },
            b"RECEIPT" => Receipt {
                receipt_id: eh(h, "receipt-id")?,
            },
            b"ERROR" => Error {
                message: fh(h, "message-id"),
                body: self.body
            },
            other => bail!("Frame not recognized: {}", String::from_utf8_lossy(other))
        };
        Ok(out)
    }
}



enum Stomp<'a> {
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
        body: Option<&'a [u8]>
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
        body: Option<&'a [u8]>
    },
    Receipt {
        receipt_id: &'a [u8]
    },
    Error {
        message: Option<&'a [u8]>,
        body: Option<&'a [u8]>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_init() {
        let mut init = b"
53 54 4f 4d 50 0a 61 63 63 65 70 74 2d 76 65 72
73 69 6f 6e 3a 31 2e 32 0a 68 6f 73 74 3a 64 61
74 61 66 65 65 64 73 2e 6e 61 74 69 6f 6e 61 6c
72 61 69 6c 2e 63 6f 2e 75 6b 0a 6c 6f 67 69 6e
3a 75 73 65 72 0a 70 61 73 73 63 6f 64 65
3a 70 61 73 73 77 6f 72 64 0a 0a 00".to_vec();
        init.retain(|elem| *elem != b' ' && *elem != b'\n');
        let raw = hex::decode(init).unwrap();
        println!("{}", String::from_utf8_lossy(&raw));
        let (_, frame) = parse_frame(&raw).unwrap();
        assert_eq!(frame.command, b"STOMP");
        let headers_expect: Vec<(&[u8], &[u8])> = vec![
            (&b"accept-version"[..], &b"1.2"[..]),
            (b"host", b"datafeeds.nationalrail.co.uk"),
            (b"login", b"user"),
            (b"passcode", b"password")
        ];
        assert_eq!(frame.headers, headers_expect);
        assert_eq!(frame.body, None);
    }
}

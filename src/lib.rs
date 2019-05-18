//! tokio-stomp - A library for asynchronous streaming of STOMP messages

#[macro_use]
extern crate failure;
#[macro_use]
extern crate nom;

use frame::Frame;

pub mod client;
mod frame;
pub mod server;

pub(crate) type Result<T> = std::result::Result<T, failure::Error>;

/// A representation of a STOMP frame
#[derive(Debug)]
pub struct Message<T> {
    /// The message content
    pub content: T,
    /// Headers present in the frame which were not required by the content
    pub extra_headers: Vec<(Vec<u8>, Vec<u8>)>,
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
    /// Sent from the server to the client once a server has successfully
    /// processed a client frame that requests a receipt
    Receipt { receipt_id: String },
    /// Something went wrong. After sending an Error, the server will close the connection
    Error {
        message: Option<String>,
        body: Option<Vec<u8>>,
    },
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

use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::cell::RefCell;
use std::rc::Rc;

use bytes::BytesMut;
use futures::sync::mpsc;
use futures::prelude::*;
use futures::future::join_all;
use tokio::codec::{Decoder, Encoder};

use {Message, ServerMsg, ClientMsg, Result};

type TxS = mpsc::UnboundedSender<Rc<Message<ServerMsg>>>;
type RxS = mpsc::UnboundedReceiver<Rc<Message<ServerMsg>>>;
type TxC = mpsc::UnboundedSender<Message<ClientMsg>>;
type RxC = mpsc::UnboundedReceiver<Message<ClientMsg>>;
type Connections = HashMap<SocketAddr, TxS>;
type State = Rc<RefCell<ServerState>>;

pub struct Server;

#[derive(Default)]
struct ServerState {
    connections: Connections,
    subs_ip_lookup: HashMap<String, Vec<SocketAddr>>,
    ip_subs_lookup: HashMap<SocketAddr, Vec<String>>,
}

fn server_state_handler(
    rx_clientmsg: RxC,
    rx_tcpsink: u8,
) -> Box<Future<Item = (), Error = failure::Error>> {
    use ClientMsg::*;
    //state.borrow_mut().connections.insert(addr, txs).unwrap();
    let mut state = ServerState::default();
    Box::new(
        rx_clientmsg
            .for_each(move |msg| {
                match msg.content {
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
            })
            .map_err(|()| format_err!("")),
    )
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
        unimplemented!()
        // let state: State = Default::default();
        // let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        // let listener = TcpListener::bind(&addr)?;

        // let (tx_clientmsg, rx_clientmsg) = mpsc::unbounded();
        // let (tx_tcpsink, rx_tcpsink) = mpsc::unbounded();

        // spawn(server_state_handler(rx_clientmsg, rx_tcpsink).map_err(|e| eprintln!("{}", e)));

        // let fut = listener.incoming().for_each(move |conn| {
        //     let addr = conn.peer_addr().unwrap();
        //     let transport = conn.framed(ServerCodec);

        //     // do the handshake here, then send to the state handler
        //     let do_handshake = transport
        //         .into_future()
        //         .and_then(|(msg, transport)| {
        //             // handshake
        //             if let Some(ClientMsg::Connect { .. }) = msg.map(|m| m.content) {
        //                 let reply = Message {
        //                     extra_headers: vec![],
        //                     content: Connected {
        //                         version: "1.2".into(),
        //                         session: None,
        //                         server: None,
        //                         heartbeat: None,
        //                     },
        //                 };
        //                 transport.send(reply)
        //             } else {
        //                 panic!("Bad handshake")
        //             }
        //         })
        //         .and_then(|transport| {
        //             // Send to state handler
        //             tx_transport(transport)
        //         });

        //     spawn(do_handshake.map_err(|e| eprintln!("{}", e)));

        //     // broadcast channel
        //     // need to get txs into state
        //     let (tx_servermsg, rx_servermsg) = mpsc::unbounded();

        //     spawn(
        //         source
        //             .forward(tx_clientmsg.clone())
        //             .map(|_| ())
        //             .map_err(|e| eprintln!("{}", e)),
        //     );

        //     fok(())
        // });
        // Ok(Server {
        //     inner: Box::new(fut.map_err(|e| e.into())),
        // })
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


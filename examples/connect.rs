extern crate futures;
extern crate tokio;
extern crate tokio_stomp;

use std::time::Duration;

use futures::future::ok;
use futures::prelude::*;
use tokio::runtime::current_thread::block_on_all;
use tokio_stomp::*;

// The example connects to a local server, then sends the following messages -
// subscribe to a destination, send a message to the destination, unsubscribe and disconnect
// It simultaneously receives and prints messages sent from the server, which in fact means
// it ends up printing it's own message.

fn main() {
    let (fut, tx) = tokio_stomp::connect("127.0.0.1:61613".into(), None, None).unwrap();

    // Sender thread
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(1));
        tx.unbounded_send(ClientMsg::Subscribe {
            destination: "rusty".into(),
            id: "myid".into(),
            ack: None,
        })
        .unwrap();
        println!("Subscribe sent");

        std::thread::sleep(Duration::from_secs(1));
        tx.unbounded_send(ClientMsg::Send {
            destination: "rusty".into(),
            transaction: None,
            body: Some(b"Hello there rustaceans!".to_vec()),
        })
        .unwrap();
        println!("Message sent");

        std::thread::sleep(Duration::from_secs(1));
        tx.unbounded_send(ClientMsg::Unsubscribe { id: "myid".into() })
            .unwrap();
        println!("Unsubscribe sent");

        std::thread::sleep(Duration::from_secs(1));
        tx.unbounded_send(ClientMsg::Disconnect { receipt: None })
            .unwrap();
        println!("Disconnect sent");

        std::thread::sleep(Duration::from_secs(1));
    });

    // Listen from the main thread. Once the Disconnect message is sent from
    // the sender thread, the server will disconnect the client and the future
    // will resolve, ending the program
    let fut = fut
        .for_each(|item| {
            if let ServerMsg::Message { body, .. } = item.content {
                println!(
                    "Message received: {:?}",
                    String::from_utf8_lossy(&body.unwrap())
                );
            } else {
                println!("{:?}", item);
            }
            ok(())
        })
        .map_err(|e| eprintln!("{}", e));

    block_on_all(fut).unwrap();
}

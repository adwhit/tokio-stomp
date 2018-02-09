extern crate futures;
extern crate tokio;
extern crate tokio_stomp;

use std::time::Duration;

use tokio_stomp::*;
use tokio::executor::current_thread::{run, spawn};
use futures::future::ok;
use futures::prelude::*;

fn main() {
    let (fut, tx) = tokio_stomp::connect("127.0.0.1:61613".into(), None, None).unwrap();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(1));
        tx.unbounded_send(ClientMsg::Subscribe {
            destination: "rusty".into(),
            id: "myid".into(),
            ack: None,
        }).unwrap();
        println!("Subscribe sent");
        std::thread::sleep(Duration::from_secs(1));
        tx.unbounded_send(ClientMsg::Send {
            destination: "rusty".into(),
            transaction: None,
            body: Some(b"Hello there rustaceans!".to_vec()),
        }).unwrap();
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
    let fut = fut.for_each(|item| {
        if let ServerMsg::Message { body, .. } = item.content {
            println!(
                "Message received: {:?}",
                String::from_utf8_lossy(&body.unwrap())
            );
        } else {
            println!("{:?}", item);
        }
        ok(())
    }).map_err(|e| eprintln!("{}", e));
    run(|_| spawn(fut));
}

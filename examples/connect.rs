#[macro_use]
extern crate failure;
extern crate futures;
extern crate tokio;
extern crate tokio_io;
extern crate tokio_stomp;

use std::time::Duration;

use tokio_stomp::*;
use tokio::executor::current_thread::{run, spawn};
use futures::future::{err as ferr, ok as fok};
use futures::prelude::*;

fn main() {
    let (fut, tx) = tokio_stomp::connect("127.0.0.1:61613").unwrap();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(1));
        tx.unbounded_send(ClientStomp::Subscribe {
            destination: "rusty".into(),
            id: "myid".into(),
            ack: None,
        }).unwrap();
        println!("Subscribe sent");
        std::thread::sleep(Duration::from_secs(1));
        tx.unbounded_send(ClientStomp::Send {
            destination: "rusty".into(),
            transaction: None,
            body: Some(b"Hello there rustaceans!".to_vec()),
        }).unwrap();
        println!("Message sent");
        std::thread::sleep(Duration::from_secs(1));
        tx.unbounded_send(ClientStomp::Unsubscribe { id: "myid".into() }).unwrap();
        println!("Unsubscribe sent");
        std::thread::sleep(Duration::from_secs(1));
        tx.unbounded_send(ClientStomp::Disconnect { receipt: None })
            .unwrap();
        println!("Disconnect sent");
        std::thread::sleep(Duration::from_secs(1));
    });
    let fut = fut.for_each(|item| {
        if let ServerStomp::Message { body, .. } = item {
            println!("Message received: {:?}", String::from_utf8_lossy(&body.unwrap()));
        } else {
            println!("{:?}", item);
        }
        fok(())
    }).map_err(|e| eprintln!("{}", e));
    run(|_| spawn(fut));
}

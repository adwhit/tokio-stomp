#![allow(dead_code)]
#![feature(conservative_impl_trait)]

#[macro_use]
extern crate failure;
extern crate futures;
extern crate tokio;
extern crate tokio_io;
extern crate tokio_stomp;

use std::io::prelude::*;

use tokio_stomp::*;
use tokio::executor::current_thread::{run, spawn};
use futures::future::{err as ferr, ok as fok};
use futures::prelude::*;

fn main() {
    let (fut, tx) = tokio_stomp::connect("127.0.0.1:61613").unwrap();
    std::thread::spawn(move || {
        std::thread::sleep_ms(1000);
        tx.unbounded_send(ClientStomp::Subscribe {
            destination: "thingy".into(),
            id: "myid".into(),
            ack: None,
        }).unwrap();
        println!("Subscribe sent");
        std::thread::sleep_ms(1000);
        tx.unbounded_send(ClientStomp::Unsubscribe { id: "myid".into() });
        println!("Unsubscribe sent");
        std::thread::sleep_ms(1000);
        tx.unbounded_send(ClientStomp::Disconnect { receipt: None }).unwrap();
        println!("Disconnect sent");
        std::thread::sleep_ms(1000);
    });
    let fut = fut.for_each(|item| {
        println!("{:?}", item);
        fok(())
    }).map_err(|e| eprintln!("{}", e));
    run(|_| spawn(fut));
}

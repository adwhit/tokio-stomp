#![allow(dead_code)]
#![feature(conservative_impl_trait)]

extern crate tokio_stomp;
extern crate tokio_io;
extern crate tokio;
extern crate futures;
#[macro_use]
extern crate failure;

use std::io::prelude::*;

use tokio_stomp::*;
use tokio::executor::current_thread::{run, spawn};
use futures::future::{ok as fok, err as ferr};
use futures::prelude::*;


fn main() {
    let (fut, tx) = tokio_stomp::connect("127.0.0.1:61613").unwrap();
    std::thread::spawn(move || {
        std::thread::sleep_ms(2000);
        tx.unbounded_send(ClientStomp::Disconnect { receipt: None }).unwrap();
        println!("Disconnect sent");
    });
    let fut = fut.for_each(|item| {
        println!("{:?}", item);
        fok(())
    }).map_err(|e| eprintln!("{}", e));
    run(|_| {
        spawn(fut)
    });
}


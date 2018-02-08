#![allow(dead_code)]
#![feature(conservative_impl_trait)]

extern crate tokio_stomp;
extern crate tokio_io;
extern crate tokio;
extern crate futures;
#[macro_use]
extern crate failure;

use std::io::prelude::*;

use futures::prelude::*;
use tokio_io::codec::Framed;
use tokio_stomp::*;
use tokio::net::TcpStream;
use futures::future::{ok as fok, err as ferr};


fn main() {
    let fut = StompStream::new("127.0.0.1:61613").unwrap()
        .for_each(|item| {
            println!("{:?}", item);
            fok(())
        });
    fut.wait().unwrap();
}


extern crate futures;
extern crate tokio;
extern crate tokio_stomp;

use std::time::Duration;

use tokio_stomp::*;
use tokio::executor::current_thread::{run, spawn};
use futures::future::ok;
use futures::prelude::*;

fn main() {
    std::thread::spawn(|| {
        let (fut1, tx1) = tokio_stomp::connect("127.0.0.1:61613".into(), None, None).unwrap();
        tx1.unbounded_send(ClientMsg::Subscribe {
            destination: "ping".into(),
            id: "myid".into(),
            ack: None,
        }).unwrap();

        tx1.unbounded_send(ClientMsg::Send {
            destination: "pong".into(),
            transaction: None,
            body: Some(b"PONG!".to_vec()),
        }).unwrap();

        std::thread::sleep(Duration::from_secs(1));

        let fut1 = fut1.for_each(move |item| {
            if let ServerMsg::Message { body, .. } = item.content {
                println!("{}", String::from_utf8_lossy(&body.unwrap()));
            }
            tx1.unbounded_send(ClientMsg::Send {
                destination: "pong".into(),
                transaction: None,
                body: Some(b"PONG!".to_vec()),
            }).unwrap();
            std::thread::sleep(Duration::from_secs(1));
            ok(())
        }).map_err(|e| eprintln!("{}", e));

        run(|_| spawn(fut1));
    });

    let (fut2, tx2) = tokio_stomp::connect("127.0.0.1:61613".into(), None, None).unwrap();

    tx2.unbounded_send(ClientMsg::Subscribe {
        destination: "pong".into(),
        id: "myid".into(),
        ack: None,
    }).unwrap();

    let fut2 = fut2.for_each(move |item| {
        if let ServerMsg::Message { body, .. } = item.content {
            println!("{}", String::from_utf8_lossy(&body.unwrap()));
        }
        tx2.unbounded_send(ClientMsg::Send {
            destination: "ping".into(),
            transaction: None,
            body: Some(b"PING!".to_vec()),
        }).unwrap();
        std::thread::sleep(Duration::from_secs(1));
        ok(())
    }).map_err(|e| eprintln!("{}", e));

    run(|_| spawn(fut2));
}

extern crate futures;
extern crate tokio;
extern crate tokio_stomp;

use std::time::Duration;

use tokio_stomp::*;
use tokio::runtime::current_thread::block_on_all;
use futures::future::ok;
use futures::prelude::*;

// Stream data from the UK national rail datafeed.
// See http://nrodwiki.rockshore.net/index.php/Darwin:Push_Port for more information

fn main() {
    // Dummy usernames/passwords
    let username = "d3user";
    let password = "d3password";
    let queue = std::env::var("STOMP_QUEUE").expect("Env var STOMP_QUEUE not found");
    let uri = "datafeeds.nationalrail.co.uk:61613";
    let (fut, tx) = tokio_stomp::connect(uri.into(), Some(username.into()), Some(password.into())).unwrap();
    tx.unbounded_send(ClientMsg::Subscribe {
        destination: queue.into(),
        id: "1".into(),
        ack: None,
    }).unwrap();

    std::thread::sleep(Duration::from_secs(1));

    let fut = fut.for_each(move |item| {
        for (k, v) in item.extra_headers {
            println!("{}:{}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v))
        }
        if let ServerMsg::Message { body, .. } = item.content {
            println!("{}\n", String::from_utf8_lossy(&body.unwrap()));
        }
        ok(())
    }).map_err(|e| eprintln!("{}", e));

    block_on_all(fut).unwrap();
}

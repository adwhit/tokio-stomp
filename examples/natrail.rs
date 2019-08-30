extern crate futures;
extern crate tokio;
extern crate tokio_stomp;

use futures::future::ok;
use futures::sink::SinkExt;
use futures::stream::TryStreamExt;
use tokio_stomp::*;

// Stream data from the UK national rail datafeed.
// See http://nrodwiki.rockshore.net/index.php/Darwin:Push_Port for more information

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
    // Dummy usernames/passwords
    let username = "d3user";
    let password = "d3password";
    let queue = std::env::var("STOMP_QUEUE").expect("Env var STOMP_QUEUE not found");
    let uri = "datafeeds.nationalrail.co.uk:61613";

    println!("Connecting...");

    let mut conn =
        tokio_stomp::client::connect(uri.into(), Some(username.into()), Some(password.into()))
            .await?;

    println!("Connected");

    conn.send(
        ClientMsg::Subscribe {
            destination: queue.into(),
            id: "1".into(),
            ack: None,
        }
        .into(),
    )
    .await?;

    println!("Sent");

    conn.try_for_each(move |item| {
        for (k, v) in item.extra_headers {
            println!(
                "{}:{}",
                String::from_utf8_lossy(&k),
                String::from_utf8_lossy(&v)
            )
        }
        if let ServerMsg::Message { body, .. } = item.content {
            println!("{}\n", String::from_utf8_lossy(&body.unwrap()));
        }
        ok(())
    })
    .await?;
    Ok(())
}

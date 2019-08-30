extern crate futures;
extern crate tokio;
extern crate tokio_stomp;

use std::time::{Duration, Instant};

use futures::prelude::*;
use tokio_stomp::*;

// This examples consists of two futures, each of which connects to a local server,
// and then sends either PING or PONG messages to the server while listening
// for replies. This continues indefinitely (ctrl-c to exit)

// You can start a simple STOMP server with docker:
// `docker run -p 61613:61613 rmohr/activemq:latest`

async fn client(listens: &str, sends: &str, msg: &[u8]) -> Result<(), failure::Error> {
    let mut conn = tokio_stomp::client::connect("127.0.0.1:61613".into(), None, None).await?;
    conn.send(
        ClientMsg::Subscribe {
            destination: listens.into(),
            id: "myid".into(),
            ack: None,
        }
        .into(),
    )
    .await?;

    loop {
        conn.send(
            ClientMsg::Send {
                destination: sends.into(),
                transaction: None,
                body: Some(msg.to_vec()),
            }
            .into(),
        )
        .await?;
        let msg = conn.next().await.transpose()?;
        if let Some(ServerMsg::Message { body, .. }) = msg.as_ref().map(|m| &m.content) {
            println!("{}", String::from_utf8_lossy(&body.as_ref().unwrap()));
        } else {
            failure::bail!("Unexpected: {:?}", msg)
        }
        tokio::timer::delay(Instant::now() + Duration::from_secs(1)).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
    let fut1 = Box::pin(client("ping", "pong", b"PONG!"));
    let fut2 = Box::pin(client("pong", "ping", b"PING!"));

    let (res, _) = futures::future::select(fut1, fut2).await.factor_first();
    res
}

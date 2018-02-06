extern crate tokio_stomp;

use std::io::prelude::*;
use std::net;

fn main() {
    let mut conn = net::TcpStream::connect("localhost:61613").unwrap();
    std::thread::sleep_ms(500);
    tokio_stomp::connect(&mut conn).unwrap();
    let mut buf = [0; 1024];
    let ct = conn.read(&mut buf).unwrap();
    println!("{}: {}", ct, String::from_utf8_lossy(&buf));
    std::thread::sleep_ms(500);
    tokio_stomp::disconnect(&mut conn).unwrap();
}

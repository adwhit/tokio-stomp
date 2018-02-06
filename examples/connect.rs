extern crate tokio_stomp;

fn main() {
    tokio_stomp::connect().unwrap();
}

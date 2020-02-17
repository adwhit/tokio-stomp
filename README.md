# tokio-stomp
[![crates.io](https://img.shields.io/crates/v/tokio-stomp.svg)](https://crates.io/crates/tokio-stomp)
[![Build Status](https://travis-ci.org/adwhit/tokio-stomp.svg?branch=master)](https://travis-ci.org/adwhit/tokio-stomp)


An async [STOMP](https://stomp.github.io/) client (and maybe eventually, server) for Rust, using the Tokio stack.

It aims to be fast and fully-featured with a simple streaming interface.

[Documentation](https://docs.rs/tokio-stomp/0.1.0/tokio_stomp/)

For full examples, see the examples directory.

License: [MIT](LICENSE)

## TODO

* Support spec v1.1
* More precise errors
* Built-in ACK/NACK handling
* Auto-disconnect on ERROR

use std::{net::TcpListener};
use clap::{Command, arg};
use kvs::{Result};

fn main() -> Result<()> {
    let version = env!("CARGO_PKG_VERSION");
    let matches = Command::new("kvs-server")
    .arg(
        arg!(--addr <ADDR> "Provide IP:PORT"
        )
        // We don't have syntax yet for optional options, so manually calling `required`
        .required(false)
        .default_value("127.0.0.1:4000")
        
    )
    .version(version)
    .get_matches();

    let address = matches.get_one::<String>("addr").unwrap();


    let tcp_listener = TcpListener::bind(address).unwrap();
    for stream in tcp_listener.incoming() {
        let _tcp_stream = stream.unwrap();
        print!("Connection Established!!")
    }

    Ok(())
}

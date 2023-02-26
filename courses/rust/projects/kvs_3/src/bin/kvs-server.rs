use std::{net::TcpListener};
use clap::{Command, arg};
use kvs::{Result};
use log::{info};

fn main() -> Result<()> {

    stderrlog::new()
        .module(module_path!())
        .verbosity(3)
        .timestamp(stderrlog::Timestamp::Millisecond)
        .init().unwrap();

    let version = env!("CARGO_PKG_VERSION");
    let matches = Command::new("kvs-server")
    .arg(
        arg!(--addr <ADDR> "Provide IP:PORT")
        .required(false)
        .default_value("127.0.0.1:4000")
        
    )
    .version(version)
    .get_matches();

    let address = matches.get_one::<String>("addr").unwrap();

    info!("KVS Server version={}", version);
    info!("Started Listening on IP:PORT={}", address);

    let tcp_listener = TcpListener::bind(address).unwrap();
    for stream in tcp_listener.incoming() {
        let _tcp_stream = stream.unwrap();
        info!("Connected to Client...")
    }

    Ok(())
}

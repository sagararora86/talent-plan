use std::env::current_dir;
use clap::{arg, Command};
use kvs::{KvStore, Result};
use log::{info, error};

fn main() -> Result<()> {

    stderrlog::new()
    .module(module_path!())
    .verbosity(3)
    .timestamp(stderrlog::Timestamp::Millisecond)
    .init().unwrap();

    let version = env!("CARGO_PKG_VERSION");
    let matches = Command::new("Kvs")
        .version(version)
        .subcommands(vec![
            Command::new("set").args(vec![
                arg!([KEY]).required(true),
                arg!([VALUE]).required(true),
            ]),
            Command::new("get").args(vec![arg!([KEY]).required(true)]),
            Command::new("rm").args(vec![arg!([KEY]).required(true)]),
        ])
        .arg(
            arg!(--addr <ADDR> "Provide IP:PORT"
            )
            .required(false)
            .default_value("127.0.0.1:4000")
            
        )
        .get_matches();

    info!("KVS Server version={}", version);

    let address = matches.get_one::<String>("addr").unwrap();
    info!("Using IP:PORT={} to send command.", address);

    let mut kv_store = KvStore::open(current_dir().unwrap())?;
    match matches.subcommand() {
        Some(("set", args))=> {
            let key = args.value_of("KEY").unwrap();
            let value = args.value_of("VALUE").unwrap();
            info!("Executing SET Key={}, Value={}", key, value);
            kv_store.set(key.to_string(), value.clone().to_string())?;
        },
        Some(("get", args)) => {
            let key = args.value_of("KEY").unwrap();
            info!("Executing GET Key={}", key);
            let value = kv_store.get(key.to_string())?;
            match value {
                Some(x) => println!("{}", x),
                None => println!("Key not found"),
            }
        },
        Some(("rm", args)) => {
            let key = args.value_of("KEY").unwrap();
            info!("Executing RM Key={}", key);
            let result = kv_store.remove(key.to_string());
            match result {
                Err(err) => {
                    println!("{}", err);
                    std::process::exit(1);
                },
                _ => {},
            }
        },
        _ => {
            error!("Unknown Command, Exiting...");
            std::process::exit(1)
        }
    }
    Ok(())
}

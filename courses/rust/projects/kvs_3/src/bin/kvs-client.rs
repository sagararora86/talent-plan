use std::env::current_dir;
use clap::{arg, Command};
use kvs::{KvStore, Result};

fn main() -> Result<()> {
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
        .get_matches();

    let mut kv_store = KvStore::open(current_dir().unwrap())?;

    match matches.subcommand() {
        Some(("set", args))=> {
            let key = args.value_of("KEY").unwrap();
            let value = args.value_of("VALUE").unwrap();
            kv_store.set(key.to_string(), value.clone().to_string())?;
        },
        Some(("get", args)) => {
            let key = args.value_of("KEY").unwrap();
            let value = kv_store.get(key.to_string())?;
            match value {
                Some(x) => println!("{}", x),
                None => println!("Key not found"),
            }
        },
        Some(("rm", args)) => {
            let key = args.value_of("KEY").unwrap();
            let result = kv_store.remove(key.to_string());
            match result {
                Err(err) => {
                    println!("{}", err);
                    std::process::exit(1);
                },
                _ => {},
            }
        },
        _ => std::process::exit(1),
    }
    Ok(())
}

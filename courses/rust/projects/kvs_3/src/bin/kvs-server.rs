use std::{net::TcpListener, io::Write, env::current_dir};
use clap::{Command, arg};
use kvs::{Result, DbCommand, KvStore};
use log::{info, error};
use serde::Deserialize;

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
    let mut kv_store = KvStore::open(current_dir().unwrap())?;
    
    info!("KVS Server version={}", version);
    info!("Started Listening on IP:PORT={}", address);

    let tcp_listener = TcpListener::bind(address).unwrap();
    for stream in tcp_listener.incoming() {
        let mut tcp_stream = stream.unwrap();
        info!("Connected to Client...");
        let mut de = serde_json::Deserializer::from_reader(&tcp_stream);
        let command = DbCommand::deserialize(&mut de)?;
        match command {
            DbCommand::Set(a, b) => {
                info!("Executing Command=SET Key={}, Value={}", a, b);
                let result = kv_store.set(a.to_string(), b.to_string());
                let data_to_write;
                if result.is_err() {
                    let error = result.err().unwrap().to_string();
                    let error = DbCommand::Error(error);
                    data_to_write = serde_json::to_string(&error).unwrap();
                    error!("Error command=SET, error={:?}", error)
                } else {
                    let command = DbCommand::SetResult(a.to_string());
                    data_to_write = serde_json::to_string(&command).unwrap(); 
                }
                tcp_stream.write(data_to_write.as_bytes()).unwrap();
                info!("Command=SET Key={}, Value={} executed successfully.", a.to_string(), b);
            },
            DbCommand::Get(a) => {
                info!("Executing Command=GET Key={}", a);
                let result = kv_store.get(a.to_string());
                let data_to_write;
                if result.is_err() {
                    let error = result.err().unwrap().to_string();
                    let error = DbCommand::Error(error);
                    data_to_write = serde_json::to_string(&error).unwrap();
                    error!("Error Command=GET, error={:?}", error)
                } else {
                    let command = DbCommand::GetResult(result.unwrap().unwrap());
                    data_to_write = serde_json::to_string(&command).unwrap(); 
                }
                tcp_stream.write(data_to_write.as_bytes()).unwrap();
                info!("Command=GET Key={} executed successfully.", a);
            },
            DbCommand::Rm(a) => {
                info!("Executing Command=RM Key={}", a);
                let result = kv_store.remove(a.to_string());
                let data_to_write;
                if result.is_err() {
                    let error = result.err().unwrap().to_string();
                    let error = DbCommand::Error(error);
                    data_to_write = serde_json::to_string(&error).unwrap();
                    error!("Error Command=RM, error={:?}", error)
                } else {
                    let command = DbCommand::RmResult();
                    data_to_write = serde_json::to_string(&command).unwrap(); 
                }
                tcp_stream.write(data_to_write.as_bytes()).unwrap();
                info!("Command=RM Key={} executed successfully.", a);
            }, 
            _ => {
                let command = DbCommand::Error("Unknown Command".to_owned());
                let data_to_write = serde_json::to_string(&command).unwrap();
                tcp_stream.write(data_to_write.as_bytes()).unwrap();
                info!("Command=UNKNOWN, Returning Error");
            }
        }
    }

    Ok(())
}

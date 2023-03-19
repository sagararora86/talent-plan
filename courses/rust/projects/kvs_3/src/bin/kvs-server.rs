use std::{net::TcpListener, io::{Write, BufReader, BufRead}, env::current_dir, path::{Path}, fs::OpenOptions};
use clap::{Command, arg};
use kvs::{Result, DbCommand, KvStore, KvsEngine, KvsError};
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
    .args(&[
        arg!(--engine <"ENGINE-NAME"> "Provide EngineName")
        .required(false)
        .value_parser(["kvs", "sled"])
        .default_value("kvs"),
        arg!(--addr <ADDR> "Provide IP:PORT")
        .required(false)
        .default_value("127.0.0.1:4000")
    ])
    .version(version)
    .get_matches();

    let address = matches.get_one::<String>("addr").unwrap();
    let engine = matches.get_one::<String>("engine").unwrap();
    let cur_dir = current_dir().unwrap();
    info!("Opening Database at Location={}", &cur_dir.display());
    let mut kv_store : Box<dyn KvsEngine> = Box::new(KvStore::open(&cur_dir)?);
    
    let engine_in_file = get_existing_engine(&cur_dir.as_path());
    if let Some(x) = engine_in_file {
        if let Some((_, engine_type)) = x.split_once("=") {
            if !str::eq(engine_type, engine) {
                std::process::exit(1);
            }
        }

        
    } else {
        update_engine_in_file(&cur_dir.as_path(), &engine).unwrap();
    }

    info!("KVS Server version={}", version);
    info!("Started Listening on IP:PORT={}", address);
    info!("KvsServer Engine={}", engine);

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
                let command : DbCommand;
                    if let Some(x) = result.unwrap() {
                        command = DbCommand::GetResult(x);
                    } else {
                        command = DbCommand::Error(KvsError::KeyNotFound.to_string());
                    }
                let data_to_write = serde_json::to_string(&command).unwrap(); 
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

fn get_existing_engine(cur_dir : &Path) ->  Option<String> {
    let config_file = cur_dir.join(".config");

    let exits = Path::new(config_file.as_path()).exists();
    if !exits {
        return None;
    }

    let file = OpenOptions::new()
        .read(true)
        .open(config_file).unwrap();

    let buf_reader = BufReader::new(file);
    buf_reader.lines()
    .filter(|x| x.is_ok())
    .map(|x| x.unwrap())
    .find(|x| {
        return x.starts_with("engine")
    })
}

fn update_engine_in_file(cur_dir : &Path, engine : &str) -> Result<()> {
    let config_file = cur_dir.join(".config");
    let mut line_to_write = String::new();
    line_to_write.push_str("engine=");
    line_to_write.push_str(engine);
    line_to_write.push('\n');
    let _op = OpenOptions::new()
        .write(true)
        .create(true)
        .open(config_file).and_then(
            |mut file| file.write(line_to_write.as_bytes())
        ).unwrap();
    Ok(())
}

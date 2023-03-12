use std::io::Write;
use std::net::TcpStream;
use clap::{arg, Command};
use kvs::DbCommand;
use log::{info, error};

fn main() -> () {

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
    let mut tcp_stream = TcpStream::connect(address).unwrap();
    info!("Connected to IP:PORT={}", address);


    match matches.subcommand() {
        Some(("set", args))=> {
            let key = args.value_of("KEY").unwrap();
            let value = args.value_of("VALUE").unwrap();
            info!("Executing SET Key={}, Value={}", key, value);
            let set_command = DbCommand::Set(key.to_string(), value.clone().to_string());
            let set_command = serde_json::to_string(&set_command).unwrap();
            tcp_stream.write(set_command.as_bytes()).unwrap();
            let result_command : DbCommand = serde_json::from_reader(tcp_stream).unwrap();
            info!("SET Result = {:?}", result_command);  
        },
        Some(("get", args)) => {
            let key = args.value_of("KEY").unwrap();
            info!("Executing GET Key={}", key);
            let get_command = DbCommand::Get(key.to_string());
            let get_command = serde_json::to_string(&get_command).unwrap();
            tcp_stream.write(get_command.as_bytes()).unwrap();
            let result_command : DbCommand = serde_json::from_reader(tcp_stream).unwrap();
            info!("GET Result = {:?}", result_command);  
        },
        Some(("rm", args)) => {
            let key = args.value_of("KEY").unwrap();
            info!("Executing RM Key={}", key);
            let rm_command = DbCommand::Rm(key.to_string());
            let rm_command = serde_json::to_string(&rm_command).unwrap();
            tcp_stream.write(rm_command.as_bytes()).unwrap();
            let result_command : DbCommand = serde_json::from_reader(tcp_stream).unwrap();
            info!("RM Result = {:?}", result_command);
        },
        _ => {
            error!("Unknown Command, Exiting...");
            std::process::exit(1)
        }
    }
    ()
}

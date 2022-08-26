extern crate core;

use std::collections::HashMap;
use std::path::PathBuf;
use failure::Fail;
use serde::{Serialize, Deserialize};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::SeekFrom;
use std::io::{BufReader, BufWriter};
use std::io::prelude::*;
use crate::KvsCommand::{Rm, Set};
use crate::KvsError::KeyNotFound;

pub type Result<T> = std::result::Result<T, KvsError>;

#[derive(Fail, Debug)]
#[fail(display = "Error in KVS")]
pub enum KvsError {
    #[fail(display = "{}", _0)]
    IO(#[cause] io::Error),
    #[fail(display = "Error Serializing or Deserializing {}", _0)]
    SerdeError(serde_json::Error),
    #[fail(display = "Key not found")]
    KeyNotFound
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> Self {
        return KvsError::IO(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> Self {
        return KvsError::SerdeError(err)
    }
}

#[derive(Serialize, Deserialize)]
pub enum KvsCommand {
    Set(String, String),
    Rm(String)
}

impl KvsCommand {

    pub fn set(key : String, value : String) -> KvsCommand {
        Set(key, value)
    }

    pub fn rm(key : String) -> KvsCommand {
        Rm(key)
    }
}
pub struct KvStore {
    buf_reader : BufReader<File>,
    buf_writer : BufWriter<File>,
    map : HashMap<String, u64>,
    cur_offset : u64
}

impl KvStore {

    fn get_file_name() -> &'static str {
        return "kvs.bin";
    }

    fn read_from_file(file_path : &mut PathBuf) -> Result<HashMap<String, u64>> {    
        let mut map = HashMap::new();
        let file_exists = file_path.as_path().exists();
        if !file_exists {
            Ok(map)
        } else {
            let file = OpenOptions::new()
            .read(true)
            .open(file_path)?;

            let mut buf_reader = BufReader::new(file);
            let mut itr = serde_json::Deserializer::from_reader(&mut buf_reader)
                .into_iter::<KvsCommand>();

            let mut offset = 0;
            loop {
                let kvs_command_option = itr.next();
                match kvs_command_option {
                    Some(kvs_command_result) => {
                        let kvs_command = kvs_command_result.unwrap();
                        match kvs_command {
                            Set(key, _) => map.insert(key, offset),
                            Rm(key) => map.remove(&key),
                        };
                    },
                    None => break
                }
                offset += itr.byte_offset() as u64;
            }
            Ok(map)
        }
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut file_path = path.into();
        file_path.push(Self::get_file_name());
        let map = Self::read_from_file(&mut file_path)?;

        let file_2 = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;
        let offset = file_2.metadata()?.len();
        let buf_writer = BufWriter::new(file_2);

        let file_1 = OpenOptions::new()
        .read(true)
        .open(&file_path)?;

        let buf_reader = BufReader::new(file_1);

        Ok(KvStore{
            buf_reader,
            buf_writer,
            map,
            cur_offset : offset
        })
    }
    
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = KvsCommand::set(key.clone(), value.clone());
        let json_str = serde_json::to_string(&command)?;
        let bytes_written = self.buf_writer.write(json_str.as_bytes())?;
        self.buf_writer.flush()?;
        self.map.insert(key,self.cur_offset);
        self.cur_offset += bytes_written as u64;
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let option = self.map.get(&key);
        match option {
            Some(offset) => {
                    self.buf_reader.seek(SeekFrom::Start(offset.clone()))?;
                    let kvs_command = serde_json::Deserializer::from_reader(&mut self.buf_reader)
                        .into_iter::<KvsCommand>().next().unwrap()?;
                    match kvs_command {
                        Set(_, v) => Ok(Some(v)),
                        _ => panic!("No Set command found")
                    }
            }
            None => Ok(None),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.map.contains_key(&key) {
            let command = KvsCommand::rm(key.clone());
            let json_str = serde_json::to_string(&command)?;
            let bytes_written = self.buf_writer.write(json_str.as_bytes())?;
            self.buf_writer.flush()?;
            self.cur_offset += bytes_written as u64;
            self.map.remove(&key);
            Ok(())
        } else {
            Err(KeyNotFound)
        }
    }
}

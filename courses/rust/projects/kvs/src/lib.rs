extern crate core;

use std::collections::HashMap;
use std::path::PathBuf;
use failure::Fail;
use serde::{Serialize, Deserialize};
use std::fs::{File, OpenOptions};
use std::io;
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
    file : File,
    map : HashMap<String, String>
}

impl KvStore {

    fn get_file_name() -> &'static str {
        return "kvs.bin";
    }

    fn read_from_file(file_path : &mut PathBuf) -> Result<HashMap<String, String>> {    
        let mut map = HashMap::new();
        let file_exists = file_path.as_path().exists();
        if !file_exists {
            Ok(map)
        } else {
            let file = OpenOptions::new()
            .read(true)
            .open(file_path)?;

            let mut buf_reader = BufReader::new(file);

            serde_json::Deserializer::from_reader(&mut buf_reader)
                .into_iter::<KvsCommand>()
                .filter_map(|it| it.ok())
                .for_each(|it| {
                    match it {
                        Set(key, val) => map.insert(key, val),
                        Rm(key) => map.remove(&key),
                    };
                });
            Ok(map)
        }
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut file_path = path.into();
        file_path.push(Self::get_file_name());
        let map = Self::read_from_file(&mut file_path)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)?;

        Ok(KvStore{
            file,
            map
        })
    }
    
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = KvsCommand::set(key.clone(), value.clone());
        let mut writer = BufWriter::new(&self.file);
        serde_json::to_writer(&mut writer, &command)?;
        writer.flush()?;
        self.map.insert(key, value);
        Ok(())
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        let option = self.map.get(&key);
        match option {
            Some(x) => Ok(Some(x.clone())),
            None => Ok(None),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.map.contains_key(&key) {
            let command = KvsCommand::rm(key.clone());
            let mut writer = BufWriter::new(&self.file);
            serde_json::to_writer(&mut writer, &command)?;
            writer.flush()?;
            self.map.remove(&key);
            Ok(())
        } else {
            Err(KeyNotFound)
        }
    }
}

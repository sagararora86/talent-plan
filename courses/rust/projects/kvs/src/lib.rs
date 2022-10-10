extern crate core;

use rand::Rng;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use failure::Fail;
use serde::{Serialize, Deserialize};
use std::fs::{File, OpenOptions};
use std::{io};
use std::io::prelude::*;
use std::io::{SeekFrom, Seek};
use std::io::{BufReader, BufWriter};

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
    Set(String, String, u128),
    Rm(String, u128)
}

impl KvsCommand {

    pub fn set(key : String, value : String, timestamp : u128) -> KvsCommand {
        Set(key, value, timestamp)
    }

    pub fn rm(key : String, timestamp : u128) -> KvsCommand {
        Rm(key, timestamp)
    }
}
pub struct KvStore {
    dir_path : PathBuf,
    files : HashMap<u32, KvsFile>,
    map : HashMap<String, KvsEntry>,
    cur_offset : u64,
    cur_index : u32
}

struct KvsEntry {
    file_index : u32,
    timestamp : u128,
    offset : u64
}

impl KvsEntry {

    fn new(file_index : u32, timestamp : u128, offset : u64) -> Self {
        KvsEntry {file_index, timestamp, offset}
    }

    fn get_timestamp(&self) -> u128 {
        self.timestamp
    }

}

struct KvsFile {
    file_path : PathBuf,
    buf_reader : BufReader<File>,
    buf_writer : BufWriter<File>,
    index : u32
}

impl KvStore {

    fn get_timestamp() -> u128 {
        SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap().as_millis()
    }
 
    fn get_file_name() -> String {
        let mut rng = rand::thread_rng();
        let n1: u8 = rng.gen();
        format!("kvs{}.bin", n1)
    }

    fn create_new_file(dir_path : &PathBuf, index : u32) -> KvsFile {
        let file_name = Self::get_file_name();
        let mut file_path = dir_path.clone();
        file_path.push(file_name);
        Self::create_kvs_file(&file_path, index)
    }

    fn find_all_file_in_dir(dir_path : &PathBuf) -> Vec<PathBuf> {
        let mut files_vec = Vec::new();
        for entry in std::fs::read_dir(dir_path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
    
            let metadata = std::fs::metadata(&path).unwrap();
            if metadata.is_file() && path.extension().unwrap() == "bin" {
                files_vec.push(path);
            }
        }
        files_vec
    }

    fn get_kvs_files(file_paths : Vec<PathBuf>) -> Vec<KvsFile> {
        let mut files = vec![];
        let mut itr = 0;
        for file_path in file_paths {
            let kvs_file = Self::create_kvs_file(&file_path, itr);
            files.push(kvs_file);
            itr += 1;
        }
        files
    }

    fn create_kvs_file(file_path : &PathBuf, index : u32) -> KvsFile {
        let buf_writer = Self::create_buf_writer(file_path);
        let buf_reader = Self::create_buf_reader(file_path);
        KvsFile {
            file_path : file_path.clone(),
            buf_reader,
            buf_writer,
            index
        }
    }

    fn create_buf_reader(file_path : &PathBuf) -> BufReader<File> {
        let file = OpenOptions::new()
        .read(true)
        .open(file_path).unwrap();

        let buf_reader = BufReader::new(file);
        buf_reader
    }

    fn create_buf_writer(file_path : &PathBuf) -> BufWriter<File> {
        let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path).unwrap();
        let buf_writer = BufWriter::new(file);
        buf_writer
    }

    fn read_from_file(kvs_file : &mut KvsFile) -> Result<(HashMap<String, KvsEntry>, HashMap<String, KvsEntry>)> {    
        let mut set_map = HashMap::new();
        let mut del_map = HashMap::new();

        let mut itr = serde_json::Deserializer::from_reader(&mut kvs_file.buf_reader)
                .into_iter::<KvsCommand>();

        let mut offset = 0;
        loop {
            let kvs_command_option = itr.next();
            match kvs_command_option {
                Some(kvs_command_result) => {
                    let kvs_command = kvs_command_result.unwrap();
                    match kvs_command {
                        Set(key, _, time_stamp) => {
                            set_map.insert(key, KvsEntry::new(kvs_file.index, time_stamp, offset));
                        },
                        Rm(key, time_stamp) => {
                            del_map.insert(key, KvsEntry::new(kvs_file.index, time_stamp, offset));
                        }
                    };
                },
                None => break
            }
            offset += itr.byte_offset() as u64;
        }
        kvs_file.buf_reader.seek(SeekFrom::Start(0)).unwrap();
        Ok((set_map, del_map))
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let dir_path = path.into();
        let file_paths = Self::find_all_file_in_dir(&dir_path);
        let mut files = Self::get_kvs_files(file_paths);
        let mut set_map : HashMap<String, KvsEntry> = HashMap::new();
        let mut del_map : HashMap<String, KvsEntry> = HashMap::new();
        for mut file in &mut files {
            let out = Self::read_from_file(&mut file).unwrap();
            let s_map = out.0;
            for s_map_entry in s_map {
                if set_map.contains_key(&s_map_entry.0) {
                    let cur_value = set_map.get(&s_map_entry.0).unwrap();
                    if s_map_entry.1.timestamp > cur_value.get_timestamp() {
                        set_map.insert(s_map_entry.0, s_map_entry.1);
                    }
                } else {
                    set_map.insert(s_map_entry.0, s_map_entry.1);
                }
            }

            let d_map = out.1;
            for d_map_entry in d_map {
                if del_map.contains_key(&d_map_entry.0) {
                    let cur_value = del_map.get(&d_map_entry.0).unwrap();
                    if d_map_entry.1.timestamp > cur_value.get_timestamp() {
                        del_map.insert(d_map_entry.0, d_map_entry.1);
                    } 
                } else {
                    del_map.insert(d_map_entry.0, d_map_entry.1);
                }
            }
        }

        let mut files_map : HashMap<u32, KvsFile> = HashMap::new();
        for file in files {
            files_map.insert(file.index, file);
        }

        let mut final_map = HashMap::new();
        for entry in set_map {
            final_map.insert(entry.0, entry.1);
        }

        for entry in del_map {
            if final_map.contains_key(&entry.0) {
                let cur_val = final_map.get(&entry.0).unwrap();
                if cur_val.get_timestamp() < entry.1.timestamp {
                    final_map.remove(&entry.0);
                }
            }
        }

        let cur_index = files_map.len() as u32;
        let cur_file = Self::create_new_file(&dir_path, cur_index);
        files_map.insert(cur_index, cur_file);

        Ok(KvStore {
            dir_path,
            files : files_map,
            map : final_map,
            cur_offset : 0,
            cur_index
        })

    }
    
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let timestamp = Self::get_timestamp();
        let command = KvsCommand::set(key.clone(), value.clone(), timestamp);
        let json_str = serde_json::to_string(&command)?;
        let cur_file = self.files.get_mut(&self.cur_index).unwrap();
        let bytes_written = cur_file.buf_writer.write(json_str.as_bytes())?;
        cur_file.buf_writer.flush()?;
        self.map.insert(key,KvsEntry::new(cur_file.index, timestamp, self.cur_offset));
        self.cur_offset += bytes_written as u64;
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.map.get(&key) {
            Some(entry) => {
                    let kvs_file = self.files.get_mut(&entry.file_index).unwrap();
                    kvs_file.buf_reader.seek(SeekFrom::Start(entry.offset.clone()))?;
                    let kvs_command = serde_json::Deserializer::from_reader(&mut kvs_file.buf_reader)
                        .into_iter::<KvsCommand>().next().unwrap()?;
                    match kvs_command {
                        Set(_, v, _) => Ok(Some(v)),
                        _ => panic!("No Set command found")
                    }
            }
            None => Ok(None),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.map.contains_key(&key) {
            let timestamp = Self::get_timestamp();
            let command = KvsCommand::rm(key.clone(), timestamp);
            let json_str = serde_json::to_string(&command)?;
            let cur_file = self.files.get_mut(&self.cur_index).unwrap();
            let bytes_written = cur_file.buf_writer.write(json_str.as_bytes())?;
            cur_file.buf_writer.flush()?;
            self.cur_offset += bytes_written as u64;
            self.map.remove(&key);
            Ok(())
        } else {
            Err(KeyNotFound)
        }
    }
}

use crate::comp::config::Config;
use crate::comp::index::Index;
use crate::comp::store::Store;
use prost::Message;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use tokio::fs::File as FileTokio;
mod log {
    include!(concat!(env!("OUT_DIR"), "../../proto_app/src/log.v1.rs"));
}

#[derive(Debug)]
struct Segment {
    store: Store,
    index: Index,
    base_offset: u64,
    next_offset: u64,
    config: Config,
    path_index: String,
}

impl Segment {
    async fn new(dir: &str, base_offset: u64, config: Config) -> Result<Self, std::io::Error> {
        let store_file: FileTokio = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(Path::new(dir).join(format!("{}.store", base_offset)))?;

        let store = Store::new(store_file).await?;

        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(Path::new(dir).join(format!("{}.index", base_offset)))?;

        let index = Index::new(&index_file, config.segment.max_index_bytes, dir.to_string())?;

        let next_offset = match index.read(-1) {
            Ok((off, _)) => base_offset + off as u64 + 1,
            Err(_) => base_offset,
        };

        Ok(Self {
            store,
            index,
            base_offset,
            next_offset,
            config,
        })
    }

    fn append(&mut self, record: &log::Record) -> Result<u64, std::io::Error> {
        let current_offset = self.next_offset;
        let mut record = record.clone();
        record.offset = current_offset;

        let mut buf = Vec::new();
        record.encode(&mut buf)?;

        let pos = self.store.append(&buf)?;
        self.index
            .write((self.next_offset - self.base_offset) as u32, pos)?;

        self.next_offset += 1;
        Ok(current_offset)
    }

    fn read(&mut self, offset: u64) -> Result<log::Record, std::io::Error> {
        let pos = self.index.read((offset - self.base_offset) as i64)?.1;
        let data = self.store.read(pos)?;

        let record = log::Record::decode(&*data)?;
        Ok(record)
    }

    fn is_maxed(&self) -> bool {
        self.store.size >= self.config.segment.max_store_bytes
            || self.index.size >= self.config.segment.max_index_bytes
    }

    fn remove(&mut self) -> Result<(), std::io::Error> {
        self.close()?;
        std::fs::remove_file(self.index.name())?;
        std::fs::remove_file(self.store.name())?;
        Ok(())
    }

    fn close(&mut self) -> Result<(), std::io::Error> {
        self.index.close()?;
        self.store.close()?;
        Ok(())
    }
}

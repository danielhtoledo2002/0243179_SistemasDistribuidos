use crate::comp::config::Config;
use crate::comp::index::Index;
use crate::comp::record::Record;
use crate::comp::store::Store;
use prost::Message;
use std::fs::{File, OpenOptions};
// use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use tokio::fs::File as AsyncFile;

#[derive(Debug)]
pub struct Segment {
    store: Box<Store>,
    index: Box<Index>,
    base_offset: u64,
    next_offset: u64,
    config: Box<Config>,
    path_index: String,
    path_store: String,
}

impl Segment {
    pub async fn new(dir: &str, base_offset: u64, config: Config) -> Result<Self, std::io::Error> {
        let store_file_path = Path::new(dir).join(format!("{}.store", base_offset));
        let store_file = AsyncFile::open(&store_file_path).await?;
        let store = Box::new(Store::new(store_file, dir.to_string()).await?);

        let index_file_path = Path::new(dir).join(format!("{}.index", base_offset));
        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&index_file_path)?;

        let index = Box::new(Index::new(&index_file, &config, dir.to_string())?);

        let next_offset = match index.read(-1) {
            Ok((off, _)) => base_offset + off as u64 + 1,
            Err(_) => base_offset,
        };
        let path_index = index_file_path.to_string_lossy().into_owned();

        let path_store = store_file_path.to_string_lossy().into_owned();

        let config = Box::new(config);

        Ok(Self {
            store,
            index,
            base_offset,
            next_offset,
            config,
            path_index,
            path_store,
        })
    }

    pub async fn append(&mut self, record: Record) -> Result<u64, std::io::Error> {
        let current_offset = self.next_offset;
        let mut record = record.clone();
        record.offset = current_offset;

        let mut buf = Vec::new();
        record.encode(&mut buf)?;

        let pos = self.store.append(&buf).await?;
        self.index
            .write((self.next_offset - self.base_offset) as u32, pos.1)?;

        self.next_offset += 1;
        Ok(current_offset)
    }

    pub async fn read(&mut self, offset: u64) -> Result<Record, std::io::Error> {
        let pos = self.index.read((offset - self.base_offset) as i64)?.1;
        let data = self.store.read(pos).await?;

        let record = Record::decode(&*data)?;
        Ok(record)
    }

    pub async fn is_maxed(&mut self) -> bool {
        self.store.size >= self.config.segment.max_store_bytes
            || self.index.size >= self.config.segment.max_index_bytes
    }

    pub async fn remove(&mut self) -> Result<(), std::io::Error> {
        self.close();
        std::fs::remove_file(self.path_index.clone());
        tokio::fs::remove_file(self.path_store.clone());
        Ok(())
    }

    pub async fn close(&mut self) -> Result<(), std::io::Error> {
        self.index.close(&mut File::open(&self.path_index)?)?;
        self.store.close().await?;
        Ok(())
    }
}

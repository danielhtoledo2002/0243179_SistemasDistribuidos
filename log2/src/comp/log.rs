use crate::comp::config::Config;
use crate::comp::record::Record;
use crate::comp::segments::Segment;
use std::collections::BTreeMap;
use std::fs::{remove_dir_all, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::fs;

pub struct Log {
    dir: PathBuf,
    config: Config,
    active_segment: RwLock<Box<Segment>>,
    segments: RwLock<Vec<Segment>>,
}

impl Log {
    pub async fn new(dir: &str, config: Config) -> io::Result<Self> {
        let mut config = config;
        if config.segment.max_store_bytes == 0 {
            config.segment.max_store_bytes = 1024;
        }
        if config.segment.max_index_bytes == 0 {
            config.segment.max_index_bytes = 1024;
        }

        let log = Log {
            dir: Path::new(dir).to_path_buf(),
            config,
            active_segment: None,
            segments: Vec::new(),
        };
        Ok(log)
    }

    pub async fn setup(&mut self) -> io::Result<()> {
        let mut base_offsets = Vec::new();
        let entries = std::fs::read_dir(&self.dir)?;

        for entry in entries {
            let entry = entry?;
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();
            let base_offset_str = file_name_str.split('.').next().unwrap_or("");
            if let Ok(base_offset) = base_offset_str.parse::<u64>() {
                base_offsets.push(base_offset);
            }
        }

        base_offsets.sort();
        for i in 0..base_offsets.len() {
            if let Err(e) = self.new_segment(base_offsets[i]) {
                return Err(e);
            }
            if i + 1 < base_offsets.len() {
                // i += 1;
            }
        }

        if self.segments.is_empty() {
            self.new_segment(self.config.segment.initial_offset)?;
        }

        Ok(())
    }

    pub async fn append(&mut self, record: Record) -> io::Result<u64> {
        let mut offset = self
            .active_segment
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "No active segment"))?
            .append(record)
            .await?;
        offset += 1;
        if self.active_segment.as_ref().unwrap().is_maxed().await {
            self.new_segment(offset)?;
        }

        Ok(offset)
    }

    pub async fn read(&self, offset: u64) -> io::Result<Record> {
        let segment = self
            .segments
            .iter()
            .find(|seg| seg.base_offset <= offset && offset < seg.next_offset)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Offset out of range"))?;

        segment.read(offset)
    }

    fn new_segment(&mut self, offset: u64) -> io::Result<()> {
        let segment = Segment::new(&self.dir, offset, &self.config)?;
        self.segments.push(segment.clone());
        self.active_segment = Some(segment);
        Ok(())
    }

    pub fn close(&mut self) -> io::Result<()> {
        let _lock = self.lock.write().unwrap(); // Lock for write
        for segment in &mut self.segments {
            segment.close()?;
        }
        Ok(())
    }

    pub fn remove(&self) -> io::Result<()> {
        self.close()?;
        remove_dir_all(&self.dir)
    }

    pub fn reset(&mut self) -> io::Result<()> {
        self.remove()?;
        self.setup()
    }

    pub fn lowest_offset(&self) -> io::Result<u64> {
        let _lock = self.lock.read().unwrap(); // Lock for read
        Ok(self.segments.first().map_or(0, |seg| seg.base_offset))
    }

    pub fn highest_offset(&self) -> io::Result<u64> {
        let _lock = self.lock.read().unwrap(); // Lock for read
        Ok(self
            .segments
            .last()
            .map_or(0, |seg| seg.next_offset.saturating_sub(1)))
    }

    pub fn truncate(&mut self, lowest: u64) -> io::Result<()> {
        let _lock = self.lock.write().unwrap(); // Lock for write

        self.segments.retain(|seg| {
            if seg.next_offset <= lowest + 1 {
                seg.remove().is_ok()
            } else {
                true
            }
        });

        Ok(())
    }

    pub fn reader(&self) -> io::Result<impl Read> {
        let _lock = self.lock.read().unwrap(); // Lock for read

        let readers: Vec<Box<dyn Read>> = self
            .segments
            .iter()
            .map(|seg| Box::new(seg.store.read()) as Box<dyn Read>)
            .collect();

        Ok(io::multi_reader(readers))
    }
}

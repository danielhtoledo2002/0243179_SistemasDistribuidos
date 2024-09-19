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
    active_segment: Option<Arc<RwLock<Segment>>>,
    segments: Vec<Arc<RwLock<Segment>>>,
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

        let mut log = Log {
            dir: Path::new(dir).to_path_buf(),
            config,
            active_segment: None,
            segments: Vec::new(),
        };

        log.setup().await?;
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
            if let Err(e) = self.new_segment(base_offsets[i]).await {
                return Err(e);
            }
            if i + 1 < base_offsets.len() {
                // i += 1;
            }
        }

        if self.segments.is_empty() {
            self.new_segment(self.config.segment.initial_offset).await?;
        }

        Ok(())
    }

    pub async fn append(&mut self, record: Record) -> io::Result<u64> {
        let mut offset = self
            .active_segment
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "No active segment"))?
            .write().unwrap()
            .append(record)
            .await?;
        offset += 1;

        if let Some(ref a) = self.active_segment {
            if a.read().unwrap().is_maxed().await {
                self.new_segment(offset).await?;
            }
        }



        Ok(offset)
    }

    pub async fn read(&self, offset: u64) -> io::Result<Record> {
        let segment = self.segments
            .iter()
            .find(|seg| {
                let guard = seg.read().unwrap();
                guard.base_offset <= offset && offset < guard.next_offset
            })
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Offset out of range"))?;

        segment.read().unwrap().read(offset).await
    }

    async fn new_segment(&mut self, offset: u64) -> io::Result<()> {
        let segment = Arc::new(RwLock::new(Segment::new(&self.dir.as_os_str().to_str().unwrap(), offset, self.config.clone()).await?));
        self.segments.push(Arc::clone(&segment));
        self.active_segment = Some(segment);
        Ok(())
    }

    pub async fn close(&mut self) -> io::Result<()> {
        for segment in &mut self.segments {
            segment.write().unwrap().close().await?;
        }
        Ok(())
    }

    pub async fn remove(&mut self) -> io::Result<()> {
        self.close().await?;
        remove_dir_all(&self.dir)
    }

    pub async fn reset(&mut self) -> io::Result<()> {
        self.remove().await?;
        self.setup().await
    }

    pub fn lowest_offset(&self) -> io::Result<u64> {
        Ok(self.segments.first().map_or(0, |seg| seg.read().unwrap().base_offset))
    }

    pub fn highest_offset(&self) -> io::Result<u64> {
        Ok(self
            .segments
            .last()
            .map_or(0, |seg| seg.read().unwrap().next_offset.saturating_sub(1)))
    }

    pub async fn truncate(&mut self, lowest: u64) -> io::Result<()> {
        let mut remove = vec![];
        for (i, seg) in self.segments.iter().enumerate() {
            let mut seg = seg.write().unwrap();
            if seg.next_offset <= lowest + 1 {
                seg.remove().await?;
                remove.push(i);
            }
        }

        let mut index = 0;
        self.segments.retain(|_| {
            let r = remove.contains(&index);
            index += 1;
            r
        });

        Ok(())
    }

    /*pub fn reader(&self) -> io::Result<impl Read> {
        let readers: Vec<Box<dyn Read>> = self
            .segments
            .iter()
            .map(|seg| Box::new(seg.read().unwrap().store.read()) as Box<dyn Read>)
            .collect();

        Ok(io::multi_reader(readers))
    }*/
}

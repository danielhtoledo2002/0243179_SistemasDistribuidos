use crate::comp::config::Config;

use memmap2::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;

const OFF_WIDTH: u64 = 4;
const POS_WIDTH: u64 = 8;
const ENT_WIDTH: u64 = OFF_WIDTH + POS_WIDTH;

#[derive(Debug)]
pub struct Index {
    mmap: MmapMut,
    size: u64,
    path: String,
}

impl Index {
    pub fn new(file: &File, config: &Config, path: String) -> io::Result<Index> {
        let size = file.metadata()?.len();

        file.set_len(config.segment.max_index_bytes)?;

        let mmap = unsafe {
            MmapOptions::new()
                .len(config.segment.max_index_bytes as usize)
                .map_mut(file)?
        };

        Ok(Index { mmap, size, path })
    }

    pub fn read(&self, idx: i64) -> io::Result<(u32, u64)> {
        if self.size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "index is empty",
            ));
        }

        let out = if idx == -1 {
            (self.size / ENT_WIDTH - 1) as u32
        } else {
            idx as u32
        };

        if (out as u64 * ENT_WIDTH) + ENT_WIDTH > self.size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "index out of bounds",
            ));
        }

        let pos = out as u64 * ENT_WIDTH;
        let offset = u32::from_le_bytes(
            self.mmap[pos as usize..(pos + OFF_WIDTH) as usize]
                .try_into()
                .unwrap(),
        );
        let position = u64::from_le_bytes(
            self.mmap[(pos + OFF_WIDTH) as usize..(pos + ENT_WIDTH) as usize]
                .try_into()
                .unwrap(),
        );

        Ok((offset, position))
    }

    pub fn write(&mut self, file: &mut File, off: u32, pos: u64) -> io::Result<()> {
        let mem_size = self.mmap.len() as u64;

        if mem_size < self.size + ENT_WIDTH {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "not enough memory to write",
            ));
        }

        self.mmap[self.size as usize..(self.size + OFF_WIDTH) as usize]
            .copy_from_slice(&off.to_le_bytes());
        self.mmap[(self.size + OFF_WIDTH) as usize..(self.size + ENT_WIDTH) as usize]
            .copy_from_slice(&pos.to_le_bytes());

        self.size += ENT_WIDTH;
        file.set_len(self.size)?;
        Ok(())
    }

    pub fn name(&self) -> io::Result<String> {
        Ok(self.path.clone())
    }

    pub fn close(&mut self, file: &mut File) -> io::Result<()> {
        self.mmap.flush_async()?;
        file.set_len(self.size)?;
        file.sync_all()?;
        Ok(())
    }
}

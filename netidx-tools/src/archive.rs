use anyhow::{Context, Error, Result};
use bytes::{Buf, BufMut};
use chrono::prelude::*;
use fs3::{allocation_granularity, FileExt};
use log::warn;
use mapr::MmapMut;
use netidx::{
    pack::{decode_varint, encode_varint, varint_len, Pack, PackError},
    path::Path,
    pool::{Pool, Pooled},
    publisher::Value,
};
use std::{
    self,
    cmp::max,
    collections::{BTreeMap, HashMap},
    fs::{File, OpenOptions},
    iter::IntoIter,
    mem,
    path::Path as FilePath,
};

#[derive(Debug, Clone)]
pub(crate) struct FileHeader {
    version: u32,
}

static FILE_MAGIC: &'static [u8] = b"netidx archive";
static FILE_VERSION: u32 = 0;

impl Pack for FileHeader {
    fn const_len() -> Option<usize> {
        Some(FILE_MAGIC.len() + mem::size_of::<u32>())
    }

    fn len(&self) -> usize {
        <FileHeader as Pack>::const_len().unwrap()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        buf.put_slice(FILE_MAGIC);
        Ok(buf.put_u32(FILE_VERSION))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        for byte in FILE_MAGIC {
            if buf.get_u8() != *byte {
                return Err(PackError::InvalidFormat);
            }
        }
        Ok(FileHeader { version: buf.get_u32() })
    }
}

#[derive(Debug, Clone)]
pub(crate) enum RecordTyp {
    PathMappings,
    Batch,
    End,
}

#[derive(Debug, Clone)]
pub(crate) struct RecordHeader {
    committed: bool,
    record_type: RecordTyp,
    record_length: u32,
    timestamp: DateTime<Utc>,
}

impl Pack for RecordHeader {
    fn const_len() -> Option<usize> {
        <DateTime<Utc> as Pack>::const_len()
            .map(|dt| mem::size_of::<u8>() + mem::size_of::<u32>() + dt)
    }

    fn len(&self) -> usize {
        <RecordHeader as Pack>::const_len().unwrap()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        let typ: u8 = match self.record_type {
            RecordTyp::PathMappings => 0,
            RecordTyp::Batch => 1,
            RecordTyp::End => 2,
        };
        let typ = if self.committed { typ } else { typ | 0x80 };
        buf.put_u8(typ);
        buf.put_u32(self.record_length);
        <DateTime<Utc> as Pack>::encode(&self.timestamp, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let typ = buf.get_u8();
        let record_length = buf.get_u32();
        let timestamp = <DateTime<Utc> as Pack>::decode(buf)?;
        Ok(RecordHeader {
            committed: typ & 0x80 > 0,
            record_type: match typ & 0x7F {
                0 => RecordTyp::PathMappings,
                1 => RecordTyp::Batch,
                2 => RecordTyp::End,
                _ => return Err(PackError::UnknownTag),
            },
            record_length,
            timestamp,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PathMapping(Path, u64);

impl Pack for PathMapping {
    fn len(&self) -> usize {
        <Path as Pack>::len(&self.0) + varint_len(self.1)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        <Path as Pack>::encode(&self.0, buf)?;
        Ok(encode_varint(self.1, buf))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let path = <Path as Pack>::decode(buf)?;
        let id = decode_varint(buf)?;
        Ok(PathMapping(path, id))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BatchItem(u64, Value);

impl Pack for BatchItem {
    fn len(&self) -> usize {
        varint_len(self.0) + <Value as Pack>::len(&self.1)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        encode_varint(self.0, buf);
        <Value as Pack>::encode(&self.1, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let id = decode_varint(buf)?;
        let value = <Value as Pack>::decode(buf)?;
        Ok(BatchItem(id, value))
    }
}

lazy_static! {
    static ref PM_POOL: Pool<Vec<PathMapping>> = Pool::new(10, 100000);
    static ref BATCH_POOL: Pool<Vec<BatchItem>> = Pool::new(10, 100000);
}

pub(crate) struct Archive {
    path_by_id: HashMap<u64, Path>,
    id_by_path: HashMap<Path, u64>,
    batchmap: BTreeMap<DateTime<Utc>, u64>,
    file: File,
    mmap: MmapMut,
    block_size: u64,
    end: usize,
    uncommitted: usize,
    next_id: u64,
}

impl Archive {
    pub(crate) fn open(path: impl AsRef<FilePath>) -> Result<Self> {
        if !FilePath::is_file(path.as_ref()) {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path.as_ref())?;
            let block_size = allocation_granularity(path.as_ref())?;
            file.try_lock_exclusive()?;
            let minsz = <FileHeader as Pack>::const_len().unwrap()
                + <RecordHeader as Pack>::const_len().unwrap();
            file.set_len(max(block_size, minsz as u64))?;
            let mut mmap = unsafe { MmapMut::map_mut(&file)? };
            let mut buf = &mut *mmap;
            <FileHeader as Pack>::encode(
                &FileHeader { version: FILE_VERSION },
                &mut buf,
            )?;
            <RecordHeader as Pack>::encode(
                &RecordHeader {
                    committed: true,
                    record_type: RecordTyp::End,
                    record_length: 0,
                    timestamp: Utc::now(),
                },
                &mut buf,
            )?;
            mmap.flush()?;
            Ok(Archive {
                path_by_id: HashMap::new(),
                id_by_path: HashMap::new(),
                batchmap: BTreeMap::new(),
                file,
                mmap,
                block_size,
                end: minsz,
                uncommitted: minsz,
                next_id: 0,
            })
        } else {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .append(true)
                .open(path.as_ref())?;
            let block_size = allocation_granularity(path.as_ref())?;
            file.try_lock_exclusive()?;
            let mut mmap = unsafe { MmapMut::map_mut(&file)? };
            let mut buf = &*mmap;
            let total_bytes = buf.remaining();
            // check the file header
            if buf.remaining() < <FileHeader as Pack>::const_len().unwrap() {
                bail!("invalid file header: too short")
            }
            let header = <FileHeader as Pack>::decode(&mut buf)
                .map_err(Error::from)
                .context("invalid file header")?;
            // this is the first version, so no upgrading can be done yet
            if header.version != FILE_VERSION {
                bail!("file version is too new, can't read it")
            }
            let mut path_by_id = HashMap::new();
            let mut id_by_path = HashMap::new();
            let mut batchmap = BTreeMap::new();
            let mut max_id = 0;
            let end = loop {
                let pos = total_bytes - buf.remaining();
                if buf.remaining() < <RecordHeader as Pack>::const_len().unwrap() {
                    warn!("file missing End marker");
                    break pos;
                }
                let rh = RecordHeader::decode(&mut buf)
                    .map_err(Error::from)
                    .context("invalid record header")?;
                if !rh.committed {
                    warn!("uncommitted records before end marker {}", pos);
                    break pos;
                }
                if buf.remaining() < rh.record_length as usize {
                    warn!("truncated record at {}", pos);
                    break pos;
                }
                match rh.record_type {
                    RecordTyp::End => break pos,
                    RecordTyp::Batch => {
                        if !batchmap.contains_key(&rh.timestamp) {
                            batchmap.insert(rh.timestamp, pos as u64);
                        }
                        buf.advance(rh.record_length as usize); // skip the contents
                    }
                    RecordTyp::PathMappings => {
                        let mut m = <Pooled<Vec<PathMapping>> as Pack>::decode(&mut buf)
                            .map_err(Error::from)
                            .context("invalid path mappings record")?;
                        for pm in m.drain(..) {
                            id_by_path.insert(pm.0.clone(), pm.1);
                            path_by_id.insert(pm.1, pm.0);
                            max_id = max(pm.1, max_id);
                        }
                    }
                }
            };
            Ok(Archive {
                pathmap,
                batchmap,
                file,
                mmap,
                block_size,
                end,
                uncommitted: end,
                next_id: max_id + 1,
            })
        }
    }

    // remap the file reserving space for at least additional_capacity bytes
    fn reserve(&mut self, additional_capacity: usize) -> Result<()> {
        let len = self.mmap.len();
        let new_len = len + max(len << 3, additional_capacity);
        let new_blocks = (new_len / self.block_size) + 1;
        let new_size = new_blocks * self.block_size;
        self.file.set_len(new_size as u64);
        Ok(mem::replace(&mut self.mmap, unsafe { MmapMut::map_mut(&self.file)? }))
    }

    pub(crate) fn flush(&mut self) -> Result<()> {
        if self.uncommitted < self.end {
            self.mmap.flush_range(self.uncommitted, self.end - self.uncommitted)?;
            let hl = <RecordHeader as Pack>::const_len().unwrap();
            let mut n = self.uncommitted;
            while n < self.end {
                let hr = <RecordHeader as Pack>::decode(&mut &self.mmap[n..])?;
                let len = hl + hr.record_length as usize;
                self.mmap[n] &= 0x7F;
                n += len;
            }
            self.mmap.flush_range(self.uncommitted, self.end - self.uncommitted)?;
            self.uncommitted = self.end;
        }
        Ok(())
    }

    pub(crate) fn add_paths(&mut self, paths: impl IntoIter<Item = &Path>) -> Result<()> {
        let mut pms = PM_POOL.take();
        for path in paths {
            if !self.id_by_path.contains_key(path) {
                let id = self.next_id;
                self.next_id += 1;
                self.id_by_path.insert(path.clone(), id);
                self.path_by_id.insert(id, path.clone());
                pms.push(PathMapping(path.clone(), id));
            }
        }
        if pms.len() > 0 {
            let record_length = <Pooled<Vec<PathMapping>> as Pack>::len(&pms);
            if record_length > u32::MAX {
                bail!("too many paths in one batch");
            }
            let len = <RecordHeader as Pack>::const_len().unwrap() + record_length;
            if self.mmap.len() - self.end < len {
                self.reserve(len)?;
            }
            let mut buf = &mut self.mmap[self.end..];
            let rh = RecordHeader {
                committed: false,
                record_type: RecordTyp::PathMappings,
                record_length: record_length as u32,
                timestamp: Utc::now(),
            };
            <RecordHeader as Pack>::encode(&rh, buf)?;
            <Pooled<Vec<PathMapping>> as Pack>::encode(&pms, buf)?;
            self.end += len;
        }
        Ok(())
    }

    pub(crate) fn id_for_path(&self, path: &Path) -> Option<u64> {
        self.id_by_path.get(path).copied()
    }

    pub(crate) fn path_for_id(&self, id: u64) -> Option<&Path> {
        self.path_by_id.get(&id)
    }
    
    pub(crate) fn add_batch(
        &mut self,
        items: impl IntoIter<Item = (u64, Value)>,
    ) -> Result<()> {
        let mut batch = BATCH_POOL.take();
        for (id, val) in items {
            if !self.path_by_id.contains_key(&id) {
                bail!("unknown subscription {}, register it first", id);
            }
            batch.push(BatchItem(id, val));
        }
    }
}

use anyhow::{Context, Error, Result};
use chrono::prelude::*;
use fs3::{allocation_granularity, FileExt};
use log::warn;
use mapr::MmapMut;
use bytes::{Buf, BufMut};
use netidx::{
    pack::{decode_varint, encode_varint, varint_len, Pack, PackError},
    path::Path,
    publisher::Value,
    pool::Pooled,
};
use std::{
    self,
    cmp::max,
    collections::{BTreeMap, HashMap},
    fs::{File, OpenOptions},
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

pub(crate) struct Archive {
    pathmap: HashMap<u64, Path>,
    batchmap: BTreeMap<DateTime<Utc>, u64>,
    file: File,
    mmap: MmapMut,
    block_size: u64,
    end: usize,
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
                &mut buf
            )?;
            mmap.flush()?;
            Ok(Archive {
                pathmap: HashMap::new(),
                batchmap: BTreeMap::new(),
                file,
                mmap,
                block_size,
                end: minsz
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
            let mut pathmap = HashMap::new();
            let mut batchmap = BTreeMap::new();
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
                            pathmap.insert(pm.1, pm.0);
                        }
                    }
                }
            };
            Ok(Archive { pathmap, batchmap, file, mmap, block_size, end })
        }
    }
}

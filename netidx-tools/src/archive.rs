use anyhow::{Context, Error, Result};
use bytes::{Buf, BufMut};
use chrono::prelude::*;
use fs3::{allocation_granularity, FileExt};
use log::warn;
use mapr::{Mmap, MmapMut};
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
    iter::{DoubleEndedIterator, IntoIterator},
    mem,
    ops::RangeBounds,
    path::Path as FilePath,
};

#[derive(Debug, Clone)]
pub struct FileHeader {
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

// This cannot be changed due to only 2 bits allocated to it in the
// header.
#[derive(PrimitiveEnum, Debug, Clone, Copy)]
pub enum RecordTyp {
    Timestamp = 0,
    PathMappings = 1,
    Batch = 2,
    End = 3,
}

const MAX_RECORD_LEN: u32 = 0x7FFFFFFF;
const MAX_TIMESTAMP: u32 = 0x03FFFFFF;

#[derive(PackedStruct, Debug, Clone)]
#[packed_struct(bit_numbering = "msb0", size_bytes = "8")]
pub struct RecordHeader {
    /// two stage commit flag
    #[packed_field(bits="0", size_bits = "1")]
    pub committed: bool,
    /// the record type
    #[packed_field(bits="1:2", size_bits = "2", ty="enum")]
    pub record_type: RecordTyp,
    /// batch length, up to 2GiB
    #[packed_field(bits="3:33", size_bits = "31", endian="msb")]
    pub record_length: u32,
    /// microsecond offset from last timestamp record
    #[packed_field(bits="34:63", size_bits = "30", endian="msb")]
    pub timestamp: u32,
}

impl Pack for RecordHeader {
    fn const_len() -> Option<usize> {
        Some(8)
    }

    fn len(&self) -> usize {
        <RecordHeader as Pack>::const_len().unwrap()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(buf.put_slice(&RecordHeader::pack(self)))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let mut v = [0u8; 8];
        buf.copy_to_slice(&mut v);
        Ok(RecordHeader::unpack(&v)?)
    }
}

#[derive(Debug, Clone)]
pub struct PathMapping(Path, u64);

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
pub struct BatchItem(u64, Value);

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

#[derive(Debug, Clone, Copy)]
struct MonotonicTimestamper(DateTime<Utc>);

impl MonotonicTimestamper {
    fn new() -> Self {
        MonotonicTimestamper(Utc::now())
    }

    fn now(&mut self) -> DateTime<Utc> {
        use chrono::Duration;
        let dt = Utc::now();
        if dt <= self.0 {
            self.0 = self.0 + Duration::nanoseconds(1);
            self.0
        } else {
            self.0 = dt;
            dt
        }
    }
}

#[derive(Debug)]
pub struct ReadOnly(Mmap);

#[derive(Debug)]
pub struct ReadWrite(MmapMut);

#[derive(Debug)]
pub struct Archive<T> {
    path_by_id: HashMap<u64, Path>,
    id_by_path: HashMap<Path, u64>,
    batchmap: BTreeMap<DateTime<Utc>, usize>,
    file: File,
    mmap: T,
    block_size: usize,
    end: usize,
    uncommitted: usize,
    next_id: u64,
    ts: MonotonicTimestamper,
}

impl Archive<T> {
    pub fn open_readwrite(path: impl AsRef<FilePath>) -> Result<Archive<ReadWrite>> {
        if mem::size_of::<usize>() < mem::size_of::<u64>() {
            bail!("the archiver cannot run on a < 64 bit platform")
        }
        let mut ts = MonotonicTimestamper::new();
        if !FilePath::is_file(path.as_ref()) {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path.as_ref())?;
            let block_size = allocation_granularity(path.as_ref())? as usize;
            file.try_lock_exclusive()?;
            let minsz = <FileHeader as Pack>::const_len().unwrap()
                + <RecordHeader as Pack>::const_len().unwrap();
            file.set_len(max(block_size, minsz) as u64)?;
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
                    timestamp: ts.now(),
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
                ts,
            })
        } else {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .append(true)
                .open(path.as_ref())?;
            let block_size = allocation_granularity(path.as_ref())? as usize;
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
                            batchmap.insert(rh.timestamp, pos);
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
                path_by_id,
                id_by_path,
                batchmap,
                file,
                mmap,
                block_size,
                end,
                uncommitted: end,
                next_id: max_id + 1,
                ts,
            })
        }
    }

    // remap the file reserving space for at least additional_capacity bytes
    fn reserve(&mut self, additional_capacity: usize) -> Result<()> {
        let len = self.mmap.len();
        let new_len = len + max(len << 4, additional_capacity);
        let new_blocks = (new_len / self.block_size as usize) + 1;
        let new_size = new_blocks * self.block_size as usize;
        self.file.set_len(new_size as u64);
        Ok(drop(mem::replace(&mut self.mmap, unsafe { MmapMut::map_mut(&self.file)? })))
    }

    fn check_reserve(&mut self, record_length: usize) -> Result<usize> {
        if record_length > u32::MAX as usize {
            bail!("record too large");
        }
        let len = <RecordHeader as Pack>::const_len().unwrap() + record_length;
        if self.mmap.len() - self.end < len {
            self.reserve(len)?;
        }
        Ok(len)
    }

    pub(crate) fn flush(&mut self) -> Result<()> {
        if self.uncommitted < self.end {
            let hl = <RecordHeader as Pack>::const_len().unwrap();
            if self.mmap.len() - self.end < hl {
                self.reserve(hl)?;
            }
            self.mmap.flush()?;
            let mut n = self.uncommitted;
            while n < self.end {
                let hr = <RecordHeader as Pack>::decode(&mut &self.mmap[n..])?;
                let len = hl + hr.record_length as usize;
                self.mmap[n] &= 0x7F;
                n += len;
            }
            let end = RecordHeader {
                committed: true,
                record_type: RecordTyp::End,
                record_length: 0,
                timestamp: self.ts.now(),
            };
            <RecordHeader as Pack>::encode(&end, &mut &mut self.mmap[self.end..])?;
            self.mmap.flush()?;
            self.uncommitted = self.end;
        }
        Ok(())
    }

    pub(crate) fn add_paths<'a>(
        &'a mut self,
        paths: impl IntoIterator<Item = &'a Path>,
    ) -> Result<()> {
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
            let len = self.check_reserve(record_length)?;
            let mut buf = &mut self.mmap[self.end..];
            let rh = RecordHeader {
                committed: false,
                record_type: RecordTyp::PathMappings,
                record_length: record_length as u32,
                timestamp: self.ts.now(),
            };
            <RecordHeader as Pack>::encode(&rh, &mut buf)?;
            <Pooled<Vec<PathMapping>> as Pack>::encode(&pms, &mut buf)?;
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
        items: impl IntoIterator<Item = (u64, Value)>,
    ) -> Result<()> {
        let mut batch = BATCH_POOL.take();
        for (id, val) in items {
            if !self.path_by_id.contains_key(&id) {
                bail!("unknown subscription {}, register it first", id);
            }
            batch.push(BatchItem(id, val));
        }
        if batch.len() > 0 {
            let record_length = <Pooled<Vec<BatchItem>> as Pack>::len(&batch);
            let len = self.check_reserve(record_length)?;
            let mut buf = &mut self.mmap[self.end..];
            let timestamp = self.ts.now();
            let rh = RecordHeader {
                committed: false,
                record_type: RecordTyp::Batch,
                record_length: record_length as u32,
                timestamp,
            };
            <RecordHeader as Pack>::encode(&rh, &mut buf)?;
            <Pooled<Vec<BatchItem>> as Pack>::encode(&batch, &mut buf)?;
            self.batchmap.insert(timestamp, self.end);
            self.end += len;
        }
        Ok(())
    }

    fn get_batch_at(&self, pos: usize) -> Result<(RecordHeader, Pooled<Vec<BatchItem>>)> {
        if pos > self.end {
            bail!("get_batch: index {} out of bounds", pos);
        }
        let mut buf = &self.mmap[pos..];
        let rh = <RecordHeader as Pack>::decode(&mut buf)?;
        if pos + rh.record_length as usize > self.end {
            bail!("get_batch: error truncated record at {}", pos);
        }
        let batch = <Pooled<Vec<BatchItem>> as Pack>::decode(&mut buf)?;
        Ok((rh, batch))
    }

    /// return an iterator over batches within the specified date
    /// range.
    pub(crate) fn range<'a, R>(
        &'a self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = Result<(RecordHeader, Pooled<Vec<BatchItem>>)>> + 'a
    where
        R: RangeBounds<DateTime<Utc>>,
    {
        self.batchmap.range(range).map(move |(_, p)| self.get_batch_at(*p))
    }
}

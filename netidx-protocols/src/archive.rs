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
use packed_struct::PackedStruct;
use std::{
    self,
    cmp::max,
    collections::{BTreeMap, HashMap},
    fs::{File, OpenOptions},
    iter::{DoubleEndedIterator, IntoIterator},
    mem,
    ops::{Deref, DerefMut, RangeBounds},
    path::Path as FilePath,
};

#[derive(Debug, Clone)]
pub struct FileHeader {
    version: u32,
}

static FILE_MAGIC: &'static [u8] = b"netidx archive";
const FILE_VERSION: u32 = 0;

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

#[derive(PrimitiveEnum, Debug, Clone, Copy)]
enum RecordTyp {
    /// A time basis record
    Timestamp = 0,
    /// A record mapping paths to ids
    PathMappings = 1,
    /// A data batch
    Batch = 2,
    /// End of archive marker
    End = 3,
}

const MAX_RECORD_LEN: u32 = 0x7FFFFFFF;
const MAX_TIMESTAMP: u32 = 0x03FFFFFF;

// Every record in the archive starts with this header
#[derive(PackedStruct, Debug, Clone)]
#[packed_struct(bit_numbering = "msb0", size_bytes = "8")]
pub struct RecordHeader {
    // two stage commit flag
    #[packed_field(bits = "0", size_bits = "1")]
    committed: bool,
    // the record type
    #[packed_field(bits = "1:2", size_bits = "2", ty = "enum")]
    record_type: RecordTyp,
    // the record length, up to MAX_RECORD_LEN, not including this header
    #[packed_field(bits = "3:33", size_bits = "31", endian = "msb")]
    record_length: u32,
    // microsecond offset from last timestamp record, up to MAX_TIMESTAMP
    #[packed_field(bits = "34:63", size_bits = "30", endian = "msb")]
    timestamp: u32,
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
        RecordHeader::unpack(&v).map_err(|_| PackError::InvalidFormat)
    }
}

#[derive(Debug, Clone)]
struct PathMapping(Path, u64);

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
pub struct BatchItem(pub u64, pub Value);

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
enum MonoTs {
    NewBasis(DateTime<Utc>),
    Offset(DateTime<Utc>, u32),
}

#[derive(Debug, Clone, Copy)]
struct MonotonicTimestamper {
    basis: Option<DateTime<Utc>>,
    offset: u32,
}

impl MonotonicTimestamper {
    fn new() -> Self {
        MonotonicTimestamper { basis: None, offset: 0 }
    }

    fn timestamp(&mut self) -> MonoTs {
        use chrono::Duration;
        let now = Utc::now();
        match self.basis {
            None => {
                self.basis = Some(now);
                self.offset = 0;
                MonoTs::NewBasis(now)
            }
            Some(basis) => match (now - basis).num_microseconds() {
                Some(off) if off <= 0 => {
                    if self.offset < MAX_TIMESTAMP {
                        self.offset += 1;
                        MonoTs::Offset(basis, self.offset)
                    } else {
                        let basis = basis + Duration::microseconds(1);
                        self.basis = Some(basis);
                        self.offset = 0;
                        MonoTs::NewBasis(basis)
                    }
                }
                Some(off) if self.offset as i64 + off <= MAX_TIMESTAMP as i64 => {
                    self.offset += off as u32;
                    MonoTs::Offset(basis, self.offset)
                }
                None | Some(_) => {
                    self.basis = Some(now);
                    self.offset = 0;
                    MonoTs::NewBasis(now)
                }
            },
        }
    }
}

/// The type associated with a read only archive
#[derive(Debug)]
pub struct ReadOnly(Mmap);

impl Deref for ReadOnly {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

/// The type associated with a read write archive
#[derive(Debug)]
pub struct ReadWrite(MmapMut);

impl Deref for ReadWrite {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl DerefMut for ReadWrite {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.deref_mut()
    }
}

/// This reads and writes the netidx archive format (as written by the
/// "record" command in the tools). The archive format is intended to be
/// a compact format for storing recordings of netidx data for long
/// term storage and access. It uses memory mapped IO for performance
/// and memory efficiency, and as such file size is limited to
/// `usize`, which on most platforms is not an issue.
///
/// Files begin with a file header, which consists of the string
/// "netidx archive" followed by the file format
/// version. Currently there is 1 version, and the version number
/// is 0.
///
/// Following the header are a series of records. Every record begins
/// with a (RecordHeader)[RecordHeader], which is followed by a data
/// item, except in the case of the end of archive record, which is
/// not followed by a data item.
///
/// Items are written to the file using a two phase commit scheme to
/// allow detection of partially committed. Initially, items are
/// marked as uncommitted, and only upon a successful flush to disk
/// are they then marked as committed.
///
/// When an archive is opened, an index of it's contents is built in
/// memory so that any part of it can be accessed quickly by
/// timestamp. As a result, there is some memory overhead.
///
/// To prevent data corruption the underling file is locked for
/// exclusive access using the advisory file locking mechanism present
/// in the OS (e.g. flock on unix). If the file is modified
/// independantly of advisory locking it should not cause UB, but it
/// could cause data corruption, or read errors.
///
/// The record header is 8 bytes. A data record starts with a LEB128
/// encoded item counter, and then a number of items. Path ids are
/// also LEB128 encoded. So, for example, in an archive containing 1
/// path, a batch with 1 u64 data item would look like.
///    
/// ```
/// 8 byte header
/// 1 byte item count
/// 1 byte path id
/// 1 byte type tag
/// 8 byte u64
/// ----------------
/// 19 bytes (11 bytes of overhead 57%)
/// ```
///
/// Better overheads can be achieved with larger batches, as should
/// naturally happen on busier systems. For example a batch of 128
/// u64s looks like.
///
/// ```
/// 8 byte header
/// 1 byte item count
/// (1 byte path id
///  1 byte type tag
///  8 byte u64) * 128
/// ---------------------
/// 1289 bytes (264 bytes of overhead 20%)
/// ```
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

impl<T> Archive<T> {
    fn scan(path: &FilePath, file: File) -> Result<Archive<ReadOnly>> {
        let block_size = allocation_granularity(path)? as usize;
        file.try_lock_exclusive()?;
        let mmap = ReadOnly(unsafe { Mmap::map(&file)? });
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
        let mut time_basis = chrono::MIN_DATETIME;
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
                RecordTyp::Timestamp => {
                    time_basis = <DateTime<Utc> as Pack>::decode(&mut buf)?;
                }
                RecordTyp::Batch => {
                    use chrono::Duration;
                    let timestamp =
                        time_basis + Duration::microseconds(rh.timestamp as i64);
                    batchmap.insert(timestamp, pos);
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
            ts: MonotonicTimestamper::new(),
        })
    }

    /// Open the specified archive read only
    pub fn open_readonly(path: impl AsRef<FilePath>) -> Result<Archive<ReadOnly>> {
        let file = OpenOptions::new().read(true).open(path.as_ref())?;
        file.try_lock_exclusive()?;
        Archive::<ReadOnly>::scan(path.as_ref(), file)
    }

    /// Open the specified archive for read/write access, if the file
    /// does not exist then an new archive will be created.
    pub fn open_readwrite(path: impl AsRef<FilePath>) -> Result<Archive<ReadWrite>> {
        if mem::size_of::<usize>() < mem::size_of::<u64>() {
            warn!("archive file size is limited to 4 GiB on this platform")
        }
        if FilePath::is_file(path.as_ref()) {
            let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
            file.try_lock_exclusive()?;
            let t = Archive::<ReadOnly>::scan(path.as_ref(), file)?;
            let mmap = ReadWrite(unsafe { MmapMut::map_mut(&t.file)? });
            Ok(Archive {
                path_by_id: t.path_by_id,
                id_by_path: t.id_by_path,
                batchmap: t.batchmap,
                file: t.file,
                mmap,
                block_size: t.block_size,
                end: t.end,
                uncommitted: t.uncommitted,
                next_id: t.next_id,
                ts: t.ts,
            })
        } else {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path.as_ref())?;
            file.try_lock_exclusive()?;
            let block_size = allocation_granularity(path.as_ref())? as usize;
            let fh_len = <FileHeader as Pack>::const_len().unwrap();
            let rh_len = <RecordHeader as Pack>::const_len().unwrap();
            file.set_len(max(block_size, fh_len + rh_len) as u64)?;
            let mut mmap = ReadWrite(unsafe { MmapMut::map_mut(&file)? });
            let mut buf = &mut *mmap;
            let fh = FileHeader { version: FILE_VERSION };
            <FileHeader as Pack>::encode(&fh, &mut buf)?;
            let rh = RecordHeader {
                committed: true,
                record_type: RecordTyp::End,
                record_length: 0,
                timestamp: 0,
            };
            <RecordHeader as Pack>::encode(&rh, &mut buf)?;
            mmap.0.flush()?;
            Ok(Archive {
                path_by_id: HashMap::new(),
                id_by_path: HashMap::new(),
                batchmap: BTreeMap::new(),
                file,
                mmap,
                block_size,
                end: fh_len,
                uncommitted: fh_len,
                next_id: 0,
                ts: MonotonicTimestamper::new(),
            })
        }
    }
}

impl Archive<ReadWrite> {
    // remap the file reserving space for at least additional_capacity bytes
    fn reserve(&mut self, additional_capacity: usize) -> Result<()> {
        let len = self.mmap.len();
        let new_len = len + max(len << 4, additional_capacity);
        let new_blocks = (new_len / self.block_size as usize) + 1;
        let new_size = new_blocks * self.block_size as usize;
        self.file.set_len(new_size as u64)?;
        Ok(drop(mem::replace(
            &mut self.mmap,
            ReadWrite(unsafe { MmapMut::map_mut(&self.file)? }),
        )))
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

    /// flush uncommitted changes to disk, mark all flushed records as
    /// committed, and update the end of archive marker. Does nothing
    /// if everything is already committed.
    pub fn flush(&mut self) -> Result<()> {
        if self.uncommitted < self.end {
            let hl = <RecordHeader as Pack>::const_len().unwrap();
            if self.mmap.len() - self.end < hl {
                self.reserve(hl)?;
            }
            self.mmap.0.flush()?;
            let mut n = self.uncommitted;
            while n < self.end {
                let mut hr = <RecordHeader as Pack>::decode(&mut &self.mmap[n..])?;
                hr.committed = true;
                <RecordHeader as Pack>::encode(&hr, &mut &mut self.mmap[n..])?;
                n += hl + hr.record_length as usize;
            }
            assert_eq!(n, self.end);
            let end = RecordHeader {
                committed: true,
                record_type: RecordTyp::End,
                record_length: 0,
                timestamp: 0,
            };
            <RecordHeader as Pack>::encode(&end, &mut &mut self.mmap[self.end..])?;
            self.mmap.0.flush()?;
            self.uncommitted = self.end;
        }
        Ok(())
    }

    /// allocate path ids for any of the specified paths that don't
    /// already have one, and if any ids were allocated then write a
    /// path mappings record containing the new assignments.
    pub fn add_paths<'a>(
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
                timestamp: 0,
            };
            <RecordHeader as Pack>::encode(&rh, &mut buf)?;
            <Pooled<Vec<PathMapping>> as Pack>::encode(&pms, &mut buf)?;
            self.end += len;
        }
        Ok(())
    }

    /// Add a data batch to the archive. This method will fail if any
    /// of the path ids in the batch are unknown.
    pub fn add_batch(
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
            if record_length > MAX_RECORD_LEN as usize {
                bail!("batch is too large")
            }
            let (time_basis, time_offset) = match self.ts.timestamp() {
                MonoTs::Offset(basis, off) => (basis, off),
                MonoTs::NewBasis(basis) => {
                    let record_length = <DateTime<Utc> as Pack>::len(&basis);
                    let rh = RecordHeader {
                        committed: false,
                        record_type: RecordTyp::Timestamp,
                        record_length: record_length as u32,
                        timestamp: 0,
                    };
                    let len = self.check_reserve(record_length)?;
                    let mut buf = &mut self.mmap[self.end..];
                    <RecordHeader as Pack>::encode(&rh, &mut buf)?;
                    <DateTime<Utc> as Pack>::encode(&basis, &mut buf)?;
                    self.end += len;
                    (basis, 0)
                }
            };
            let len = self.check_reserve(record_length)?;
            let mut buf = &mut self.mmap[self.end..];
            let rh = RecordHeader {
                committed: false,
                record_type: RecordTyp::Batch,
                record_length: record_length as u32,
                timestamp: time_offset,
            };
            <RecordHeader as Pack>::encode(&rh, &mut buf)?;
            <Pooled<Vec<BatchItem>> as Pack>::encode(&batch, &mut buf)?;
            let timestamp =
                time_basis + chrono::Duration::microseconds(time_offset as i64);
            self.batchmap.insert(timestamp, self.end);
            self.end += len;
        }
        Ok(())
    }
}

impl<T: Deref<Target = [u8]>> Archive<T> {
    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    pub fn id_for_path(&self, path: &Path) -> Option<u64> {
        self.id_by_path.get(path).copied()
    }

    pub fn path_for_id(&self, id: u64) -> Option<&Path> {
        self.path_by_id.get(&id)
    }

    fn get_batch_at(&self, pos: usize) -> Result<Pooled<Vec<BatchItem>>> {
        if pos > self.end {
            bail!("get_batch: index {} out of bounds", pos);
        }
        let mut buf = &self.mmap[pos..];
        let rh = <RecordHeader as Pack>::decode(&mut buf)?;
        if pos + rh.record_length as usize > self.end {
            bail!("get_batch: error truncated record at {}", pos);
        }
        Ok(<Pooled<Vec<BatchItem>> as Pack>::decode(&mut buf)?)
    }

    /// return an iterator over batches within the specified time
    /// range.
    pub fn range<'a, R>(
        &'a self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = Result<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>> + 'a
    where
        R: RangeBounds<DateTime<Utc>>,
    {
        self.batchmap.range(range).map(move |(ts, p)| Ok((*ts, self.get_batch_at(*p)?)))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{
        fs,
        ops::Bound::{self, *},
    };

    fn check_contents<T: Deref<Target = [u8]>>(
        t: &Archive<T>,
        paths: &[Path],
        batches: usize,
    ) {
        let mut batch = t
            .range::<(Bound<DateTime<Utc>>, Bound<DateTime<Utc>>)>((Unbounded, Unbounded))
            .collect::<Vec<_>>();
        assert_eq!(batch.len(), batches);
        let now = Utc::now();
        for r in batch.drain(..) {
            let (ts, b) = r.unwrap();
            let elapsed = (now - ts).num_seconds();
            assert!(elapsed <= 10 && elapsed >= -10);
            assert_eq!(Vec::len(&b), paths.len());
            for (BatchItem(id, v), p) in b.iter().zip(paths.iter()) {
                assert_eq!(Some(p), t.path_for_id(*id));
                assert_eq!(v, &Value::U64(42))
            }
        }
    }

    #[test]
    fn basic_test() {
        let file = FilePath::new("test-data");
        let paths = [Path::from("/foo/bar"), Path::from("/foo/baz")];
        if FilePath::is_file(&file) {
            fs::remove_file(file).unwrap();
        }
        let initial_size = { // check that we can open, and write an archive
            let mut t = Archive::<ReadWrite>::open_readwrite(&file).unwrap();
            t.add_paths(&paths).unwrap();
            let ids = paths.iter().map(|p| t.id_for_path(p).unwrap()).collect::<Vec<_>>();
            t.add_batch(ids.iter().map(|id| (*id, Value::U64(42)))).unwrap();
            t.flush().unwrap();
            check_contents(&t, &paths, 1);
            t.len()
        };
        { // check that we can close, reopen, and read the archive back
            let t = Archive::<ReadOnly>::open_readonly(file).unwrap();
            check_contents(&t, &paths, 1);
        }
        { // check that we can reopen, and add to an archive
            let mut t = Archive::<ReadWrite>::open_readwrite(&file).unwrap();
            let ids = paths.iter().map(|p| t.id_for_path(p).unwrap()).collect::<Vec<_>>();
            t.add_batch(ids.iter().map(|id| (*id, Value::U64(42)))).unwrap();
            t.flush().unwrap();
            check_contents(&t, &paths, 2);
        }
        { // check that we can reopen, and read the archive we added to
            let t = Archive::<ReadOnly>::open_readonly(&file).unwrap();
            check_contents(&t, &paths, 2);
        }
        let n = { // check that we can grow the archive by remapping it on the fly
            let mut t = Archive::<ReadWrite>::open_readwrite(&file).unwrap();
            let mut n = 2;
            while t.len() == initial_size {
                let ids =
                    paths.iter().map(|p| t.id_for_path(p).unwrap()).collect::<Vec<_>>();
                t.add_batch(ids.iter().map(|id| (*id, Value::U64(42)))).unwrap();
                n += 1;
                check_contents(&t, &paths, n);
            }
            t.flush().unwrap();
            check_contents(&t, &paths, n);
            n
        };
        { // check that we can reopen, and read the archive we grew
            let t = Archive::<ReadOnly>::open_readonly(&file).unwrap();
            check_contents(&t, &paths, n);
        }
        { // check that we can't open the archive twice
            let t = Archive::<ReadOnly>::open_readonly(&file).unwrap();
            check_contents(&t, &paths, n);
            assert!(Archive::<ReadOnly>::open_readonly(&file).is_err());
        }
    }
}

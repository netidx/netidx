use anyhow::{Context, Error, Result};
use bytes::{Buf, BufMut};
use chrono::prelude::*;
use fs3::{allocation_granularity, FileExt};
use indexmap::IndexMap;
use log::warn;
use mapr::{Mmap, MmapMut};
use netidx::{
    chars::Chars,
    pack::{decode_varint, encode_varint, varint_len, Pack, PackError},
    path::Path,
    pool::{Pool, Pooled},
    subscriber::{Event, FromValue, Value},
};
use packed_struct::PackedStruct;
use parking_lot::{
    lock_api::{RwLockUpgradableReadGuard, RwLockWriteGuard},
    RwLock,
};
use std::{
    self,
    cmp::max,
    collections::{BTreeMap, HashMap, VecDeque},
    error, fmt,
    fs::{File, OpenOptions},
    iter::IntoIterator,
    mem,
    ops::{Bound, Deref, DerefMut, RangeBounds},
    path::Path as FilePath,
    str::FromStr,
    sync::Arc,
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
    /// A data batch containing deltas from the previous batch
    DeltaBatch = 2,
    /// A data batch containing a full image
    ImageBatch = 3,
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

#[derive(Debug, Clone, Copy)]
pub enum Seek {
    Absolute(DateTime<Utc>),
    BatchRelative(i8),
    TimeRelative(chrono::Duration),
}

impl FromStr for Seek {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        use diligent_date_parser::parse_date;
        let s = s.trim();
        if let Ok(steps) = s.parse::<i8>() {
            Ok(Seek::BatchRelative(steps))
        } else if let Some(dt) = parse_date(s) {
            Ok(Seek::Absolute(dt.with_timezone(&Utc)))
        } else if s.starts_with(['+', '-'].as_ref())
            && s.ends_with(['y', 'M', 'd', 'h', 'm', 's'].as_ref())
            && s.is_ascii()
            && s.len() > 2
        {
            let dir = s.chars().next().unwrap();
            let mag = s.chars().next_back().unwrap();
            match s[1..s.len() - 1].parse::<f64>() {
                Err(_) => bail!("invalid position expression"),
                Ok(quantity) => {
                    let quantity = if mag == 'y' {
                        quantity * 365.24 * 86400.
                    } else if mag == 'M' {
                        quantity * (365.24 / 12.) * 86400.
                    } else if mag == 'd' {
                        quantity * 86400.
                    } else if mag == 'h' {
                        quantity * 3600.
                    } else if mag == 'm' {
                        quantity * 60.
                    } else {
                        quantity
                    };
                    let offset = chrono::Duration::nanoseconds(if dir == '+' {
                        (quantity * 1e9).trunc() as i64
                    } else {
                        (-1. * quantity * 1e9).trunc() as i64
                    });
                    if dir == '+' {
                        Ok(Seek::TimeRelative(offset))
                    } else {
                        Ok(Seek::TimeRelative(offset))
                    }
                }
            }
        } else {
            bail!("{} is not a valid seek expression", s)
        }
    }
}

impl FromValue for Seek {
    type Error = Error;

    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::DateTime(ts) => Ok(Seek::Absolute(ts)),
            v if v.is_number() => Ok(Seek::BatchRelative(v.cast_to::<i8>()?)),
            v => v.cast_to::<Chars>()?.parse::<Seek>(),
        }
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
pub struct BatchItem(pub u64, pub Event);

impl Pack for BatchItem {
    fn len(&self) -> usize {
        varint_len(self.0) + Pack::len(&self.1)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        encode_varint(self.0, buf);
        Pack::encode(&self.1, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let id = decode_varint(buf)?;
        Ok(BatchItem(id, <Event as Pack>::decode(buf)?))
    }
}

lazy_static! {
    static ref PM_POOL: Pool<Vec<PathMapping>> = Pool::new(10, 100000);
    pub static ref BATCH_POOL: Pool<Vec<BatchItem>> = Pool::new(100, 100000);
    static ref CURSOR_BATCH_POOL: Pool<VecDeque<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>> =
        Pool::new(100, 100000);
    static ref POS_POOL: Pool<Vec<(DateTime<Utc>, usize)>> = Pool::new(10, 100000);
    static ref IDX_POOL: Pool<Vec<(u64, Path)>> = Pool::new(10, 20_000_000);
    static ref IMG_POOL: Pool<HashMap<u64, Event>> = Pool::new(10, 20_000_000);
    static ref EPSILON: chrono::Duration = chrono::Duration::microseconds(1);
}

#[derive(Debug, Clone, Copy)]
pub enum Timestamp {
    NewBasis(DateTime<Utc>),
    Offset(DateTime<Utc>, u32),
}

impl Timestamp {
    pub fn datetime(&self) -> DateTime<Utc> {
        match self {
            Timestamp::NewBasis(ts) => *ts,
            Timestamp::Offset(ts, off) => {
                *ts + chrono::Duration::microseconds(*off as i64)
            }
        }
    }

    pub fn offset(&self) -> u32 {
        match self {
            Timestamp::NewBasis(_) => 0,
            Timestamp::Offset(_, off) => *off,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MonotonicTimestamper {
    basis: Option<DateTime<Utc>>,
    offset: u32,
}

impl MonotonicTimestamper {
    pub fn new() -> Self {
        MonotonicTimestamper { basis: None, offset: 0 }
    }

    fn update_basis(&mut self, new_basis: DateTime<Utc>) -> DateTime<Utc> {
        use chrono::Duration;
        match self.basis {
            None => {
                self.basis = Some(new_basis);
                self.offset = 0;
                new_basis
            }
            Some(old_basis) => {
                let old_ts = old_basis + Duration::microseconds(self.offset as i64);
                if old_ts > new_basis {
                    self.basis = Some(old_ts);
                    self.offset = 0;
                    old_ts
                } else {
                    self.basis = Some(new_basis);
                    self.offset = 0;
                    new_basis
                }
            }
        }
    }

    pub fn timestamp(&mut self) -> Timestamp {
        use chrono::Duration;
        let now = Utc::now();
        match self.basis {
            None => Timestamp::NewBasis(self.update_basis(now)),
            Some(basis) => match (now - basis).num_microseconds() {
                Some(off) if off <= 0 => {
                    if self.offset < MAX_TIMESTAMP {
                        self.offset += 1;
                        Timestamp::Offset(basis, self.offset)
                    } else {
                        let basis = self.update_basis(basis + Duration::microseconds(1));
                        Timestamp::NewBasis(basis)
                    }
                }
                Some(off) if (self.offset as i64 + off) <= MAX_TIMESTAMP as i64 => {
                    self.offset += off as u32;
                    Timestamp::Offset(basis, self.offset)
                }
                None | Some(_) => Timestamp::NewBasis(self.update_basis(now)),
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Cursor {
    start: Bound<DateTime<Utc>>,
    end: Bound<DateTime<Utc>>,
    current: Option<DateTime<Utc>>,
}

impl Cursor {
    pub fn new() -> Self {
        Cursor { start: Bound::Unbounded, end: Bound::Unbounded, current: None }
    }

    pub fn reset(&mut self) {
        self.current = None;
    }

    /// Move the current to the specified position in the archive. If
    /// `pos` is outside the bounds of the cursor, then set the
    /// current to the closest value that is in bounds.
    pub fn set_current(&mut self, pos: DateTime<Utc>) {
        if (self.start, self.end).contains(&pos) {
            self.current = Some(pos);
        } else {
            match (self.start, self.end) {
                (Bound::Unbounded, Bound::Unbounded) => unreachable!(),
                (Bound::Unbounded, Bound::Included(ts)) => {
                    self.current = Some(ts);
                }
                (Bound::Unbounded, Bound::Excluded(ts)) => {
                    self.current = Some(ts - *EPSILON);
                }
                (Bound::Included(ts), Bound::Unbounded) => {
                    self.current = Some(ts);
                }
                (Bound::Excluded(ts), Bound::Unbounded) => {
                    self.current = Some(ts + *EPSILON);
                }
                (Bound::Included(start), Bound::Included(end)) => {
                    if pos < start {
                        self.current = Some(start);
                    } else {
                        self.current = Some(end);
                    }
                }
                (Bound::Excluded(start), Bound::Excluded(end)) => {
                    if pos <= start {
                        self.current = Some(start + *EPSILON);
                    } else {
                        self.current = Some(end - *EPSILON);
                    }
                }
                (Bound::Excluded(start), Bound::Included(end)) => {
                    if pos <= start {
                        self.current = Some(start + *EPSILON);
                    } else {
                        self.current = Some(end);
                    }
                }
                (Bound::Included(start), Bound::Excluded(end)) => {
                    if pos < start {
                        self.current = Some(start);
                    } else {
                        self.current = Some(end - *EPSILON);
                    }
                }
            }
        }
    }

    pub fn start(&self) -> Bound<DateTime<Utc>> {
        self.start
    }

    pub fn end(&self) -> Bound<DateTime<Utc>> {
        self.end
    }

    pub fn current(&self) -> Option<DateTime<Utc>> {
        self.current
    }

    pub fn contains(&self, ts: &DateTime<Utc>) -> bool {
        (self.start, self.end).contains(ts)
    }

    fn maybe_reset(&mut self) {
        if let Some(ref current) = self.current {
            if !(self.start, self.end).contains(current) {
                self.current = None;
            }
        }
    }

    pub fn set_start(&mut self, start: Bound<DateTime<Utc>>) {
        self.start = start;
        self.maybe_reset();
    }

    pub fn set_end(&mut self, end: Bound<DateTime<Utc>>) {
        self.end = end;
        self.maybe_reset();
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

/// This error will be raised if you try to write a record that is too
/// large to represent in 31 bits to the file. Nothing will be written
/// in that case, so you can just split the record and try again.
#[derive(Debug, Clone, Copy)]
pub struct RecordTooLarge;

impl fmt::Display for RecordTooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl error::Error for RecordTooLarge {}

/// This error will be raised on an attempt to read a record that is
/// beyond the end of the current memory map. In that case you must
/// remap the file in order to read that record.
#[derive(Debug, Clone, Copy)]
pub struct RemapRequired;

impl fmt::Display for RemapRequired {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl error::Error for RemapRequired {}

#[derive(Debug)]
struct ArchiveInner {
    path_by_id: IndexMap<u64, Path>,
    id_by_path: HashMap<Path, u64>,
    imagemap: BTreeMap<DateTime<Utc>, usize>,
    deltamap: BTreeMap<DateTime<Utc>, usize>,
    file: File,
    block_size: usize,
    end: usize,
    uncommitted: usize,
    next_id: u64,
}

/// This reads and writes the netidx archive format (as written by the
/// "record" command in the tools). The archive format is intended to
/// be a compact format for storing recordings of netidx data for long
/// term storage and access. It uses memory mapped IO for performance
/// and memory efficiency, and as such file size is limited to
/// `usize`, which on 64 bit platforms should not be an issue.
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
/// allow detection of possibly corrupted data. Initially, items are
/// marked as uncommitted, and only upon a successful flush to disk
/// are they then marked as committed.
///
/// When an archive is opened, an index of it's contents is built in
/// memory so that any part of it can be accessed quickly by
/// timestamp. As a result, there is some memory overhead.
///
/// In order to facilitate full reconstruction of the state at any
/// point without requiring to decode the entire file up to that point
/// there are two types of data records, image records contain the
/// entire state of every archived value, and delta records contain
/// only values that changed since the last delta record. The full
/// state of the values can be constructed at a given time `t` by
/// seeking to the nearest image record that is before `t`, and then
/// processing all the delta records up to `t`.
///
/// Because data sets vary in requirements and size the writing of
/// image records is configurable in the archiver (e.g. write 1 image
/// per 512 MiB of deltas), and it is not required to write any image
/// records, however this will mean that reconstructing the state at
/// any point will require processing the entire file before that
/// point.
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
/// 8 byte header
/// 1 byte item count
/// 1 byte path id
/// 1 byte type tag
/// 8 byte u64
/// ----------------
/// 19 bytes (11 bytes of overhead 57%)
///
/// Better overheads can be achieved with larger batches, as should
/// naturally happen on busier systems. For example a batch of 128
/// u64s looks like.
///
/// 8 byte header
/// 1 byte item count
/// (1 byte path id
///  1 byte type tag
///  8 byte u64) * 128
/// ---------------------
/// 1289 bytes (264 bytes of overhead 20%)
#[derive(Debug)]
pub struct Archive<T> {
    inner: Arc<RwLock<ArchiveInner>>,
    mmap: T,
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
        let mut path_by_id = IndexMap::new();
        let mut id_by_path = HashMap::new();
        let mut imagemap = BTreeMap::new();
        let mut deltamap = BTreeMap::new();
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
                // End of archive is marked by an uncommitted record with length 0
                if rh.record_length != 0 {
                    warn!("uncommitted records before end marker {}", pos);
                }
                break pos;
            }
            if buf.remaining() < rh.record_length as usize {
                warn!("truncated record at {}", pos);
                break pos;
            }
            use chrono::Duration;
            match rh.record_type {
                RecordTyp::DeltaBatch => {
                    let timestamp =
                        time_basis + Duration::microseconds(rh.timestamp as i64);
                    deltamap.insert(timestamp, pos);
                    buf.advance(rh.record_length as usize); // skip the contents
                }
                RecordTyp::Timestamp => {
                    time_basis = <DateTime<Utc> as Pack>::decode(&mut buf)?;
                }
                RecordTyp::ImageBatch => {
                    let timestamp =
                        time_basis + Duration::microseconds(rh.timestamp as i64);
                    imagemap.insert(timestamp, pos);
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
            inner: Arc::new(RwLock::new(ArchiveInner {
                path_by_id,
                id_by_path,
                deltamap,
                imagemap,
                file,
                block_size,
                end,
                uncommitted: end,
                next_id: max_id + 1,
            })),
            mmap,
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
            let mmap = ReadWrite(unsafe { MmapMut::map_mut(&t.inner.read().file)? });
            Ok(Archive { inner: t.inner, mmap })
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
                committed: false,
                record_type: RecordTyp::DeltaBatch,
                record_length: 0,
                timestamp: 0,
            };
            <RecordHeader as Pack>::encode(&rh, &mut buf)?;
            mmap.0.flush()?;
            Ok(Archive {
                inner: Arc::new(RwLock::new(ArchiveInner {
                    path_by_id: IndexMap::new(),
                    id_by_path: HashMap::new(),
                    deltamap: BTreeMap::new(),
                    imagemap: BTreeMap::new(),
                    file,
                    block_size,
                    end: fh_len,
                    uncommitted: fh_len,
                    next_id: 0,
                })),
                mmap,
            })
        }
    }
}

// remap the file reserving space for at least additional_capacity bytes
fn reserve(
    mmap: &mut ReadWrite,
    inner: &ArchiveInner,
    additional_capacity: usize,
) -> Result<()> {
    let len = mmap.len();
    let new_len = len + max(len >> 6, additional_capacity);
    let new_blocks = (new_len / inner.block_size as usize) + 1;
    let new_size = new_blocks * inner.block_size as usize;
    inner.file.set_len(new_size as u64)?;
    Ok(drop(mem::replace(mmap, ReadWrite(unsafe { MmapMut::map_mut(&inner.file)? }))))
}

fn check_reserve(
    mmap: &mut ReadWrite,
    inner: &ArchiveInner,
    record_length: usize,
) -> Result<usize> {
    if record_length > MAX_RECORD_LEN as usize {
        bail!(RecordTooLarge);
    }
    let len = <RecordHeader as Pack>::const_len().unwrap() + record_length;
    if mmap.len() - inner.end < len {
        reserve(mmap, inner, len)?;
    }
    Ok(len)
}

impl Archive<ReadWrite> {
    /// flush uncommitted changes to disk, mark all flushed records as
    /// committed, and update the end of archive marker. Does nothing
    /// if everything is already committed.
    pub fn flush(&mut self) -> Result<()> {
        let inner = self.inner.upgradable_read();
        if inner.uncommitted < inner.end {
            let hl = <RecordHeader as Pack>::const_len().unwrap();
            if self.mmap.len() - inner.end < hl {
                reserve(&mut self.mmap, &*inner, hl)?;
            }
            self.mmap.0.flush()?;
            let mut n = inner.uncommitted;
            while n < inner.end {
                let mut hr = <RecordHeader as Pack>::decode(&mut &self.mmap[n..])?;
                hr.committed = true;
                <RecordHeader as Pack>::encode(&hr, &mut &mut self.mmap[n..])?;
                n += hl + hr.record_length as usize;
            }
            assert_eq!(n, inner.end);
            let end = RecordHeader {
                committed: false,
                record_type: RecordTyp::DeltaBatch,
                record_length: 0,
                timestamp: 0,
            };
            <RecordHeader as Pack>::encode(&end, &mut &mut self.mmap[inner.end..])?;
            self.mmap.0.flush()?;
            let mut inner = RwLockUpgradableReadGuard::upgrade(inner);
            inner.uncommitted = inner.end;
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
        let mut inner = self.inner.write();
        let mut pms = PM_POOL.take();
        for path in paths {
            if !inner.id_by_path.contains_key(path) {
                let id = inner.next_id;
                inner.next_id += 1;
                inner.id_by_path.insert(path.clone(), id);
                inner.path_by_id.insert(id, path.clone());
                pms.push(PathMapping(path.clone(), id));
            }
        }
        if pms.len() > 0 {
            let inner = RwLockWriteGuard::downgrade_to_upgradable(inner);
            let record_length = <Pooled<Vec<PathMapping>> as Pack>::len(&pms);
            let len = check_reserve(&mut self.mmap, &*inner, record_length)?;
            let mut buf = &mut self.mmap[inner.end..];
            let rh = RecordHeader {
                committed: false,
                record_type: RecordTyp::PathMappings,
                record_length: record_length as u32,
                timestamp: 0,
            };
            <RecordHeader as Pack>::encode(&rh, &mut buf)?;
            <Pooled<Vec<PathMapping>> as Pack>::encode(&pms, &mut buf)?;
            let mut inner = RwLockUpgradableReadGuard::upgrade(inner);
            inner.end += len;
        }
        Ok(())
    }

    /// Add a data batch to the archive. If `image` is true then it
    /// will be marked as an image batch, and should contain a value
    /// for every subscriped path whether it changed or not, otherwise
    /// it will be marked as a delta batch, and should contain only
    /// values that changed since the last delta batch. This method
    /// will fail if any of the path ids in the batch are unknown.
    ///
    /// batch timestamps are monotonicly increasing, with the
    /// granularity of 1us. As such, one should avoid writing
    /// "spurious" batches, and generally for efficiency and
    /// correctness write as few batches as possible.
    pub fn add_batch(
        &mut self,
        image: bool,
        timestamp: Timestamp,
        batch: &Pooled<Vec<BatchItem>>,
    ) -> Result<()> {
        let inner = self.inner.upgradable_read();
        for BatchItem(id, _) in batch.iter() {
            if !inner.path_by_id.contains_key(id) {
                bail!("unknown subscription {}, register it first", id);
            }
        }
        if batch.len() > 0 {
            let record_length = <Pooled<Vec<BatchItem>> as Pack>::len(&batch);
            let inner = match timestamp {
                Timestamp::Offset(_, _) => inner,
                Timestamp::NewBasis(basis) => {
                    let record_length = <DateTime<Utc> as Pack>::len(&basis);
                    let rh = RecordHeader {
                        committed: false,
                        record_type: RecordTyp::Timestamp,
                        record_length: record_length as u32,
                        timestamp: 0,
                    };
                    let len = check_reserve(&mut self.mmap, &*inner, record_length)?;
                    let mut buf = &mut self.mmap[inner.end..];
                    <RecordHeader as Pack>::encode(&rh, &mut buf)?;
                    <DateTime<Utc> as Pack>::encode(&basis, &mut buf)?;
                    let mut inner = RwLockUpgradableReadGuard::upgrade(inner);
                    inner.end += len;
                    RwLockWriteGuard::downgrade_to_upgradable(inner)
                }
            };
            let len = check_reserve(&mut self.mmap, &*inner, record_length)?;
            let mut buf = &mut self.mmap[inner.end..];
            let rh = RecordHeader {
                committed: false,
                record_type: if image {
                    RecordTyp::ImageBatch
                } else {
                    RecordTyp::DeltaBatch
                },
                record_length: record_length as u32,
                timestamp: timestamp.offset(),
            };
            <RecordHeader as Pack>::encode(&rh, &mut buf)?;
            <Pooled<Vec<BatchItem>> as Pack>::encode(&batch, &mut buf)?;
            let ts = timestamp.datetime();
            let mut inner = RwLockUpgradableReadGuard::upgrade(inner);
            let end = inner.end;
            if image {
                inner.imagemap.insert(ts, end);
            } else {
                inner.deltamap.insert(ts, end);
            }
            inner.end += len;
        }
        Ok(())
    }
}

impl<T: Deref<Target = [u8]>> Archive<T> {
    /// Create a read-only mirror of the archive. This is a low cost
    /// operation, it duplicates the memory map and shares everything
    /// else. Beware, on 32 bit platforms address space might become
    /// an issue if the archive is large.
    ///
    /// Since the memory map is seperate, if the file grows due to new
    /// records being written then the mirrored map may not cover the
    /// entire file. In that case, read operations trying to access
    /// those new records will return None. Access to records in newly
    /// allocated parts of the file can be enabled by creating a new
    /// mirror.
    pub fn mirror(&self) -> Result<Archive<ReadOnly>> {
        let inner = self.inner.clone();
        let mmap = {
            let g = inner.read();
            ReadOnly(unsafe { Mmap::map(&g.file)? })
        };
        Ok(Archive { inner, mmap })
    }

    pub fn capacity(&self) -> usize {
        self.mmap.len()
    }

    pub fn delta_batches(&self) -> usize {
        self.inner.read().deltamap.len()
    }

    pub fn image_batches(&self) -> usize {
        self.inner.read().imagemap.len()
    }

    pub fn id_for_path(&self, path: &Path) -> Option<u64> {
        self.inner.read().id_by_path.get(path).copied()
    }

    pub fn path_for_id(&self, id: u64) -> Option<Path> {
        self.inner.read().path_by_id.get(&id).cloned()
    }

    /// Move the cursor according to the `Seek` instruction. If the
    /// cursor has no current position then positive offsets begin at
    /// the cursor start, and negative offsets begin at the cursor
    /// end. If the seek instruction would move the cursor out of
    /// bounds, then it will move to the closest in bounds position.
    pub fn seek(&self, cursor: &mut Cursor, seek: Seek) {
        match seek {
            Seek::Absolute(ts) => {
                cursor.set_current(ts);
            }
            Seek::TimeRelative(offset) => match cursor.current() {
                Some(ts) => cursor.set_current(ts + offset),
                None => {
                    if offset >= chrono::Duration::microseconds(0) {
                        match cursor.start() {
                            Bound::Included(ts) => cursor.set_current(ts + offset),
                            Bound::Excluded(ts) => {
                                cursor.set_current(ts + *EPSILON + offset)
                            }
                            Bound::Unbounded => {
                                match self.inner.read().deltamap.keys().next() {
                                    None => (),
                                    Some(ts) => cursor.set_current(*ts + offset),
                                }
                            }
                        };
                    } else {
                        match cursor.end() {
                            Bound::Included(ts) => cursor.set_current(ts + offset),
                            Bound::Excluded(ts) => {
                                cursor.set_current(ts - *EPSILON + offset)
                            }
                            Bound::Unbounded => {
                                match self.inner.read().deltamap.keys().next_back() {
                                    None => (),
                                    Some(ts) => cursor.set_current(*ts + offset),
                                }
                            }
                        }
                    }
                }
            },
            Seek::BatchRelative(steps) => {
                let inner = self.inner.read();
                if steps >= 0 {
                    let init =
                        cursor.current.map(Bound::Excluded).unwrap_or(cursor.start);
                    let mut iter = inner.deltamap.range((init, cursor.end));
                    for _ in 0..steps as usize {
                        match iter.next() {
                            None => break,
                            Some((ts, _)) => {
                                cursor.current = Some(*ts);
                            }
                        }
                    }
                } else {
                    let init = cursor.current.map(Bound::Excluded).unwrap_or(cursor.end);
                    let mut iter = inner.deltamap.range((cursor.start, init));
                    for _ in 0..steps.abs() as usize {
                        match iter.next_back() {
                            None => break,
                            Some((ts, _)) => {
                                cursor.current = Some(*ts);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Return a vector of all id/path pairs present in the
    /// archive. This may change if the archive is being written to.
    pub fn get_index(&self) -> Pooled<Vec<(u64, Path)>> {
        let mut idx = IDX_POOL.take();
        let mut i = 0;
        // we must ensure we don't hold the lock for too long in the
        // case where the index is huge.
        'main: loop {
            let inner = self.inner.read();
            for _ in 0..1000 {
                let (id, path) = inner.path_by_id.get_index(i).unwrap();
                idx.push((*id, path.clone()));
                i += 1;
                if i >= inner.path_by_id.len() {
                    break 'main;
                }
            }
        }
        idx
    }

    fn get_batch_at(&self, pos: usize, end: usize) -> Result<Pooled<Vec<BatchItem>>> {
        if pos >= end {
            bail!("get_batch: error record out of bounds")
        } else if pos >= self.capacity() {
            bail!(RemapRequired)
        } else {
            let mut buf = &self.mmap[pos..];
            let rh = <RecordHeader as Pack>::decode(&mut buf)?;
            if pos + rh.record_length as usize > end {
                bail!("get_batch: error truncated record at {}", pos);
            }
            Ok(<Pooled<Vec<BatchItem>> as Pack>::decode(&mut buf)?)
        }
    }

    /// Builds an image corresponding to the state at the beginning of
    /// the cursor. If the cursor beginning is Unbounded, then the
    /// image will be empty. If there are no images in the archive,
    /// then all the deltas between the beginning and the cursor start
    /// will be read, otherwise only the deltas between the closest
    /// image that is older, and the cursor start need to be read.
    pub fn build_image(
        &self,
        cursor: &Cursor,
    ) -> Result<Pooled<HashMap<u64, Event>>> {
        match cursor.start {
            Bound::Unbounded => Ok(Pooled::orphan(HashMap::new())),
            _ => {
                let (to_read, end) = {
                    // we need to invert the excluded/included to get
                    // the correct initial state.
                    let cs = match cursor.start {
                        Bound::Excluded(t) => Bound::Included(t),
                        Bound::Included(t) => Bound::Excluded(t),
                        Bound::Unbounded => unreachable!(),
                    };
                    let inner = self.inner.read();
                    let mut to_read: Vec<usize> = Vec::new();
                    let mut iter = inner.imagemap.range((Bound::Unbounded, cs));
                    let s = match iter.next_back() {
                        None => Bound::Unbounded,
                        Some((ts, pos)) => {
                            to_read.push(*pos);
                            Bound::Included(*ts)
                        }
                    };
                    to_read.extend(inner.deltamap.range((s, cs)).map(|v| v.1));
                    (to_read, inner.end)
                };
                let mut image = IMG_POOL.take();
                for pos in to_read {
                    let mut batch = self.get_batch_at(pos as usize, end)?;
                    image.extend(batch.drain(..).map(|b| (b.0, b.1)));
                }
                Ok(image)
            }
        }
    }

    /// read at most `n` delta items from the specified cursor, and
    /// advance it by the number of items read. The cursor will not be
    /// invalidated even if no items can be read, however depending on
    /// it's bounds it may never read any more items.
    pub fn read_deltas(
        &self,
        cursor: &mut Cursor,
        n: usize,
    ) -> Result<Pooled<VecDeque<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>>> {
        let mut idxs = POS_POOL.take();
        let mut res = CURSOR_BATCH_POOL.take();
        let start = match cursor.current {
            None => cursor.start,
            Some(dt) => Bound::Excluded(dt),
        };
        let mut current = cursor.current;
        let end = {
            let inner = self.inner.read();
            idxs.extend(
                inner
                    .deltamap
                    .range((start, cursor.end))
                    .map(|(ts, pos)| (*ts, *pos))
                    .take(n),
            );
            inner.end
        };
        for (ts, pos) in idxs.drain(..) {
            let batch = self.get_batch_at(pos as usize, end)?;
            current = Some(ts);
            res.push_back((ts, batch));
        }
        cursor.current = current;
        Ok(res)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use netidx::subscriber::Value;
    use std::fs;

    fn check_contents<T: Deref<Target = [u8]>>(
        t: &Archive<T>,
        paths: &[Path],
        batches: usize,
    ) {
        assert_eq!(t.delta_batches(), batches);
        let mut cursor = Cursor::new();
        let mut batch = t.read_deltas(&mut cursor, batches).unwrap();
        let now = Utc::now();
        for (ts, b) in batch.drain(..) {
            let elapsed = (now - ts).num_seconds();
            assert!(elapsed <= 10 && elapsed >= -10);
            assert_eq!(Vec::len(&b), paths.len());
            for (BatchItem(id, v), p) in b.iter().zip(paths.iter()) {
                assert_eq!(Some(p), t.path_for_id(*id).as_ref());
                assert_eq!(v, &Event::Update(Value::U64(42)))
            }
        }
    }

    #[test]
    fn basic_test() {
        let file = FilePath::new("test-data");
        let paths = [Path::from("/foo/bar"), Path::from("/foo/baz")];
        let mut timestamper = MonotonicTimestamper::new();
        if FilePath::is_file(&file) {
            fs::remove_file(file).unwrap();
        }
        let mut batch = BATCH_POOL.take();
        let initial_size = {
            // check that we can open, and write an archive
            let mut t = Archive::<ReadWrite>::open_readwrite(&file).unwrap();
            t.add_paths(&paths).unwrap();
            batch.extend(paths.iter().map(|p| {
                BatchItem(t.id_for_path(p).unwrap(), Event::Update(Value::U64(42)))
            }));
            t.add_batch(false, timestamper.timestamp(), &batch).unwrap();
            t.flush().unwrap();
            check_contents(&t, &paths, 1);
            t.capacity()
        };
        {
            // check that we can close, reopen, and read the archive back
            let t = Archive::<ReadOnly>::open_readonly(file).unwrap();
            check_contents(&t, &paths, 1);
        }
        {
            // check that we can reopen, and add to an archive
            let mut t = Archive::<ReadWrite>::open_readwrite(&file).unwrap();
            t.add_batch(false, timestamper.timestamp(), &batch).unwrap();
            t.flush().unwrap();
            check_contents(&t, &paths, 2);
        }
        {
            // check that we can reopen, and read the archive we added to
            let t = Archive::<ReadOnly>::open_readonly(&file).unwrap();
            check_contents(&t, &paths, 2);
        }
        let n = {
            // check that we can grow the archive by remapping it on the fly
            let mut t = Archive::<ReadWrite>::open_readwrite(&file).unwrap();
            let mut n = 2;
            while t.capacity() == initial_size {
                t.add_batch(false, timestamper.timestamp(), &batch).unwrap();
                n += 1;
                check_contents(&t, &paths, n);
            }
            t.flush().unwrap();
            check_contents(&t, &paths, n);
            n
        };
        {
            // check that we can reopen, and read the archive we grew
            let t = Archive::<ReadOnly>::open_readonly(&file).unwrap();
            check_contents(&t, &paths, n);
        }
        {
            // check that we can't open the archive twice
            let t = Archive::<ReadOnly>::open_readonly(&file).unwrap();
            check_contents(&t, &paths, n);
            assert!(Archive::<ReadOnly>::open_readonly(&file).is_err());
        }
    }
}

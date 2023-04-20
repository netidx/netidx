use anyhow::{Context, Error, Result};
use bytes::{Buf, BufMut};
use chrono::prelude::*;
use fs3::{allocation_granularity, FileExt};
use fxhash::FxBuildHasher;
use indexmap::IndexMap;
use log::warn;
use memmap2::{Mmap, MmapMut};
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
    RwLock, RwLockReadGuard,
};
use std::{
    self,
    cmp::max,
    collections::{BTreeMap, HashMap, VecDeque},
    error, fmt,
    fs::{File, OpenOptions},
    iter::IntoIterator,
    mem,
    ops::{Bound, Drop, RangeBounds},
    path::Path as FilePath,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

#[derive(Debug, Clone)]
pub struct FileHeader {
    version: u32,
    committed: u64,
}

static FILE_MAGIC: &'static [u8] = b"netidx archive";
static COMMITTED_OFFSET: usize = FILE_MAGIC.len() + mem::size_of::<u32>();
const FILE_VERSION: u32 = 0;

impl Pack for FileHeader {
    fn const_encoded_len() -> Option<usize> {
        Some(COMMITTED_OFFSET + mem::size_of::<u64>())
    }

    fn encoded_len(&self) -> usize {
        <FileHeader as Pack>::const_encoded_len().unwrap()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        buf.put_slice(FILE_MAGIC);
        buf.put_u32(FILE_VERSION);
        buf.put_u64(self.committed);
        Ok(())
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        for byte in FILE_MAGIC {
            if buf.get_u8() != *byte {
                return Err(PackError::InvalidFormat);
            }
        }
        let version = buf.get_u32();
        let committed = buf.get_u64();
        Ok(FileHeader { version, committed })
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

const MAX_RECORD_LEN: u32 = u32::MAX;
const MAX_TIMESTAMP: u32 = 0x03FFFFFF;

// Every record in the archive starts with this header
#[derive(PackedStruct, Debug, Clone)]
#[packed_struct(bit_numbering = "msb0", size_bytes = "8")]
pub struct RecordHeader {
    // the record type
    #[packed_field(bits = "0:1", size_bits = "2", ty = "enum")]
    record_type: RecordTyp,
    // the record length, up to MAX_RECORD_LEN, not including this header
    #[packed_field(bits = "2:33", size_bits = "32", endian = "msb")]
    record_length: u32,
    // microsecond offset from last timestamp record, up to MAX_TIMESTAMP
    #[packed_field(bits = "34:63", size_bits = "30", endian = "msb")]
    timestamp: u32,
}

impl Pack for RecordHeader {
    fn const_encoded_len() -> Option<usize> {
        Some(8)
    }

    fn encoded_len(&self) -> usize {
        <RecordHeader as Pack>::const_encoded_len().unwrap()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        let hdr = RecordHeader::pack(self).map_err(|_| PackError::InvalidFormat)?;
        Ok(buf.put(&hdr[..]))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let mut v = [0u8; 8];
        buf.copy_to_slice(&mut v);
        RecordHeader::unpack(&v).map_err(|_| PackError::InvalidFormat)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Hash, Eq, PartialOrd, Ord)]
pub struct Id(u64);

impl Pack for Id {
    fn encoded_len(&self) -> usize {
        varint_len(self.0)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(encode_varint(self.0, buf))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        Ok(Id(decode_varint(buf)?))
    }
}

#[derive(Debug, Clone)]
struct PathMapping(Path, Id);

impl Pack for PathMapping {
    fn encoded_len(&self) -> usize {
        <Path as Pack>::encoded_len(&self.0) + <Id as Pack>::encoded_len(&self.1)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        <Path as Pack>::encode(&self.0, buf)?;
        <Id as Pack>::encode(&self.1, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let path = <Path as Pack>::decode(buf)?;
        let id = <Id as Pack>::decode(buf)?;
        Ok(PathMapping(path, id))
    }
}

#[derive(Debug, Clone)]
pub struct BatchItem(pub Id, pub Event);

impl Pack for BatchItem {
    fn encoded_len(&self) -> usize {
        <Id as Pack>::encoded_len(&self.0) + Pack::encoded_len(&self.1)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        <Id as Pack>::encode(&self.0, buf)?;
        <Event as Pack>::encode(&self.1, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let id = <Id as Pack>::decode(buf)?;
        Ok(BatchItem(id, <Event as Pack>::decode(buf)?))
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Seek {
    Beginning,
    End,
    Absolute(DateTime<Utc>),
    BatchRelative(i8),
    TimeRelative(chrono::Duration),
}

impl ToString for Seek {
    fn to_string(&self) -> String {
        match self {
            Seek::Beginning => "beginning".into(),
            Seek::End => "end".into(),
            Seek::Absolute(dt) => dt.to_rfc3339(),
            Seek::BatchRelative(i) => i.to_string(),
            Seek::TimeRelative(d) => {
                if d < &chrono::Duration::zero() {
                    format!("{}s", d.num_seconds())
                } else {
                    format!("+{}s", d.num_seconds())
                }
            }
        }
    }
}

impl FromStr for Seek {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        use diligent_date_parser::parse_date;
        let s = s.trim();
        if s == "beginning" {
            Ok(Seek::Beginning)
        } else if s == "end" {
            Ok(Seek::End)
        } else if let Ok(steps) = s.parse::<i8>() {
            Ok(Seek::BatchRelative(steps))
        } else if let Some(dt) = parse_date(s) {
            Ok(Seek::Absolute(dt.with_timezone(&Utc)))
        } else if s.starts_with(['+', '-'].as_ref())
            && s.ends_with(['y', 'M', 'd', 'h', 'm', 's', 'u'].as_ref())
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
                    } else if mag == 's' {
                        quantity
                    } else {
                        quantity * 1e-6
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
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::DateTime(ts) => Ok(Seek::Absolute(ts)),
            v if v.number() => Ok(Seek::BatchRelative(v.cast_to::<i8>()?)),
            v => v.cast_to::<Chars>()?.parse::<Seek>(),
        }
    }

    fn get(_: Value) -> Option<Self> {
        None
    }
}

lazy_static! {
    static ref PM_POOL: Pool<Vec<PathMapping>> = Pool::new(10, 100000);
    pub static ref BATCH_POOL: Pool<Vec<BatchItem>> = Pool::new(100, 100000);
    pub(crate) static ref CURSOR_BATCH_POOL: Pool<VecDeque<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>> =
        Pool::new(100, 100000);
    static ref POS_POOL: Pool<Vec<(DateTime<Utc>, usize)>> = Pool::new(10, 100000);
    static ref IDX_POOL: Pool<Vec<(Id, Path)>> = Pool::new(10, 20_000_000);
    static ref IMG_POOL: Pool<HashMap<Id, Event>> = Pool::new(10, 20_000_000);
    static ref EPSILON: chrono::Duration = chrono::Duration::microseconds(1);
    static ref TO_READ_POOL: Pool<Vec<usize>> = Pool::new(10, 20_000_000);
}

#[derive(Debug, Clone, Copy)]
enum Timestamp {
    NewBasis(DateTime<Utc>),
    Offset(DateTime<Utc>, u32),
}

impl Timestamp {
    pub fn offset(&self) -> u32 {
        match self {
            Timestamp::NewBasis(_) => 0,
            Timestamp::Offset(_, off) => *off,
        }
    }
}

/// The goal of this structure are as follows in order of importance
/// 1. Monotonic. subsuquent calls to timestamp() will always be greater than previous calls.
/// 2. Steady. Clock skew should be minimized where possible.
/// 3. Accurate. Time stamps should be close to the actual time
/// 4. Precise. Small differences in time should be representable.
/// 5. Compact. Time stamps should use as little space as possible.
///
/// Unfortunatly because system provided time functions are often
/// awful some careful and elaborate logic is required in order to
/// meet the above goals.
#[derive(Debug, Clone, Copy)]
struct MonotonicTimestamper {
    prev: DateTime<Utc>,
    basis: Option<DateTime<Utc>>,
    offset: u32,
}

impl MonotonicTimestamper {
    fn new() -> Self {
        MonotonicTimestamper { prev: Utc::now(), basis: None, offset: 0 }
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

    fn timestamp(&mut self, now: DateTime<Utc>) -> Timestamp {
        use chrono::Duration;
        let ts = match self.basis {
            None => Timestamp::NewBasis(self.update_basis(now)),
            Some(basis) => match (now - self.prev).num_microseconds() {
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
        };
        self.prev = now;
        ts
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

fn scan_records(
    path_by_id: &mut IndexMap<Id, Path, FxBuildHasher>,
    id_by_path: &mut HashMap<Path, Id>,
    mut imagemap: Option<&mut BTreeMap<DateTime<Utc>, usize>>,
    mut deltamap: Option<&mut BTreeMap<DateTime<Utc>, usize>>,
    time_basis: &mut DateTime<Utc>,
    max_id: &mut u64,
    end: usize,
    start_pos: usize,
    buf: &mut impl Buf,
) -> Result<usize> {
    let total_size = buf.remaining();
    loop {
        let pos = start_pos + (total_size - buf.remaining());
        if pos >= end {
            break Ok(pos);
        }
        if buf.remaining() < <RecordHeader as Pack>::const_encoded_len().unwrap() {
            break Ok(pos);
        }
        let rh = RecordHeader::decode(buf)
            .map_err(Error::from)
            .context("invalid record header")?;
        if buf.remaining() < rh.record_length as usize {
            warn!("truncated record at {}", pos);
            break Ok(pos);
        }
        use chrono::Duration;
        match rh.record_type {
            RecordTyp::DeltaBatch => {
                if let Some(deltamap) = &mut deltamap {
                    let timestamp =
                        *time_basis + Duration::microseconds(rh.timestamp as i64);
                    deltamap.insert(timestamp, pos);
                }
                buf.advance(rh.record_length as usize); // skip the contents
            }
            RecordTyp::Timestamp => {
                *time_basis = <DateTime<Utc> as Pack>::decode(buf)?;
            }
            RecordTyp::ImageBatch => {
                if let Some(imagemap) = &mut imagemap {
                    let timestamp =
                        *time_basis + Duration::microseconds(rh.timestamp as i64);
                    imagemap.insert(timestamp, pos);
                }
                buf.advance(rh.record_length as usize); // skip the contents
            }
            RecordTyp::PathMappings => {
                let mut m = <Pooled<Vec<PathMapping>> as Pack>::decode(buf)
                    .map_err(Error::from)
                    .context("invalid path mappings record")?;
                for pm in m.drain(..) {
                    if let Some(old) = id_by_path.insert(pm.0.clone(), pm.1) {
                        warn!(
                            "duplicate path mapping for {}, {:?}, {:?}",
                            &*pm.0, old, pm.1
                        );
                    }
                    if let Some(old) = path_by_id.insert(pm.1, pm.0.clone()) {
                        warn!("duplicate id mapping for {}, {}, {:?}", &*pm.0, old, pm.1)
                    }
                    *max_id = max(pm.1 .0, *max_id);
                }
            }
        }
    }
}

fn scan_file(
    path_by_id: &mut IndexMap<Id, Path, FxBuildHasher>,
    id_by_path: &mut HashMap<Path, Id>,
    imagemap: Option<&mut BTreeMap<DateTime<Utc>, usize>>,
    deltamap: Option<&mut BTreeMap<DateTime<Utc>, usize>>,
    time_basis: &mut DateTime<Utc>,
    max_id: &mut u64,
    buf: &mut impl Buf,
) -> Result<usize> {
    let total_bytes = buf.remaining();
    // check the file header
    if buf.remaining() < <FileHeader as Pack>::const_encoded_len().unwrap() {
        bail!("invalid file header: too short")
    }
    let header = <FileHeader as Pack>::decode(buf)
        .map_err(Error::from)
        .context("invalid file header")?;
    // this is the first version, so no upgrading can be done yet
    if header.version != FILE_VERSION {
        bail!("file version is too new, can't read it")
    }
    scan_records(
        path_by_id,
        id_by_path,
        imagemap,
        deltamap,
        time_basis,
        max_id,
        header.committed as usize,
        total_bytes - buf.remaining(),
        buf,
    )
}

/// This reads and writes the netidx archive format (as written by the
/// "record" command in the tools). The archive format is intended to
/// be a compact format for storing recordings of netidx data for long
/// term storage and access. It uses memory mapped IO for performance
/// and memory efficiency, and as such file size is limited to
/// `usize`.
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
/// When an archive is opened read-only, an index of it's contents is
/// built in memory so that any part of it can be accessed quickly by
/// timestamp. As a result, there is some memory overhead.
///
/// In order to facilitate full reconstruction of the state at any
/// point without requiring to decode the entire file up to that point
/// there are two types of data records, image records contain the
/// entire state of every archived value at a given time, and delta
/// records contain only values that changed since the last delta
/// record. The full state of the values can be constructed at a given
/// time `t` by seeking to the nearest image record that is before
/// `t`, and then processing all the delta records up to `t`.
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
/// independent of advisory locking it could cause data corruption.
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
pub struct ArchiveWriter {
    time: MonotonicTimestamper,
    path_by_id: IndexMap<Id, Path, FxBuildHasher>,
    id_by_path: HashMap<Path, Id>,
    file: Arc<File>,
    end: Arc<AtomicUsize>,
    committed: usize,
    next_id: u64,
    block_size: usize,
    mmap: MmapMut,
}

impl Drop for ArchiveWriter {
    fn drop(&mut self) {
        let _ = self.flush();
        let _ = self.mmap.flush(); // for the committed header
    }
}

impl ArchiveWriter {
    /// Open the specified archive for read/write access, if the file
    /// does not exist then a new archive will be created.
    pub fn open(path: impl AsRef<FilePath>) -> Result<Self> {
        if mem::size_of::<usize>() < mem::size_of::<u64>() {
            warn!("archive file size is limited to 4 GiB on this platform")
        }
        let time = MonotonicTimestamper::new();
        if FilePath::is_file(path.as_ref()) {
            let mut time_basis = DateTime::<Utc>::MIN_UTC;
            let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
            file.try_lock_exclusive()?;
            let block_size = allocation_granularity(path)? as usize;
            let mmap = unsafe { MmapMut::map_mut(&file)? };
            let mut t = ArchiveWriter {
                time,
                path_by_id: IndexMap::with_hasher(FxBuildHasher::default()),
                id_by_path: HashMap::new(),
                file: Arc::new(file),
                end: Arc::new(AtomicUsize::new(0)),
                committed: 0,
                next_id: 0,
                block_size,
                mmap,
            };
            let end = scan_file(
                &mut t.path_by_id,
                &mut t.id_by_path,
                None,
                None,
                &mut time_basis,
                &mut t.next_id,
                &mut &*t.mmap,
            )?;
            t.next_id += 1;
            t.end.store(end, Ordering::Relaxed);
            t.committed = end;
            Ok(t)
        } else {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path.as_ref())?;
            file.try_lock_exclusive()?;
            let block_size = allocation_granularity(path.as_ref())? as usize;
            let fh_len = <FileHeader as Pack>::const_encoded_len().unwrap();
            let rh_len = <RecordHeader as Pack>::const_encoded_len().unwrap();
            file.set_len(max(block_size, fh_len + rh_len) as u64)?;
            let mut mmap = unsafe { MmapMut::map_mut(&file)? };
            let mut buf = &mut *mmap;
            let fh = FileHeader { version: FILE_VERSION, committed: fh_len as u64 };
            <FileHeader as Pack>::encode(&fh, &mut buf)?;
            mmap.flush()?;
            Ok(ArchiveWriter {
                time,
                path_by_id: IndexMap::with_hasher(FxBuildHasher::default()),
                id_by_path: HashMap::new(),
                file: Arc::new(file),
                end: Arc::new(AtomicUsize::new(fh_len)),
                committed: fh_len,
                next_id: 0,
                block_size,
                mmap,
            })
        }
    }

    // remap the file reserving space for at least additional_capacity bytes
    fn reserve(&mut self, additional_capacity: usize) -> Result<()> {
        let len = self.mmap.len();
        let new_len = len + max(len >> 6, additional_capacity);
        let new_blocks = (new_len / self.block_size as usize) + 1;
        let new_size = new_blocks * self.block_size as usize;
        self.file.set_len(new_size as u64)?;
        Ok(drop(mem::replace(&mut self.mmap, unsafe { MmapMut::map_mut(&self.file)? })))
    }

    fn check_reserve(&mut self, record_length: usize) -> Result<usize> {
        if record_length > MAX_RECORD_LEN as usize {
            bail!(RecordTooLarge);
        }
        let len = <RecordHeader as Pack>::const_encoded_len().unwrap() + record_length;
        if self.mmap.len() - self.end.load(Ordering::Relaxed) < len {
            self.reserve(len)?;
        }
        Ok(len)
    }

    /// flush uncommitted changes to disk, mark all flushed records as
    /// committed, and update the end of archive marker. Does nothing
    /// if everything is already committed.
    pub fn flush(&mut self) -> Result<()> {
        let end = self.end.load(Ordering::Relaxed);
        if self.committed < end {
            self.mmap.flush()?;
            let mut buf = &mut self.mmap[COMMITTED_OFFSET..];
            buf.put_u64(end as u64);
            self.committed = end;
        }
        Ok(())
    }

    /// allocate path ids for any of the specified paths that don't
    /// already have one, and write a path mappings record containing
    /// the new assignments.
    pub fn add_paths<'a>(
        &'a mut self,
        paths: impl IntoIterator<Item = &'a Path>,
    ) -> Result<()> {
        let mut pms = PM_POOL.take();
        for path in paths {
            if !self.id_by_path.contains_key(path) {
                let id = Id(self.next_id);
                self.next_id += 1;
                self.id_by_path.insert(path.clone(), id);
                self.path_by_id.insert(id, path.clone());
                pms.push(PathMapping(path.clone(), id));
            }
        }
        if pms.len() > 0 {
            let record_length = <Pooled<Vec<PathMapping>> as Pack>::encoded_len(&pms);
            let len = self.check_reserve(record_length)?;
            let end = self.end.load(Ordering::Relaxed);
            let mut buf = &mut self.mmap[end..];
            let rh = RecordHeader {
                record_type: RecordTyp::PathMappings,
                record_length: record_length as u32,
                timestamp: 0,
            };
            <RecordHeader as Pack>::encode(&rh, &mut buf)?;
            <Pooled<Vec<PathMapping>> as Pack>::encode(&pms, &mut buf)?;
            self.end.fetch_add(len, Ordering::AcqRel);
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
        timestamp: DateTime<Utc>,
        batch: &Pooled<Vec<BatchItem>>,
    ) -> Result<()> {
        if batch.len() > 0 {
            let timestamp = self.time.timestamp(timestamp);
            for BatchItem(id, _) in batch.iter() {
                if !self.path_by_id.contains_key(id) {
                    bail!("unknown id: {:?} in batch", id)
                }
            }
            let record_length = <Pooled<Vec<BatchItem>> as Pack>::encoded_len(&batch);
            if record_length > MAX_RECORD_LEN as usize {
                bail!(RecordTooLarge)
            }
            match timestamp {
                Timestamp::Offset(_, _) => (),
                Timestamp::NewBasis(basis) => {
                    let record_length = <DateTime<Utc> as Pack>::encoded_len(&basis);
                    let rh = RecordHeader {
                        record_type: RecordTyp::Timestamp,
                        record_length: record_length as u32,
                        timestamp: 0,
                    };
                    let len = self.check_reserve(record_length)?;
                    let mut buf = &mut self.mmap[self.end.load(Ordering::Relaxed)..];
                    <RecordHeader as Pack>::encode(&rh, &mut buf)?;
                    <DateTime<Utc> as Pack>::encode(&basis, &mut buf)?;
                    self.end.fetch_add(len, Ordering::AcqRel);
                }
            }
            let len = self.check_reserve(record_length)?;
            let mut buf = &mut self.mmap[self.end.load(Ordering::Relaxed)..];
            let rh = RecordHeader {
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
            self.end.fetch_add(len, Ordering::AcqRel);
        }
        Ok(())
    }

    pub fn id_for_path(&self, path: &Path) -> Option<Id> {
        self.id_by_path.get(path).copied()
    }

    pub fn path_for_id(&self, id: &Id) -> Option<&Path> {
        self.path_by_id.get(id)
    }

    pub fn capacity(&self) -> usize {
        self.mmap.len()
    }

    pub fn len(&self) -> usize {
        self.end.load(Ordering::Relaxed)
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Create an archive reader from this writer by creating a
    /// read-only duplicate of the memory map.
    ///
    /// If you need lots of readers it's best to create just one using
    /// this method, and then clone it, that way the same memory map
    /// can be shared by all the readers.
    pub fn reader(&self) -> Result<ArchiveReader> {
        Ok(ArchiveReader {
            index: Arc::new(RwLock::new(ArchiveIndex::new())),
            file: self.file.clone(),
            end: self.end.clone(),
            mmap: Arc::new(RwLock::new(unsafe { Mmap::map(&self.file)? })),
        })
    }
}

#[derive(Debug)]
pub struct ArchiveIndex {
    path_by_id: IndexMap<Id, Path, FxBuildHasher>,
    id_by_path: HashMap<Path, Id>,
    imagemap: BTreeMap<DateTime<Utc>, usize>,
    deltamap: BTreeMap<DateTime<Utc>, usize>,
    time_basis: DateTime<Utc>,
    end: usize,
}

impl ArchiveIndex {
    fn new() -> Self {
        ArchiveIndex {
            path_by_id: IndexMap::with_hasher(FxBuildHasher::default()),
            id_by_path: HashMap::new(),
            imagemap: BTreeMap::new(),
            deltamap: BTreeMap::new(),
            time_basis: DateTime::<Utc>::MIN_UTC,
            end: <FileHeader as Pack>::const_encoded_len().unwrap(),
        }
    }

    pub fn deltamap(&self) -> &BTreeMap<DateTime<Utc>, usize> {
        &self.deltamap
    }

    /// check if the specifed timestamp could be in the file, meaning
    /// it is equal or after the start and before or equal to the end
    pub fn check_in_file(&self, ts: DateTime<Utc>) -> bool {
        match (self.deltamap.first_key_value(), self.deltamap.last_key_value()) {
            (Some((fst, _)), Some((lst, _))) => *fst <= ts && ts <= *lst,
            (_, _) => false,
        }
    }

    /// seek the specified number of batches forward or back in the
    /// file. Return true if it was possible, false otherwise. If
    /// false, the cursor is moved as far as possible.
    pub fn seek_steps(&self, cursor: &mut Cursor, steps: i8) -> bool {
        let mut success = true;
        if steps >= 0 {
            let init = cursor.current.map(Bound::Excluded).unwrap_or(cursor.start);
            let mut iter = self.deltamap.range((init, cursor.end));
            for _ in 0..steps as usize {
                match iter.next() {
                    None => {
                        success = false;
                        break;
                    }
                    Some((ts, _)) => {
                        cursor.current = Some(*ts);
                    }
                }
            }
        } else {
            let init = cursor.current.map(Bound::Excluded).unwrap_or(cursor.end);
            let mut iter = self.deltamap.range((cursor.start, init));
            for _ in 0..steps.abs() as usize {
                match iter.next_back() {
                    None => {
                        success = false;
                        break;
                    }
                    Some((ts, _)) => {
                        cursor.current = Some(*ts);
                    }
                }
            }
        }
        success
    }

    /// Seek relative to the current position
    pub fn seek_time_relative(
        &self,
        cursor: &mut Cursor,
        offset: chrono::Duration,
    ) -> (bool, DateTime<Utc>) {
        match cursor.current() {
            Some(ts) => {
                let new_ts = ts + offset;
                cursor.set_current(new_ts);
                (self.check_in_file(new_ts), new_ts)
            }
            None => {
                if offset >= chrono::Duration::microseconds(0) {
                    match cursor.start() {
                        Bound::Included(ts) => {
                            let new_ts = ts + offset;
                            cursor.set_current(new_ts);
                            (self.check_in_file(new_ts), new_ts)
                        }
                        Bound::Excluded(ts) => {
                            let new_ts = ts + *EPSILON + offset;
                            cursor.set_current(new_ts);
                            (self.check_in_file(new_ts), new_ts)
                        }
                        Bound::Unbounded => match self.deltamap.keys().next() {
                            None => (false, DateTime::<Utc>::MAX_UTC),
                            Some(ts) => {
                                let new_ts = *ts + offset;
                                cursor.set_current(new_ts);
                                (self.check_in_file(new_ts), new_ts)
                            }
                        },
                    }
                } else {
                    match cursor.end() {
                        Bound::Included(ts) => {
                            let new_ts = ts + offset;
                            cursor.set_current(new_ts);
                            (self.check_in_file(new_ts), new_ts)
                        }
                        Bound::Excluded(ts) => {
                            let new_ts = ts - *EPSILON + offset;
                            cursor.set_current(new_ts);
                            (self.check_in_file(new_ts), new_ts)
                        }
                        Bound::Unbounded => match self.deltamap.keys().next_back() {
                            None => (false, DateTime::<Utc>::MIN_UTC),
                            Some(ts) => {
                                let new_ts = *ts + offset;
                                cursor.set_current(new_ts);
                                (self.check_in_file(new_ts), new_ts)
                            }
                        },
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArchiveReader {
    index: Arc<RwLock<ArchiveIndex>>,
    file: Arc<File>,
    end: Arc<AtomicUsize>,
    mmap: Arc<RwLock<Mmap>>,
}

impl ArchiveReader {
    /// Open the specified archive read only. Note, it is possible to
    /// read and write to an archive simultaneously, however to do so
    /// you must open an [ArchiveWriter](ArchiveWriter) and then use
    /// the [ArchiveWriter::reader](ArchiveWriter::reader) method to
    /// get a reader.
    pub fn open(path: impl AsRef<FilePath>) -> Result<Self> {
        let file = OpenOptions::new().read(true).open(path.as_ref())?;
        file.try_lock_shared()?;
        let mmap = unsafe { Mmap::map(&file)? };
        let mut index = ArchiveIndex::new();
        let mut max_id = 0;
        let end = scan_file(
            &mut index.path_by_id,
            &mut index.id_by_path,
            Some(&mut index.imagemap),
            Some(&mut index.deltamap),
            &mut index.time_basis,
            &mut max_id,
            &mut &*mmap,
        )?;
        index.end = end;
        Ok(ArchiveReader {
            index: Arc::new(RwLock::new(index)),
            file: Arc::new(file),
            end: Arc::new(AtomicUsize::new(end)),
            mmap: Arc::new(RwLock::new(mmap)),
        })
    }

    pub(crate) fn strong_count(&self) -> usize {
        Arc::strong_count(&self.index)
    }

    pub fn index(&self) -> RwLockReadGuard<ArchiveIndex> {
        self.index.read()
    }

    pub fn capacity(&self) -> usize {
        self.mmap.read().len()
    }

    pub fn delta_batches(&self) -> usize {
        self.index.read().deltamap.len()
    }

    pub fn image_batches(&self) -> usize {
        self.index.read().imagemap.len()
    }

    pub fn id_for_path(&self, path: &Path) -> Option<Id> {
        self.index.read().id_by_path.get(path).copied()
    }

    pub fn path_for_id(&self, id: &Id) -> Option<Path> {
        self.index.read().path_by_id.get(id).cloned()
    }

    /// Check if the memory map needs to be remapped due to growth,
    /// and check if additional records exist that need to be
    /// indexed. This method is only relevant if this `ArchiveReader`
    /// was created from an `ArchiveWriter`, this method is called
    /// automatically by `read_deltas` and `build_image`.
    pub fn check_remap_rescan(&self) -> Result<()> {
        let end = self.end.load(Ordering::Relaxed);
        let mmap = self.mmap.upgradable_read();
        let mmap = if end > mmap.len() {
            let mut mmap = RwLockUpgradableReadGuard::upgrade(mmap);
            drop(mem::replace(&mut *mmap, unsafe { Mmap::map(&*self.file)? }));
            RwLockWriteGuard::downgrade_to_upgradable(mmap)
        } else {
            mmap
        };
        let index = self.index.upgradable_read();
        if index.end < end {
            let mut index = RwLockUpgradableReadGuard::upgrade(index);
            let mut max_id = 0;
            let r = &mut *index;
            r.end = scan_records(
                &mut r.path_by_id,
                &mut r.id_by_path,
                Some(&mut r.imagemap),
                Some(&mut r.deltamap),
                &mut r.time_basis,
                &mut max_id,
                end,
                r.end,
                &mut &mmap[r.end..end],
            )?;
        }
        Ok(())
    }

    /// Move the cursor according to the `Seek` instruction. If the
    /// cursor has no current position then positive offsets begin at
    /// the cursor start, and negative offsets begin at the cursor
    /// end. If the seek instruction would move the cursor out of
    /// bounds, then it will move to the closest in bounds position.
    pub fn seek(&self, cursor: &mut Cursor, seek: Seek) {
        match seek {
            Seek::Beginning => match self.index.read().deltamap.keys().next() {
                None => {
                    cursor.current = None;
                }
                Some(ts) => {
                    cursor.set_current(*ts);
                }
            },
            Seek::End => match self.index.read().deltamap.keys().next_back() {
                None => {
                    cursor.current = None;
                }
                Some(ts) => {
                    cursor.set_current(*ts);
                }
            },
            Seek::Absolute(ts) => {
                cursor.set_current(ts);
            }
            Seek::TimeRelative(offset) => {
                self.index.read().seek_time_relative(cursor, offset);
            }
            Seek::BatchRelative(steps) => {
                self.index.read().seek_steps(cursor, steps);
            }
        }
    }

    /// Return a vector of all id/path pairs present in the
    /// archive. This may change if the archive is being written to.
    pub fn get_index(&self) -> Pooled<Vec<(Id, Path)>> {
        let mut idx = IDX_POOL.take();
        let mut i = 0;
        // we must ensure we don't hold the lock for too long in the
        // case where the index is huge.
        'main: loop {
            let inner = self.index.read();
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

    fn get_batch_at(
        mmap: &Mmap,
        pos: usize,
        end: usize,
    ) -> Result<Pooled<Vec<BatchItem>>> {
        if pos >= end {
            bail!("record out of bounds")
        } else {
            let mut buf = &mmap[pos..];
            let rh = <RecordHeader as Pack>::decode(&mut buf)?;
            if pos + rh.record_length as usize > end {
                bail!("get_batch: error truncated record at {}", pos);
            }
            Ok(<Pooled<Vec<BatchItem>> as Pack>::decode(&mut buf)?)
        }
    }

    /// Builds an image corresponding to the state at the cursor, or
    /// if the cursor has no current position then at the beginning of
    /// the cursor. If the cursor has no position and then beginning
    /// is Unbounded, then the image will be empty. If there are no
    /// images in the archive, then all the deltas between the
    /// beginning and the cursor position will be read, otherwise only
    /// the deltas between the closest image that is older, and the
    /// cursor start need to be read.
    pub fn build_image(&self, cursor: &Cursor) -> Result<Pooled<HashMap<Id, Event>>> {
        self.check_remap_rescan()?;
        let pos = match cursor.current {
            None => cursor.start,
            Some(pos) => Bound::Included(pos),
        };
        match pos {
            Bound::Unbounded => Ok(Pooled::orphan(HashMap::new())),
            _ => {
                let (mut to_read, end) = {
                    // we need to invert the excluded/included to get
                    // the correct initial state.
                    let pos = match pos {
                        Bound::Excluded(t) => Bound::Included(t),
                        Bound::Included(t) => Bound::Excluded(t),
                        Bound::Unbounded => unreachable!(),
                    };
                    let index = self.index.read();
                    let mut to_read = TO_READ_POOL.take();
                    let mut iter = index.imagemap.range((Bound::Unbounded, pos));
                    let s = match iter.next_back() {
                        None => Bound::Unbounded,
                        Some((ts, pos)) => {
                            to_read.push(*pos);
                            Bound::Included(*ts)
                        }
                    };
                    to_read.extend(index.deltamap.range((s, pos)).map(|v| v.1));
                    (to_read, index.end)
                };
                let mut image = IMG_POOL.take();
                let mmap = self.mmap.read();
                for pos in to_read.drain(..) {
                    let mut batch =
                        ArchiveReader::get_batch_at(&*mmap, pos as usize, end)?;
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
        self.check_remap_rescan()?;
        let mut idxs = POS_POOL.take();
        let mut res = CURSOR_BATCH_POOL.take();
        let start = match cursor.current {
            None => cursor.start,
            Some(dt) => Bound::Excluded(dt),
        };
        let end = {
            let index = self.index.read();
            idxs.extend(
                index
                    .deltamap
                    .range((start, cursor.end))
                    .map(|(ts, pos)| (*ts, *pos))
                    .take(n),
            );
            index.end
        };
        let mut current = cursor.current;
        let mmap = self.mmap.read();
        for (ts, pos) in idxs.drain(..) {
            let batch = ArchiveReader::get_batch_at(&*mmap, pos as usize, end)?;
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

    fn check_contents(t: &ArchiveReader, paths: &[Path], batches: usize) {
        t.check_remap_rescan().unwrap();
        assert_eq!(t.delta_batches(), batches);
        let mut cursor = Cursor::new();
        let mut batch = t.read_deltas(&mut cursor, batches).unwrap();
        let now = Utc::now();
        for (ts, b) in batch.drain(..) {
            let elapsed = (now - ts).num_seconds();
            assert!(elapsed <= 10 && elapsed >= -10);
            assert_eq!(Vec::len(&b), paths.len());
            for (BatchItem(id, v), p) in b.iter().zip(paths.iter()) {
                assert_eq!(Some(p), t.path_for_id(id).as_ref());
                assert_eq!(v, &Event::Update(Value::U64(42)))
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
        let mut batch = BATCH_POOL.take();
        let initial_size = {
            // check that we can open, and write an archive
            let mut t = ArchiveWriter::open(&file).unwrap();
            t.add_paths(&paths).unwrap();
            batch.extend(paths.iter().map(|p| {
                BatchItem(t.id_for_path(p).unwrap(), Event::Update(Value::U64(42)))
            }));
            t.add_batch(false, Utc::now(), &batch).unwrap();
            t.flush().unwrap();
            check_contents(&t.reader().unwrap(), &paths, 1);
            t.capacity()
        };
        {
            // check that we can close, reopen, and read the archive back
            let t = ArchiveReader::open(file).unwrap();
            check_contents(&t, &paths, 1);
        }
        {
            // check that we can reopen, and add to an archive
            let mut t = ArchiveWriter::open(&file).unwrap();
            t.add_batch(false, Utc::now(), &batch).unwrap();
            t.flush().unwrap();
            check_contents(&t.reader().unwrap(), &paths, 2);
        }
        {
            // check that we can reopen, and read the archive we added to
            let t = ArchiveReader::open(&file).unwrap();
            check_contents(&t, &paths, 2);
        }
        let n = {
            // check that we can grow the archive by remapping it on the fly
            let mut t = ArchiveWriter::open(&file).unwrap();
            let r = t.reader().unwrap();
            let mut n = 2;
            while t.capacity() == initial_size {
                t.add_batch(false, Utc::now(), &batch).unwrap();
                n += 1;
                check_contents(&r, &paths, n);
            }
            t.flush().unwrap();
            check_contents(&r, &paths, n);
            n
        };
        {
            // check that we can reopen, and read the archive we grew
            let t = ArchiveReader::open(&file).unwrap();
            check_contents(&t, &paths, n);
        }
        {
            // check that we can't open the archive twice for write
            let t = ArchiveWriter::open(&file).unwrap();
            let reader = t.reader().unwrap();
            check_contents(&reader, &paths, n);
            assert!(ArchiveWriter::open(&file).is_err());
        }
        if FilePath::is_file(&file) {
            fs::remove_file(file).unwrap();
        }
    }
}

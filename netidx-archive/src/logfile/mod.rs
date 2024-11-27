pub mod arraymap;
mod reader;
mod writer;

#[cfg(test)]
mod test;

use self::arraymap::ArrayMap;
pub use self::{
    reader::{AlreadyCompressed, ArchiveReader},
    writer::ArchiveWriter,
};
use anyhow::{Context, Error, Result};
use bytes::{Buf, BufMut};
use chrono::prelude::*;
use fxhash::{FxBuildHasher, FxHashMap};
use indexmap::IndexMap;
use log::warn;
use memmap2::Mmap;
use netidx::{
    chars::Chars,
    pack::{decode_varint, encode_varint, varint_len, Pack, PackError},
    path::Path,
    pool::{Pool, Pooled},
    subscriber::{Event, FromValue, Value},
};
use netidx_derive::Pack;
use packed_struct::PackedStruct;
use std::{
    self,
    cmp::max,
    collections::VecDeque,
    error, fmt,
    fs::OpenOptions,
    mem,
    ops::{Bound, RangeBounds},
    path::Path as FilePath,
    str::FromStr,
};

#[derive(Debug, Clone)]
pub struct FileHeader {
    pub compressed: bool,
    pub indexed: bool,
    pub version: u32,
    pub committed: u64,
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
        buf.put_u32(
            ((self.compressed as u32) << 31)
                | ((self.indexed as u32) << 30)
                | FILE_VERSION,
        );
        buf.put_u64(self.committed);
        Ok(())
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        for byte in FILE_MAGIC {
            if buf.get_u8() != *byte {
                return Err(PackError::InvalidFormat);
            }
        }
        let v = buf.get_u32();
        let version = v & 0x3FFF_FFFF;
        let compressed = (v & 0x8000_0000) > 0;
        let indexed = (v & 0x4000_0000) > 0;
        let committed = buf.get_u64();
        Ok(FileHeader { compressed, indexed, version, committed })
    }
}

/// this is only present if the file has compressed batches
#[derive(Pack)]
pub struct CompressionHeader {
    pub dictionary: Vec<u8>,
}

#[derive(PrimitiveEnum, Debug, Clone, Copy)]
pub enum RecordTyp {
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
#[derive(PackedStruct, Debug, Clone, Copy)]
#[packed_struct(bit_numbering = "msb0", size_bytes = "8")]
pub struct RecordHeader {
    // the record type
    #[packed_field(bits = "0:1", size_bits = "2", ty = "enum")]
    pub record_type: RecordTyp,
    // the record length, up to MAX_RECORD_LEN, not including this header
    #[packed_field(bits = "2:33", size_bits = "32", endian = "msb")]
    pub record_length: u32,
    // microsecond offset from last timestamp record, up to MAX_TIMESTAMP
    #[packed_field(bits = "34:63", size_bits = "30", endian = "msb")]
    pub timestamp: u32,
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

#[derive(Debug, Clone, Pack)]
pub struct RecordIndex {
    pub index: Vec<Id>,
}

#[derive(Debug, Clone, Copy, PartialEq, Hash, Eq, PartialOrd, Ord)]
pub struct Id(u32);

impl Pack for Id {
    fn encoded_len(&self) -> usize {
        varint_len(self.0 as u64)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(encode_varint(self.0 as u64, buf))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        Ok(Id(decode_varint(buf)? as u32))
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

#[derive(Debug, Clone, PartialEq, PartialOrd)]
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

#[derive(Debug, Clone, Copy, Pack)]
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

impl Into<Value> for Seek {
    fn into(self) -> Value {
        self.to_string().into()
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
    static ref PM_POOL: Pool<Vec<PathMapping>> = Pool::new(10, 100_000);
    pub static ref BATCH_POOL: Pool<Vec<BatchItem>> = Pool::new(10, 100_000);
    pub(crate) static ref CURSOR_BATCH_POOL: Pool<VecDeque<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>> =
        Pool::new(100, 10_000);
    static ref IDX_POOL: Pool<Vec<(Id, Path)>> = Pool::new(10, 100_000);
    pub(crate) static ref IMG_POOL: Pool<FxHashMap<Id, Event>> = Pool::new(100, 10_000);
    static ref EPSILON: chrono::Duration = chrono::Duration::microseconds(1);
    static ref TO_READ_POOL: Pool<Vec<usize>> = Pool::new(10, 100_000);
}

#[derive(Debug, Clone, Copy)]
enum Timestamp {
    NewBasis(DateTime<Utc>),
    Offset(u32),
}

impl Timestamp {
    pub fn offset(&self) -> u32 {
        match self {
            Timestamp::NewBasis(_) => 0,
            Timestamp::Offset(off) => *off,
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
                        Timestamp::Offset(self.offset)
                    } else {
                        let basis = self.update_basis(basis + Duration::microseconds(1));
                        Timestamp::NewBasis(basis)
                    }
                }
                Some(off) if (self.offset as i64 + off) <= MAX_TIMESTAMP as i64 => {
                    self.offset += off as u32;
                    Timestamp::Offset(self.offset)
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

    /// create a cursor with pre initialized start, end, and pos. pos
    /// will be adjusted so it falls within the bounds of start and
    /// end.
    pub fn create_from(
        start: Bound<DateTime<Utc>>,
        end: Bound<DateTime<Utc>>,
        pos: Option<DateTime<Utc>>,
    ) -> Self {
        let mut t = Self::new();
        t.set_start(start);
        t.set_end(end);
        if let Some(pos) = pos {
            t.set_current(pos);
        }
        t
    }

    pub fn reset(&mut self) {
        self.current = None;
    }

    /// return true if the cursor is at the start. If the start is
    /// unbounded then this will always return false.
    ///
    /// if the cursor doesn't have a position then this method will
    /// return false.
    pub fn at_start(&self) -> bool {
        match self.start {
            Bound::Unbounded => false,
            Bound::Excluded(st) => {
                self.current.map(|pos| st + *EPSILON == pos).unwrap_or(false)
            }
            Bound::Included(st) => self.current.map(|pos| st == pos).unwrap_or(false),
        }
    }

    /// return true if the cursor is at the end. If the end is
    /// unbounded then this will always return false.
    ///
    /// if the cursor doesn't have a position then this method will
    /// return false.
    pub fn at_end(&self) -> bool {
        match self.end {
            Bound::Unbounded => false,
            Bound::Excluded(en) => {
                self.current.map(|pos| en - *EPSILON == pos).unwrap_or(false)
            }
            Bound::Included(en) => self.current.map(|pos| en == pos).unwrap_or(false),
        }
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

    pub fn start(&self) -> &Bound<DateTime<Utc>> {
        &self.start
    }

    pub fn end(&self) -> &Bound<DateTime<Utc>> {
        &self.end
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
    id_by_path: &mut FxHashMap<Path, Id>,
    mut imagemap: Option<&mut ArrayMap<DateTime<Utc>, usize>>,
    mut deltamap: Option<&mut ArrayMap<DateTime<Utc>, usize>>,
    time_basis: &mut DateTime<Utc>,
    max_id: &mut u32,
    end: usize,
    start_pos: usize,
    buf: &mut impl Buf,
) -> Result<usize> {
    let total_size = buf.remaining();
    let res = loop {
        let pos = start_pos + (total_size - buf.remaining());
        if pos >= end {
            break Ok(pos);
        }
        if buf.remaining() < <RecordHeader as Pack>::const_encoded_len().unwrap() {
            break Ok(pos);
        }
        let rh = RecordHeader::decode(buf)
            .map_err(Error::from)
            .context("read record header")?;
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
    };
    if let Some(deltamap) = deltamap {
        deltamap.shrink_to_fit();
    }
    if let Some(imagemap) = imagemap {
        imagemap.shrink_to_fit();
    }
    res
}

fn scan_header(buf: &mut impl Buf) -> Result<FileHeader> {
    // check the file header
    if buf.remaining() < <FileHeader as Pack>::const_encoded_len().unwrap() {
        bail!("invalid file header: too short")
    }
    let header = <FileHeader as Pack>::decode(buf)
        .map_err(Error::from)
        .context("read file header")?;
    // this is the first version, so no upgrading can be done yet
    if header.version != FILE_VERSION {
        bail!("file version mismatch, expected {} got {}", header.version, FILE_VERSION)
    }
    Ok(header)
}

/// just read the file header directly from the file, bypass locking,
/// and don't touch any other part of the file.
pub fn read_file_header(path: impl AsRef<FilePath>) -> Result<FileHeader> {
    let file = OpenOptions::new().read(true).open(path.as_ref()).context("open file")?;
    let mmap = unsafe { Mmap::map(&file)? };
    scan_header(&mut &mmap[..])
}

fn scan_file(
    indexed: &mut bool,
    compressed: &mut Option<CompressionHeader>,
    path_by_id: &mut IndexMap<Id, Path, FxBuildHasher>,
    id_by_path: &mut FxHashMap<Path, Id>,
    imagemap: Option<&mut ArrayMap<DateTime<Utc>, usize>>,
    deltamap: Option<&mut ArrayMap<DateTime<Utc>, usize>>,
    time_basis: &mut DateTime<Utc>,
    max_id: &mut u32,
    buf: &mut impl Buf,
) -> Result<usize> {
    let total_bytes = buf.remaining();
    let header = scan_header(buf).context("scan header")?;
    if header.compressed {
        *compressed =
            Some(CompressionHeader::decode(buf).context("read compression header")?);
    }
    *indexed = header.indexed;
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

use crate::error::{clear_error, set_error, NetidxError};
use netidx::subscriber::Value;
use std::os::raw::c_char;
use std::ptr;

/// Discriminant tag for Value, matching the Rust enum's discriminants.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    U8 = 0x0000_0001,
    I8 = 0x0000_0002,
    U16 = 0x0000_0004,
    I16 = 0x0000_0008,
    U32 = 0x0000_0010,
    V32 = 0x0000_0020,
    I32 = 0x0000_0040,
    Z32 = 0x0000_0080,
    U64 = 0x0000_0100,
    V64 = 0x0000_0200,
    I64 = 0x0000_0400,
    Z64 = 0x0000_0800,
    F32 = 0x0000_1000,
    F64 = 0x0000_2000,
    Bool = 0x0000_4000,
    Null = 0x0000_8000,
    String = 0x8000_0000,
    Bytes = 0x4000_0000,
    Error = 0x2000_0000,
    Array = 0x1000_0000,
    Map = 0x0800_0000,
    Decimal = 0x0400_0000,
    DateTime = 0x0200_0000,
    Duration = 0x0100_0000,
    Abstract = 0x0080_0000,
}

impl From<&Value> for ValueType {
    fn from(v: &Value) -> Self {
        match v {
            Value::U8(_) => ValueType::U8,
            Value::I8(_) => ValueType::I8,
            Value::U16(_) => ValueType::U16,
            Value::I16(_) => ValueType::I16,
            Value::U32(_) => ValueType::U32,
            Value::V32(_) => ValueType::V32,
            Value::I32(_) => ValueType::I32,
            Value::Z32(_) => ValueType::Z32,
            Value::U64(_) => ValueType::U64,
            Value::V64(_) => ValueType::V64,
            Value::I64(_) => ValueType::I64,
            Value::Z64(_) => ValueType::Z64,
            Value::F32(_) => ValueType::F32,
            Value::F64(_) => ValueType::F64,
            Value::Bool(_) => ValueType::Bool,
            Value::Null => ValueType::Null,
            Value::String(_) => ValueType::String,
            Value::Bytes(_) => ValueType::Bytes,
            Value::Error(_) => ValueType::Error,
            Value::Array(_) => ValueType::Array,
            Value::Map(_) => ValueType::Map,
            Value::Decimal(_) => ValueType::Decimal,
            Value::DateTime(_) => ValueType::DateTime,
            Value::Duration(_) => ValueType::Duration,
            Value::Abstract(_) => ValueType::Abstract,
        }
    }
}

/// Opaque handle to a Value on the Rust heap.
pub struct NetidxValue {
    pub(crate) inner: Value,
}

// --- Constructors ---

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_null() -> *mut NetidxValue {
    Box::into_raw(Box::new(NetidxValue { inner: Value::Null }))
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_u8(v: u8) -> *mut NetidxValue {
    Box::into_raw(Box::new(NetidxValue { inner: Value::U8(v) }))
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_i8(v: i8) -> *mut NetidxValue {
    Box::into_raw(Box::new(NetidxValue { inner: Value::I8(v) }))
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_u16(v: u16) -> *mut NetidxValue {
    Box::into_raw(Box::new(NetidxValue { inner: Value::U16(v) }))
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_i16(v: i16) -> *mut NetidxValue {
    Box::into_raw(Box::new(NetidxValue { inner: Value::I16(v) }))
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_u32(v: u32) -> *mut NetidxValue {
    Box::into_raw(Box::new(NetidxValue { inner: Value::U32(v) }))
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_v32(v: u32) -> *mut NetidxValue {
    Box::into_raw(Box::new(NetidxValue { inner: Value::V32(v) }))
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_i32(v: i32) -> *mut NetidxValue {
    Box::into_raw(Box::new(NetidxValue { inner: Value::I32(v) }))
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_z32(v: i32) -> *mut NetidxValue {
    Box::into_raw(Box::new(NetidxValue { inner: Value::Z32(v) }))
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_u64(v: u64) -> *mut NetidxValue {
    Box::into_raw(Box::new(NetidxValue { inner: Value::U64(v) }))
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_v64(v: u64) -> *mut NetidxValue {
    Box::into_raw(Box::new(NetidxValue { inner: Value::V64(v) }))
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_i64(v: i64) -> *mut NetidxValue {
    Box::into_raw(Box::new(NetidxValue { inner: Value::I64(v) }))
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_z64(v: i64) -> *mut NetidxValue {
    Box::into_raw(Box::new(NetidxValue { inner: Value::Z64(v) }))
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_f32(v: f32) -> *mut NetidxValue {
    Box::into_raw(Box::new(NetidxValue { inner: Value::F32(v) }))
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_f64(v: f64) -> *mut NetidxValue {
    Box::into_raw(Box::new(NetidxValue { inner: Value::F64(v) }))
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_bool(v: bool) -> *mut NetidxValue {
    Box::into_raw(Box::new(NetidxValue { inner: Value::Bool(v) }))
}

/// Create a Value::String from a UTF-8 byte range.
/// `data` does not need to be null-terminated; `len` bytes are used.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_value_string(
    data: *const c_char,
    len: usize,
) -> *mut NetidxValue {
    let s = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(data as *const u8, len)) };
    Box::into_raw(Box::new(NetidxValue {
        inner: Value::String(arcstr::ArcStr::from(s)),
    }))
}

/// Create a Value::Bytes from a raw byte range.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_value_bytes(
    data: *const u8,
    len: usize,
) -> *mut NetidxValue {
    let slice = unsafe { std::slice::from_raw_parts(data, len) };
    Box::into_raw(Box::new(NetidxValue {
        inner: Value::from(bytes::Bytes::copy_from_slice(slice)),
    }))
}

/// Create a Value::Error wrapping another Value. Consumes `inner`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_value_error(inner: *mut NetidxValue) -> *mut NetidxValue {
    let inner_val = unsafe { Box::from_raw(inner) }.inner;
    Box::into_raw(Box::new(NetidxValue {
        inner: Value::Error(triomphe::Arc::new(inner_val)),
    }))
}

/// Create a Value::Array from an array of Value pointers. Consumes all values.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_value_array(
    values: *const *mut NetidxValue,
    len: usize,
) -> *mut NetidxValue {
    let slice = unsafe { std::slice::from_raw_parts(values, len) };
    let vec: Vec<Value> = slice.iter().map(|p| unsafe { Box::from_raw(*p) }.inner).collect();
    Box::into_raw(Box::new(NetidxValue { inner: Value::from(vec) }))
}

/// Create a Value::Map from parallel arrays of keys and values. Consumes all keys and values.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_value_map(
    keys: *const *mut NetidxValue,
    values: *const *mut NetidxValue,
    len: usize,
) -> *mut NetidxValue {
    let keys_slice = unsafe { std::slice::from_raw_parts(keys, len) };
    let values_slice = unsafe { std::slice::from_raw_parts(values, len) };
    let iter = keys_slice.iter().zip(values_slice.iter()).map(|(k, v)| {
        let k = unsafe { Box::from_raw(*k) }.inner;
        let v = unsafe { Box::from_raw(*v) }.inner;
        (k, v)
    });
    let map = immutable_chunkmap::map::Map::from_iter(iter);
    Box::into_raw(Box::new(NetidxValue { inner: Value::Map(map) }))
}

/// Get the number of entries in a Map. Returns 0 if not a Map.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_map_len(val: *const NetidxValue) -> usize {
    match unsafe { &(*val).inner } {
        Value::Map(m) => m.len(),
        _ => 0,
    }
}

/// Look up a key in a Map, returning a cloned value. Borrows both arguments.
/// Returns NULL if not a Map or key not found.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_map_get_clone(
    val: *const NetidxValue,
    key: *const NetidxValue,
) -> *mut NetidxValue {
    match unsafe { &(*val).inner } {
        Value::Map(m) => match m.get(&unsafe { &*key }.inner) {
            Some(v) => Box::into_raw(Box::new(NetidxValue { inner: v.clone() })),
            None => ptr::null_mut(),
        },
        _ => ptr::null_mut(),
    }
}

/// Get all entries from a Map as parallel arrays of cloned keys and values.
/// Caller must free each array with netidx_value_array_free and each
/// value with netidx_value_destroy. Returns false if not a Map.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_value_map_entries_clone(
    val: *const NetidxValue,
    out_keys: *mut *mut *mut NetidxValue,
    out_values: *mut *mut *mut NetidxValue,
    out_len: *mut usize,
) -> bool {
    match unsafe { &(*val).inner } {
        Value::Map(m) => {
            let len = m.len();
            if len == 0 {
                unsafe {
                    *out_keys = ptr::null_mut();
                    *out_values = ptr::null_mut();
                    *out_len = 0;
                }
                return true;
            }
            let mut keys_vec: Vec<*mut NetidxValue> = Vec::with_capacity(len);
            let mut vals_vec: Vec<*mut NetidxValue> = Vec::with_capacity(len);
            for (k, v) in m.into_iter() {
                keys_vec.push(Box::into_raw(Box::new(NetidxValue { inner: k.clone() })));
                vals_vec.push(Box::into_raw(Box::new(NetidxValue { inner: v.clone() })));
            }
            unsafe {
                *out_len = len;
                *out_keys = Box::into_raw(keys_vec.into_boxed_slice()) as *mut *mut NetidxValue;
                *out_values = Box::into_raw(vals_vec.into_boxed_slice()) as *mut *mut NetidxValue;
            }
            true
        }
        _ => false,
    }
}

/// Free an array of pointers returned by map_entries_clone, update receivers, etc.
/// Individual elements must be destroyed separately with their respective destroy functions.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_value_array_free(arr: *mut *mut NetidxValue, len: usize) {
    if !arr.is_null() && len > 0 {
        drop(unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(arr, len)) });
    }
}

/// Create a Value::Decimal by parsing a string like "3.14".
/// Returns NULL and sets error on parse failure.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_value_decimal(
    data: *const c_char,
    len: usize,
    err: *mut NetidxError,
) -> *mut NetidxValue {
    unsafe { clear_error(err) };
    let s = unsafe {
        std::str::from_utf8_unchecked(std::slice::from_raw_parts(data as *const u8, len))
    };
    match s.parse::<rust_decimal::Decimal>() {
        Ok(d) => Box::into_raw(Box::new(NetidxValue {
            inner: Value::Decimal(triomphe::Arc::new(d)),
        })),
        Err(e) => {
            unsafe { set_error(err, anyhow::anyhow!("{}", e)) };
            ptr::null_mut()
        }
    }
}

/// Get the string representation of a Decimal value.
/// Returns an allocated string that must be freed with netidx_str_free.
/// Returns false if not a Decimal.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_value_get_decimal_string(
    val: *const NetidxValue,
    data: *mut *mut c_char,
    len: *mut usize,
) -> bool {
    match unsafe { &(*val).inner } {
        Value::Decimal(d) => {
            let s = d.to_string();
            let cstr = std::ffi::CString::new(s).unwrap_or_default();
            unsafe {
                *len = cstr.as_bytes().len();
                *data = cstr.into_raw();
            }
            true
        }
        _ => false,
    }
}

/// Create a Value::DateTime from a UNIX timestamp (seconds since epoch + nanoseconds).
#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_datetime(secs: i64, nsecs: u32) -> *mut NetidxValue {
    let dt = chrono::DateTime::from_timestamp(secs, nsecs)
        .unwrap_or_else(|| chrono::DateTime::UNIX_EPOCH);
    Box::into_raw(Box::new(NetidxValue {
        inner: Value::DateTime(triomphe::Arc::new(dt)),
    }))
}

/// Get the UNIX timestamp components of a DateTime value.
/// Returns false if not a DateTime.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_value_get_datetime(
    val: *const NetidxValue,
    secs: *mut i64,
    nsecs: *mut u32,
) -> bool {
    match unsafe { &(*val).inner } {
        Value::DateTime(dt) => {
            unsafe {
                *secs = dt.timestamp();
                *nsecs = dt.timestamp_subsec_nanos();
            }
            true
        }
        _ => false,
    }
}

/// Create a Value::Duration from seconds + nanoseconds.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_duration(secs: u64, nsecs: u32) -> *mut NetidxValue {
    Box::into_raw(Box::new(NetidxValue {
        inner: Value::Duration(triomphe::Arc::new(std::time::Duration::new(secs, nsecs))),
    }))
}

/// Get the components of a Duration value.
/// Returns false if not a Duration.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_value_get_duration(
    val: *const NetidxValue,
    secs: *mut u64,
    nsecs: *mut u32,
) -> bool {
    match unsafe { &(*val).inner } {
        Value::Duration(d) => {
            unsafe {
                *secs = d.as_secs();
                *nsecs = d.subsec_nanos();
            }
            true
        }
        _ => false,
    }
}

/// Get the UUID of an Abstract value as 16 raw bytes.
/// Returns false if not an Abstract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_value_abstract_id(
    val: *const NetidxValue,
    out: *mut [u8; 16],
) -> bool {
    match unsafe { &(*val).inner } {
        Value::Abstract(a) => {
            unsafe { *out = *a.id().as_bytes() };
            true
        }
        _ => false,
    }
}

/// Pack-encode an Abstract value to bytes.
/// Sets *out_data and *out_len. Caller must free with netidx_bytes_free.
/// Returns false if not an Abstract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_value_abstract_encode(
    val: *const NetidxValue,
    out_data: *mut *mut u8,
    out_len: *mut usize,
) -> bool {
    use netidx_core::pack::Pack;
    match unsafe { &(*val).inner } {
        Value::Abstract(a) => {
            let len = a.encoded_len();
            let mut buf = Vec::with_capacity(len);
            if a.encode(&mut buf).is_err() {
                return false;
            }
            let mut boxed = buf.into_boxed_slice();
            unsafe {
                *out_len = boxed.len();
                *out_data = boxed.as_mut_ptr();
            }
            std::mem::forget(boxed);
            true
        }
        _ => false,
    }
}

/// Decode a Value::Abstract from Pack-encoded bytes.
/// The type must be registered in the process. Returns NULL on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_value_abstract_decode(
    data: *const u8,
    len: usize,
    err: *mut NetidxError,
) -> *mut NetidxValue {
    use netidx_core::pack::Pack;
    use netidx::protocol::value::Abstract;
    unsafe { clear_error(err) };
    let slice = unsafe { std::slice::from_raw_parts(data, len) };
    let mut cursor = std::io::Cursor::new(slice);
    match Abstract::decode(&mut cursor) {
        Ok(a) => Box::into_raw(Box::new(NetidxValue { inner: Value::Abstract(a) })),
        Err(e) => {
            unsafe { set_error(err, anyhow::anyhow!("{}", e)) };
            ptr::null_mut()
        }
    }
}

/// Free bytes allocated by netidx_value_abstract_encode.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_bytes_free(data: *mut u8, len: usize) {
    if !data.is_null() && len > 0 {
        drop(unsafe { Box::from_raw(std::slice::from_raw_parts_mut(data, len)) });
    }
}

// --- Type Query ---

/// Get the type tag of a value.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_type(val: *const NetidxValue) -> ValueType {
    ValueType::from(&unsafe { &*val }.inner)
}

// --- Scalar Accessors ---
// Return true on success, false on type mismatch.

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_get_u8(val: *const NetidxValue, out: *mut u8) -> bool {
    match unsafe { &(*val).inner } {
        Value::U8(v) => { unsafe { *out = *v }; true }
        _ => false,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_get_i8(val: *const NetidxValue, out: *mut i8) -> bool {
    match unsafe { &(*val).inner } {
        Value::I8(v) => { unsafe { *out = *v }; true }
        _ => false,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_get_u16(val: *const NetidxValue, out: *mut u16) -> bool {
    match unsafe { &(*val).inner } {
        Value::U16(v) => { unsafe { *out = *v }; true }
        _ => false,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_get_i16(val: *const NetidxValue, out: *mut i16) -> bool {
    match unsafe { &(*val).inner } {
        Value::I16(v) => { unsafe { *out = *v }; true }
        _ => false,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_get_u32(val: *const NetidxValue, out: *mut u32) -> bool {
    match unsafe { &(*val).inner } {
        Value::U32(v) | Value::V32(v) => { unsafe { *out = *v }; true }
        _ => false,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_get_i32(val: *const NetidxValue, out: *mut i32) -> bool {
    match unsafe { &(*val).inner } {
        Value::I32(v) | Value::Z32(v) => { unsafe { *out = *v }; true }
        _ => false,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_get_u64(val: *const NetidxValue, out: *mut u64) -> bool {
    match unsafe { &(*val).inner } {
        Value::U64(v) | Value::V64(v) => { unsafe { *out = *v }; true }
        _ => false,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_get_i64(val: *const NetidxValue, out: *mut i64) -> bool {
    match unsafe { &(*val).inner } {
        Value::I64(v) | Value::Z64(v) => { unsafe { *out = *v }; true }
        _ => false,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_get_f32(val: *const NetidxValue, out: *mut f32) -> bool {
    match unsafe { &(*val).inner } {
        Value::F32(v) => { unsafe { *out = *v }; true }
        _ => false,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_get_f64(val: *const NetidxValue, out: *mut f64) -> bool {
    match unsafe { &(*val).inner } {
        Value::F64(v) => { unsafe { *out = *v }; true }
        _ => false,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_get_bool(val: *const NetidxValue, out: *mut bool) -> bool {
    match unsafe { &(*val).inner } {
        Value::Bool(v) => { unsafe { *out = *v }; true }
        _ => false,
    }
}

/// Get string data. Borrows from the Value — valid while the Value lives.
/// Returns false if not a String variant.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_get_string(
    val: *const NetidxValue,
    data: *mut *const c_char,
    len: *mut usize,
) -> bool {
    match unsafe { &(*val).inner } {
        Value::String(s) => {
            unsafe {
                *data = s.as_ptr() as *const c_char;
                *len = s.len();
            }
            true
        }
        _ => false,
    }
}

/// Get bytes data. Borrows from the Value — valid while the Value lives.
/// Returns false if not a Bytes variant.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_get_bytes(
    val: *const NetidxValue,
    data: *mut *const u8,
    len: *mut usize,
) -> bool {
    match unsafe { &(*val).inner } {
        Value::Bytes(b) => {
            unsafe {
                *data = b.as_ref().as_ptr();
                *len = b.as_ref().len();
            }
            true
        }
        _ => false,
    }
}

/// Clone the inner value of an Error variant. Returns a new owned Value.
/// Returns NULL if not an Error.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_get_error_clone(
    val: *const NetidxValue,
) -> *mut NetidxValue {
    match unsafe { &(*val).inner } {
        Value::Error(arc) => {
            Box::into_raw(Box::new(NetidxValue { inner: (**arc).clone() }))
        }
        _ => ptr::null_mut(),
    }
}

/// Get array length. Returns 0 if not an Array.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_array_len(val: *const NetidxValue) -> usize {
    match unsafe { &(*val).inner } {
        Value::Array(a) => a.len(),
        _ => 0,
    }
}

/// Clone an element from an Array. Returns a new owned Value.
/// Returns NULL if not an Array or index out of bounds.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_array_get_clone(
    val: *const NetidxValue,
    index: usize,
) -> *mut NetidxValue {
    match unsafe { &(*val).inner } {
        Value::Array(a) => {
            if index < a.len() {
                Box::into_raw(Box::new(NetidxValue { inner: a[index].clone() }))
            } else {
                ptr::null_mut()
            }
        }
        _ => ptr::null_mut(),
    }
}

/// Get the display string representation of a Value.
/// The returned string is newly allocated and must be freed with netidx_str_free.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_to_string(
    val: *const NetidxValue,
    data: *mut *mut c_char,
    len: *mut usize,
) {
    let s = format!("{}", unsafe { &(*val).inner });
    let cstr = std::ffi::CString::new(s).unwrap_or_default();
    unsafe {
        *len = cstr.as_bytes().len();
        *data = cstr.into_raw();
    }
}

/// Free a string returned by netidx_value_to_string.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_str_free(s: *mut c_char) {
    if !s.is_null() {
        drop(unsafe { std::ffi::CString::from_raw(s) });
    }
}

// --- Lifecycle ---

/// Clone a Value. Returns a new owned handle.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_clone(val: *const NetidxValue) -> *mut NetidxValue {
    Box::into_raw(Box::new(NetidxValue {
        inner: unsafe { &*val }.inner.clone(),
    }))
}

/// Destroy a Value, freeing its memory.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_value_destroy(val: *mut NetidxValue) {
    if !val.is_null() {
        drop(unsafe { Box::from_raw(val) });
    }
}

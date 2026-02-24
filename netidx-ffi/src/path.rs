use netidx::path::Path;
use std::os::raw::c_char;

/// Opaque handle to a Path.
pub struct NetidxPath {
    pub(crate) inner: Path,
}

/// Create a Path from a UTF-8 byte range. Does not need null termination.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_path_new(
    data: *const c_char,
    len: usize,
) -> *mut NetidxPath {
    let s =
        unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(data as *const u8, len)) };
    Box::into_raw(Box::new(NetidxPath { inner: Path::from(String::from(s)) }))
}

/// Append a segment to a path, returning a new Path. Does not consume `base`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_path_append(
    base: *const NetidxPath,
    segment: *const c_char,
    segment_len: usize,
) -> *mut NetidxPath {
    let seg = unsafe {
        std::str::from_utf8_unchecked(std::slice::from_raw_parts(segment as *const u8, segment_len))
    };
    let new_path = unsafe { &*base }.inner.append(seg);
    Box::into_raw(Box::new(NetidxPath { inner: new_path }))
}

/// Get the string representation of a path. Borrows from the Path handle.
/// Valid while the Path lives.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_path_as_str(
    path: *const NetidxPath,
    data: *mut *const c_char,
    len: *mut usize,
) {
    let s: &str = unsafe { &*path }.inner.as_ref();
    unsafe {
        *data = s.as_ptr() as *const c_char;
        *len = s.len();
    }
}

/// Get the number of levels (components) in the path.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_path_levels(path: *const NetidxPath) -> usize {
    Path::levels(&unsafe { &*path }.inner)
}

/// Get the basename (last component) of the path.
/// Returns false if the path has no basename (e.g. "/").
#[unsafe(no_mangle)]
pub extern "C" fn netidx_path_basename(
    path: *const NetidxPath,
    data: *mut *const c_char,
    len: *mut usize,
) -> bool {
    match Path::basename(&unsafe { &*path }.inner) {
        Some(s) => {
            unsafe {
                *data = s.as_ptr() as *const c_char;
                *len = s.len();
            }
            true
        }
        None => false,
    }
}

/// Get the dirname (parent path) of the path.
/// Returns false if the path has no parent (e.g. "/").
#[unsafe(no_mangle)]
pub extern "C" fn netidx_path_dirname(
    path: *const NetidxPath,
    data: *mut *const c_char,
    len: *mut usize,
) -> bool {
    match Path::dirname(&unsafe { &*path }.inner) {
        Some(s) => {
            unsafe {
                *data = s.as_ptr() as *const c_char;
                *len = s.len();
            }
            true
        }
        None => false,
    }
}

/// Clone a Path.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_path_clone(path: *const NetidxPath) -> *mut NetidxPath {
    Box::into_raw(Box::new(NetidxPath { inner: unsafe { &*path }.inner.clone() }))
}

/// Destroy a Path.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_path_destroy(path: *mut NetidxPath) {
    if !path.is_null() {
        drop(unsafe { Box::from_raw(path) });
    }
}

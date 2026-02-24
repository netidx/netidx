use crate::error::{clear_error, set_error, NetidxError};
use netidx::config::Config;
use std::os::raw::c_char;

/// Opaque handle to a Config.
pub struct NetidxConfig {
    pub(crate) inner: Config,
}

/// Load config from the default platform-specific location.
/// Returns NULL on failure.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_config_load_default(err: *mut NetidxError) -> *mut NetidxConfig {
    unsafe { clear_error(err) };
    match Config::load_default() {
        Ok(cfg) => Box::into_raw(Box::new(NetidxConfig { inner: cfg })),
        Err(e) => {
            unsafe { set_error(err, e) };
            std::ptr::null_mut()
        }
    }
}

/// Load config from a specific file path.
/// `path` is a UTF-8 string of `len` bytes (not null-terminated).
/// Returns NULL on failure.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_config_load(
    path: *const c_char,
    path_len: usize,
    err: *mut NetidxError,
) -> *mut NetidxConfig {
    unsafe { clear_error(err) };
    let s = unsafe {
        std::str::from_utf8_unchecked(std::slice::from_raw_parts(path as *const u8, path_len))
    };
    match Config::load(s) {
        Ok(cfg) => Box::into_raw(Box::new(NetidxConfig { inner: cfg })),
        Err(e) => {
            unsafe { set_error(err, e) };
            std::ptr::null_mut()
        }
    }
}

/// Clone a Config.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_config_clone(cfg: *const NetidxConfig) -> *mut NetidxConfig {
    Box::into_raw(Box::new(NetidxConfig { inner: unsafe { &*cfg }.inner.clone() }))
}

/// Destroy a Config.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_config_destroy(cfg: *mut NetidxConfig) {
    if !cfg.is_null() {
        drop(unsafe { Box::from_raw(cfg) });
    }
}

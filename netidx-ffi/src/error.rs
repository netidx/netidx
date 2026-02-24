use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr;

/// Opaque error type. NULL means no error.
/// The caller must free with `netidx_error_free`.
#[repr(C)]
pub struct NetidxError {
    msg: *mut c_char,
}

impl NetidxError {
    fn none() -> Self {
        NetidxError { msg: ptr::null_mut() }
    }
}

/// Set the error from an anyhow::Error. Returns false (the FFI "failure" value).
pub(crate) unsafe fn set_error(err: *mut NetidxError, e: anyhow::Error) -> bool {
    if !err.is_null() {
        unsafe { clear_error(err) };
        let msg =
            CString::new(format!("{:#}", e)).unwrap_or_else(|_| CString::new("error").unwrap());
        unsafe {
            (*err).msg = msg.into_raw();
        }
    }
    false
}

/// Clear the error pointer before a new call
pub(crate) unsafe fn clear_error(err: *mut NetidxError) {
    if !err.is_null() {
        unsafe {
            if !(*err).msg.is_null() {
                drop(CString::from_raw((*err).msg));
                (*err).msg = ptr::null_mut();
            }
        }
    }
}

/// Create a zeroed error for use from C
#[unsafe(no_mangle)]
pub extern "C" fn netidx_error_init() -> NetidxError {
    NetidxError::none()
}

/// Get the error message. Returns NULL if no error. The returned pointer
/// is valid until the next call to `netidx_error_free`.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_error_message(err: *const NetidxError) -> *const c_char {
    if err.is_null() {
        return ptr::null();
    }
    unsafe { (*err).msg }
}

/// Free the error message. Safe to call on a zeroed error.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_error_free(err: *mut NetidxError) {
    unsafe { clear_error(err) }
}

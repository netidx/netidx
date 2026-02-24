use crate::error::{clear_error, set_error, NetidxError};
use tokio::runtime::Runtime;

/// Opaque handle to a tokio runtime.
pub struct NetidxRuntime {
    pub(crate) rt: Runtime,
}

/// Create a new tokio multi-threaded runtime.
/// `worker_threads`: number of worker threads, 0 = use tokio default (number of cores).
/// Returns NULL on failure.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_runtime_new(
    worker_threads: usize,
    err: *mut NetidxError,
) -> *mut NetidxRuntime {
    unsafe { clear_error(err) };
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if worker_threads > 0 {
        builder.worker_threads(worker_threads);
    }
    match builder.build() {
        Ok(rt) => Box::into_raw(Box::new(NetidxRuntime { rt })),
        Err(e) => {
            unsafe { set_error(err, e.into()) };
            std::ptr::null_mut()
        }
    }
}

/// Destroy the runtime. All outstanding work will be cancelled.
/// Consumes the handle.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_runtime_destroy(rt: *mut NetidxRuntime) {
    if !rt.is_null() {
        drop(unsafe { Box::from_raw(rt) });
    }
}

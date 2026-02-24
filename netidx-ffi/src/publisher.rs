use crate::{
    config::NetidxConfig,
    error::{clear_error, set_error, NetidxError},
    path::NetidxPath,
    runtime::NetidxRuntime,
    timeout_from_millis,
    value::NetidxValue,
};
use futures::channel::mpsc;
use futures::prelude::*;
use netidx::publisher::{
    BindCfg, Publisher, PublisherBuilder, UpdateBatch, Val, WriteRequest,
};
use poolshark::global::GPooled;
use std::os::raw::c_char;

// --- PublisherBuilder ---

pub struct NetidxPublisherBuilder {
    inner: PublisherBuilder,
}

/// Create a new PublisherBuilder. Does not consume `cfg`.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_publisher_builder_new(
    cfg: *const NetidxConfig,
) -> *mut NetidxPublisherBuilder {
    let cfg = unsafe { &*cfg }.inner.clone();
    Box::into_raw(Box::new(NetidxPublisherBuilder { inner: PublisherBuilder::new(cfg) }))
}

/// Set the bind config from a string (e.g. "local", "192.168.0.0/24").
/// Returns true on success.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_publisher_builder_bind_cfg(
    pb: *mut NetidxPublisherBuilder,
    bind: *const c_char,
    bind_len: usize,
    err: *mut NetidxError,
) -> bool {
    unsafe { clear_error(err) };
    let s = unsafe {
        std::str::from_utf8_unchecked(std::slice::from_raw_parts(
            bind as *const u8,
            bind_len,
        ))
    };
    match s.parse::<BindCfg>() {
        Ok(cfg) => {
            unsafe { &mut *pb }.inner.bind_cfg(Some(cfg));
            true
        }
        Err(e) => unsafe { set_error(err, e) },
    }
}

/// Set max clients.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_publisher_builder_max_clients(
    pb: *mut NetidxPublisherBuilder,
    max_clients: usize,
) {
    unsafe { &mut *pb }.inner.max_clients(max_clients);
}

/// Set slack (max queued batches per client).
#[unsafe(no_mangle)]
pub extern "C" fn netidx_publisher_builder_slack(
    pb: *mut NetidxPublisherBuilder,
    slack: usize,
) {
    unsafe { &mut *pb }.inner.slack(slack);
}

/// Build the publisher. Consumes the builder. Returns NULL on failure.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_publisher_builder_build(
    rt: *const NetidxRuntime,
    pb: *mut NetidxPublisherBuilder,
    err: *mut NetidxError,
) -> *mut NetidxPublisher {
    unsafe { clear_error(err) };
    let mut builder = unsafe { Box::from_raw(pb) };
    let rt = unsafe { &*rt };
    match rt.rt.block_on(builder.inner.build()) {
        Ok(pub_) => Box::into_raw(Box::new(NetidxPublisher { inner: pub_ })),
        Err(e) => {
            unsafe { set_error(err, e) };
            std::ptr::null_mut()
        }
    }
}

/// Destroy the builder without building (e.g. on error path).
#[unsafe(no_mangle)]
pub extern "C" fn netidx_publisher_builder_destroy(pb: *mut NetidxPublisherBuilder) {
    if !pb.is_null() {
        drop(unsafe { Box::from_raw(pb) });
    }
}

// --- Publisher ---

pub struct NetidxPublisher {
    pub(crate) inner: Publisher,
}

/// Publish a value at a path. Consumes `init` (the initial value) and `path`.
/// Returns NULL on failure.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_publisher_publish(
    pub_: *const NetidxPublisher,
    path: *mut NetidxPath,
    init: *mut NetidxValue,
    err: *mut NetidxError,
) -> *mut NetidxPublisherVal {
    unsafe { clear_error(err) };
    let path = unsafe { Box::from_raw(path) }.inner;
    let init = unsafe { Box::from_raw(init) }.inner;
    match unsafe { &*pub_ }.inner.publish(path, init) {
        Ok(val) => Box::into_raw(Box::new(NetidxPublisherVal { inner: val })),
        Err(e) => {
            unsafe { set_error(err, e) };
            std::ptr::null_mut()
        }
    }
}

/// Publish a value with a write channel, so you can receive writes from subscribers.
/// Consumes `init` and `path`. Returns NULL on failure.
/// `channel_buffer`: channel capacity; 0 uses the default.
/// `write_rx` is an out-parameter: on success, receives a write receiver handle.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_publisher_publish_with_writes(
    pub_: *const NetidxPublisher,
    path: *mut NetidxPath,
    init: *mut NetidxValue,
    channel_buffer: usize,
    write_rx: *mut *mut NetidxWriteReceiver,
    err: *mut NetidxError,
) -> *mut NetidxPublisherVal {
    unsafe { clear_error(err) };
    let path = unsafe { Box::from_raw(path) }.inner;
    let init = unsafe { Box::from_raw(init) }.inner;
    let buf =
        if channel_buffer == 0 { crate::FFI_CHANNEL_BUFFER } else { channel_buffer };
    let (tx, rx) = mpsc::channel(buf);
    match unsafe { &*pub_ }.inner.publish_with_flags_and_writes(
        netidx::publisher::PublishFlags::empty(),
        path,
        init,
        Some(tx),
    ) {
        Ok(val) => {
            unsafe {
                *write_rx = Box::into_raw(Box::new(NetidxWriteReceiver { inner: rx }));
            }
            Box::into_raw(Box::new(NetidxPublisherVal { inner: val }))
        }
        Err(e) => {
            unsafe { set_error(err, e) };
            std::ptr::null_mut()
        }
    }
}

/// Register to receive writes for an already-published value.
/// `channel_buffer`: channel capacity; 0 uses the default.
/// Returns a write receiver. The `val` is borrowed (not consumed).
#[unsafe(no_mangle)]
pub extern "C" fn netidx_publisher_writes(
    pub_: *const NetidxPublisher,
    val: *const NetidxPublisherVal,
    channel_buffer: usize,
) -> *mut NetidxWriteReceiver {
    let buf =
        if channel_buffer == 0 { crate::FFI_CHANNEL_BUFFER } else { channel_buffer };
    let (tx, rx) = mpsc::channel(buf);
    unsafe { &*pub_ }.inner.writes(unsafe { &*val }.inner.id(), tx);
    Box::into_raw(Box::new(NetidxWriteReceiver { inner: rx }))
}

/// Start an update batch.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_publisher_start_batch(
    pub_: *const NetidxPublisher,
) -> *mut NetidxUpdateBatch {
    Box::into_raw(Box::new(NetidxUpdateBatch {
        inner: unsafe { &*pub_ }.inner.start_batch(),
    }))
}

/// Wait until all previously published paths are registered with the resolver.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_publisher_flushed(
    rt: *const NetidxRuntime,
    pub_: *const NetidxPublisher,
) {
    unsafe { &*rt }.rt.block_on(unsafe { &*pub_ }.inner.flushed());
}

/// Clone a publisher handle (Arc clone, cheap).
#[unsafe(no_mangle)]
pub extern "C" fn netidx_publisher_clone(
    pub_: *const NetidxPublisher,
) -> *mut NetidxPublisher {
    Box::into_raw(Box::new(NetidxPublisher { inner: unsafe { &*pub_ }.inner.clone() }))
}

/// Destroy a publisher.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_publisher_destroy(pub_: *mut NetidxPublisher) {
    if !pub_.is_null() {
        drop(unsafe { Box::from_raw(pub_) });
    }
}

// --- Val (published value handle) ---

pub struct NetidxPublisherVal {
    inner: Val,
}

/// Update a published value in a batch. Consumes `value`.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_publisher_val_update(
    val: *const NetidxPublisherVal,
    batch: *mut NetidxUpdateBatch,
    value: *mut NetidxValue,
) {
    let v = unsafe { Box::from_raw(value) }.inner;
    unsafe { &*val }.inner.update(&mut unsafe { &mut *batch }.inner, v);
}

/// Update a published value only if it changed. Consumes `value`.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_publisher_val_update_changed(
    val: *const NetidxPublisherVal,
    batch: *mut NetidxUpdateBatch,
    value: *mut NetidxValue,
) {
    let v = unsafe { Box::from_raw(value) }.inner;
    unsafe { &*val }.inner.update_changed(&mut unsafe { &mut *batch }.inner, v);
}

/// Destroy a published value handle. This will unpublish the value.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_publisher_val_destroy(val: *mut NetidxPublisherVal) {
    if !val.is_null() {
        drop(unsafe { Box::from_raw(val) });
    }
}

// --- UpdateBatch ---

pub struct NetidxUpdateBatch {
    inner: UpdateBatch,
}

/// Commit an update batch. Consumes the batch.
/// `timeout_ms`: timeout in milliseconds, -1 = no timeout.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_update_batch_commit(
    rt: *const NetidxRuntime,
    batch: *mut NetidxUpdateBatch,
    timeout_ms: i64,
) {
    let batch = unsafe { Box::from_raw(batch) };
    let timeout = timeout_from_millis(timeout_ms);
    unsafe { &*rt }.rt.block_on(batch.inner.commit(timeout));
}

/// Destroy a batch without committing.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_update_batch_destroy(batch: *mut NetidxUpdateBatch) {
    if !batch.is_null() {
        drop(unsafe { Box::from_raw(batch) });
    }
}

// --- WriteReceiver ---

pub struct NetidxWriteReceiver {
    inner: mpsc::Receiver<GPooled<Vec<WriteRequest>>>,
}

/// A single write request from a subscriber.
pub struct NetidxWriteRequest {
    pub(crate) path: netidx::path::Path,
    pub(crate) value: netidx::subscriber::Value,
}

/// Try to receive write requests without blocking.
/// Returns at most one batch. `out_len` receives the count.
/// Returns NULL if no writes are available. The returned array and its
/// elements must be freed by the caller.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_write_receiver_try_recv(
    rx: *mut NetidxWriteReceiver,
    out_len: *mut usize,
) -> *mut *mut NetidxWriteRequest {
    let rx = unsafe { &mut *rx };
    match rx.inner.try_recv() {
        Ok(mut batch) => {
            let mut results: Vec<*mut NetidxWriteRequest> =
                Vec::with_capacity(batch.len());
            for req in batch.drain(..) {
                results.push(Box::into_raw(Box::new(NetidxWriteRequest {
                    path: req.path,
                    value: req.value,
                })));
            }
            unsafe { *out_len = results.len() };
            let raw = Box::into_raw(results.into_boxed_slice());
            raw as *mut *mut NetidxWriteRequest
        }
        _ => {
            unsafe { *out_len = 0 };
            std::ptr::null_mut()
        }
    }
}

/// Receive write requests, blocking up to `timeout_ms` (-1 = forever).
/// Returns an array of write requests. `out_len` receives the count.
/// Returns NULL on timeout or channel closed.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_write_receiver_recv(
    rt: *const NetidxRuntime,
    rx: *mut NetidxWriteReceiver,
    timeout_ms: i64,
    out_len: *mut usize,
) -> *mut *mut NetidxWriteRequest {
    let rx = unsafe { &mut *rx };
    let rt = unsafe { &*rt };
    let result = crate::block_on_timeout_opt(&rt.rt, timeout_ms, rx.inner.next());
    match result {
        Some(mut batch) => {
            let mut results: Vec<*mut NetidxWriteRequest> = Vec::new();
            for req in batch.drain(..) {
                results.push(Box::into_raw(Box::new(NetidxWriteRequest {
                    path: req.path,
                    value: req.value,
                })));
            }
            unsafe { *out_len = results.len() };
            let raw = Box::into_raw(results.into_boxed_slice());
            raw as *mut *mut NetidxWriteRequest
        }
        None => {
            unsafe { *out_len = 0 };
            std::ptr::null_mut()
        }
    }
}

/// Get the path from a write request. Borrows from the request.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_write_request_path(
    req: *const NetidxWriteRequest,
    data: *mut *const c_char,
    len: *mut usize,
) {
    let s: &str = unsafe { &*req }.path.as_ref();
    unsafe {
        *data = s.as_ptr() as *const c_char;
        *len = s.len();
    }
}

/// Get the value from a write request. Returns a new owned Value (clone).
#[unsafe(no_mangle)]
pub extern "C" fn netidx_write_request_value(
    req: *const NetidxWriteRequest,
) -> *mut NetidxValue {
    Box::into_raw(Box::new(NetidxValue { inner: unsafe { &*req }.value.clone() }))
}

/// Destroy a write request.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_write_request_destroy(req: *mut NetidxWriteRequest) {
    if !req.is_null() {
        drop(unsafe { Box::from_raw(req) });
    }
}

/// Free an array of write request pointers returned by recv/try_recv.
/// Does NOT free the individual requests — call netidx_write_request_destroy on each first.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_write_request_array_free(
    arr: *mut *mut NetidxWriteRequest,
    len: usize,
) {
    if !arr.is_null() && len > 0 {
        drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(arr, len)) });
    }
}

/// Destroy a write receiver.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_write_receiver_destroy(rx: *mut NetidxWriteReceiver) {
    if !rx.is_null() {
        drop(unsafe { Box::from_raw(rx) });
    }
}

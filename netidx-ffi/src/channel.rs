use crate::{
    error::{clear_error, set_error, NetidxError},
    path::NetidxPath,
    publisher::NetidxPublisher,
    runtime::NetidxRuntime,
    subscriber::NetidxSubscriber,
    value::NetidxValue,
    timeout_from_millis,
};
use netidx_protocols::channel;

// --- Channel Server ---

pub struct NetidxChannelListener {
    inner: channel::server::Listener,
}

/// Create a new channel listener.
/// Does not consume `pub_` or `path`.
/// `timeout_ms`: timeout for individual connections, -1 = no timeout.
/// Returns NULL on failure.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_channel_listener_new(
    rt: *const NetidxRuntime,
    pub_: *const NetidxPublisher,
    path: *const NetidxPath,
    timeout_ms: i64,
    err: *mut NetidxError,
) -> *mut NetidxChannelListener {
    unsafe { clear_error(err) };
    let rt = unsafe { &*rt };
    let pub_ = unsafe { &*pub_ };
    let path = unsafe { &*path }.inner.clone();
    let timeout = timeout_from_millis(timeout_ms);
    match rt.rt.block_on(channel::server::Listener::new(&pub_.inner, timeout, path)) {
        Ok(listener) => Box::into_raw(Box::new(NetidxChannelListener { inner: listener })),
        Err(e) => {
            unsafe { set_error(err, e) };
            std::ptr::null_mut()
        }
    }
}

/// Accept a new connection from the listener. Blocks until a client connects.
/// `timeout_ms`: -1 = no timeout.
/// Returns NULL on failure.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_channel_listener_accept(
    rt: *const NetidxRuntime,
    listener: *mut NetidxChannelListener,
    timeout_ms: i64,
    err: *mut NetidxError,
) -> *mut NetidxChannelServerConn {
    unsafe { clear_error(err) };
    let rt = unsafe { &*rt };
    let listener = unsafe { &mut *listener };
    match crate::block_on_timeout(&rt.rt, timeout_ms, "accept timed out", listener.inner.accept()) {
        Ok(conn) => Box::into_raw(Box::new(NetidxChannelServerConn { inner: conn })),
        Err(e) => {
            unsafe { set_error(err, e) };
            std::ptr::null_mut()
        }
    }
}

/// Destroy a channel listener.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_channel_listener_destroy(l: *mut NetidxChannelListener) {
    if !l.is_null() {
        drop(unsafe { Box::from_raw(l) });
    }
}

// --- Server Connection ---

pub struct NetidxChannelServerConn {
    inner: channel::server::Connection,
}

/// Send one value to the client. Blocks until sent.
/// Consumes `value`.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_channel_server_conn_send_one(
    rt: *const NetidxRuntime,
    conn: *const NetidxChannelServerConn,
    value: *mut NetidxValue,
    err: *mut NetidxError,
) -> bool {
    unsafe { clear_error(err) };
    let v = unsafe { Box::from_raw(value) }.inner;
    match unsafe { &*rt }
        .rt
        .block_on(unsafe { &*conn }.inner.send_one(v))
    {
        Ok(()) => true,
        Err(e) => unsafe { set_error(err, e) },
    }
}

/// Receive one value from the client. Blocks until a value arrives.
/// `timeout_ms`: -1 = no timeout.
/// Returns a new owned Value, or NULL on error.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_channel_server_conn_recv_one(
    rt: *const NetidxRuntime,
    conn: *const NetidxChannelServerConn,
    timeout_ms: i64,
    err: *mut NetidxError,
) -> *mut NetidxValue {
    unsafe { clear_error(err) };
    let rt = unsafe { &*rt };
    let conn = unsafe { &*conn };
    match crate::block_on_timeout(&rt.rt, timeout_ms, "recv timed out", conn.inner.recv_one()) {
        Ok(v) => Box::into_raw(Box::new(NetidxValue { inner: v })),
        Err(e) => {
            unsafe { set_error(err, e) };
            std::ptr::null_mut()
        }
    }
}

/// Check if the server connection is dead.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_channel_server_conn_is_dead(
    conn: *const NetidxChannelServerConn,
) -> bool {
    unsafe { &*conn }.inner.is_dead()
}

/// Destroy a server connection.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_channel_server_conn_destroy(
    conn: *mut NetidxChannelServerConn,
) {
    if !conn.is_null() {
        drop(unsafe { Box::from_raw(conn) });
    }
}

// --- Channel Client ---

pub struct NetidxChannelClientConn {
    inner: channel::client::Connection,
}

/// Connect to a channel endpoint. Blocks until connected.
/// Does not consume `sub` or `path`.
/// Returns NULL on failure.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_channel_client_connect(
    rt: *const NetidxRuntime,
    sub: *const NetidxSubscriber,
    path: *const NetidxPath,
    err: *mut NetidxError,
) -> *mut NetidxChannelClientConn {
    unsafe { clear_error(err) };
    let rt = unsafe { &*rt };
    let path = unsafe { &*path }.inner.clone();
    match rt
        .rt
        .block_on(channel::client::Connection::connect(&unsafe { &*sub }.inner, path))
    {
        Ok(conn) => Box::into_raw(Box::new(NetidxChannelClientConn { inner: conn })),
        Err(e) => {
            unsafe { set_error(err, e) };
            std::ptr::null_mut()
        }
    }
}

/// Send a value to the server. Non-blocking (queues the write).
/// Consumes `value`.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_channel_client_conn_send(
    conn: *const NetidxChannelClientConn,
    value: *mut NetidxValue,
    err: *mut NetidxError,
) -> bool {
    unsafe { clear_error(err) };
    let v = unsafe { Box::from_raw(value) }.inner;
    match unsafe { &*conn }.inner.send(v) {
        Ok(()) => true,
        Err(e) => unsafe { set_error(err, e) },
    }
}

/// Flush previously sent values, blocking until they reach OS buffers.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_channel_client_conn_flush(
    rt: *const NetidxRuntime,
    conn: *const NetidxChannelClientConn,
    err: *mut NetidxError,
) -> bool {
    unsafe { clear_error(err) };
    match unsafe { &*rt }
        .rt
        .block_on(unsafe { &*conn }.inner.flush())
    {
        Ok(()) => true,
        Err(e) => unsafe { set_error(err, e) },
    }
}

/// Receive one value from the server. Blocks until a value arrives.
/// `timeout_ms`: -1 = no timeout.
/// Returns a new owned Value, or NULL on error.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_channel_client_conn_recv_one(
    rt: *const NetidxRuntime,
    conn: *const NetidxChannelClientConn,
    timeout_ms: i64,
    err: *mut NetidxError,
) -> *mut NetidxValue {
    unsafe { clear_error(err) };
    let rt = unsafe { &*rt };
    let conn = unsafe { &*conn };
    match crate::block_on_timeout(&rt.rt, timeout_ms, "recv timed out", conn.inner.recv_one()) {
        Ok(v) => Box::into_raw(Box::new(NetidxValue { inner: v })),
        Err(e) => {
            unsafe { set_error(err, e) };
            std::ptr::null_mut()
        }
    }
}

/// Check if the client connection is dead.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_channel_client_conn_is_dead(
    conn: *const NetidxChannelClientConn,
) -> bool {
    unsafe { &*conn }.inner.is_dead()
}

/// Destroy a client connection.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_channel_client_conn_destroy(
    conn: *mut NetidxChannelClientConn,
) {
    if !conn.is_null() {
        drop(unsafe { Box::from_raw(conn) });
    }
}

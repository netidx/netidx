use crate::{
    error::{clear_error, set_error, NetidxError},
    path::NetidxPath,
    publisher::NetidxPublisher,
    runtime::NetidxRuntime,
    subscriber::NetidxSubscriber,
    value::NetidxValue,
};
use arcstr::ArcStr;
use netidx::subscriber::Value;
use netidx_protocols::rpc;
use std::os::raw::c_char;

// --- RPC Server ---

/// Argument specification for an RPC procedure.
#[repr(C)]
pub struct NetidxArgSpec {
    /// Argument name (UTF-8, not null-terminated)
    pub name: *const c_char,
    pub name_len: usize,
    /// Documentation value (owned, consumed)
    pub doc: *mut NetidxValue,
    /// Default value (owned, consumed)
    pub default_value: *mut NetidxValue,
}

/// C function pointer type for RPC handlers.
/// `userdata` is the opaque pointer passed at registration.
/// `call` is the RPC call handle — the handler must call netidx_rpc_call_reply
/// to send a response. The call is consumed by reply.
pub type NetidxRpcHandler =
    Option<unsafe extern "C" fn(userdata: *mut std::ffi::c_void, call: *mut NetidxRpcCall)>;

pub struct NetidxRpcProc {
    _inner: rpc::server::Proc,
}

pub struct NetidxRpcCall {
    args: std::collections::HashMap<ArcStr, Value>,
    reply: rpc::server::RpcReply,
}

/// Create a new RPC procedure on the publisher.
/// `args` and `args_len`: array of argument specs (consumed).
/// `handler`: C callback invoked for each call.
/// `userdata`: opaque pointer passed to handler.
/// Returns NULL on failure.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_rpc_proc_new(
    pub_: *const NetidxPublisher,
    path: *mut NetidxPath,
    doc: *mut NetidxValue,
    args: *const NetidxArgSpec,
    args_len: usize,
    handler: NetidxRpcHandler,
    userdata: *mut std::ffi::c_void,
    err: *mut NetidxError,
) -> *mut NetidxRpcProc {
    unsafe { clear_error(err) };
    let path = unsafe { Box::from_raw(path) }.inner;
    let doc_val = unsafe { Box::from_raw(doc) }.inner;
    let handler = match handler {
        Some(h) => h,
        None => {
            unsafe {
                set_error(err, anyhow::anyhow!("handler must not be null"));
            }
            return std::ptr::null_mut();
        }
    };
    let arg_specs: Vec<rpc::server::ArgSpec> = unsafe {
        std::slice::from_raw_parts(args, args_len)
            .iter()
            .map(|a| {
                let name = std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                    a.name as *const u8,
                    a.name_len,
                ));
                let doc = Box::from_raw(a.doc).inner;
                let default_value = Box::from_raw(a.default_value).inner;
                rpc::server::ArgSpec {
                    name: ArcStr::from(name),
                    doc,
                    default_value,
                }
            })
            .collect()
    };
    // Wrap the closure in a Send wrapper.
    // Safety: the caller guarantees userdata outlives the Proc and is safe to call from any thread.
    struct SendMap(Box<dyn FnMut(rpc::server::RpcCall) -> Option<()>>);
    unsafe impl Send for SendMap {}
    impl SendMap {
        fn call(&mut self, c: rpc::server::RpcCall) -> Option<()> {
            (self.0)(c)
        }
    }
    let mut send_map = SendMap(Box::new(move |mut call: rpc::server::RpcCall| -> Option<()> {
        let ffi_call = Box::into_raw(Box::new(NetidxRpcCall {
            args: call.args.drain().collect(),
            reply: call.reply,
        }));
        unsafe { handler(userdata, ffi_call) };
        None
    }));
    let map = move |call: rpc::server::RpcCall| -> Option<()> {
        send_map.call(call)
    };
    match rpc::server::Proc::new(
        &unsafe { &*pub_ }.inner,
        path,
        doc_val,
        arg_specs,
        map,
        None::<futures::channel::mpsc::Sender<()>>,
    ) {
        Ok(proc_) => Box::into_raw(Box::new(NetidxRpcProc { _inner: proc_ })),
        Err(e) => {
            unsafe { set_error(err, e) };
            std::ptr::null_mut()
        }
    }
}

/// Take an argument value from an RPC call by name, removing it.
/// Returns a new owned Value, or NULL if the argument is not present or `call` is null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_rpc_call_take_arg(
    call: *mut NetidxRpcCall,
    name: *const c_char,
    name_len: usize,
) -> *mut NetidxValue {
    if call.is_null() {
        return std::ptr::null_mut();
    }
    let name = unsafe {
        std::str::from_utf8_unchecked(std::slice::from_raw_parts(name as *const u8, name_len))
    };
    let call = unsafe { &mut *call };
    match call.args.remove(name) {
        Some(v) => Box::into_raw(Box::new(NetidxValue { inner: v })),
        None => std::ptr::null_mut(),
    }
}

/// Reply to an RPC call with a value. Consumes both the call and the value.
/// No-op if either `call` or `value` is null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_rpc_call_reply(
    call: *mut NetidxRpcCall,
    value: *mut NetidxValue,
) {
    if call.is_null() || value.is_null() {
        return;
    }
    let mut call = unsafe { Box::from_raw(call) };
    let v = unsafe { Box::from_raw(value) }.inner;
    call.reply.send(v);
}

/// Destroy an RPC call without replying (sends an error to the caller).
#[unsafe(no_mangle)]
pub extern "C" fn netidx_rpc_call_destroy(call: *mut NetidxRpcCall) {
    if !call.is_null() {
        drop(unsafe { Box::from_raw(call) });
    }
}

/// Destroy an RPC procedure (unpublishes it).
#[unsafe(no_mangle)]
pub extern "C" fn netidx_rpc_proc_destroy(proc_: *mut NetidxRpcProc) {
    if !proc_.is_null() {
        drop(unsafe { Box::from_raw(proc_) });
    }
}

// --- RPC Client ---

pub struct NetidxRpcClient {
    inner: rpc::client::Proc,
}

/// Create a new RPC client for calling a remote procedure.
/// Does not consume `path` or `sub`.
/// Returns NULL on failure.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_rpc_client_new(
    sub: *const NetidxSubscriber,
    path: *const NetidxPath,
    err: *mut NetidxError,
) -> *mut NetidxRpcClient {
    unsafe { clear_error(err) };
    let path = unsafe { &*path }.inner.clone();
    match rpc::client::Proc::new(&unsafe { &*sub }.inner, path) {
        Ok(proc_) => Box::into_raw(Box::new(NetidxRpcClient { inner: proc_ })),
        Err(e) => {
            unsafe { set_error(err, e) };
            std::ptr::null_mut()
        }
    }
}

/// An argument for an RPC call.
#[repr(C)]
pub struct NetidxRpcArg {
    pub name: *const c_char,
    pub name_len: usize,
    /// Owned value, consumed by the call.
    pub value: *mut NetidxValue,
}

/// Call an RPC procedure. Blocks until a reply is received.
/// `args` and `args_len`: array of arguments (values consumed).
/// `timeout_ms`: -1 = no timeout.
/// Returns a new owned Value, or NULL on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_rpc_client_call(
    rt: *const NetidxRuntime,
    client: *const NetidxRpcClient,
    args: *const NetidxRpcArg,
    args_len: usize,
    timeout_ms: i64,
    err: *mut NetidxError,
) -> *mut NetidxValue {
    unsafe { clear_error(err) };
    let rt = unsafe { &*rt };
    let client = unsafe { &*client };
    let call_args: Vec<(String, Value)> = unsafe {
        std::slice::from_raw_parts(args, args_len)
            .iter()
            .map(|a| {
                let name = std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                    a.name as *const u8,
                    a.name_len,
                ));
                let value = Box::from_raw(a.value).inner;
                (name.to_string(), value)
            })
            .collect()
    };
    match crate::block_on_timeout(
        &rt.rt, timeout_ms, "rpc call timed out",
        client.inner.call(call_args),
    ) {
        Ok(v) => Box::into_raw(Box::new(NetidxValue { inner: v })),
        Err(e) => {
            unsafe { set_error(err, e) };
            std::ptr::null_mut()
        }
    }
}

/// Clone an RPC client.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_rpc_client_clone(
    client: *const NetidxRpcClient,
) -> *mut NetidxRpcClient {
    Box::into_raw(Box::new(NetidxRpcClient {
        inner: unsafe { &*client }.inner.clone(),
    }))
}

/// Destroy an RPC client.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_rpc_client_destroy(client: *mut NetidxRpcClient) {
    if !client.is_null() {
        drop(unsafe { Box::from_raw(client) });
    }
}

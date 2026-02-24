use crate::{
    config::NetidxConfig,
    error::{clear_error, set_error, NetidxError},
    path::NetidxPath,
    runtime::NetidxRuntime,
    value::NetidxValue,
};
use futures::channel::mpsc;
use futures::prelude::*;
use netidx::subscriber::{
    Dval, Event, SubId, Subscriber, SubscriberBuilder, UpdatesFlags,
};
use poolshark::global::GPooled;

// --- UpdatesFlags ---

/// Bitmask flags controlling update delivery behavior.
/// Combine with bitwise OR (e.g. `BEGIN_WITH_LAST | NO_SPURIOUS`).
#[repr(u32)]
pub enum NetidxUpdatesFlag {
    /// Send the last known value immediately upon registration.
    BeginWithLast = 0x01,
    /// Stop storing the last value (improves performance).
    StopCollectingLast = 0x02,
    /// When re-registering the same channel with BEGIN_WITH_LAST,
    /// do not re-send the last value.
    NoSpurious = 0x04,
}

// --- UpdateChannel ---

/// A shared sender for aggregating updates from multiple Dvals into one receiver.
pub struct NetidxUpdateChannel {
    inner: mpsc::Sender<GPooled<Vec<(SubId, Event)>>>,
}

/// Create a shared update channel. Returns the sender; writes the receiver to `rx_out`.
/// `channel_buffer`: channel capacity; 0 uses the default.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_update_channel_new(
    channel_buffer: usize,
    rx_out: *mut *mut NetidxUpdateReceiver,
) -> *mut NetidxUpdateChannel {
    let buf = if channel_buffer == 0 { crate::FFI_CHANNEL_BUFFER } else { channel_buffer };
    let (tx, rx) = mpsc::channel(buf);
    unsafe {
        *rx_out = Box::into_raw(Box::new(NetidxUpdateReceiver { inner: rx }));
    }
    Box::into_raw(Box::new(NetidxUpdateChannel { inner: tx }))
}

/// Clone an update channel sender (cheap, shares the same underlying channel).
#[unsafe(no_mangle)]
pub extern "C" fn netidx_update_channel_clone(
    ch: *const NetidxUpdateChannel,
) -> *mut NetidxUpdateChannel {
    Box::into_raw(Box::new(NetidxUpdateChannel {
        inner: unsafe { &*ch }.inner.clone(),
    }))
}

/// Destroy an update channel sender. The receiver remains valid until dropped separately.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_update_channel_destroy(ch: *mut NetidxUpdateChannel) {
    if !ch.is_null() {
        drop(unsafe { Box::from_raw(ch) });
    }
}

/// Subscribe using an existing shared update channel.
/// Does not consume `path` or `channel`. `flags` is a bitmask of `NetidxUpdatesFlag`.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_subscriber_subscribe_updates(
    sub: *const NetidxSubscriber,
    path: *const NetidxPath,
    channel: *const NetidxUpdateChannel,
    flags: u32,
) -> *mut NetidxDval {
    let path = unsafe { &*path }.inner.clone();
    let tx = unsafe { &*channel }.inner.clone();
    let flags = UpdatesFlags::from_bits_truncate(flags);
    let dval = unsafe { &*sub }.inner.subscribe_updates(path, [(flags, tx)]);
    Box::into_raw(Box::new(NetidxDval { inner: dval }))
}

/// Register a shared update channel on an existing Dval.
/// Does not consume `dval` or `channel`. `flags` is a bitmask of `NetidxUpdatesFlag`.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_dval_updates(
    dval: *const NetidxDval,
    channel: *const NetidxUpdateChannel,
    flags: u32,
) {
    let tx = unsafe { &*channel }.inner.clone();
    let flags = UpdatesFlags::from_bits_truncate(flags);
    unsafe { &*dval }.inner.updates(flags, tx);
}

// --- SubscriberBuilder ---

pub struct NetidxSubscriberBuilder {
    inner: SubscriberBuilder,
}

/// Create a new SubscriberBuilder. Does not consume `cfg`.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_subscriber_builder_new(
    cfg: *const NetidxConfig,
) -> *mut NetidxSubscriberBuilder {
    let cfg = unsafe { &*cfg }.inner.clone();
    Box::into_raw(Box::new(NetidxSubscriberBuilder {
        inner: SubscriberBuilder::new(cfg),
    }))
}

/// Build the subscriber. Consumes the builder. Returns NULL on failure.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_subscriber_builder_build(
    rt: *const NetidxRuntime,
    sb: *mut NetidxSubscriberBuilder,
    err: *mut NetidxError,
) -> *mut NetidxSubscriber {
    unsafe { clear_error(err) };
    let mut builder = unsafe { Box::from_raw(sb) };
    let rt = unsafe { &*rt };
    let _guard = rt.rt.enter();
    match builder.inner.build() {
        Ok(sub) => Box::into_raw(Box::new(NetidxSubscriber { inner: sub })),
        Err(e) => {
            unsafe { set_error(err, e) };
            std::ptr::null_mut()
        }
    }
}

/// Destroy builder without building.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_subscriber_builder_destroy(sb: *mut NetidxSubscriberBuilder) {
    if !sb.is_null() {
        drop(unsafe { Box::from_raw(sb) });
    }
}

// --- Subscriber ---

pub struct NetidxSubscriber {
    pub(crate) inner: Subscriber,
}

/// Subscribe to a path, creating a durable subscription. Does not consume `path`.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_subscriber_subscribe(
    sub: *const NetidxSubscriber,
    path: *const NetidxPath,
) -> *mut NetidxDval {
    let path = unsafe { &*path }.inner.clone();
    let dval = unsafe { &*sub }.inner.subscribe(path);
    Box::into_raw(Box::new(NetidxDval { inner: dval }))
}


/// Clone a subscriber handle (cheap Arc clone).
#[unsafe(no_mangle)]
pub extern "C" fn netidx_subscriber_clone(
    sub: *const NetidxSubscriber,
) -> *mut NetidxSubscriber {
    Box::into_raw(Box::new(NetidxSubscriber { inner: unsafe { &*sub }.inner.clone() }))
}

/// Destroy a subscriber.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_subscriber_destroy(sub: *mut NetidxSubscriber) {
    if !sub.is_null() {
        drop(unsafe { Box::from_raw(sub) });
    }
}

// --- Dval ---

pub struct NetidxDval {
    pub(crate) inner: Dval,
}

/// Wait for the durable subscription to be connected.
/// `timeout_ms`: timeout in milliseconds, -1 = no timeout.
/// Returns true on success, false on error.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_dval_wait_subscribed(
    rt: *const NetidxRuntime,
    dval: *const NetidxDval,
    timeout_ms: i64,
    err: *mut NetidxError,
) -> bool {
    unsafe { clear_error(err) };
    let rt = unsafe { &*rt };
    let dval = unsafe { &*dval };
    let result = crate::block_on_timeout(
        &rt.rt,
        timeout_ms,
        "timeout waiting for subscription",
        dval.inner.wait_subscribed(),
    );
    match result {
        Ok(()) => true,
        Err(e) => unsafe { set_error(err, e) },
    }
}

/// Get the last event for a durable subscription.
/// Returns a new owned event handle. Caller must free with netidx_event_destroy.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_dval_last(dval: *const NetidxDval) -> *mut NetidxEvent {
    let ev = unsafe { &*dval }.inner.last();
    Box::into_raw(Box::new(NetidxEvent { inner: ev }))
}

/// Get the unique subscription ID for a Dval, for matching against SubscriberUpdate.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_dval_id(dval: *const NetidxDval) -> u64 {
    unsafe { &*dval }.inner.id().inner()
}

/// Write a value back to the publisher through this subscription.
/// Consumes `value`. Returns true if sent immediately, false if queued.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_dval_write(
    dval: *const NetidxDval,
    value: *mut NetidxValue,
) -> bool {
    let v = unsafe { Box::from_raw(value) }.inner;
    unsafe { &*dval }.inner.write(v)
}

/// Clone a Dval.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_dval_clone(dval: *const NetidxDval) -> *mut NetidxDval {
    Box::into_raw(Box::new(NetidxDval { inner: unsafe { &*dval }.inner.clone() }))
}

/// Destroy a Dval (unsubscribes).
#[unsafe(no_mangle)]
pub extern "C" fn netidx_dval_destroy(dval: *mut NetidxDval) {
    if !dval.is_null() {
        drop(unsafe { Box::from_raw(dval) });
    }
}

// --- Event ---

/// Discriminant for subscriber events.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    Unsubscribed = 0,
    Update = 1,
}

pub struct NetidxEvent {
    inner: Event,
}

/// Get the event type.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_event_type(ev: *const NetidxEvent) -> EventType {
    match unsafe { &*ev }.inner {
        Event::Unsubscribed => EventType::Unsubscribed,
        Event::Update(_) => EventType::Update,
    }
}

/// Clone the value from an Update event. Returns a new owned Value.
/// Returns NULL if the event is Unsubscribed.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_event_value_clone(ev: *const NetidxEvent) -> *mut NetidxValue {
    match &unsafe { &*ev }.inner {
        Event::Update(v) => Box::into_raw(Box::new(NetidxValue { inner: v.clone() })),
        Event::Unsubscribed => std::ptr::null_mut(),
    }
}

/// Destroy an event.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_event_destroy(ev: *mut NetidxEvent) {
    if !ev.is_null() {
        drop(unsafe { Box::from_raw(ev) });
    }
}

// --- SubscriberUpdate ---

/// An update from the update receiver, pairing a SubId with an Event.
pub struct NetidxSubscriberUpdate {
    sub_id: SubId,
    event: Event,
}

/// Get the raw subscription ID from an update.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_subscriber_update_sub_id(
    update: *const NetidxSubscriberUpdate,
) -> u64 {
    unsafe { &*update }.sub_id.inner()
}

/// Get the event type from an update.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_subscriber_update_event_type(
    update: *const NetidxSubscriberUpdate,
) -> EventType {
    match unsafe { &*update }.event {
        Event::Unsubscribed => EventType::Unsubscribed,
        Event::Update(_) => EventType::Update,
    }
}

/// Clone the value from an update's event. Returns NULL if Unsubscribed.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_subscriber_update_value_clone(
    update: *const NetidxSubscriberUpdate,
) -> *mut NetidxValue {
    match &unsafe { &*update }.event {
        Event::Update(v) => Box::into_raw(Box::new(NetidxValue { inner: v.clone() })),
        Event::Unsubscribed => std::ptr::null_mut(),
    }
}

/// Destroy a subscriber update.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_subscriber_update_destroy(update: *mut NetidxSubscriberUpdate) {
    if !update.is_null() {
        drop(unsafe { Box::from_raw(update) });
    }
}

/// Free an array of subscriber update pointers. Does NOT free individual updates.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netidx_subscriber_update_array_free(
    arr: *mut *mut NetidxSubscriberUpdate,
    len: usize,
) {
    if !arr.is_null() && len > 0 {
        drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(arr, len)) });
    }
}

// --- UpdateReceiver ---

pub struct NetidxUpdateReceiver {
    inner: mpsc::Receiver<GPooled<Vec<(SubId, Event)>>>,
}

/// Try to receive subscriber updates without blocking.
/// Returns at most one batch. `out_len` receives the count.
/// Returns NULL if no updates are available.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_update_receiver_try_recv(
    rx: *mut NetidxUpdateReceiver,
    out_len: *mut usize,
) -> *mut *mut NetidxSubscriberUpdate {
    let rx = unsafe { &mut *rx };
    match rx.inner.try_recv() {
        Ok(mut batch) => {
            let mut results: Vec<*mut NetidxSubscriberUpdate> =
                Vec::with_capacity(batch.len());
            for (sub_id, event) in batch.drain(..) {
                results.push(Box::into_raw(Box::new(NetidxSubscriberUpdate {
                    sub_id,
                    event,
                })));
            }
            unsafe { *out_len = results.len() };
            let raw = Box::into_raw(results.into_boxed_slice());
            raw as *mut *mut NetidxSubscriberUpdate
        }
        _ => {
            unsafe { *out_len = 0 };
            std::ptr::null_mut()
        }
    }
}

/// Receive subscriber updates, blocking up to `timeout_ms` (-1 = forever).
/// Returns one batch. `out_len` receives the count.
/// Returns NULL on timeout or channel closed.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_update_receiver_recv(
    rt: *const NetidxRuntime,
    rx: *mut NetidxUpdateReceiver,
    timeout_ms: i64,
    out_len: *mut usize,
) -> *mut *mut NetidxSubscriberUpdate {
    let rx = unsafe { &mut *rx };
    let rt = unsafe { &*rt };
    let result = crate::block_on_timeout_opt(&rt.rt, timeout_ms, rx.inner.next());
    match result {
        Some(mut batch) => {
            let mut results: Vec<*mut NetidxSubscriberUpdate> =
                Vec::with_capacity(batch.len());
            for (sub_id, event) in batch.drain(..) {
                results.push(Box::into_raw(Box::new(NetidxSubscriberUpdate {
                    sub_id,
                    event,
                })));
            }
            unsafe { *out_len = results.len() };
            let raw = Box::into_raw(results.into_boxed_slice());
            raw as *mut *mut NetidxSubscriberUpdate
        }
        None => {
            unsafe { *out_len = 0 };
            std::ptr::null_mut()
        }
    }
}

/// Destroy an update receiver.
#[unsafe(no_mangle)]
pub extern "C" fn netidx_update_receiver_destroy(rx: *mut NetidxUpdateReceiver) {
    if !rx.is_null() {
        drop(unsafe { Box::from_raw(rx) });
    }
}

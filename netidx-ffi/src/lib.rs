mod channel;
mod config;
mod error;
mod path;
mod publisher;
mod rpc;
mod runtime;
mod subscriber;
mod value;

pub use channel::*;
pub use config::*;
pub use error::*;
pub use path::*;
pub use publisher::*;
pub use rpc::*;
pub use runtime::*;
pub use subscriber::*;
pub use value::*;

use std::time::Duration;

pub(crate) const FFI_CHANNEL_BUFFER: usize = 100;

fn timeout_from_millis(timeout_ms: i64) -> Option<Duration> {
    if timeout_ms < 0 { None } else { Some(Duration::from_millis(timeout_ms as u64)) }
}

/// Block on a future with optional timeout, returning `Result<T>`.
pub(crate) fn block_on_timeout<F, T>(
    rt: &tokio::runtime::Runtime,
    timeout_ms: i64,
    msg: &str,
    fut: F,
) -> anyhow::Result<T>
where
    F: std::future::Future<Output = anyhow::Result<T>>,
{
    rt.block_on(async {
        if timeout_ms < 0 {
            fut.await
        } else {
            match tokio::time::timeout(Duration::from_millis(timeout_ms as u64), fut).await {
                Ok(r) => r,
                Err(_) => Err(anyhow::anyhow!("{}", msg)),
            }
        }
    })
}

/// Block on a future with optional timeout, returning `Option<T>`.
pub(crate) fn block_on_timeout_opt<F, T>(
    rt: &tokio::runtime::Runtime,
    timeout_ms: i64,
    fut: F,
) -> Option<T>
where
    F: std::future::Future<Output = Option<T>>,
{
    rt.block_on(async {
        if timeout_ms < 0 {
            fut.await
        } else {
            tokio::time::timeout(Duration::from_millis(timeout_ms as u64), fut)
                .await
                .unwrap_or_default()
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::raw::c_char;
    use std::ptr;

    // --- Error tests ---

    #[test]
    fn error_init_has_no_message() {
        let mut err = netidx_error_init();
        assert!(netidx_error_message(&err).is_null());
        netidx_error_free(&mut err);
    }

    #[test]
    fn error_set_and_read() {
        let mut err = netidx_error_init();
        unsafe { set_error(&mut err, anyhow::anyhow!("test error")) };
        let msg = netidx_error_message(&err);
        assert!(!msg.is_null());
        let s = unsafe { std::ffi::CStr::from_ptr(msg) }.to_str().unwrap();
        assert!(s.contains("test error"));
        netidx_error_free(&mut err);
        assert!(netidx_error_message(&err).is_null());
    }

    #[test]
    fn error_clear_idempotent() {
        let mut err = netidx_error_init();
        netidx_error_free(&mut err);
        netidx_error_free(&mut err); // double free is safe
    }

    // --- Runtime tests ---

    #[test]
    fn runtime_create_destroy() {
        let mut err = netidx_error_init();
        let rt = netidx_runtime_new(2, &mut err);
        assert!(!rt.is_null());
        assert!(netidx_error_message(&err).is_null());
        netidx_runtime_destroy(rt);
        netidx_error_free(&mut err);
    }

    // --- Path tests ---

    #[test]
    fn path_new_and_as_str() {
        let s = "/hello/world";
        let path = unsafe { netidx_path_new(s.as_ptr() as *const c_char, s.len()) };
        assert!(!path.is_null());

        let mut data: *const c_char = ptr::null();
        let mut len: usize = 0;
        netidx_path_as_str(path, &mut data, &mut len);
        let result = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(data as *const u8, len)) };
        assert_eq!(result, "/hello/world");

        netidx_path_destroy(path);
    }

    #[test]
    fn path_append() {
        let s = "/hello";
        let path = unsafe { netidx_path_new(s.as_ptr() as *const c_char, s.len()) };
        let seg = "world";
        let appended = unsafe { netidx_path_append(path, seg.as_ptr() as *const c_char, seg.len()) };

        let mut data: *const c_char = ptr::null();
        let mut len: usize = 0;
        netidx_path_as_str(appended, &mut data, &mut len);
        let result = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(data as *const u8, len)) };
        assert_eq!(result, "/hello/world");

        netidx_path_destroy(path);
        netidx_path_destroy(appended);
    }

    #[test]
    fn path_levels() {
        let s = "/a/b/c";
        let path = unsafe { netidx_path_new(s.as_ptr() as *const c_char, s.len()) };
        assert_eq!(netidx_path_levels(path), 3);
        netidx_path_destroy(path);
    }

    #[test]
    fn path_basename_dirname() {
        let s = "/a/b/c";
        let path = unsafe { netidx_path_new(s.as_ptr() as *const c_char, s.len()) };

        let mut data: *const c_char = ptr::null();
        let mut len: usize = 0;
        assert!(netidx_path_basename(path, &mut data, &mut len));
        let bn = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(data as *const u8, len)) };
        assert_eq!(bn, "c");

        assert!(netidx_path_dirname(path, &mut data, &mut len));
        let dn = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(data as *const u8, len)) };
        assert_eq!(dn, "/a/b");

        netidx_path_destroy(path);
    }

    #[test]
    fn path_clone() {
        let s = "/test";
        let path = unsafe { netidx_path_new(s.as_ptr() as *const c_char, s.len()) };
        let cloned = netidx_path_clone(path);
        assert!(!cloned.is_null());

        let mut data: *const c_char = ptr::null();
        let mut len: usize = 0;
        netidx_path_as_str(cloned, &mut data, &mut len);
        let result = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(data as *const u8, len)) };
        assert_eq!(result, "/test");

        netidx_path_destroy(path);
        netidx_path_destroy(cloned);
    }

    // --- Value tests ---

    #[test]
    fn value_null() {
        let v = netidx_value_null();
        assert_eq!(netidx_value_type(v), ValueType::Null);
        netidx_value_destroy(v);
    }

    #[test]
    fn value_scalars() {
        // u8
        let v = netidx_value_u8(42);
        assert_eq!(netidx_value_type(v), ValueType::U8);
        let mut out: u8 = 0;
        assert!(netidx_value_get_u8(v, &mut out));
        assert_eq!(out, 42);
        assert!(!netidx_value_get_f64(v, &mut 0.0f64 as *mut f64)); // wrong type
        netidx_value_destroy(v);

        // i8
        let v = netidx_value_i8(-7);
        assert_eq!(netidx_value_type(v), ValueType::I8);
        let mut out: i8 = 0;
        assert!(netidx_value_get_i8(v, &mut out));
        assert_eq!(out, -7);
        netidx_value_destroy(v);

        // u16
        let v = netidx_value_u16(1000);
        assert_eq!(netidx_value_type(v), ValueType::U16);
        let mut out: u16 = 0;
        assert!(netidx_value_get_u16(v, &mut out));
        assert_eq!(out, 1000);
        netidx_value_destroy(v);

        // i16
        let v = netidx_value_i16(-500);
        assert_eq!(netidx_value_type(v), ValueType::I16);
        let mut out: i16 = 0;
        assert!(netidx_value_get_i16(v, &mut out));
        assert_eq!(out, -500);
        netidx_value_destroy(v);

        // u32
        let v = netidx_value_u32(100_000);
        assert_eq!(netidx_value_type(v), ValueType::U32);
        let mut out: u32 = 0;
        assert!(netidx_value_get_u32(v, &mut out));
        assert_eq!(out, 100_000);
        netidx_value_destroy(v);

        // i32
        let v = netidx_value_i32(-99);
        assert_eq!(netidx_value_type(v), ValueType::I32);
        let mut out: i32 = 0;
        assert!(netidx_value_get_i32(v, &mut out));
        assert_eq!(out, -99);
        netidx_value_destroy(v);

        // u64
        let v = netidx_value_u64(u64::MAX);
        assert_eq!(netidx_value_type(v), ValueType::U64);
        let mut out: u64 = 0;
        assert!(netidx_value_get_u64(v, &mut out));
        assert_eq!(out, u64::MAX);
        netidx_value_destroy(v);

        // i64
        let v = netidx_value_i64(i64::MIN);
        assert_eq!(netidx_value_type(v), ValueType::I64);
        let mut out: i64 = 0;
        assert!(netidx_value_get_i64(v, &mut out));
        assert_eq!(out, i64::MIN);
        netidx_value_destroy(v);

        // f32
        let v = netidx_value_f32(3.14);
        assert_eq!(netidx_value_type(v), ValueType::F32);
        let mut out: f32 = 0.0;
        assert!(netidx_value_get_f32(v, &mut out));
        assert!((out - 3.14).abs() < 0.001);
        netidx_value_destroy(v);

        // f64
        let v = netidx_value_f64(2.718281828);
        assert_eq!(netidx_value_type(v), ValueType::F64);
        let mut out: f64 = 0.0;
        assert!(netidx_value_get_f64(v, &mut out));
        assert!((out - 2.718281828).abs() < 1e-9);
        netidx_value_destroy(v);

        // bool
        let v = netidx_value_bool(true);
        assert_eq!(netidx_value_type(v), ValueType::Bool);
        let mut out: bool = false;
        assert!(netidx_value_get_bool(v, &mut out));
        assert!(out);
        netidx_value_destroy(v);
    }

    #[test]
    fn value_varint_types() {
        // V32 - should be accessible via get_u32
        let v = netidx_value_v32(12345);
        assert_eq!(netidx_value_type(v), ValueType::V32);
        let mut out: u32 = 0;
        assert!(netidx_value_get_u32(v, &mut out));
        assert_eq!(out, 12345);
        netidx_value_destroy(v);

        // Z32 - should be accessible via get_i32
        let v = netidx_value_z32(-999);
        assert_eq!(netidx_value_type(v), ValueType::Z32);
        let mut out: i32 = 0;
        assert!(netidx_value_get_i32(v, &mut out));
        assert_eq!(out, -999);
        netidx_value_destroy(v);

        // V64 - should be accessible via get_u64
        let v = netidx_value_v64(99999);
        assert_eq!(netidx_value_type(v), ValueType::V64);
        let mut out: u64 = 0;
        assert!(netidx_value_get_u64(v, &mut out));
        assert_eq!(out, 99999);
        netidx_value_destroy(v);

        // Z64 - should be accessible via get_i64
        let v = netidx_value_z64(-77777);
        assert_eq!(netidx_value_type(v), ValueType::Z64);
        let mut out: i64 = 0;
        assert!(netidx_value_get_i64(v, &mut out));
        assert_eq!(out, -77777);
        netidx_value_destroy(v);
    }

    #[test]
    fn value_string() {
        let s = "hello world";
        let v = unsafe { netidx_value_string(s.as_ptr() as *const c_char, s.len()) };
        assert_eq!(netidx_value_type(v), ValueType::String);

        let mut data: *const c_char = ptr::null();
        let mut len: usize = 0;
        assert!(netidx_value_get_string(v, &mut data, &mut len));
        let result = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(data as *const u8, len)) };
        assert_eq!(result, "hello world");

        // wrong type accessor
        let mut out: u32 = 0;
        assert!(!netidx_value_get_u32(v, &mut out));

        netidx_value_destroy(v);
    }

    #[test]
    fn value_bytes() {
        let b: &[u8] = &[1, 2, 3, 4, 5];
        let v = unsafe { netidx_value_bytes(b.as_ptr(), b.len()) };
        assert_eq!(netidx_value_type(v), ValueType::Bytes);

        let mut data: *const u8 = ptr::null();
        let mut len: usize = 0;
        assert!(netidx_value_get_bytes(v, &mut data, &mut len));
        let result = unsafe { std::slice::from_raw_parts(data, len) };
        assert_eq!(result, &[1, 2, 3, 4, 5]);

        netidx_value_destroy(v);
    }

    #[test]
    fn value_error() {
        let inner = netidx_value_i64(42);
        let v = unsafe { netidx_value_error(inner) };
        assert_eq!(netidx_value_type(v), ValueType::Error);

        let cloned = netidx_value_get_error_clone(v);
        assert!(!cloned.is_null());
        assert_eq!(netidx_value_type(cloned), ValueType::I64);
        let mut out: i64 = 0;
        assert!(netidx_value_get_i64(cloned, &mut out));
        assert_eq!(out, 42);

        netidx_value_destroy(cloned);
        netidx_value_destroy(v);
    }

    #[test]
    fn value_array() {
        let v0 = netidx_value_i64(10);
        let v1 = netidx_value_i64(20);
        let v2 = netidx_value_i64(30);
        let ptrs = [v0, v1, v2];
        let arr = unsafe { netidx_value_array(ptrs.as_ptr(), 3) };
        assert_eq!(netidx_value_type(arr), ValueType::Array);
        assert_eq!(netidx_value_array_len(arr), 3);

        let elem = netidx_value_array_get_clone(arr, 1);
        assert!(!elem.is_null());
        let mut out: i64 = 0;
        assert!(netidx_value_get_i64(elem, &mut out));
        assert_eq!(out, 20);

        // out of bounds
        assert!(netidx_value_array_get_clone(arr, 5).is_null());

        netidx_value_destroy(elem);
        netidx_value_destroy(arr);
    }

    #[test]
    fn value_clone() {
        let v = netidx_value_f64(1.5);
        let cloned = netidx_value_clone(v);
        let mut out: f64 = 0.0;
        assert!(netidx_value_get_f64(cloned, &mut out));
        assert_eq!(out, 1.5);
        netidx_value_destroy(v);
        netidx_value_destroy(cloned);
    }

    #[test]
    fn value_to_string() {
        let v = netidx_value_i64(42);
        let mut data: *mut c_char = ptr::null_mut();
        let mut len: usize = 0;
        netidx_value_to_string(v, &mut data, &mut len);
        assert!(!data.is_null());
        let s = unsafe { std::ffi::CStr::from_ptr(data) }.to_str().unwrap();
        assert_eq!(s, "i64:42");
        netidx_str_free(data);
        netidx_value_destroy(v);
    }

    #[test]
    fn value_destroy_null_is_safe() {
        netidx_value_destroy(ptr::null_mut());
    }

    #[test]
    fn value_map() {
        let k0 = unsafe { netidx_value_string("a".as_ptr() as *const c_char, 1) };
        let v0 = netidx_value_i64(1);
        let k1 = unsafe { netidx_value_string("b".as_ptr() as *const c_char, 1) };
        let v1 = netidx_value_i64(2);
        let keys = [k0, k1];
        let values = [v0, v1];
        let map = unsafe { netidx_value_map(keys.as_ptr(), values.as_ptr(), 2) };
        assert_eq!(netidx_value_type(map), ValueType::Map);
        assert_eq!(netidx_value_map_len(map), 2);

        // Look up key "a"
        let key_a = unsafe { netidx_value_string("a".as_ptr() as *const c_char, 1) };
        let got = netidx_value_map_get_clone(map, key_a);
        assert!(!got.is_null());
        let mut out: i64 = 0;
        assert!(netidx_value_get_i64(got, &mut out));
        assert_eq!(out, 1);
        netidx_value_destroy(got);
        netidx_value_destroy(key_a);

        // Missing key
        let key_z = unsafe { netidx_value_string("z".as_ptr() as *const c_char, 1) };
        assert!(netidx_value_map_get_clone(map, key_z).is_null());
        netidx_value_destroy(key_z);

        // Entries
        let mut out_keys: *mut *mut NetidxValue = ptr::null_mut();
        let mut out_vals: *mut *mut NetidxValue = ptr::null_mut();
        let mut out_len: usize = 0;
        assert!(unsafe {
            netidx_value_map_entries_clone(map, &mut out_keys, &mut out_vals, &mut out_len)
        });
        assert_eq!(out_len, 2);
        // Clean up entries
        for i in 0..out_len {
            unsafe {
                netidx_value_destroy(*out_keys.add(i));
                netidx_value_destroy(*out_vals.add(i));
            }
        }
        unsafe {
            netidx_value_array_free(out_keys, out_len);
            netidx_value_array_free(out_vals, out_len);
        }

        netidx_value_destroy(map);
    }

    #[test]
    fn value_decimal() {
        let s = "3.14";
        let mut err = netidx_error_init();
        let v = unsafe { netidx_value_decimal(s.as_ptr() as *const c_char, s.len(), &mut err) };
        assert!(!v.is_null());
        assert!(netidx_error_message(&err).is_null());
        assert_eq!(netidx_value_type(v), ValueType::Decimal);

        let mut data: *mut c_char = ptr::null_mut();
        let mut len: usize = 0;
        assert!(unsafe { netidx_value_get_decimal_string(v, &mut data, &mut len) });
        let result = unsafe { std::ffi::CStr::from_ptr(data) }.to_str().unwrap();
        assert_eq!(result, "3.14");
        netidx_str_free(data);
        netidx_value_destroy(v);

        // Invalid decimal
        let bad = "not_a_number";
        let v = unsafe { netidx_value_decimal(bad.as_ptr() as *const c_char, bad.len(), &mut err) };
        assert!(v.is_null());
        assert!(!netidx_error_message(&err).is_null());
        netidx_error_free(&mut err);
    }

    #[test]
    fn value_datetime() {
        let v = netidx_value_datetime(1_000_000, 500);
        assert_eq!(netidx_value_type(v), ValueType::DateTime);

        let mut secs: i64 = 0;
        let mut nsecs: u32 = 0;
        assert!(unsafe { netidx_value_get_datetime(v, &mut secs, &mut nsecs) });
        assert_eq!(secs, 1_000_000);
        assert_eq!(nsecs, 500);
        netidx_value_destroy(v);

        // Type mismatch
        let v = netidx_value_null();
        assert!(!unsafe { netidx_value_get_datetime(v, &mut secs, &mut nsecs) });
        netidx_value_destroy(v);
    }

    #[test]
    fn value_duration() {
        let v = netidx_value_duration(60, 123_456_789);
        assert_eq!(netidx_value_type(v), ValueType::Duration);

        let mut secs: u64 = 0;
        let mut nsecs: u32 = 0;
        assert!(unsafe { netidx_value_get_duration(v, &mut secs, &mut nsecs) });
        assert_eq!(secs, 60);
        assert_eq!(nsecs, 123_456_789);
        netidx_value_destroy(v);

        // Type mismatch
        let v = netidx_value_null();
        assert!(!unsafe { netidx_value_get_duration(v, &mut secs, &mut nsecs) });
        netidx_value_destroy(v);
    }

    #[test]
    fn value_type_mismatch_returns_false() {
        let v = netidx_value_null();
        let mut u8_out: u8 = 0;
        let mut i8_out: i8 = 0;
        let mut u16_out: u16 = 0;
        let mut i16_out: i16 = 0;
        let mut u32_out: u32 = 0;
        let mut i32_out: i32 = 0;
        let mut u64_out: u64 = 0;
        let mut i64_out: i64 = 0;
        let mut f32_out: f32 = 0.0;
        let mut f64_out: f64 = 0.0;
        let mut bool_out: bool = false;
        assert!(!netidx_value_get_u8(v, &mut u8_out));
        assert!(!netidx_value_get_i8(v, &mut i8_out));
        assert!(!netidx_value_get_u16(v, &mut u16_out));
        assert!(!netidx_value_get_i16(v, &mut i16_out));
        assert!(!netidx_value_get_u32(v, &mut u32_out));
        assert!(!netidx_value_get_i32(v, &mut i32_out));
        assert!(!netidx_value_get_u64(v, &mut u64_out));
        assert!(!netidx_value_get_i64(v, &mut i64_out));
        assert!(!netidx_value_get_f32(v, &mut f32_out));
        assert!(!netidx_value_get_f64(v, &mut f64_out));
        assert!(!netidx_value_get_bool(v, &mut bool_out));
        let mut str_data: *const c_char = ptr::null();
        let mut str_len: usize = 0;
        assert!(!netidx_value_get_string(v, &mut str_data, &mut str_len));
        let mut bytes_data: *const u8 = ptr::null();
        let mut bytes_len: usize = 0;
        assert!(!netidx_value_get_bytes(v, &mut bytes_data, &mut bytes_len));
        assert!(netidx_value_get_error_clone(v).is_null());
        assert_eq!(netidx_value_array_len(v), 0);
        netidx_value_destroy(v);
    }
}

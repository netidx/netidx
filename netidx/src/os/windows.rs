use super::{Krb5Ctx, Krb5ServerCtx};
use anyhow::{bail, Result};
use bytes::{Buf, BytesMut};
use log::debug;
use parking_lot::Mutex;
use std::{
    default::Default,
    ffi::OsString,
    fmt, mem,
    ops::Drop,
    os::windows::ffi::{OsStrExt, OsStringExt},
    ptr,
    sync::Arc,
    time::Duration,
};
use tokio::task;
use winapi::{
    ctypes::*,
    shared::{
        minwindef::FILETIME,
        sspi::{
            self, SecBuffer, SecBufferDesc, SecHandle, SecPkgContext_NativeNamesW,
            SecPkgContext_Sizes, SecPkgCredentials_NamesW, SecPkgInfoW,
            SECPKG_ATTR_SIZES,
        },
        winerror::{
            SEC_E_OK, SEC_I_COMPLETE_AND_CONTINUE, SEC_I_COMPLETE_NEEDED, SUCCEEDED,
        },
    },
    um::{
        errhandlingapi::GetLastError,
        minwinbase::SYSTEMTIME,
        ntsecapi::KERB_WRAP_NO_ENCRYPT,
        sysinfoapi::GetSystemTime,
        timezoneapi::{SystemTimeToFileTime, SystemTimeToTzSpecificLocalTime},
        winbase::{
            lstrlenW, FormatMessageW, FORMAT_MESSAGE_FROM_SYSTEM,
            FORMAT_MESSAGE_IGNORE_INSERTS,
        },
        winnt::{LANG_NEUTRAL, LARGE_INTEGER, MAKELANGID, SUBLANG_NEUTRAL},
    },
};

unsafe fn string_from_wstr(s: *const u16) -> String {
    let slen = lstrlenW(s);
    let slice = &*ptr::slice_from_raw_parts(s, slen as usize);
    OsString::from_wide(slice).to_string_lossy().to_string()
}

fn str_to_wstr(s: &str) -> Vec<u16> {
    let mut v = OsString::from(s).encode_wide().collect::<Vec<_>>();
    v.push(0);
    v
}

fn format_error(error: i32) -> String {
    const BUF: usize = 512;
    let mut msg = [0u16; BUF];
    unsafe {
        FormatMessageW(
            FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
            ptr::null_mut(),
            error as u32,
            MAKELANGID(LANG_NEUTRAL, SUBLANG_NEUTRAL) as u32,
            msg.as_mut_ptr(),
            (BUF - 1) as u32,
            ptr::null_mut(),
        );
    }
    unsafe { string_from_wstr(msg.as_mut_ptr()) }
}

struct Cred(SecHandle);

impl Drop for Cred {
    fn drop(&mut self) {
        unsafe {
            sspi::FreeCredentialsHandle(&mut self.0 as *mut _);
        }
    }
}

impl Cred {
    fn acquire(principal: Option<&str>, server: bool) -> Result<Cred> {
        let mut cred = SecHandle::default();
        let principal = principal.map(str_to_wstr);
        let principal_ptr =
            principal.map(|mut p| p.as_mut_ptr()).unwrap_or(ptr::null_mut());
        let mut package = str_to_wstr("Kerberos");
        let dir =
            if server { sspi::SECPKG_CRED_INBOUND } else { sspi::SECPKG_CRED_OUTBOUND };
        let mut lifetime = LARGE_INTEGER::default();
        let res = unsafe {
            sspi::AcquireCredentialsHandleW(
                principal_ptr,
                package.as_mut_ptr(),
                dir,
                ptr::null_mut(),
                ptr::null_mut(),
                None,
                ptr::null_mut(),
                &mut cred as *mut _,
                &mut lifetime as *mut _,
            )
        };
        if !SUCCEEDED(res) {
            bail!("failed to acquire credentials {}", format_error(res));
        } else {
            Ok(Cred(cred))
        }
    }

    pub(crate) fn name(&mut self) -> Result<String> {
        let mut names = SecPkgCredentials_NamesW::default();
        unsafe {
            let res = sspi::QueryCredentialsAttributesW(
                &mut self.0 as *mut _,
                sspi::SECPKG_CRED_ATTR_NAMES,
                &mut names as *mut _ as *mut c_void,
            );
            if !SUCCEEDED(res) {
                bail!("failed to query cred names {}", format_error(res))
            }
            Ok(string_from_wstr(names.sUserName))
        }
    }
}

fn alloc_krb5_buf() -> Result<Vec<u8>> {
    let mut ifo = ptr::null_mut::<SecPkgInfoW>();
    let mut pkg = str_to_wstr("Kerberos");
    let res =
        unsafe { sspi::QuerySecurityPackageInfoW(pkg.as_mut_ptr(), &mut ifo as *mut _) };
    if !SUCCEEDED(res) {
        if ifo != ptr::null_mut() {
            unsafe {
                sspi::FreeContextBuffer(ifo as *mut _);
            }
        }
        bail!("failed to query pkg info for Kerberos {}", format_error(res));
    }
    let max_len = unsafe { (*ifo).cbMaxToken };
    unsafe {
        sspi::FreeContextBuffer(ifo as *mut _);
    }
    let mut buf = Vec::with_capacity(max_len as usize);
    buf.extend((0..max_len).into_iter().map(|_| 0));
    Ok(buf)
}

fn query_pkg_sizes(ctx: &mut SecHandle, sz: &mut SecPkgContext_Sizes) -> Result<()> {
    let res = unsafe {
        sspi::QueryContextAttributesW(
            ctx as *mut _,
            SECPKG_ATTR_SIZES,
            sz as *mut _ as *mut c_void,
        )
    };
    if !SUCCEEDED(res) {
        bail!("failed to query package sizes {}", format_error(res))
    }
    Ok(())
}

fn wrap_iov(
    ctx: &mut SecHandle,
    sizes: &SecPkgContext_Sizes,
    encrypt: bool,
    header: &mut BytesMut,
    data: &mut BytesMut,
    padding: &mut BytesMut,
) -> Result<()> {
    task::block_in_place(|| {
        header.resize(sizes.cbSecurityTrailer as usize, 0);
        padding.resize(sizes.cbBlockSize as usize, 0);
        let mut buffers = [
            SecBuffer {
                BufferType: sspi::SECBUFFER_TOKEN,
                cbBuffer: sizes.cbSecurityTrailer,
                pvBuffer: &mut **header as *mut _ as *mut c_void,
            },
            SecBuffer {
                BufferType: sspi::SECBUFFER_DATA,
                cbBuffer: data.len() as u32,
                pvBuffer: &mut **data as *mut _ as *mut c_void,
            },
            SecBuffer {
                BufferType: sspi::SECBUFFER_PADDING,
                cbBuffer: sizes.cbBlockSize,
                pvBuffer: &mut **padding as *mut _ as *mut c_void,
            },
        ];
        let mut buf_desc = SecBufferDesc {
            ulVersion: sspi::SECBUFFER_VERSION,
            cBuffers: 3,
            pBuffers: buffers.as_mut_ptr(),
        };
        let flags = if !encrypt { KERB_WRAP_NO_ENCRYPT } else { 0 };
        let res = unsafe {
            sspi::EncryptMessage(ctx as *mut _, flags, &mut buf_desc as *mut _, 0)
        };
        if !SUCCEEDED(res) {
            bail!("EncryptMessage failed {}", format_error(res))
        }
        header.resize(buffers[0].cbBuffer as usize, 0);
        assert_eq!(buffers[1].cbBuffer as usize, data.len());
        padding.resize(buffers[2].cbBuffer as usize, 0);
        Ok(())
    })
}

fn wrap(
    ctx: &mut SecHandle,
    sizes: &SecPkgContext_Sizes,
    encrypt: bool,
    msg: &[u8],
) -> Result<BytesMut> {
    let mut header = BytesMut::new();
    header.resize(sizes.cbSecurityTrailer as usize, 0);
    let mut data = BytesMut::from(msg);
    let mut padding = BytesMut::new();
    padding.resize(sizes.cbBlockSize as usize, 0);
    wrap_iov(ctx, sizes, encrypt, &mut header, &mut data, &mut padding)?;
    let mut msg = BytesMut::with_capacity(header.len() + data.len() + padding.len());
    msg.extend_from_slice(&*header);
    msg.extend_from_slice(&*data);
    msg.extend_from_slice(&*padding);
    Ok(msg)
}

fn unwrap_iov(ctx: &mut SecHandle, len: usize, msg: &mut BytesMut) -> Result<BytesMut> {
    task::block_in_place(|| {
        let mut bufs = [
            SecBuffer {
                BufferType: sspi::SECBUFFER_STREAM,
                cbBuffer: len as u32,
                pvBuffer: &mut msg[0..len] as *mut _ as *mut c_void,
            },
            SecBuffer {
                BufferType: sspi::SECBUFFER_DATA,
                cbBuffer: 0,
                pvBuffer: ptr::null_mut(),
            },
        ];
        let mut bufs_desc = SecBufferDesc {
            ulVersion: sspi::SECBUFFER_VERSION,
            cBuffers: 2,
            pBuffers: bufs.as_mut_ptr(),
        };
        let mut qop: u32 = 0;
        let res = unsafe {
            sspi::DecryptMessage(
                ctx as *mut _,
                &mut bufs_desc as *mut _,
                0,
                &mut qop as *mut _,
            )
        };
        if !SUCCEEDED(res) {
            bail!("decrypt message failed {}", format_error(res))
        }
        let hdr_len = bufs[1].pvBuffer as usize - bufs[0].pvBuffer as usize;
        let data_len = bufs[1].cbBuffer as usize;
        msg.advance(hdr_len);
        let data = msg.split_to(data_len);
        msg.advance(len - hdr_len - data_len); // padding
        Ok(data)
    })
}

fn convert_lifetime(lifetime: LARGE_INTEGER) -> Result<Duration> {
    let mut st = SYSTEMTIME::default();
    let mut lt = SYSTEMTIME::default();
    let mut ft = FILETIME::default();
    unsafe {
        GetSystemTime(&mut st as *mut _);
        if 0 == SystemTimeToTzSpecificLocalTime(
            ptr::null_mut(),
            &st as *const _,
            &mut lt as *mut _,
        ) {
            bail!(
                "failed to convert to local time {}",
                format_error(GetLastError() as i32)
            )
        }
        if 0 == SystemTimeToFileTime(&lt as *const _, &mut ft as *mut _) {
            bail!(
                "failed to convert current time to a filetime: {}",
                format_error(GetLastError() as i32)
            )
        }
        let now: u64 = ((ft.dwHighDateTime as u64) << 32) | (ft.dwLowDateTime as u64);
        let expires = *lifetime.QuadPart() as u64;
        if expires <= now {
            Ok(Duration::from_secs(0))
        } else {
            Ok(Duration::from_secs((expires - now) / 10))
        }
    }
}

struct ClientCtxInner {
    ctx: SecHandle,
    cred: Cred,
    target: Vec<u16>,
    attrs: u32,
    lifetime: LARGE_INTEGER,
    buf: Vec<u8>,
    done: bool,
    sizes: SecPkgContext_Sizes,
}

impl Drop for ClientCtxInner {
    fn drop(&mut self) {
        unsafe {
            sspi::DeleteSecurityContext(&mut self.ctx as *mut _);
        }
    }
}

#[derive(Clone)]
pub(crate) struct ClientCtx(Arc<Mutex<ClientCtxInner>>);

impl fmt::Debug for ClientCtx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "netidx::os::windows::ClientCtx")
    }
}

impl ClientCtx {
    fn new(cred: Cred, target: &str) -> Result<ClientCtx> {
        Ok(ClientCtx(Arc::new(Mutex::new(ClientCtxInner {
            ctx: SecHandle::default(),
            cred,
            target: str_to_wstr(target),
            attrs: 0,
            lifetime: LARGE_INTEGER::default(),
            buf: alloc_krb5_buf()?,
            done: false,
            sizes: SecPkgContext_Sizes::default(),
        }))))
    }

    fn do_step(&self, tok: Option<&[u8]>) -> Result<Option<BytesMut>> {
        let mut guard = self.0.lock();
        let inner = &mut *guard;
        if inner.done {
            return Ok(None);
        }
        for i in 0..inner.buf.len() {
            inner.buf[i] = 0;
        }
        let mut out_buf = SecBuffer {
            cbBuffer: inner.buf.len() as u32,
            BufferType: sspi::SECBUFFER_TOKEN,
            pvBuffer: inner.buf.as_mut_ptr() as *mut _,
        };
        let mut out_buf_desc = SecBufferDesc {
            ulVersion: sspi::SECBUFFER_VERSION,
            cBuffers: 1,
            pBuffers: &mut out_buf as *mut _,
        };
        let mut in_buf = SecBuffer {
            cbBuffer: tok.map(|t| t.len()).unwrap_or(0) as u32,
            BufferType: sspi::SECBUFFER_TOKEN,
            pvBuffer: tok
                .map(|t| unsafe { mem::transmute::<*const u8, *mut c_void>(t.as_ptr()) })
                .unwrap_or(ptr::null_mut()),
        };
        let mut in_buf_desc = SecBufferDesc {
            ulVersion: sspi::SECBUFFER_VERSION,
            cBuffers: 1,
            pBuffers: &mut in_buf as *mut _,
        };
        let ctx_ptr =
            if tok.is_none() { ptr::null_mut() } else { &mut inner.ctx as *mut _ };
        let in_buf_ptr =
            if tok.is_some() { &mut in_buf_desc as *mut _ } else { ptr::null_mut() };
        let res = unsafe {
            sspi::InitializeSecurityContextW(
                &mut inner.cred.0 as *mut _,
                ctx_ptr,
                inner.target.as_mut_ptr(),
                sspi::ISC_REQ_CONFIDENTIALITY | sspi::ISC_REQ_MUTUAL_AUTH,
                0,
                sspi::SECURITY_NATIVE_DREP,
                in_buf_ptr,
                0,
                &mut inner.ctx as *mut _,
                &mut out_buf_desc as *mut _,
                &mut inner.attrs as *mut _,
                &mut inner.lifetime as *mut _,
            )
        };
        if !SUCCEEDED(res) {
            bail!("ClientCtx::step failed {}", format_error(res))
        }
        if res == SEC_E_OK {
            query_pkg_sizes(&mut inner.ctx, &mut inner.sizes)?;
            inner.done = true;
        }
        if res == SEC_I_COMPLETE_AND_CONTINUE || res == SEC_I_COMPLETE_NEEDED {
            let res =
                unsafe { sspi::CompleteAuthToken(ctx_ptr, &mut out_buf_desc as *mut _) };
            if !SUCCEEDED(res) {
                bail!("complete token failed {}", format_error(res))
            }
        }
        if out_buf.cbBuffer > 0 {
            Ok(Some(BytesMut::from(&inner.buf[0..(out_buf.cbBuffer as usize)])))
        } else if inner.done {
            Ok(None)
        } else {
            bail!("ClientCtx::step no token was generated but we are not done")
        }
    }
}

impl Krb5Ctx for ClientCtx {
    type Buf = BytesMut;

    fn step(&self, token: Option<&[u8]>) -> Result<Option<Self::Buf>> {
        task::block_in_place(|| self.do_step(token))
    }

    fn wrap(&self, encrypt: bool, msg: &[u8]) -> Result<BytesMut> {
        let mut guard = self.0.lock();
        let inner = &mut *guard;
        wrap(&mut inner.ctx, &inner.sizes, encrypt, msg)
    }

    fn wrap_iov(
        &self,
        encrypt: bool,
        header: &mut BytesMut,
        data: &mut BytesMut,
        padding: &mut BytesMut,
        _trailer: &mut BytesMut,
    ) -> Result<()> {
        let mut guard = self.0.lock();
        let inner = &mut *guard;
        wrap_iov(&mut inner.ctx, &mut inner.sizes, encrypt, header, data, padding)
    }

    fn unwrap_iov(&self, len: usize, msg: &mut BytesMut) -> Result<BytesMut> {
        let mut inner = self.0.lock();
        unwrap_iov(&mut inner.ctx, len, msg)
    }

    fn unwrap(&self, msg: &[u8]) -> Result<BytesMut> {
        let mut buf = BytesMut::from(msg);
        self.unwrap_iov(buf.len(), &mut buf)
    }

    fn ttl(&self) -> Result<Duration> {
        convert_lifetime(self.0.lock().lifetime)
    }
}

struct ServerCtxInner {
    ctx: SecHandle,
    cred: Cred,
    buf: Vec<u8>,
    attrs: u32,
    lifetime: LARGE_INTEGER,
    done: bool,
    sizes: SecPkgContext_Sizes,
}

impl Drop for ServerCtxInner {
    fn drop(&mut self) {
        unsafe {
            sspi::DeleteSecurityContext(&mut self.ctx as *mut _);
        }
    }
}

#[derive(Clone)]
pub(crate) struct ServerCtx(Arc<Mutex<ServerCtxInner>>);

impl fmt::Debug for ServerCtx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "netidx::os::windows::ServerCtx")
    }
}

impl ServerCtx {
    fn new(cred: Cred) -> Result<ServerCtx> {
        Ok(ServerCtx(Arc::new(Mutex::new(ServerCtxInner {
            ctx: SecHandle::default(),
            cred,
            buf: alloc_krb5_buf()?,
            attrs: 0,
            lifetime: LARGE_INTEGER::default(),
            done: false,
            sizes: SecPkgContext_Sizes::default(),
        }))))
    }

    fn do_step(&self, tok: Option<&[u8]>) -> Result<Option<BytesMut>> {
        let tok = tok.ok_or_else(|| anyhow!("no token supplied"))?;
        let mut guard = self.0.lock();
        let inner = &mut *guard;
        if inner.done {
            return Ok(None);
        }
        for i in 0..inner.buf.len() {
            inner.buf[i] = 0;
        }
        let mut out_buf = SecBuffer {
            cbBuffer: inner.buf.len() as u32,
            BufferType: sspi::SECBUFFER_TOKEN,
            pvBuffer: inner.buf.as_mut_ptr() as *mut _,
        };
        let mut out_buf_desc = SecBufferDesc {
            ulVersion: sspi::SECBUFFER_VERSION,
            cBuffers: 1,
            pBuffers: &mut out_buf as *mut _,
        };
        let mut in_buf = SecBuffer {
            cbBuffer: tok.len() as u32,
            BufferType: sspi::SECBUFFER_TOKEN,
            pvBuffer: unsafe { mem::transmute::<*const u8, *mut c_void>(tok.as_ptr()) },
        };
        let mut in_buf_desc = SecBufferDesc {
            ulVersion: sspi::SECBUFFER_VERSION,
            cBuffers: 1,
            pBuffers: &mut in_buf as *mut _,
        };
        let dfsh = SecHandle::default();
        let ctx_ptr =
            if inner.ctx.dwLower == dfsh.dwLower && inner.ctx.dwUpper == dfsh.dwUpper {
                ptr::null_mut()
            } else {
                &mut inner.ctx as *mut _
            };
        let res = unsafe {
            sspi::AcceptSecurityContext(
                &mut inner.cred.0 as *mut _,
                ctx_ptr,
                &mut in_buf_desc as *mut _,
                sspi::ISC_REQ_CONFIDENTIALITY | sspi::ISC_REQ_MUTUAL_AUTH,
                sspi::SECURITY_NATIVE_DREP,
                &mut inner.ctx as *mut _,
                &mut out_buf_desc as *mut _,
                &mut inner.attrs as *mut _,
                &mut inner.lifetime as *mut _,
            )
        };
        if !SUCCEEDED(res) {
            bail!("ServerCtx::step failed {}", format_error(res));
        }
        if res == SEC_E_OK {
            query_pkg_sizes(&mut inner.ctx, &mut inner.sizes)?;
            inner.done = true;
        }
        if res == SEC_I_COMPLETE_AND_CONTINUE || res == SEC_I_COMPLETE_NEEDED {
            let res =
                unsafe { sspi::CompleteAuthToken(ctx_ptr, &mut out_buf_desc as *mut _) };
            if !SUCCEEDED(res) {
                bail!("complete token failed {}", format_error(res))
            }
        }
        if out_buf.cbBuffer > 0 {
            Ok(Some(BytesMut::from(&inner.buf[0..(out_buf.cbBuffer as usize)])))
        } else if inner.done {
            Ok(None)
        } else {
            bail!("ServerCtx::step no token was generated but we are not done")
        }
    }
}

impl Krb5Ctx for ServerCtx {
    type Buf = BytesMut;

    fn step(&self, token: Option<&[u8]>) -> Result<Option<Self::Buf>> {
        task::block_in_place(|| self.do_step(token))
    }

    fn wrap(&self, encrypt: bool, msg: &[u8]) -> Result<BytesMut> {
        let mut guard = self.0.lock();
        let inner = &mut *guard;
        wrap(&mut inner.ctx, &inner.sizes, encrypt, msg)
    }

    fn wrap_iov(
        &self,
        encrypt: bool,
        header: &mut BytesMut,
        data: &mut BytesMut,
        padding: &mut BytesMut,
        _trailer: &mut BytesMut,
    ) -> Result<()> {
        let mut guard = self.0.lock();
        let inner = &mut *guard;
        wrap_iov(&mut inner.ctx, &mut inner.sizes, encrypt, header, data, padding)
    }

    fn unwrap_iov(&self, len: usize, msg: &mut BytesMut) -> Result<BytesMut> {
        let mut inner = self.0.lock();
        unwrap_iov(&mut inner.ctx, len, msg)
    }

    fn unwrap(&self, msg: &[u8]) -> Result<BytesMut> {
        let mut buf = BytesMut::from(msg);
        self.unwrap_iov(buf.len(), &mut buf)
    }

    fn ttl(&self) -> Result<Duration> {
        convert_lifetime(self.0.lock().lifetime)
    }
}

impl Krb5ServerCtx for ServerCtx {
    fn client(&self) -> Result<String> {
        let mut names = SecPkgContext_NativeNamesW::default();
        let mut inner = self.0.lock();
        unsafe {
            let res = sspi::QueryContextAttributesW(
                &mut inner.ctx as *mut _,
                sspi::SECPKG_ATTR_NATIVE_NAMES,
                &mut names as *mut _ as *mut c_void,
            );
            if !SUCCEEDED(res) {
                bail!("failed to get client name {}", format_error(res))
            }
            Ok(string_from_wstr(names.sClientName))
        }
    }
}

pub(crate) fn create_client_ctx(
    principal: Option<&str>,
    target_principal: &str,
) -> Result<ClientCtx> {
    task::block_in_place(|| {
        let cred = Cred::acquire(principal, false)?;
        ClientCtx::new(cred, target_principal)
    })
}

pub(crate) fn create_server_ctx(principal: Option<&str>) -> Result<ServerCtx> {
    task::block_in_place(|| {
        debug!("loading server credentials for: {}", principal.unwrap_or("<default>"));
        let mut cred = Cred::acquire(principal, true)?;
        debug!("loaded server credentials for: {}", cred.name()?);
        ServerCtx::new(cred)
    })
}

pub(crate) struct Mapper;

impl Mapper {
    pub(crate) fn new() -> Result<Mapper> {
        Ok(Mapper)
    }

    pub(crate) fn groups(&mut self, _user: &str) -> Result<Vec<String>> {
        todo!("group listing is not implemented on windows")
    }
}

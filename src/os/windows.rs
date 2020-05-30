use anyhow::{bail, Result};
use std::{
    default::Default,
    ffi::OsString,
    mem,
    ops::Drop,
    os::windows::ffi::{OsStrExt, OsStringExt},
    ptr,
};
use winapi::{
    ctypes::*,
    shared::{
        sspi::{self, PCredHandle, PCtxtHandle, SecBuffer, SecBufferDesc, SecHandle, SecPkgInfoW},
        winerror::{
            SEC_E_OK, SEC_I_COMPLETE_AND_CONTINUE, SEC_I_COMPLETE_NEEDED, SEC_I_CONTINUE_NEEDED,
            SUCCEEDED,
        },
    },
    um::{
        winbase::{
            lstrlenW, FormatMessageW, FORMAT_MESSAGE_ALLOCATE_BUFFER,
            FORMAT_MESSAGE_FROM_SYSTEM, FORMAT_MESSAGE_IGNORE_INSERTS,
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
    v.push(0); // ensure null termination
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
            BUF - 1,
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
        let principal_ptr = principal
            .map(|mut p| p.as_mut_ptr())
            .unwrap_or(ptr::null_mut());
        let mut package = str_to_wstr("Kerberos");
        let dir = if server {
            sspi::SECPKG_CRED_INBOUND
        } else {
            sspi::SECPKG_CRED_OUTBOUND
        };
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
}

fn alloc_krb5_buf() -> Result<Vec<u8>> {
    let mut ifo = ptr::null_mut::<SecPkgInfoW>();
    let mut pkg = str_to_wstr("Kerberos");
    let res = unsafe { sspi::QuerySecurityPackageInfoW(pkg.as_mut_ptr(), &mut ifo as *mut _) };
    if !SUCCEEDED(res) {
        if ifo != ptr::null_mut() {
            unsafe {
                sspi::FreeContextBuffer(ifo as *mut _);
            }
        }
        bail!(
            "failed to query pkg info for Kerberos {}",
            format_error(res)
        );
    }
    let max_len = unsafe { (*ifo).cbMaxToken };
    unsafe {
        sspi::FreeContextBuffer(ifo as *mut _);
    }
    let mut buf = Vec::with_capacity(max_len as usize);
    buf.extend((0..max_len).into_iter().map(|_| 0));
    Ok(buf)
}

fn wrap_iov(
    ctx: &mut SecHandle,
    sizes: &SecPkgContext_Sizes,
    stream_sizes: &SecPkgContext_StreamSizes,
    header: &mut BytesMut,
    data: &mut BytesMut,
    padding: &mut BytesMut,
) -> Result<()> {
    if data.len() > stream_sizes.cbMaximumMessage as usize {
        bail!(
            "message too large, {} exceeds max size {}",
            data.len(),
            stream_sizes.cbMaximumMessage
        )
    }
    header.resize(sizes.cbSecurityTrailer as usize, 0x0);
    padding.resize(sizes.cbBlockSize as usize, 0x0);
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
    let res = unsafe { sspi::EncryptMessage(ctx as *mut _, 0, &mut buf_desc as *mut _, 0) };
    if !SUCCEEDED(res) {
        bail!("EncryptMessage failed {}", format_error(res))
    }
    Ok(())
}

struct ClientCtx {
    ctx: SecHandle,
    cred: Cred,
    target: Vec<u16>,
    attrs: u32,
    lifetime: LARGE_INTEGER,
    buf: Vec<u8>,
    done: bool,
}

impl Drop for ClientCtx {
    fn drop(&mut self) {
        unsafe {
            sspi::DeleteSecurityContext(&mut self.ctx as *mut _);
        }
    }
}

impl ClientCtx {
    fn new(cred: Cred, target: &str) -> Result<ClientCtx> {
        Ok(ClientCtx {
            ctx: SecHandle::default(),
            cred,
            target: str_to_wstr(target),
            attrs: 0,
            lifetime: LARGE_INTEGER::default(),
            buf: alloc_krb5_buf()?,
            done: false,
        })
    }

    fn step(&mut self, tok: Option<&[u8]>) -> Result<Option<&[u8]>> {
        if self.done {
            return Ok(None);
        }
        for i in 0..self.buf.len() {
            self.buf[i] = 0;
        }
        let mut out_buf = SecBuffer {
            cbBuffer: self.buf.len() as u32,
            BufferType: sspi::SECBUFFER_TOKEN,
            pvBuffer: self.buf.as_mut_ptr() as *mut _,
        };
        let mut out_buf_desc = SecBufferDesc {
            ulVersion: 0,
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
            ulVersion: 0,
            cBuffers: 1,
            pBuffers: &mut in_buf as *mut _,
        };
        let ctx_ptr = if tok.is_none() {
            ptr::null_mut()
        } else {
            &mut self.ctx as *mut _
        };
        let in_buf_ptr = if tok.is_some() {
            &mut in_buf_desc as *mut _
        } else {
            ptr::null_mut()
        };
        let res = unsafe {
            sspi::InitializeSecurityContextW(
                &mut self.cred.0 as *mut _,
                ctx_ptr,
                self.target.as_mut_ptr(),
                sspi::ISC_REQ_CONFIDENTIALITY | sspi::ISC_REQ_MUTUAL_AUTH,
                0,
                sspi::SECURITY_NATIVE_DREP,
                in_buf_ptr,
                0,
                &mut self.ctx as *mut _,
                &mut out_buf_desc as *mut _,
                &mut self.attrs as *mut _,
                &mut self.lifetime as *mut _,
            )
        };
        if !SUCCEEDED(res) {
            bail!("ClientCtx::step failed {}", format_error(res))
        }
        if res == SEC_E_OK {
            self.done = true;
        }
        if res == SEC_I_COMPLETE_AND_CONTINUE || res == SEC_I_COMPLETE_NEEDED {
            let res = unsafe { sspi::CompleteAuthToken(ctx_ptr, &mut out_buf_desc as *mut _) };
            if !SUCCEEDED(res) {
                bail!("complete token failed {}", format_error(res))
            }
        }
        if out_buf.cbBuffer > 0 {
            Ok(Some(&self.buf[0..(out_buf.cbBuffer as usize)]))
        } else {
            Ok(None)
        }
    }
}

struct ServerCtx {
    ctx: SecHandle,
    cred: Cred,
    buf: Vec<u8>,
    attrs: u32,
    lifetime: LARGE_INTEGER,
    done: bool,
}

impl Drop for ServerCtx {
    fn drop(&mut self) {
        unsafe {
            sspi::DeleteSecurityContext(&mut self.ctx as *mut _);
        }
    }
}

impl ServerCtx {
    fn new(cred: Cred) -> Result<ServerCtx> {
        Ok(ServerCtx {
            ctx: SecHandle::default(),
            cred,
            buf: alloc_krb5_buf()?,
            attrs: 0,
            lifetime: LARGE_INTEGER::default(),
            done: false,
        })
    }

    fn step(&mut self, tok: &[u8]) -> Result<Option<&[u8]>> {
        if self.done {
            return Ok(None);
        }
        for i in 0..self.buf.len() {
            self.buf[i] = 0;
        }
        let mut out_buf = SecBuffer {
            cbBuffer: self.buf.len() as u32,
            BufferType: sspi::SECBUFFER_TOKEN,
            pvBuffer: self.buf.as_mut_ptr() as *mut _,
        };
        let mut out_buf_desc = SecBufferDesc {
            ulVersion: 0,
            cBuffers: 1,
            pBuffers: &mut out_buf as *mut _,
        };
        let mut in_buf = SecBuffer {
            cbBuffer: tok.len() as u32,
            BufferType: sspi::SECBUFFER_TOKEN,
            pvBuffer: unsafe { mem::transmute::<*const u8, *mut c_void>(tok.as_ptr()) },
        };
        let mut in_buf_desc = SecBufferDesc {
            ulVersion: 0,
            cBuffers: 1,
            pBuffers: &mut in_buf as *mut _,
        };
        let dfsh = SecHandle::default();
        let ctx_ptr = if self.ctx.dwLower == dfsh.dwLower && self.ctx.dwUpper == dfsh.dwUpper {
            ptr::null_mut()
        } else {
            &mut self.ctx as *mut _
        };
        let res = unsafe {
            sspi::AcceptSecurityContext(
                &mut self.cred.0 as *mut _,
                ctx_ptr,
                &mut in_buf_desc as *mut _,
                sspi::ISC_REQ_CONFIDENTIALITY | sspi::ISC_REQ_MUTUAL_AUTH,
                sspi::SECURITY_NATIVE_DREP,
                &mut self.ctx as *mut _,
                &mut out_buf_desc as *mut _,
                &mut self.attrs as *mut _,
                &mut self.lifetime as *mut _,
            )
        };
        if !SUCCEEDED(res) {
            bail!("ServerCtx::step failed {}", format_error(res));
        }
        if res == SEC_E_OK {
            self.done = true;
        }
        if res == SEC_I_COMPLETE_AND_CONTINUE || res == SEC_I_COMPLETE_NEEDED {
            let res = unsafe { sspi::CompleteAuthToken(ctx_ptr, &mut out_buf_desc as *mut _) };
            if !SUCCEEDED(res) {
                bail!("complete token failed {}", format_error(res))
            }
        }
        if out_buf.cbBuffer > 0 {
            Ok(Some(&self.buf[0..(out_buf.cbBuffer as usize)]))
        } else {
            Ok(None)
        }
    }
}

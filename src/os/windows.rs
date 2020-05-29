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
        winerror::SUCCEEDED,
    },
    um::{winbase::lstrlenW, winnt::LARGE_INTEGER},
};

fn str_to_wstr(s: &str) -> Vec<u16> {
    OsString::from(s).encode_wide().collect::<Vec<_>>()
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
        let mut package = str_to_wstr("Kerberos\0");
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
            bail!("failed to acquire credentials {}", res);
        } else {
            Ok(Cred(cred))
        }
    }
}

fn alloc_krb5_buf() -> Result<Vec<u8>> {
    let mut ifo = ptr::null_mut::<SecPkgInfoW>();
    let mut pkg = str_to_wstr("Kerberos\0");
    let res = unsafe { sspi::QuerySecurityPackageInfoW(pkg.as_mut_ptr(), &mut ifo as *mut _) };
    if !SUCCEEDED(res) {
        if ifo != ptr::null_mut() {
            unsafe {
                sspi::FreeContextBuffer(ifo as *mut _);
            }
        }
        bail!("failed to query pkg info for Kerberos {}", res);
    }
    let max_len = unsafe { (*ifo).cbMaxToken };
    unsafe {
        sspi::FreeContextBuffer(ifo as *mut _);
    }
    let mut buf = Vec::with_capacity(max_len as usize);
    buf.extend((0..max_len).into_iter().map(|_| 0));
    Ok(buf)
}

struct ClientCtx {
    ctx: SecHandle,
    cred: Cred,
    target: Vec<u16>,
    attrs: u32,
    lifetime: LARGE_INTEGER,
    buf: Vec<u8>,
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
        })
    }

    fn step(&mut self, tok: Option<&[u8]>) -> Result<Option<&[u8]>> {
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
            bail!("ClientCtx::step failed {}", res)
        } else if out_buf.cbBuffer > 0 {
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
        })
    }

    fn step(&mut self, tok: &[u8]) -> Result<Option<&[u8]>> {
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
                0,
                sspi::SECURITY_NATIVE_DREP,
                &mut self.ctx as *mut _,
                &mut out_buf_desc as *mut _,
                &mut self.attrs as *mut _,
                &mut self.lifetime as *mut _,
            )
        };
        if !SUCCEEDED(res) {
            bail!("ServerCtx::step failed {}", res);
        }
        if out_buf.cbBuffer > 0 {
            Ok(Some(&self.buf[0..(out_buf.cbBuffer as usize)]))
        } else {
            Ok(None)
        }
    }
}

fn print_pkgs() {
    let mut len: u32 = 0;
    let mut pkgs = ptr::null_mut::<SecPkgInfoW>();
    let res = unsafe { sspi::EnumerateSecurityPackagesW(&mut len as *mut _, &mut pkgs as *mut _) };
    if !SUCCEEDED(res) {
        panic!("failed to print security packages {}", res);
    } else {
        let pkgs = unsafe { &*ptr::slice_from_raw_parts(pkgs, len as usize) };
        for pkg in pkgs {
            unsafe {
                let slen = lstrlenW(pkg.Name);
                let slice = &*ptr::slice_from_raw_parts(pkg.Name, slen as usize);
                println!("{}", OsString::from_wide(slice).to_string_lossy());
            }
        }
    }
}

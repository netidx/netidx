use super::{Krb5Ctx, Krb5ServerCtx};
use anyhow::{bail, Result};
use std::{
    default::Default,
    ffi::OsString,
    ops::Drop,
    os::windows::ffi::{OsStrExt, OsStringExt},
    ptr,
};
use winapi::{
    shared::{
        sspi::{self, PCredHandle, PCtxtHandle, SecHandle, SecPkgInfoW},
        winerror::SUCCEEDED,
    },
    um::{winnt::LARGE_INTEGER, winbase::lstrlenW},
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
        let mut package = OsString::from("Kerberos\0").encode_wide().collect::<Vec<_>>();
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

struct ClientCtx {
    ctx: SecHandle,
    cred: Cred,
    target: Vec<u16>,
}

impl ClientCtx {
    fn new(cred: Cred, target: &str) -> ClientCtx {
        ClientCtx {
            ctx: SecHandle::default(),
            cred,
            target: str_to_wstr(target),
        }
    }

    fn step(&self, tok: &[u8]) -> Result<ClientCtx> {
        
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

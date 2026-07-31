//! Resolve the operating-system identity produced by netidx Local auth.
//!
//! Permission grants must use exactly the transport authenticator's spelling:
//! the passwd username on Unix and the canonical `DOMAIN\user` SAM name on
//! Windows.

use anyhow::Result;
use arcstr::ArcStr;

/// Return the current process user's canonical netidx Local-auth identity.
#[cfg(unix)]
pub fn current_user() -> Result<ArcStr> {
    let uid = nix::unistd::Uid::current();
    match nix::unistd::User::from_uid(uid) {
        Ok(Some(user)) => Ok(ArcStr::from(user.name.as_str())),
        Ok(None) => bail!(
            "could not resolve current uid ({uid}) to a passwd entry. This usually \
             means the process is running in a container or namespace without an \
             /etc/passwd entry for its uid"
        ),
        Err(e) => bail!("getpwuid_r failed for current uid ({uid}): {e}"),
    }
}

/// Windows Local auth identifies peers by their canonical down-level name.
#[cfg(windows)]
pub fn current_user() -> Result<ArcStr> {
    use windows::{
        Win32::Security::Authentication::Identity::{GetUserNameExW, NameSamCompatible},
        core::PWSTR,
    };

    let mut buf = vec![0u16; 1024];
    let mut len = buf.len() as u32;
    let ok = unsafe {
        GetUserNameExW(NameSamCompatible, Some(PWSTR(buf.as_mut_ptr())), &mut len)
    };
    if !ok {
        bail!(
            "could not determine the current Windows user via \
             GetUserNameEx(NameSamCompatible)"
        );
    }
    Ok(ArcStr::from(String::from_utf16_lossy(&buf[..len as usize])))
}

#[cfg(not(any(unix, windows)))]
pub fn current_user() -> Result<ArcStr> {
    bail!("netidx Local authentication is not supported on this platform")
}

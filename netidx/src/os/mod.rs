#[cfg(unix)]
pub(crate) mod unix;

#[cfg(windows)]
pub(crate) mod windows;

#[cfg(unix)]
pub(crate) use unix::*;

#[cfg(windows)]
pub(crate) use windows::*;

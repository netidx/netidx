use anyhow::Result;

#[cfg(unix)]
pub(crate) mod unix;

#[cfg(windows)]
pub(crate) mod windows;

pub(crate) mod local_auth {
    use super::*;
    use bytes::{Buf, BufMut, Bytes};
    use netidx_core::{
        chars::Chars,
        pack::{Pack, PackError},
    };

    pub(crate) struct Credential {
        pub(crate) salt: u64,
        pub(crate) user: Chars,
        pub(crate) token: Bytes,
    }

    impl Pack for Credential {
        fn const_encoded_len() -> Option<usize> {
            None
        }

        fn encoded_len(&self) -> usize {
            Pack::encoded_len(&self.salt)
                + Pack::encoded_len(&self.user)
                + Pack::encoded_len(&self.token)
        }

        fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
            Pack::encode(&self.salt, buf)?;
            Pack::encode(&self.user, buf)?;
            Pack::encode(&self.token, buf)
        }

        fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
            let salt: u64 = Pack::decode(buf)?;
            let user: Chars = Pack::decode(buf)?;
            let token: Bytes = Pack::decode(buf)?;
            Ok(Credential { salt, user, token })
        }
    }

    #[cfg(windows)]
    pub(crate) use windows::local_auth::*;

    #[cfg(unix)]
    pub(crate) use unix::local_auth::*;
}

#[cfg(unix)]
pub(crate) use unix::Mapper;

#[cfg(windows)]
pub(crate) use windows::Mapper;

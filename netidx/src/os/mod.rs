use anyhow::Result;

#[cfg(unix)]
pub(crate) mod unix;

#[cfg(windows)]
pub(crate) mod windows;

pub(crate) mod local_auth {
    use super::*;
    use arcstr::ArcStr;
    use bytes::{Buf, BufMut, Bytes};
    use netidx_core::{
        pack::{Pack, PackError},
    };
    use netidx_netproto::resolver::HashMethod;

    pub(crate) struct Credential {
        pub(crate) hash_method: HashMethod,
        pub(crate) salt: u128,
        pub(crate) user: ArcStr,
        pub(crate) token: Bytes,
    }

    impl Pack for Credential {
        fn encoded_len(&self) -> usize {
            Pack::encoded_len(&self.hash_method)
                + Pack::encoded_len(&self.salt)
                + Pack::encoded_len(&self.user)
                + Pack::encoded_len(&self.token)
        }

        fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
            Pack::encode(&self.hash_method, buf)?;
            Pack::encode(&self.salt, buf)?;
            Pack::encode(&self.user, buf)?;
            Pack::encode(&self.token, buf)
        }

        fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
            let hash_method = Pack::decode(buf)?;
            let salt = Pack::decode(buf)?;
            let user = Pack::decode(buf)?;
            let token = Pack::decode(buf)?;
            Ok(Credential { hash_method, salt, user, token })
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

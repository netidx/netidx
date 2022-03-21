#[cfg(unix)]
pub(crate) mod unix;

#[cfg(windows)]
pub(crate) mod windows;

pub(crate) mod local_auth {
    pub(crate) struct Credential {
        salt: u64,
        user: String,
        token: Bytes,
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
            let user: String = Pack::decode(buf)?;
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

use super::{Krb5Ctx, Krb5ServerCtx};
use anyhow::{anyhow, Error, Result};
use bytes::{Buf as _, BytesMut};
use libgssapi::{
    context::{
        ClientCtx as GssClientCtx, CtxFlags, SecurityContext, ServerCtx as GssServerCtx,
    },
    credential::{Cred, CredUsage},
    error::{Error as GssError, MajorFlags},
    name::Name,
    oid::{OidSet, GSS_MECH_KRB5, GSS_NT_KRB5_PRINCIPAL},
    util::{Buf, GssIov, GssIovFake, GssIovType},
};
use std::{process::Command, time::Duration, net::IpAddr};
use tokio::task;

fn wrap_iov(
    ctx: &impl SecurityContext,
    encrypt: bool,
    header: &mut BytesMut,
    data: &mut BytesMut,
    padding: &mut BytesMut,
    trailer: &mut BytesMut,
) -> Result<()> {
    let mut len_iovs = [
        GssIovFake::new(GssIovType::Header),
        GssIov::new(GssIovType::Data, &mut **data).as_fake(),
        GssIovFake::new(GssIovType::Padding),
        GssIovFake::new(GssIovType::Trailer),
    ];
    ctx.wrap_iov_length(encrypt, &mut len_iovs[..])?;
    header.resize(len_iovs[0].len(), 0x0);
    padding.resize(len_iovs[2].len(), 0x0);
    trailer.resize(len_iovs[3].len(), 0x0);
    let mut iovs = [
        GssIov::new(GssIovType::Header, &mut **header),
        GssIov::new(GssIovType::Data, &mut **data),
        GssIov::new(GssIovType::Padding, &mut **padding),
        GssIov::new(GssIovType::Trailer, &mut **trailer),
    ];
    Ok(ctx.wrap_iov(encrypt, &mut iovs)?)
}

fn unwrap_iov(
    ctx: &impl SecurityContext,
    len: usize,
    msg: &mut BytesMut,
) -> Result<BytesMut> {
    let (hdr_len, data_len) = {
        let mut iov = [
            GssIov::new(GssIovType::Stream, &mut msg[0..len]),
            GssIov::new(GssIovType::Data, &mut []),
        ];
        ctx.unwrap_iov(&mut iov[..])?;
        let hdr_len = iov[0].header_length(&iov[1]).unwrap();
        let data_len = iov[1].len();
        (hdr_len, data_len)
    };
    msg.advance(hdr_len);
    let data = msg.split_to(data_len);
    msg.advance(len - hdr_len - data_len);
    Ok(data) // return the decrypted contents
}

#[derive(Debug, Clone)]
pub(crate) struct ClientCtx(GssClientCtx);

impl Krb5Ctx for ClientCtx {
    type Buf = Buf;

    fn step(&self, token: Option<&[u8]>) -> Result<Option<Self::Buf>> {
        task::block_in_place(|| self.0.step(token).map_err(|e| Error::from(e)))
    }

    fn wrap(&self, encrypt: bool, msg: &[u8]) -> Result<Self::Buf> {
        self.0.wrap(encrypt, msg).map_err(|e| Error::from(e))
    }

    fn wrap_iov(
        &self,
        encrypt: bool,
        header: &mut BytesMut,
        data: &mut BytesMut,
        padding: &mut BytesMut,
        trailer: &mut BytesMut,
    ) -> Result<()> {
        wrap_iov(&self.0, encrypt, header, data, padding, trailer)
    }

    fn unwrap(&self, msg: &[u8]) -> Result<Self::Buf> {
        self.0.unwrap(msg).map_err(|e| Error::from(e))
    }

    fn unwrap_iov(&self, len: usize, msg: &mut BytesMut) -> Result<BytesMut> {
        unwrap_iov(&self.0, len, msg)
    }

    fn ttl(&self) -> Result<Duration> {
        self.0.lifetime().map_err(|e| Error::from(e))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ServerCtx(GssServerCtx);

impl Krb5Ctx for ServerCtx {
    type Buf = Buf;

    fn step(&self, token: Option<&[u8]>) -> Result<Option<Self::Buf>> {
        task::block_in_place(|| {
            match token {
                Some(token) => self.0.step(token),
                None => {
                    Err(GssError { major: MajorFlags::GSS_S_DEFECTIVE_TOKEN, minor: 0 })
                }
            }
            .map_err(|e| Error::from(e))
        })
    }

    fn wrap(&self, encrypt: bool, msg: &[u8]) -> Result<Self::Buf> {
        self.0.wrap(encrypt, msg).map_err(|e| Error::from(e))
    }

    fn wrap_iov(
        &self,
        encrypt: bool,
        header: &mut BytesMut,
        data: &mut BytesMut,
        padding: &mut BytesMut,
        trailer: &mut BytesMut,
    ) -> Result<()> {
        wrap_iov(&self.0, encrypt, header, data, padding, trailer)
    }

    fn unwrap(&self, msg: &[u8]) -> Result<Self::Buf> {
        self.0.unwrap(msg).map_err(|e| Error::from(e))
    }

    fn unwrap_iov(&self, len: usize, msg: &mut BytesMut) -> Result<BytesMut> {
        unwrap_iov(&self.0, len, msg)
    }

    fn ttl(&self) -> Result<Duration> {
        self.0.lifetime().map_err(|e| Error::from(e))
    }
}

impl Krb5ServerCtx for ServerCtx {
    fn client(&self) -> Result<String> {
        let n = self.0.source_name().map_err(|e| Error::from(e))?;
        Ok(format!("{}", n))
    }
}

pub(crate) fn create_client_ctx(
    principal: Option<&str>,
    target_principal: &str,
) -> Result<ClientCtx> {
    task::block_in_place(|| {
        let name = principal
            .map(|n| {
                Name::new(n.to_bytes(), Some(&GSS_NT_KRB5_PRINCIPAL))?
                    .canonicalize(Some(&GSS_MECH_KRB5))
            })
            .transpose()?;
        let target = Name::new(target_principal.to_bytes(), Some(&GSS_NT_KRB5_PRINCIPAL))?
            .canonicalize(Some(&GSS_MECH_KRB5))?;
        let cred = {
            let mut s = OidSet::new()?;
            s.add(&GSS_MECH_KRB5)?;
            Cred::acquire(name.as_ref(), None, CredUsage::Initiate, Some(&s))?
        };
        Ok(ClientCtx(GssClientCtx::new(
            cred,
            target,
            CtxFlags::GSS_C_MUTUAL_FLAG,
            Some(&GSS_MECH_KRB5),
        )))
    })
}

pub(crate) fn create_server_ctx(principal: Option<&str>) -> Result<ServerCtx> {
    task::block_in_place(|| {
        let name = principal
            .map(|principal| -> Result<Name> {
                Ok(Name::new(principal.to_bytes(), Some(&GSS_NT_KRB5_PRINCIPAL))?
                    .canonicalize(Some(&GSS_MECH_KRB5))?)
            })
            .transpose()?;
        let cred = {
            let mut s = OidSet::new()?;
            s.add(&GSS_MECH_KRB5)?;
            Cred::acquire(name.as_ref(), None, CredUsage::Accept, Some(&s))?
        };
        Ok(ServerCtx(GssServerCtx::new(cred)))
    })
}

// Unix group membership is a little complex, it can come from a
// lot of places, and it's not entirely standardized at the api
// level, it seems libc provides getgrouplist on most platforms,
// but unfortunatly Apple doesn't implement it. Luckily the 'id'
// command is specified in POSIX.
pub(crate) struct Mapper(String);

impl Mapper {
    pub(crate) fn new() -> Result<Mapper> {
        task::block_in_place(|| {
            let out = Command::new("sh").arg("-c").arg("which id").output()?;
            let buf = String::from_utf8_lossy(&out.stdout);
            let path =
                buf.lines().next().ok_or_else(|| anyhow!("can't find the id command"))?;
            Ok(Mapper(String::from(path)))
        })
    }

    pub(crate) fn groups(&mut self, user: &str) -> Result<Vec<String>> {
        task::block_in_place(|| {
            let out = Command::new(&self.0).arg(user).output()?;
            Mapper::parse_output(&String::from_utf8_lossy(&out.stdout))
        })
    }

    fn parse_output(out: &str) -> Result<Vec<String>> {
        let mut groups = Vec::new();
        match out.find("groups=") {
            None => Ok(Vec::new()),
            Some(i) => {
                let mut s = &out[i..];
                while let Some(i_op) = s.find('(') {
                    match s.find(')') {
                        None => {
                            return Err(anyhow!(
                                "invalid id command output, expected ')'"
                            ))
                        }
                        Some(i_cp) => {
                            groups.push(String::from(&s[i_op + 1..i_cp]));
                            s = &s[i_cp + 1..];
                        }
                    }
                }
                Ok(groups)
            }
        }
    }
}

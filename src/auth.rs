use crate::path::Path;
use failure::Error;
use std::{
    collections::HashMap, convert::TryFrom, error, iter, ops::Deref, sync::Arc,
    time::Duration,
};

bitflags! {
    pub struct Permissions: u32 {
        const DENY             = 0x01;
        const SUBSCRIBE        = 0x02;
        const PUBLISH          = 0x04;
        const PUBLISH_DEFAULT  = 0x08;
        const PUBLISH_REFERRAL = 0x10;
    }
}

impl TryFrom<&str> for Permissions {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let mut p = Permissions::empty();
        for (i, c) in s.chars().enumerate() {
            match c {
                '!' => {
                    if i == 0 {
                        p |= Permissions::DENY;
                    } else {
                        bail!("! may only be used as the first character")
                    }
                }
                's' => {
                    p |= Permissions::SUBSCRIBE;
                }
                'p' => {
                    p |= Permissions::PUBLISH;
                }
                'd' => {
                    p |= Permissions::PUBLISH_DEFAULT;
                }
                'r' => {
                    p |= Permissions::PUBLISH_REFERRAL;
                }
                c => bail!("unrecognized permission bit {}, valid bits are !spdr", c),
            }
        }
        Ok(p)
    }
}

pub trait GMapper {
    fn groups(&mut self, user: &str) -> Result<Vec<String>, Error>;
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Entity(u32);

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct UserInfo {
    id: Entity,
    groups: Vec<Entity>,
}

impl UserInfo {
    fn entities(&self) -> impl Iterator<Item = &Entity> {
        iter::once(&self.id).chain(self.groups.iter())
    }
}

pub struct UserDb<M: GMapper> {
    next: u32,
    mapper: M,
    entities: HashMap<String, Entity>,
    users: HashMap<String, UserInfo>,
}

impl<M: GMapper> UserDb<M> {
    pub fn new(mapper: M) -> UserDb<M> {
        UserDb {
            next: 0,
            mapper,
            entities: HashMap::new(),
            users: HashMap::new(),
        }
    }

    fn entity(&mut self, name: &str) -> Entity {
        match self.entities.get(name) {
            Some(e) => *e,
            None => {
                let e = Entity(self.next);
                self.next += 1;
                self.entities.insert(String::from(name), e);
                e
            }
        }
    }

    pub fn get_info(&self, user: &str) -> Option<&UserInfo> {
        self.users.get(user)
    }

    pub fn add_info(&mut self, user: &str) -> Result<(), Error> {
        let ifo = UserInfo {
            id: self.entity(user),
            groups: self
                .mapper
                .groups(user)?
                .into_iter()
                .map(|b| self.entity(&b))
                .collect::<Vec<_>>(),
        };
        self.users.insert(String::from(user), ifo);
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PMapFile(HashMap<Path, HashMap<String, String>>);

#[derive(Debug)]
pub struct PMap(HashMap<Path, HashMap<Entity, Permissions>>);

impl PMap {
    fn from_file<M: GMapper>(file: PMapFile, db: &mut UserDb<M>) -> Result<Self, Error> {
        let mut pmap = HashMap::with_capacity(file.0.len());
        for (path, tbl) in file.0.iter() {
            let mut entry = HashMap::with_capacity(tbl.len());
            for (ent, perm) in tbl.iter() {
                entry.insert(db.entity(ent), Permissions::try_from(perm.as_str())?);
            }
            pmap.insert(path.clone(), entry);
        }
        Ok(PMap(pmap))
    }

    fn permissions(&self, path: &Path, user: &UserInfo) -> Permissions {
        Path::basenames(&*path).fold(Permissions::empty(), |p, s| match self.0.get(s) {
            None => p,
            Some(set) => {
                let init = (p, Permissions::empty());
                let (ap, dp) =
                    user.entities().fold(init, |(ap, dp), e| match set.get(e) {
                        None => (ap, dp),
                        Some(p_) => {
                            if p_.contains(Permissions::DENY) {
                                (ap, dp | *p_)
                            } else {
                                (ap | *p_, dp)
                            }
                        }
                    });
                ap & !dp
            }
        })
    }
}

pub trait Krb5Ctx {
    type Buf: Deref<Target = [u8]>;

    fn step(&self, token: Option<&[u8]>) -> Result<Option<Self::Buf>, Error>;
    fn wrap(&self, encrypt: bool, msg: &[u8]) -> Result<Self::Buf, Error>;
    fn unwrap(&self, msg: &[u8]) -> Result<Self::Buf, Error>;
    fn ttl(&self) -> Result<Duration, Error>;
    fn open(&self) -> Result<bool, Error>;
}

pub trait Krb5ServerCtx: Krb5Ctx {
    fn client(&self) -> Result<String, Error>;
}

pub trait Krb5 {
    type Buf: Deref<Target = [u8]>;
    type Krb5ClientCtx: Krb5Ctx<Buf = Self::Buf>;
    type Krb5ServerCtx: Krb5ServerCtx<Buf = Self::Buf>;

    fn create_client_ctx(
        &self,
        principal: Option<&[u8]>,
        target_principal: &[u8],
    ) -> Result<Self::Krb5ClientCtx, Error>;

    fn create_server_ctx(&self, principal: &[u8]) -> Result<Self::Krb5ServerCtx, Error>;
}

#[cfg(unix)]
pub(crate) mod syskrb5 {
    use super::{Krb5, Krb5Ctx, Krb5ServerCtx};
    use failure::Error;
    use libgssapi::{
        context::{
            ClientCtx as GssClientCtx, CtxFlags, CtxInfo, SecurityContext,
            ServerCtx as GssServerCtx,
        },
        credential::{Cred, CredUsage},
        error::{Error as GssError, MajorFlags},
        name::Name,
        oid::{OidSet, GSS_MECH_KRB5, GSS_NT_KRB5_PRINCIPAL},
        util::Buf,
    };
    use std::{collections::HashMap, sync::Arc, time::Duration};

    #[derive(Clone)]
    pub(crate) struct ClientCtx(GssClientCtx);

    impl Krb5Ctx for ClientCtx {
        type Buf = Buf;

        fn step(&self, token: Option<&[u8]>) -> Result<Option<Self::Buf>, Error> {
            self.0
                .step(token)
                .map_err(|e| Error::from_boxed_compat(Box::new(e)))
        }

        fn wrap(&self, encrypt: bool, msg: &[u8]) -> Result<Self::Buf, Error> {
            self.0
                .wrap(encrypt, msg)
                .map_err(|e| Error::from_boxed_compat(Box::new(e)))
        }

        fn unwrap(&self, msg: &[u8]) -> Result<Self::Buf, Error> {
            self.0
                .unwrap(msg)
                .map_err(|e| Error::from_boxed_compat(Box::new(e)))
        }

        fn ttl(&self) -> Result<Duration, Error> {
            self.0
                .lifetime()
                .map_err(|e| Error::from_boxed_compat(Box::new(e)))
        }

        fn open(&self) -> Result<bool, Error> {
            self.0
                .open()
                .map_err(|e| Error::from_boxed_compat(Box::new(e)))
        }
    }

    #[derive(Clone)]
    pub(crate) struct ServerCtx(GssServerCtx);

    impl Krb5Ctx for ServerCtx {
        type Buf = Buf;

        fn step(&self, token: Option<&[u8]>) -> Result<Option<Self::Buf>, Error> {
            match token {
                Some(token) => self.0.step(token),
                None => Err(GssError {
                    major: MajorFlags::GSS_S_DEFECTIVE_TOKEN,
                    minor: 0,
                }),
            }
            .map_err(|e| Error::from_boxed_compat(Box::new(e)))
        }

        fn wrap(&self, encrypt: bool, msg: &[u8]) -> Result<Self::Buf, Error> {
            self.0
                .wrap(encrypt, msg)
                .map_err(|e| Error::from_boxed_compat(Box::new(e)))
        }

        fn unwrap(&self, msg: &[u8]) -> Result<Self::Buf, Error> {
            self.0
                .unwrap(msg)
                .map_err(|e| Error::from_boxed_compat(Box::new(e)))
        }

        fn ttl(&self) -> Result<Duration, Error> {
            self.0
                .lifetime()
                .map_err(|e| Error::from_boxed_compat(Box::new(e)))
        }

        fn open(&self) -> Result<bool, Error> {
            self.0
                .open()
                .map_err(|e| Error::from_boxed_compat(Box::new(e)))
        }
    }

    impl Krb5ServerCtx for ServerCtx {
        fn client(&self) -> Result<String, Error> {
            let n = self
                .0
                .source_name()
                .map_err(|e| Error::from_boxed_compat(Box::new(e)))?;
            Ok(format!("{}", n))
        }
    }

    pub(crate) struct SysKrb5;

    pub(crate) static sys_krb5: SysKrb5 = SysKrb5;

    impl Krb5 for SysKrb5 {
        type Buf = Buf;
        type Krb5ClientCtx = ClientCtx;
        type Krb5ServerCtx = ServerCtx;

        // CR estokes: this has to read files and potentially talk to
        // the KDC, so it can block, we need to figure out how best to
        // deal with that.
        fn create_client_ctx(
            &self,
            principal: Option<&[u8]>,
            target_principal: &[u8],
        ) -> Result<Self::Krb5ClientCtx, Error> {
            let name = principal
                .map(|n| {
                    Name::new(n, Some(&GSS_NT_KRB5_PRINCIPAL))?
                        .canonicalize(Some(&GSS_NT_KRB5_PRINCIPAL))
                })
                .transpose()?;
            let target = Name::new(target_principal, Some(&GSS_NT_KRB5_PRINCIPAL))?
                .canonicalize(Some(&GSS_NT_KRB5_PRINCIPAL))?;
            let cred = {
                let mut s = OidSet::new()?;
                s.add(&GSS_MECH_KRB5)?;
                Cred::acquire(name, None, CredUsage::Initiate, Some(&s))?
            };
            Ok(ClientCtx(GssClientCtx::new(
                cred,
                target,
                CtxFlags::GSS_C_MUTUAL_FLAG,
                Some(&GSS_MECH_KRB5),
            )?))
        }

        // CR estokes: Should we offer an api to set KRB5_KTNAME, or
        // just let the user do it? At the moment I'm not sure heimdal
        // uses the same environment variable/format, so maybe leave
        // it to the user.
        fn create_server_ctx(
            &self,
            principal: &[u8],
        ) -> Result<Self::Krb5ServerCtx, Error> {
            let name = Some(
                Name::new(principal, Some(&GSS_NT_KRB5_PRINCIPAL))?
                    .canonicalize(Some(&GSS_NT_KRB5_PRINCIPAL)),
            );
            let cred = {
                let mut s = OidSet::new()?;
                s.add(&GSS_MECH_KRB5)?;
                Cred::acquire(name, None, CredUsage::Accept, Some(&s))?
            };
            Ok(ServerCtx(GssServerCtx::new(cred)?))
        }
    }
}

#[cfg(unix)]
mod sysgmapper {
    // Unix group membership is a little complex, it can come from a
    // lot of places, and it's not entirely standardized at the api
    // level, it seems libc provides getgrouplist on most platforms,
    // but unfortunatly Apple doesn't implement it. Luckily the 'id'
    // command is specified in POSIX.
    use super::GMapper;
    use failure::Error;
    use std::process::Command;

    pub struct Mapper(String);

    impl GMapper for Mapper {
        fn groups(&mut self, user: &str) -> Result<Vec<String>, Error> {
            let out = Command::new(&self.0).arg(user).output()?;
            Mapper::parse_output(&String::from_utf8_lossy(&out.stdout))
        }
    }

    impl Mapper {
        fn new() -> Result<Mapper, Error> {
            let out = Command::new("sh").arg("-c").arg("which id").output()?;
            let buf = String::from_utf8_lossy(&out.stdout);
            let path = buf
                .lines()
                .next()
                .ok_or_else(|| format_err!("can't find the id command"))?;
            Ok(Mapper(String::from(path)))
        }

        fn parse_output(out: &str) -> Result<Vec<String>, Error> {
            let mut groups = Vec::new();
            match out.find("groups=") {
                None => Ok(Vec::new()),
                Some(i) => {
                    let mut s = &out[i..];
                    while let Some(i_op) = s.find('(') {
                        match s.find(')') {
                            None => bail!("invalid id command output, expected ')'"),
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
}

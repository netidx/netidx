use crate::{config, path::Path};
use failure::Error;
use std::{
    collections::HashMap, convert::TryFrom, iter, ops::Deref, sync::Arc, time::Duration,
};

bitflags! {
    pub struct Permissions: u32 {
        const DENY             = 0x01;
        const SUBSCRIBE        = 0x02;
        const LIST             = 0x04;
        const PUBLISH          = 0x08;
        const PUBLISH_DEFAULT  = 0x10;
        const PUBLISH_REFERRAL = 0x20;
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
                'l' => {
                    p |= Permissions::LIST;
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
                c => bail!("unrecognized permission bit {}, valid bits are !spldr", c),
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
pub(crate) struct UserInfo {
    pub(crate) id: Entity,
    pub(crate) groups: Vec<Entity>,
}

impl UserInfo {
    fn entities(&self) -> impl Iterator<Item = &Entity> {
        iter::once(&self.id).chain(self.groups.iter())
    }
}

lazy_static! {
    pub(crate) static ref ANONYMOUS: Arc<UserInfo> = Arc::new(UserInfo {
        id: Entity(0),
        groups: Vec::new(),
    });
}

pub(crate) struct UserDb<M: GMapper> {
    next: u32,
    mapper: M,
    entities: HashMap<String, Entity>,
    users: HashMap<String, Arc<UserInfo>>,
}

impl<M: GMapper> UserDb<M> {
    pub(crate) fn new(mapper: M) -> UserDb<M> {
        UserDb {
            next: 1,
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

    pub(crate) fn ifo(&mut self, user: Option<&str>) -> Result<Arc<UserInfo>, Error> {
        match dbg!(user) {
            None => Ok(ANONYMOUS.clone()),
            Some(user) => match self.users.get(user) {
                Some(user) => Ok(user.clone()),
                None => {
                    let ifo = Arc::new(UserInfo {
                        id: self.entity(user),
                        groups: dbg!(self
                            .mapper
                            .groups(user)?
                            .into_iter()
                            .map(|b| self.entity(&b))
                            .collect::<Vec<_>>()),
                    });
                    self.users.insert(String::from(user), ifo.clone());
                    Ok(ifo)
                }
            },
        }
    }
}

#[derive(Debug)]
pub(crate) struct PMap(HashMap<Path, HashMap<Entity, Permissions>>);

impl PMap {
    pub(crate) fn from_file<M: GMapper>(
        file: config::resolver_server::PMap,
        db: &mut UserDb<M>,
    ) -> Result<Self, Error> {
        let mut pmap = HashMap::with_capacity(file.0.len());
        for (path, tbl) in file.0.iter() {
            let mut entry = HashMap::with_capacity(tbl.len());
            for (ent, perm) in tbl.iter() {
                let entity = if ent == "" {
                    ANONYMOUS.id
                } else {
                    db.entity(ent)
                };
                entry.insert(entity, Permissions::try_from(perm.as_str())?);
            }
            pmap.insert(path.clone(), entry);
        }
        Ok(PMap(pmap))
    }

    pub(crate) fn allowed_forall<'a>(
        &'a self,
        mut paths: impl Iterator<Item = &'a str>,
        desired_rights: Permissions,
        user: &UserInfo,
    ) -> bool {
        paths.all(|p| {
            let actual_rights = self.permissions(p, user);
            actual_rights & desired_rights == desired_rights
        })
    }

    pub(crate) fn allowed(
        &self,
        path: &str,
        desired_rights: Permissions,
        user: &UserInfo,
    ) -> bool {
        dbg!(self);
        dbg!(user);
        let actual_rights = self.permissions(path, user);
        dbg!(dbg!(actual_rights) & dbg!(desired_rights) == desired_rights)
    }

    pub(crate) fn permissions(&self, path: &str, user: &UserInfo) -> Permissions {
        Path::basenames(path).fold(Permissions::empty(), |p, s| match self.0.get(s) {
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

pub(crate) trait Krb5Ctx {
    type Buf: Deref<Target = [u8]>;

    fn step(&self, token: Option<&[u8]>) -> Result<Option<Self::Buf>, Error>;
    fn wrap(&self, encrypt: bool, msg: &[u8]) -> Result<Self::Buf, Error>;
    fn unwrap(&self, msg: &[u8]) -> Result<Self::Buf, Error>;
    fn ttl(&self) -> Result<Duration, Error>;
}

pub(crate) trait Krb5ServerCtx: Krb5Ctx {
    fn client(&self) -> Result<String, Error>;
}

pub(crate) trait Krb5 {
    type Buf: Deref<Target = [u8]>;
    type Krb5ClientCtx: Krb5Ctx<Buf = Self::Buf>;
    type Krb5ServerCtx: Krb5ServerCtx<Buf = Self::Buf>;

    fn create_client_ctx(
        &self,
        principal: Option<&[u8]>,
        target_principal: &[u8],
    ) -> Result<Self::Krb5ClientCtx, Error>;

    fn create_server_ctx(
        &self,
        principal: Option<&[u8]>,
    ) -> Result<Self::Krb5ServerCtx, Error>;
}

#[cfg(unix)]
pub(crate) mod syskrb5 {
    use super::{Krb5, Krb5Ctx, Krb5ServerCtx};
    use failure::Error;
    use libgssapi::{
        context::{
            ClientCtx as GssClientCtx, CtxFlags, SecurityContext,
            ServerCtx as GssServerCtx,
        },
        credential::{Cred, CredUsage},
        error::{Error as GssError, MajorFlags},
        name::Name,
        oid::{OidSet, GSS_MECH_KRB5, GSS_NT_KRB5_PRINCIPAL},
        util::Buf,
    };
    use std::time::Duration;
    use tokio::task;

    #[derive(Debug, Clone)]
    pub(crate) struct ClientCtx(GssClientCtx);

    impl Krb5Ctx for ClientCtx {
        type Buf = Buf;

        fn step(&self, token: Option<&[u8]>) -> Result<Option<Self::Buf>, Error> {
            task::block_in_place(|| {
                self.0
                    .step(token)
                    .map_err(|e| Error::from_boxed_compat(Box::new(e)))
            })
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
    }

    #[derive(Debug, Clone)]
    pub(crate) struct ServerCtx(GssServerCtx);

    impl Krb5Ctx for ServerCtx {
        type Buf = Buf;

        fn step(&self, token: Option<&[u8]>) -> Result<Option<Self::Buf>, Error> {
            task::block_in_place(|| {
                match token {
                    Some(token) => self.0.step(token),
                    None => Err(GssError {
                        major: MajorFlags::GSS_S_DEFECTIVE_TOKEN,
                        minor: 0,
                    }),
                }
                .map_err(|e| Error::from_boxed_compat(Box::new(e)))
            })
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

    pub(crate) static SYS_KRB5: SysKrb5 = SysKrb5;

    impl Krb5 for SysKrb5 {
        type Buf = Buf;
        type Krb5ClientCtx = ClientCtx;
        type Krb5ServerCtx = ServerCtx;

        fn create_client_ctx(
            &self,
            principal: Option<&[u8]>,
            target_principal: &[u8],
        ) -> Result<Self::Krb5ClientCtx, Error> {
            task::block_in_place(|| {
                let name = principal
                    .map(|n| {
                        Name::new(n, Some(&GSS_NT_KRB5_PRINCIPAL))?
                            .canonicalize(Some(&GSS_MECH_KRB5))
                    })
                    .transpose()?;
                let target = Name::new(target_principal, Some(&GSS_NT_KRB5_PRINCIPAL))?
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

        fn create_server_ctx(
            &self,
            principal: Option<&[u8]>,
        ) -> Result<Self::Krb5ServerCtx, Error> {
            task::block_in_place(|| {
                let name = principal
                    .map(|principal| -> Result<Name, Error> {
                        Ok(Name::new(principal, Some(&GSS_NT_KRB5_PRINCIPAL))?
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
    }
}

#[cfg(unix)]
pub(crate) mod sysgmapper {
    // Unix group membership is a little complex, it can come from a
    // lot of places, and it's not entirely standardized at the api
    // level, it seems libc provides getgrouplist on most platforms,
    // but unfortunatly Apple doesn't implement it. Luckily the 'id'
    // command is specified in POSIX.
    use super::GMapper;
    use failure::Error;
    use std::process::Command;
    use tokio::task;

    pub(crate) struct Mapper(String);

    impl GMapper for Mapper {
        fn groups(&mut self, user: &str) -> Result<Vec<String>, Error> {
            task::block_in_place(|| {
                let out = Command::new(&self.0).arg(user).output()?;
                Mapper::parse_output(&String::from_utf8_lossy(&out.stdout))
            })
        }
    }

    impl Mapper {
        pub(crate) fn new() -> Result<Mapper, Error> {
            task::block_in_place(|| {
                let out = Command::new("sh").arg("-c").arg("which id").output()?;
                let buf = String::from_utf8_lossy(&out.stdout);
                let path = buf
                    .lines()
                    .next()
                    .ok_or_else(|| format_err!("can't find the id command"))?;
                Ok(Mapper(String::from(path)))
            })
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

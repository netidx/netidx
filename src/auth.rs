use crate::Path;
use std::{sync::Arc, collections::HashMap, ops::Deref, error, convert::TryFrom};
use failure::Error;

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
                },
                's' => { p |= Permissions::SUBSCRIBE; },
                'p' => { p |= Permissions::PUBLISH; },
                'd' => { p |= Permissions::PUBLISH_DEFAULT; },
                'r' => { p |= Permissions::PUBLISH_REFERRAL; },
                c => bail!("unrecognized permission bit {}, valid bits are !spdr", c)
            }
        }
        Ok(p)
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Entity(u32);

pub struct EntityDb {
    next: u32,
    entities: HashMap<String, Entity>,
}

impl EntityDb {
    pub fn new() -> EntityDb {
        EntityDb {
            next: 0,
            entities: HashMap::new(),
        }
    }

    pub fn entity(&mut self, name: &str) -> Entity {
        match self.entities.get(name) {
            Some(e) => e,
            None => {
                let e = Entity(self.next);
                self.next += 1;
                self.entities.insert(String::from(name), e);
                e
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthMapFile(HashMap<Path, HashMap<String, String>>);

#[derive(Debug)]
pub struct AuthMap(HashMap<Path, HashMap<Entity, Permissions>>);

impl AuthMap {
    fn from_file(
        file: AuthMapFile,
        db: &mut EntityDb
    ) -> Result<AuthMap, Error> {
        let mut authmap = HashMap::with_capacity(file.0.len());
        for (path, tbl) in file.0.iter() {
            let mut entry = HashMap::with_capacity(tbl.len());
            for (ent, perm) in tbl.iter() {
                entry.insert(db.entity(ent), perm.parse::<Permissions>()?)
            }
            authmap.insert(path.clone(), entry);
        }
        Ok(AuthMap(authmap))
    }
}

pub trait Krb5Ctx {
    type Error = error::Error;
    type Buf = Deref<Target = [u8]>;

    fn step(&self, token: Option<&[u8]>) -> Result<Option<Buf>, Error>;
    fn wrap(&self, encrypt: bool, msg: &[u8]) -> Result<Buf, Error>;
    fn unwrap(&self, msg: &[u8]) -> Result<Buf, Error>;
    fn ttl(&self) -> Result<u32, Error>;
}

pub trait Krb5ServerCtx: Krb5Ctx {
    fn client(&self) -> Result<Self::Buf, Self::Error>;
}

pub trait Krb5 {
    type Error = error::Error;
    type Buf = Deref<Target = [u8]>;
    type Krb5Ctx = Krb5Ctx<Error = Error, Buf = Buf>;

    fn create_client_ctx(&mut self, target_principal: &[u8]) -> Result<Krb5Ctx, Error>;
    fn create_server_ctx(&mut self, principal: &[u8]) -> Result<Krb5ServerCtx, Error>;
}

#[cfg(unix)]
mod krb5 {
    use super::{Krb5, Krb5Ctx, Krb5CtxInfo, Entity};
    use libgssapi::{
        context::{ClientCtx, ServerCtx, SecurityContext, CtxFlags, CtxInfo},
        name::Name,
        credentials::Cred,
        oid::{GSS_NT_KRB5_PRINCIPAL, GSS_MECH_KRB5},
        error::{Error, MajorFlags},
        util::Buf,
    };
    use std::{sync::Arc, collections::HashMap};

    struct UserDbInner {
        next: u32,
        cache: HashMap<Buf, ClientInfo>
    }

    struct UserDb(Arc<Mutex<UserDbInner>>);

    impl UserDb {
        fn new() -> UserDb {
            UserDb(Arc::new(Mutex::new(UserDbInner {
                next: 0,
                cache: HashMap::new()
            })))
        }

        fn translate(ifo: CtxInfo) -> Krb5CtxInfo {
            
        }
    }


    pub struct Krb5ClientCtx {
        gss: ClientCtx,
        db: UserDb,
    }

    impl Krb5Ctx for Krb5ClientCtx {
        type Error = Error;
        type Buf = Buf;

        fn step(&self, token: Option<&[u8]>) -> Result<Option<Buf>, Error> {
            self.gss.step(token)
        }

        fn wrap(&self, encrypt: bool, msg: &[u8]) -> Result<Buf, Error> {
            self.gss.wrap(encrypt, msg)
        }

        fn unwrap(&self, msg: &[u8]) -> Result<Buf, Error> {
            self.gss.unwrap(msg)
        }

        fn info(&self) -> Result<Krb5CtxInfo, Error> {
            self.db.translate(self.gss.info()?)
        }
    }

    pub struct Krb5ServerCtx {
        gss: ServerCtx,
        db: UserDb,
    }

    impl Krb5Ctx for  {
        type Error = Error;
        type Buf = Buf;

        fn step(&self, token: Option<&[u8]>) -> Result<Buf, Error> {
            match token {
                Some(token) => self.gss.step(token),
                None => Err(Error {major: MajorFlags::GSS_S_DEFECTIVE_TOKEN, minor: 0})
            }
        }

        fn wrap(&self, encrypt: bool, msg: &[u8]) -> Result<Buf, Error> {
            self.gss.wrap(encrypt, msg)
        }

        fn unwrap(&self, msg: &[u8]) -> Result<Buf, Error> {
            self.gss.unwrap(msg)
        }

        fn info(&self) -> Result<Krb5CtxInfo, Error> {
            self.db.translate(self.gss.info()?)
        }
    }
}

use std::{sync::Arc, collections::BTreeMap, ops::Deref, error};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Entity(u32);

bitflags! {
    pub struct Permissions: u32 {
        const DENY            = 0x01;
        const SUBSCRIBE       = 0x02;
        const PUBLISH         = 0x04;
        const PUBLISH_DEFAULT = 0x08;
        const DELEGATE        = 0x10;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Control(Arc<BTreeMap<Entity, Permissions>>);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AclNode {
    control: Control,
    next: Option<Acl>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Acl(Arc<AclNode>);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientInfo {
    pub user: Entity,
    pub groups: Vec<Entity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Krb5CtxInfo {
    pub source: ClientInfo,
    pub target: ClientInfo,
    pub lifetime: u32,
}

pub trait Krb5Ctx {
    type Error = error::Error;
    type Buf = Deref<Target = [u8]>;

    fn step(&self, token: Option<&[u8]>) -> Result<Option<Buf>, Error>;
    fn wrap(&self, encrypt: bool, msg: &[u8]) -> Result<Buf, Error>;
    fn unwrap(&self, msg: &[u8]) -> Result<Buf, Error>;
    fn info(&self) -> Result<Krb5CtxInfo, Self::Error>;
}

pub trait Krb5 {
    type Error = error::Error;
    type Buf = Deref<Target = [u8]>;
    type Krb5Ctx = Krb5Ctx<Error = Error, Buf = Buf>;

    fn create_client_ctx(&mut self, target_principal: &[u8]) -> Result<Krb5Ctx, Error>;
    fn create_server_ctx(&mut self, principal: &[u8]) -> Result<Krb5Ctx, Error>;
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

    struct UserDbInner {
        
    }
}

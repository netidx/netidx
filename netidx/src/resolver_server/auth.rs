use super::config;
use crate::{
    os::Mapper,
    path::Path,
    protocol::{glob::Scope, resolver::Referral},
};
use anyhow::{anyhow, Error, Result};
use arcstr::ArcStr;
use fxhash::FxHashMap;
use netidx_core::pool::Pool;
use netidx_netproto::resolver;
use std::{
    collections::{BTreeMap, Bound, HashMap},
    convert::TryFrom,
    iter,
    net::SocketAddr,
    sync::Arc,
};

bitflags! {
    pub struct Permissions: u32 {
        const DENY             = 0x01;
        const SUBSCRIBE        = 0x02;
        const WRITE            = 0x04;
        const LIST             = 0x08;
        const PUBLISH          = 0x10;
        const PUBLISH_DEFAULT  = 0x20;
    }
}

impl TryFrom<&str> for Permissions {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self> {
        let mut p = Permissions::empty();
        for (i, c) in s.chars().enumerate() {
            match c {
                '!' => {
                    if i == 0 {
                        p |= Permissions::DENY;
                    } else {
                        return Err(anyhow!("! may only be used as the first character"));
                    }
                }
                's' => {
                    p |= Permissions::SUBSCRIBE;
                }
                'w' => {
                    p |= Permissions::WRITE;
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
                c => {
                    return Err(anyhow!(
                        "unrecognized permission bit {}, valid bits are !swlpd",
                        c
                    ))
                }
            }
        }
        Ok(p)
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Entity(u32);

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct UserInfo {
    pub(crate) id: Entity,
    pub(crate) primary_group: Entity,
    pub(crate) groups: Vec<Entity>,
    pub(crate) user_info: Option<resolver::UserInfo>,
}

impl UserInfo {
    fn entities(&self) -> impl Iterator<Item = &Entity> {
        iter::once(&self.id)
            .chain(iter::once(&self.primary_group))
            .chain(self.groups.iter())
    }
}

lazy_static! {
    pub(crate) static ref ANONYMOUS: Arc<UserInfo> = Arc::new(UserInfo {
        id: Entity(0),
        primary_group: Entity(0),
        groups: vec![Entity(0)],
        user_info: None,
    });
    static ref GROUPS: Pool<Vec<ArcStr>> = Pool::new(200, 100);
}

pub(crate) struct UserDb {
    next: u32,
    mapper: Mapper,
    names: FxHashMap<Entity, ArcStr>,
    entities: FxHashMap<ArcStr, Entity>,
    users: FxHashMap<ArcStr, Arc<UserInfo>>,
}

impl UserDb {
    pub(crate) fn new(mapper: Mapper) -> UserDb {
        UserDb {
            next: 1,
            mapper,
            names: HashMap::default(),
            entities: HashMap::default(),
            users: HashMap::default(),
        }
    }

    fn entity(&mut self, name: &str) -> Entity {
        match self.entities.get(name) {
            Some(e) => *e,
            None => {
                let e = Entity(self.next);
                self.next += 1;
                let name = ArcStr::from(name);
                self.entities.insert(name.clone(), e);
                self.names.insert(e, name);
                e
            }
        }
    }

    pub(crate) fn ifo(
        &mut self,
        resolver: SocketAddr,
        user: Option<&str>,
    ) -> Result<Arc<UserInfo>> {
        match user {
            None => Ok(ANONYMOUS.clone()),
            Some(user) => match self.users.get(user) {
                Some(user) => Ok(user.clone()),
                None => {
                    let (primary_group_s, groups_s) = self.mapper.groups(user)?;
                    let primary_group = self.entity(&primary_group_s);
                    let groups =
                        groups_s.iter().map(|b| self.entity(b)).collect::<Vec<_>>();
                    let id = self.entity(user);
                    let ifo = Arc::new(UserInfo {
                        id,
                        primary_group,
                        groups,
                        user_info: Some(resolver::UserInfo {
                            name: ArcStr::from(user),
                            primary_group: primary_group_s,
                            groups: groups_s.into(),
                            resolver,
                            token: bytes::Bytes::new(),
                        }),
                    });
                    self.users.insert(self.names[&id].clone(), ifo.clone());
                    Ok(ifo)
                }
            },
        }
    }
}

#[derive(Debug)]
pub(super) struct PMap(BTreeMap<Path, HashMap<Entity, Permissions>>);

impl PMap {
    pub(super) fn from_file(
        file: &config::PMap,
        db: &mut UserDb,
        root: &str,
        children: &BTreeMap<Path, Referral>,
    ) -> Result<Self> {
        let mut pmap = BTreeMap::new();
        for (path, tbl) in file.0.iter() {
            let path = Path::from(path);
            if !Path::is_parent(root, &path) {
                bail!("permission entry for parent: {}, entry: {}", root, path)
            }
            for child in children.keys() {
                if Path::is_parent(child, &path) {
                    bail!("permission entry for child: {}, entry: {}", child, path)
                }
            }
            let mut entry = HashMap::with_capacity(tbl.len());
            for (ent, perm) in tbl.iter() {
                let entity = if ent == "" { ANONYMOUS.id } else { db.entity(ent) };
                entry.insert(entity, Permissions::try_from(perm.as_str())?);
            }
            pmap.insert(path, entry);
        }
        Ok(PMap(pmap))
    }

    pub(crate) fn allowed(
        &self,
        path: &str,
        desired_rights: Permissions,
        user: &UserInfo,
    ) -> bool {
        let actual_rights = self.permissions(path, user);
        actual_rights & desired_rights == desired_rights
    }

    pub(crate) fn allowed_in_scope(
        &self,
        base_path: &str,
        scope: &Scope,
        desired_rights: Permissions,
        user: &UserInfo,
    ) -> bool {
        let rights_at_base = self.permissions(base_path, user);
        let mut rights = rights_at_base;
        let mut iter = self.0.range::<str, (Bound<&str>, Bound<&str>)>((
            Bound::Excluded(base_path),
            Bound::Unbounded,
        ));
        while let Some((path, set)) = iter.next() {
            if !Path::is_parent(base_path, path) || !scope.contains(Path::levels(&*path))
            {
                break;
            }
            let deny =
                user.entities().fold(Permissions::empty(), |dp, e| match set.get(e) {
                    None => dp,
                    Some(p) => {
                        if p.contains(Permissions::DENY) {
                            dp | *p
                        } else {
                            dp
                        }
                    }
                });
            rights &= !deny;
        }
        rights & desired_rights == desired_rights
    }

    pub(crate) fn permissions(&self, path: &str, user: &UserInfo) -> Permissions {
        Path::dirnames(path).fold(Permissions::empty(), |p, s| match self.0.get(s) {
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

use crate::{config, os::Mapper, path::Path, protocol::resolver::v1::Referral};
use anyhow::{anyhow, Error, Result};
use std::{collections::{BTreeMap, HashMap}, convert::TryFrom, iter, sync::Arc};

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
    pub(crate) groups: Vec<Entity>,
}

impl UserInfo {
    fn entities(&self) -> impl Iterator<Item = &Entity> {
        iter::once(&self.id).chain(self.groups.iter())
    }
}

lazy_static! {
    pub(crate) static ref ANONYMOUS: Arc<UserInfo> =
        Arc::new(UserInfo { id: Entity(0), groups: Vec::new() });
}

pub(crate) struct UserDb {
    next: u32,
    mapper: Mapper,
    entities: HashMap<String, Entity>,
    users: HashMap<String, Arc<UserInfo>>,
}

impl UserDb {
    pub(crate) fn new(mapper: Mapper) -> UserDb {
        UserDb { next: 1, mapper, entities: HashMap::new(), users: HashMap::new() }
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

    pub(crate) fn ifo(&mut self, user: Option<&str>) -> Result<Arc<UserInfo>> {
        match user {
            None => Ok(ANONYMOUS.clone()),
            Some(user) => match self.users.get(user) {
                Some(user) => Ok(user.clone()),
                None => {
                    let ifo = Arc::new(UserInfo {
                        id: self.entity(user),
                        groups: self
                            .mapper
                            .groups(user)?
                            .into_iter()
                            .map(|b| self.entity(&b))
                            .collect::<Vec<_>>(),
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
    pub(crate) fn from_file(
        file: config::PMap,
        db: &mut UserDb,
        root: &str,
        children: &BTreeMap<Path, Referral>,
    ) -> Result<Self> {
        let mut pmap = HashMap::with_capacity(file.0.len());
        for (path, tbl) in file.0.iter() {
            if !path.starts_with(root) {
                bail!("permission entry for parent: {}, entry: {}", root, path)
            }
            for child in children.keys() {
                if path.starts_with(child.as_ref()) {
                    bail!("permission entry for child: {}, entry: {}", child, path)
                }
            }
            let mut entry = HashMap::with_capacity(tbl.len());
            for (ent, perm) in tbl.iter() {
                let entity = if ent == "" { ANONYMOUS.id } else { db.entity(ent) };
                entry.insert(entity, Permissions::try_from(perm.as_str())?);
            }
            pmap.insert(Path::from(path), entry);
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

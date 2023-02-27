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
pub(super) struct PMap {
    normal: BTreeMap<Path, FxHashMap<Entity, Permissions>>,
    user_dynamic: BTreeMap<Path, Permissions>,
    group_dynamic: BTreeMap<Path, Permissions>,
}

impl PMap {
    pub(super) fn from_file(
        file: &config::PMap,
        db: &mut UserDb,
        root: &str,
        children: &BTreeMap<Path, Referral>,
    ) -> Result<Self> {
        fn build_dynamic(
            store: &mut BTreeMap<Path, Permissions>,
            tbl: &HashMap<String, String>,
            path: &Path,
            suffix: &'static str,
        ) -> Result<()> {
            let path = Path::from(ArcStr::from(Path::dirname(path).unwrap()));
            let mut dynamic = Permissions::empty();
            for (ent, perm) in tbl.iter() {
                let p = Permissions::try_from(perm.as_str())?;
                if ent == suffix {
                    dynamic = p;
                } else {
                    bail!("dynamic permissions may only include the $[user] or $[group] permission set")
                }
            }
            store.insert(path, dynamic);
            Ok(())
        }
        let mut normal = BTreeMap::new();
        let mut user_dynamic = BTreeMap::new();
        let mut group_dynamic = BTreeMap::new();
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
            if path.contains("$[user]") && !path.ends_with("$[user]") {
                bail!("user dynamic permissions must end in $[user]")
            }
            if path.contains("$[group]") && !path.ends_with("$[group]") {
                bail!("group dynamic permissions must end in $[group]")
            }
            if path.ends_with("$[user]") {
                build_dynamic(&mut user_dynamic, tbl, &path, "$[user]")?
            } else if path.ends_with("$[group]") {
                build_dynamic(&mut group_dynamic, tbl, &path, "$[group]")?
            } else {
                let mut entry = HashMap::default();
                for (ent, perm) in tbl.iter() {
                    let entity = if ent == "" { ANONYMOUS.id } else { db.entity(ent) };
                    entry.insert(entity, Permissions::try_from(perm.as_str())?);
                }
                normal.insert(path, entry);
            }
        }
        Ok(PMap { normal, user_dynamic, group_dynamic })
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

    // should only be used by list_matching
    pub(crate) fn allowed_in_scope(
        &self,
        base_path: &str,
        scope: &Scope,
        desired_rights: Permissions,
        user: &UserInfo,
    ) -> bool {
        let rights_at_base = self.permissions(base_path, user);
        let mut rights = rights_at_base;
        let mut iter = self.normal.range::<str, (Bound<&str>, Bound<&str>)>((
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
        Path::dirnames(path).fold(Permissions::empty(), |p, s| {
            let (basename, dirname) = (Path::basename(s), Path::dirname(s));
            let uifo = user.user_info.as_ref();
            let p = {
                match (basename, dirname, uifo) {
                    (_, _, None) | (_, None, _) | (None, _, _) => p,
                    (Some(sub), Some(parent), Some(uifo)) => {
                        let p = match self.user_dynamic.get(parent) {
                            None => p,
                            Some(ud) if sub == uifo.name => {
                                if ud.contains(Permissions::DENY) {
                                    p & !*ud
                                } else {
                                    p | *ud
                                }
                            }
                            Some(_) => p,
                        };
                        let p = match self.group_dynamic.get(parent) {
                            None => p,
                            Some(gd)
                                if uifo.groups.iter().find(|s| &**s == sub).is_some() =>
                            {
                                if gd.contains(Permissions::DENY) {
                                    p & !*gd
                                } else {
                                    p | *gd
                                }
                            }
                            Some(_) => p,
                        };
                        p
                    }
                }
            };
            let p = match self.normal.get(s) {
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
            };
            p
        })
    }
}

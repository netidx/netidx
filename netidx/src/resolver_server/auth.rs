use super::config;
use crate::{
    os::Mapper,
    path::Path,
    protocol::{glob::Scope, resolver::Referral},
};
use anyhow::{anyhow, Error, Result};
use arcstr::ArcStr;
use chrono::prelude::*;
use fxhash::FxHashMap;
use netidx_netproto::resolver;
use std::{
    cell::RefCell,
    collections::{BTreeMap, Bound, HashMap},
    convert::TryFrom,
    iter,
    net::SocketAddr,
    sync::{Arc, LazyLock},
};

bitflags! {
    #[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
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
    pub(crate) timestamp: DateTime<Utc>,
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

pub(crate) static ANONYMOUS: LazyLock<Arc<UserInfo>> = LazyLock::new(|| {
    Arc::new(UserInfo {
        timestamp: Utc::now(),
        id: Entity(0),
        primary_group: Entity(0),
        groups: vec![Entity(0)],
        user_info: None,
    })
});

pub(crate) struct UserDb {
    next: u32,
    timeout: chrono::Duration,
    mapper: Mapper,
    names: FxHashMap<Entity, ArcStr>,
    entities: FxHashMap<ArcStr, Entity>,
    users: FxHashMap<ArcStr, Arc<UserInfo>>,
}

impl UserDb {
    pub(crate) fn new(timeout: chrono::Duration, mapper: Mapper) -> UserDb {
        UserDb {
            next: 1,
            timeout,
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

    pub(crate) async fn ifo(
        &mut self,
        resolver: SocketAddr,
        user: Option<&str>,
    ) -> Result<Arc<UserInfo>> {
        let now = Utc::now();
        match user {
            None => Ok(ANONYMOUS.clone()),
            Some(user) => match self.users.get(user) {
                Some(user) if now - user.timestamp < self.timeout => Ok(user.clone()),
                Some(_) | None => {
                    let (primary_group_s, groups_s) = self.mapper.groups(user).await?;
                    let primary_group = self.entity(&primary_group_s);
                    let groups =
                        groups_s.iter().map(|b| self.entity(b)).collect::<Vec<_>>();
                    let id = self.entity(user);
                    let ifo = Arc::new(UserInfo {
                        timestamp: Utc::now(),
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

fn subst(expr: &str, dst: &mut String, sub: &str) {
    dst.clear();
    for c in expr.chars() {
        if c == ']' {
            if dst.ends_with("$[group") {
                dst.replace_range(dst.len() - 7.., sub);
            } else {
                dst.push(c)
            }
        } else {
            dst.push(c);
        }
    }
}

thread_local! {
    static BUF: RefCell<String> = RefCell::new(String::new());
}

#[derive(Debug)]
pub(super) struct PMap {
    normal: BTreeMap<Path, FxHashMap<Entity, Permissions>>,
    user_dynamic: BTreeMap<Path, Permissions>,
    group_dynamic: BTreeMap<Path, Vec<(ArcStr, Permissions)>>,
}

impl PMap {
    pub(super) fn from_file(
        file: &config::PMap,
        db: &mut UserDb,
        root: &str,
        children: &BTreeMap<Path, Referral>,
    ) -> Result<Self> {
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
            if path.contains("$[group]") {
                match Path::basename(&path) {
                    None => bail!("$[group] with no parent"),
                    Some(ent) => {
                        if !ent.contains("$[group]") {
                            bail!("group dynamic permissions basename must contain $[group]")
                        }
                    }
                }
            }
            if path.ends_with("$[user]") {
                let path = Path::from(ArcStr::from(Path::dirname(&path).unwrap()));
                let mut dynamic = Permissions::empty();
                for (ent, perm) in tbl.iter() {
                    let p = Permissions::try_from(perm.as_str())?;
                    if ent == "$[user]" {
                        dynamic = p;
                    } else {
                        bail!("user dynamic permissions may only include the $[user] permission set")
                    }
                }
                user_dynamic.insert(path, dynamic);
            } else if path.ends_with("$[group]") {
                let path = Path::from(ArcStr::from(Path::dirname(&path).unwrap()));
                let mut dynamic = Vec::new();
                for (ent, perm) in tbl.iter() {
                    let p = Permissions::try_from(perm.as_str())?;
                    if ent.contains("$[group]") {
                        dynamic.push((ent.clone(), p));
                    } else {
                        bail!("group dynamic permissions may only contain .*$[group].*")
                    }
                }
                group_dynamic.insert(path, dynamic);
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
                            Some(gd) => gd.iter().fold(p, |p, (expr, gd)| {
                                BUF.with(|buf| {
                                    let mut buf = buf.borrow_mut();
                                    subst(&expr, &mut *buf, sub);
                                    if uifo
                                        .groups
                                        .iter()
                                        .find(|g| &**buf == &**g)
                                        .is_some()
                                    {
                                        if gd.contains(Permissions::DENY) {
                                            p & !*gd
                                        } else {
                                            p | *gd
                                        }
                                    } else {
                                        p
                                    }
                                })
                            }),
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

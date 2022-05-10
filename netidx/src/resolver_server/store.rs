use super::{auth::Permissions, secctx::SecCtxDataReadGuard};
use crate::{
    pack::Z64,
    path::Path,
    pool::{Pool, Pooled},
    protocol::{
        glob::{GlobSet, Scope},
        resolver::{Publisher, PublisherId, PublisherRef, Referral},
    },
    utils,
};
use bytes::Bytes;
use fxhash::FxHashMap;
use immutable_chunkmap::set::Set as ISet;
use log::debug;
use std::{
    clone::Clone,
    collections::{
        hash_map::Entry,
        BTreeMap, BTreeSet, Bound,
        Bound::{Excluded, Included, Unbounded},
        HashMap, HashSet,
    },
    convert::AsRef,
    hash::Hash,
    iter::{self, FromIterator},
    net::SocketAddr,
    sync::Arc,
};

lazy_static! {
    static ref SIGNED_PUBS_POOL: Pool<Vec<PublisherRef>> = Pool::new(256, 100);
    static ref BY_ID_POOL: Pool<FxHashMap<PublisherId, PublisherRef>> =
        Pool::new(256, 100);
    pub(super) static ref PATH_POOL: Pool<Vec<Path>> = Pool::new(256, 10000);
    pub(super) static ref COLS_POOL: Pool<Vec<(Path, Z64)>> = Pool::new(256, 10000);
    pub(super) static ref REF_POOL: Pool<Vec<Referral>> = Pool::new(256, 100);
}

type Set<T> = ISet<T, 8>;
pub(super) const MAX_WRITE_BATCH: usize = 100_000;
pub(super) const MAX_READ_BATCH: usize = 1_000_000;
pub(super) const GC_THRESHOLD: usize = 100_000;

// We hashcons the address sets because on average a publisher should
// publish many paths.
#[derive(Debug)]
struct HCSet<T: 'static + Ord + Clone + Hash> {
    ops: usize,
    sets: FxHashMap<Set<T>, ()>,
}

impl<T: 'static + Ord + Clone + Hash> HCSet<T> {
    fn new() -> Self {
        Self { ops: 0, sets: HashMap::default() }
    }

    fn hashcons(&mut self, set: Set<T>) -> Set<T> {
        let new = match self.sets.entry(set) {
            Entry::Occupied(e) => e.key().clone(),
            Entry::Vacant(e) => {
                let r = e.key().clone();
                e.insert(());
                r
            }
        };
        self.ops += 1;
        if self.ops > GC_THRESHOLD {
            self.gc()
        }
        new
    }

    fn add(&mut self, current: &Set<T>, v: T) -> Set<T> {
        let (new, existed) = current.insert(v);
        if existed {
            new
        } else {
            self.hashcons(new)
        }
    }

    fn remove(&mut self, current: &Set<T>, v: &T) -> Option<Set<T>> {
        let (new, _) = current.remove(v);
        if new.len() == 0 {
            None
        } else {
            Some(self.hashcons(new))
        }
    }

    fn gc(&mut self) {
        self.ops = 0;
        self.sets.retain(|s, ()| s.strong_count() > 1)
    }
}

fn column_path_parts<S: AsRef<str>>(path: &S) -> Option<(&str, &str)> {
    let name = Path::basename(path)?;
    let root = Path::dirname(Path::dirname(path)?)?;
    Some((root, name))
}

#[derive(Debug)]
pub(super) struct Store {
    publishers_by_id: FxHashMap<PublisherId, Arc<Publisher>>,
    publishers_by_addr: FxHashMap<SocketAddr, PublisherId>,
    published_by_path: HashMap<Path, Set<PublisherId>>,
    flags_by_path: HashMap<Path, u32>,
    published_by_id: FxHashMap<PublisherId, HashSet<Path>>,
    published_by_level: FxHashMap<usize, BTreeMap<Path, Z64>>,
    columns: HashMap<Path, HashMap<Path, Z64>>,
    defaults: BTreeSet<Path>,
    parent: Option<Referral>,
    children: BTreeMap<Path, Referral>,
    sets: HCSet<PublisherId>,
}

impl Store {
    pub(super) fn new(
        parent: Option<Referral>,
        children: BTreeMap<Path, Referral>,
    ) -> Self {
        let mut t = Store {
            publishers_by_id: HashMap::default(),
            publishers_by_addr: HashMap::default(),
            published_by_path: HashMap::default(),
            flags_by_path: HashMap::default(),
            published_by_id: HashMap::default(),
            published_by_level: HashMap::default(),
            columns: HashMap::new(),
            defaults: BTreeSet::new(),
            parent,
            children,
            sets: HCSet::new(),
        };
        let children = t.children.keys().cloned().collect::<Vec<_>>();
        for child in children {
            // since we want child to be in levels as well as
            // dirname(child) we add a fake level below child. This
            // will never be seen, since all requests for any path
            // under child result in a referral, and anyway it won't
            // even be added anywhere.
            t.add_parents(child.append("z").as_ref());
        }
        t
    }

    fn remove_parents(&mut self, mut p: &str) {
        let mut save = false;
        loop {
            let n = Path::levels(p);
            if !save {
                save = p == "/"
                    || self.published_by_path.contains_key(p)
                    || self.children.contains_key(p)
                    || self
                        .published_by_level
                        .get(&(n + 1))
                        .map(|l| {
                            let mut r = l.range::<str, (Bound<&str>, Bound<&str>)>((
                                Excluded(p),
                                Unbounded,
                            ));
                            r.next().map(|(o, _)| Path::is_parent(p, o)).unwrap_or(false)
                        })
                        .unwrap_or(false);
            }
            if save {
                let m = self.published_by_level.entry(n).or_insert_with(BTreeMap::new);
                if let Some(cn) = m.get_mut(p) {
                    **cn += 1;
                }
            } else {
                self.published_by_level.get_mut(&n).into_iter().for_each(|l| {
                    l.remove(p);
                })
            }
            if p == "/" {
                break;
            }
            p = Path::dirname(p).unwrap_or("/");
        }
    }

    fn add_parents(&mut self, mut p: &str) {
        loop {
            p = Path::dirname(p).unwrap_or("/");
            let n = Path::levels(p);
            let l = self.published_by_level.entry(n).or_insert_with(BTreeMap::new);
            match l.get_mut(p) {
                Some(cn) => {
                    **cn += 1;
                }
                None => {
                    l.insert(Path::from(String::from(p)), Z64(1));
                }
            }
            if p == "/" {
                break;
            }
        }
    }

    pub(super) fn check_referral(&self, path: &Path) -> Option<Referral> {
        if let Some(r) = self.parent.as_ref() {
            if !Path::is_parent(&r.path, path) {
                return Some(Referral {
                    path: Path::from("/"),
                    ttl: r.ttl,
                    addrs: r.addrs.clone(),
                });
            }
        }
        let r = self
            .children
            .range::<str, (Bound<&str>, Bound<&str>)>((
                Unbounded,
                Included(path.as_ref()),
            ))
            .next_back();
        match r {
            None => None,
            Some((p, r)) if Path::is_parent(p, path) => Some(r.clone()),
            Some(_) => None,
        }
    }

    pub(super) fn referrals_in_scope<T: AsRef<str> + ?Sized>(
        &self,
        refs: &mut Vec<Referral>,
        base_path: &T,
        scope: &Scope,
    ) {
        let base_path = base_path.as_ref();
        if let Some(r) = self.parent.as_ref() {
            if !Path::is_parent(&r.path, base_path) {
                refs.push(Referral {
                    path: Path::from("/"),
                    ttl: r.ttl,
                    addrs: r.addrs.clone(),
                });
            }
        }
        for (p, r) in self.children.iter() {
            if Path::is_parent(base_path, p) && scope.contains(Path::levels(&*p)) {
                refs.push(r.clone())
            } else if Path::is_parent(p, base_path) {
                refs.push(r.clone())
            }
        }
    }

    fn add_column<S: AsRef<str>>(&mut self, path: &S) {
        let (root, name) = match column_path_parts(path) {
            None => return,
            Some((r, n)) => (r, n),
        };
        match self.columns.get_mut(root) {
            Some(cols) => match cols.get_mut(name) {
                Some(c) => {
                    *&mut **c += 1;
                }
                None => {
                    cols.insert(Path::from(String::from(name)), Z64(1));
                }
            },
            None => {
                self.columns.insert(
                    Path::from(String::from(root)),
                    HashMap::from_iter(iter::once((
                        Path::from(String::from(name)),
                        Z64(1),
                    ))),
                );
            }
        }
    }

    fn remove_column<S: AsRef<str>>(&mut self, path: &S) {
        let (root, name) = match column_path_parts(path) {
            None => return,
            Some((r, n)) => (r, n),
        };
        match self.columns.get_mut(root) {
            None => (),
            Some(cols) => match cols.get_mut(name) {
                None => (),
                Some(c) => {
                    *&mut **c -= 1;
                    if **c == 0 {
                        cols.remove(name);
                        if cols.is_empty() {
                            self.columns.remove(root);
                        }
                    }
                }
            },
        }
    }

    pub(super) fn publish(
        &mut self,
        path: Path,
        publisher: &Arc<Publisher>,
        default: bool,
        flags: Option<u32>,
    ) {
        let publisher = self.publishers_by_id.entry(publisher.id).or_insert_with(|| {
            let p = publisher.clone();
            self.publishers_by_addr.insert(publisher.addr, publisher.id);
            p
        });
        self.published_by_id
            .entry(publisher.id)
            .or_insert_with(HashSet::new)
            .insert(path.clone());
        let pubs = self.published_by_path.entry(path.clone()).or_insert_with(Set::new);
        let len = pubs.len();
        *pubs = self.sets.add(pubs, publisher.id);
        if let Some(flags) = flags {
            self.flags_by_path.insert(path.clone(), flags);
        }
        if default {
            self.defaults.insert(path.clone());
        }
        if pubs.len() > len {
            self.add_column(&path);
            self.add_parents(path.as_ref());
            let n = Path::levels(path.as_ref());
            let cn = self
                .published_by_level
                .entry(n)
                .or_insert_with(BTreeMap::new)
                .entry(path.clone())
                .or_insert(Z64(0));
            **cn += 1;
        }
    }

    pub(super) fn unpublish(&mut self, publisher: &Arc<Publisher>, path: Path) {
        let client_gone = self
            .published_by_id
            .get_mut(&publisher.id)
            .map(|s| {
                s.remove(&path);
                s.is_empty()
            })
            .unwrap_or(true);
        if client_gone {
            self.published_by_id.remove(&publisher.id);
            self.publishers_by_id.remove(&publisher.id);
            self.publishers_by_addr.remove(&publisher.addr);
        }
        match self.published_by_path.get_mut(&path) {
            None => (),
            Some(pubs) => {
                let len = pubs.len();
                let n = Path::levels(path.as_ref());
                match self.sets.remove(pubs, &publisher.id) {
                    Some(new_pubs) => {
                        *pubs = new_pubs;
                        if pubs.len() < len {
                            self.remove_column(&path);
                            self.remove_parents(path.as_ref());
                            let cn = self
                                .published_by_level
                                .entry(n)
                                .or_insert_with(BTreeMap::new)
                                .entry(path.clone())
                                .or_insert(Z64(0));
                            **cn += 1;
                        }
                    }
                    None => {
                        self.published_by_path.remove(&path);
                        self.flags_by_path.remove(&path);
                        self.defaults.remove(&path);
                        self.remove_column(&path);
                        self.published_by_level.get_mut(&n).into_iter().for_each(|s| {
                            s.remove(&path);
                        });
                        self.remove_parents(path.as_ref());
                    }
                }
            }
        }
    }

    pub(super) fn published_for_id(&self, id: &PublisherId) -> HashSet<Path> {
        self.published_by_id.get(id).map(|s| s.clone()).unwrap_or_else(HashSet::new)
    }

    pub(super) fn clear(&mut self, publisher: &Arc<Publisher>) {
        for path in self.published_for_id(&publisher.id).drain() {
            self.unpublish(publisher, path);
        }
    }

    fn get_flags(&self, path: &str) -> u32 {
        self.flags_by_path.get(path).copied().unwrap_or(0)
    }

    fn record_publisher(
        &self,
        publishers: &mut FxHashMap<PublisherId, Publisher>,
        id: &PublisherId,
    ) {
        publishers.entry(*id).or_insert_with(|| (*self.publishers_by_id[id]).clone());
    }

    fn resolve_default(
        &self,
        publishers: &mut FxHashMap<PublisherId, Publisher>,
        path: &Path,
    ) -> (u32, Pooled<Vec<PublisherRef>>) {
        let default = self
            .defaults
            .range::<str, (Bound<&str>, Bound<&str>)>((
                Unbounded,
                Included(path.as_ref()),
            ))
            .next_back();
        match default {
            None => (0, SIGNED_PUBS_POOL.take()),
            Some(p) if Path::is_parent(p, path) => {
                match self.published_by_path.get(p.as_ref()) {
                    None => (0, SIGNED_PUBS_POOL.take()),
                    Some(ids) => {
                        let mut pubs = SIGNED_PUBS_POOL.take();
                        let refs = ids.into_iter().map(|id| {
                            self.record_publisher(publishers, id);
                            PublisherRef { id: *id, token: Bytes::new() }
                        });
                        pubs.extend(refs);
                        (self.get_flags(p.as_ref()), pubs)
                    }
                }
            }
            Some(_) => (0, SIGNED_PUBS_POOL.take()),
        }
    }

    pub(super) fn resolve(
        &self,
        publishers: &mut FxHashMap<PublisherId, Publisher>,
        path: &Path,
    ) -> (u32, Pooled<Vec<PublisherRef>>) {
        let (flags, mut pubs) = self.resolve_default(publishers, path);
        let flags = match self.published_by_path.get(path.as_ref()) {
            None => flags,
            Some(ids) => {
                if pubs.len() == 0 {
                    pubs.extend(ids.into_iter().map(|id| {
                        self.record_publisher(publishers, id);
                        PublisherRef { id: *id, token: Bytes::new() }
                    }));
                    self.get_flags(path.as_ref())
                } else {
                    let mut by_id = BY_ID_POOL.take();
                    by_id.extend(pubs.drain(..).map(|r| (r.id, r)));
                    by_id.extend(ids.into_iter().map(|id| {
                        self.record_publisher(publishers, id);
                        (*id, PublisherRef { id: *id, token: Bytes::new() })
                    }));
                    pubs.extend(by_id.drain().map(|(_, r)| r));
                    self.get_flags(path.as_ref())
                }
            }
        };
        (flags, pubs)
    }

    pub(super) fn resolve_and_sign(
        &self,
        publishers: &mut FxHashMap<PublisherId, Publisher>,
        sec: &SecCtxDataReadGuard,
        now: u64,
        perm: Permissions,
        path: &Path,
    ) -> (u32, Pooled<Vec<PublisherRef>>) {
        let sign = |id: PublisherId| {
            let secret = match sec {
                SecCtxDataReadGuard::Anonymous => None,
                SecCtxDataReadGuard::Local(sec) => sec.secret(&id),
                SecCtxDataReadGuard::Krb5(sec) => sec.secret(&id),
            };
            match secret {
                None => PublisherRef { id, token: Bytes::new() },
                Some(secret) => PublisherRef {
                    id,
                    token: utils::make_sha3_token(&[
                        &secret.to_be_bytes(),
                        &now.to_be_bytes(),
                        &perm.bits().to_be_bytes(),
                        path.as_bytes(),
                    ]),
                },
            }
        };
        let (flags, mut pubs) = self.resolve_default(publishers, path);
        for i in 0..pubs.len() {
            pubs[i] = sign(pubs[i].id);
        }
        let flags = match self.published_by_path.get(&*path) {
            None => flags,
            Some(ids) => {
                if pubs.len() == 0 {
                    pubs.extend(ids.into_iter().map(|id| {
                        self.record_publisher(publishers, id);
                        sign(*id)
                    }));
                    self.get_flags(&*path)
                } else {
                    let mut by_id = BY_ID_POOL.take();
                    by_id.extend(pubs.drain(..).map(|r| (r.id, r)));
                    by_id.extend(ids.into_iter().map(|id| {
                        self.record_publisher(publishers, id);
                        (*id, sign(*id))
                    }));
                    pubs.extend(by_id.drain().map(|(_, r)| r));
                    self.get_flags(&*path)
                }
            }
        };
        (flags, pubs)
    }

    pub(super) fn list(&self, parent: &Path) -> Pooled<Vec<Path>> {
        let n = Path::levels(parent);
        let mut paths = PATH_POOL.take();
        if let Some(l) = self.published_by_level.get(&(n + 1)) {
            paths.extend(
                l.range::<str, (Bound<&str>, Bound<&str>)>((
                    Excluded(parent.as_ref()),
                    Unbounded,
                ))
                .map(|(p, _)| p)
                .take_while(|p| Path::is_parent(parent, p))
                .cloned(),
            )
        }
        paths
    }

    pub(super) fn list_matching(&self, pat: &GlobSet) -> Pooled<Vec<Path>> {
        let mut paths = PATH_POOL.take();
        let mut cur: Option<&str> = None;
        for glob in pat.iter() {
            if !cur.map(|p| Path::is_parent(p, glob.base())).unwrap_or(false) {
                let base = glob.base();
                let mut n = Path::levels(base) + 1;
                while glob.scope().contains(n) {
                    match self.published_by_level.get(&n) {
                        None => break,
                        Some(l) => {
                            let iter = l
                                .range::<str, (Bound<&str>, Bound<&str>)>((
                                    Excluded(base),
                                    Unbounded,
                                ))
                                .map(|(p, _)| p)
                                .take_while(move |p| Path::is_parent(base, p));
                            for path in iter {
                                let dn = Path::dirname(path).unwrap_or("/");
                                if pat.is_match(path)
                                    && !self.children.contains_key(dn)
                                    && (!pat.published_only()
                                        || self.published_by_path.contains_key(path))
                                {
                                    paths.push(path.clone());
                                }
                            }
                        }
                    }
                    n += 1;
                }
            }
            cur = Some(glob.base());
        }
        paths
    }

    pub(super) fn get_change_nr(&self, path: &Path) -> Z64 {
        self.published_by_level
            .get(&Path::levels(path))
            .and_then(|l| l.get(path).map(|cn| *cn))
            .unwrap_or(Z64(0))
    }

    pub(super) fn columns(&self, root: &Path) -> Pooled<Vec<(Path, Z64)>> {
        let mut cols = COLS_POOL.take();
        if let Some(c) = self.columns.get(root) {
            cols.extend(c.iter().map(|(name, cnt)| (name.clone(), *cnt)));
        }
        cols
    }

    #[allow(dead_code)]
    pub(super) fn invariant(&self) {
        debug!("resolver_store: checking invariants");
        for (id, paths) in self.published_by_id.iter() {
            for path in paths.iter() {
                match self.published_by_path.get(path) {
                    None => panic!(
                        "path {} in published_by_id but not in published_by_path",
                        path
                    ),
                    Some(pubs) => {
                        if !pubs.into_iter().any(|a| a == id) {
                            panic!(
                                "path {} in {:?} published_by_id, but {:?} not present in pubs",
                                path, pubs, id
                            )
                        }
                    }
                }
            }
        }
    }
}

use crate::{
    auth::Permissions,
    chars::Chars,
    pack::Z64,
    path::{self, Path},
    pool::{Pool, Pooled},
    protocol::resolver::v1::Referral,
    secstore::SecStoreInner,
    utils::{self, Addr},
};
use bytes::Bytes;
use fxhash::FxBuildHasher;
use immutable_chunkmap::set::Set;
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
    iter::{self, FromIterator},
    net::SocketAddr,
};

lazy_static! {
    static ref SPN_POOL: Pool<HashMap<SocketAddr, Chars, FxBuildHasher>> = Pool::new(256);
    static ref SIGNED_ADDRS_POOL: Pool<Vec<(SocketAddr, Bytes)>> = Pool::new(256);
    static ref ADDRS_POOL: Pool<Vec<SocketAddr>> = Pool::new(256);
    pub(crate) static ref PATH_POOL: Pool<Vec<Path>> = Pool::new(256);
    pub(crate) static ref COLS_POOL: Pool<Vec<(Path, Z64)>> = Pool::new(256);
}

pub(crate) const MAX_WRITE_BATCH: usize = 100_000;
pub(crate) const MAX_READ_BATCH: usize = 1_000_000;
pub(crate) const GC_THRESHOLD: usize = 100_000;

// We hashcons the address sets. On average, a publisher should publish many paths.
// for each published value we only store the path once, since it's an Arc<str>,
// and a pointer to the set of addresses it is published by.
#[derive(Debug)]
struct HCAddrs {
    ops: usize,
    addrs: HashMap<Set<Addr>, (), FxBuildHasher>,
}

impl HCAddrs {
    fn new() -> HCAddrs {
        HCAddrs { ops: 0, addrs: HashMap::with_hasher(FxBuildHasher::default()) }
    }

    fn hashcons(&mut self, set: Set<Addr>) -> Set<Addr> {
        let new = match self.addrs.entry(set) {
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

    fn add_address(&mut self, current: &Set<Addr>, addr: Addr) -> Set<Addr> {
        let (new, existed) = current.insert(addr);
        if existed {
            new
        } else {
            self.hashcons(new)
        }
    }

    fn remove_address(&mut self, current: &Set<Addr>, addr: &Addr) -> Option<Set<Addr>> {
        let (new, _) = current.remove(addr);
        if new.len() == 0 {
            None
        } else {
            Some(self.hashcons(new))
        }
    }

    fn gc(&mut self) {
        self.ops = 0;
        self.addrs.retain(|s, ()| s.strong_count() > 1)
    }
}

fn column_path_parts<S: AsRef<str>>(path: &S) -> Option<(&str, &str)> {
    let name = Path::basename(path)?;
    let root = Path::dirname(Path::dirname(path)?)?;
    Some((root, name))
}

#[derive(Debug)]
pub(crate) struct Store {
    by_path: HashMap<Path, Set<Addr>>,
    by_addr: HashMap<SocketAddr, HashSet<Path>, FxBuildHasher>,
    by_level: HashMap<usize, BTreeSet<Path>, FxBuildHasher>,
    columns: HashMap<Path, HashMap<Path, Z64>>,
    defaults: BTreeSet<Path>,
    parent: Option<Referral>,
    children: BTreeMap<Path, Referral>,
    addrs: HCAddrs,
}

impl Store {
    pub(crate) fn new(
        parent: Option<Referral>,
        children: BTreeMap<Path, Referral>,
    ) -> Self {
        let mut t = Store {
            by_path: HashMap::new(),
            by_addr: HashMap::with_hasher(FxBuildHasher::default()),
            by_level: HashMap::with_hasher(FxBuildHasher::default()),
            columns: HashMap::new(),
            defaults: BTreeSet::new(),
            parent,
            children,
            addrs: HCAddrs::new(),
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
        loop {
            let p_with_sep = match Path::dirname_with_sep(p) {
                None => break,
                Some(p) => p,
            };
            match Path::dirname(p) {
                None => break,
                Some(parent) => p = parent,
            }
            let n = Path::levels(p) + 1;
            let save = self.by_path.contains_key(p)
                || self.children.contains_key(p)
                || self
                    .by_level
                    .get(&n)
                    .map(|l| {
                        let mut r = l.range::<str, (Bound<&str>, Bound<&str>)>((
                            Included(p_with_sep),
                            Unbounded,
                        ));
                        r.next()
                            .map(|o| o.as_ref().starts_with(p_with_sep))
                            .unwrap_or(false)
                    })
                    .unwrap_or(false);
            if save {
                break;
            } else {
                let n = n - 1;
                self.by_level.get_mut(&n).into_iter().for_each(|l| {
                    l.remove(p);
                })
            }
        }
    }

    fn add_parents(&mut self, mut p: &str) {
        loop {
            match Path::dirname(p) {
                None => break,
                Some(parent) => p = parent,
            }
            let n = Path::levels(p);
            let l = self.by_level.entry(n).or_insert_with(BTreeSet::new);
            if !l.contains(p) {
                l.insert(Path::from(String::from(p)));
            }
        }
    }

    pub(crate) fn check_referral(&self, path: &Path) -> Option<Referral> {
        if let Some(r) = self.parent.as_ref() {
            if !path.starts_with(r.path.as_ref()) {
                return Some(Referral {
                    path: Path::from("/"),
                    ttl: r.ttl,
                    addrs: r.addrs.clone(),
                    krb5_spns: r.krb5_spns.clone(),
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
            Some((p, r)) if path.starts_with(p.as_ref()) => Some(r.clone()),
            Some(_) => None,
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

    pub(crate) fn publish(&mut self, path: Path, addr: SocketAddr, default: bool) {
        self.by_addr.entry(addr).or_insert_with(HashSet::new).insert(path.clone());
        let addrs = self.by_path.entry(path.clone()).or_insert_with(Set::new);
        let len = addrs.len();
        *addrs = self.addrs.add_address(addrs, Addr(addr));
        if addrs.len() > len {
            self.add_column(&path);
            let n = Path::levels(path.as_ref());
            self.by_level.entry(n).or_insert_with(BTreeSet::new).insert(path.clone());
        }
        if len == 0 {
            self.add_parents(path.as_ref());
        }
        if default {
            self.defaults.insert(path);
        }
    }

    pub(crate) fn unpublish(&mut self, path: Path, addr: SocketAddr) {
        let client_gone = self
            .by_addr
            .get_mut(&addr)
            .map(|s| {
                s.remove(&path);
                s.is_empty()
            })
            .unwrap_or(true);
        if client_gone {
            self.by_addr.remove(&addr);
        }
        match self.by_path.get_mut(&path) {
            None => (),
            Some(addrs) => {
                let len = addrs.len();
                match self.addrs.remove_address(addrs, &Addr(addr)) {
                    Some(new_addrs) => {
                        *addrs = new_addrs;
                        if addrs.len() < len {
                            self.remove_column(&path);
                        }
                    }
                    None => {
                        self.by_path.remove(&path);
                        self.defaults.remove(&path);
                        let n = Path::levels(path.as_ref());
                        self.by_level.get_mut(&n).into_iter().for_each(|s| {
                            s.remove(&path);
                        });
                        self.remove_parents(path.as_ref());
                        self.remove_column(&path);
                    }
                }
            }
        }
    }

    pub(crate) fn published_for_addr(&self, addr: &SocketAddr) -> HashSet<Path> {
        match self.by_addr.get(addr) {
            None => HashSet::new(),
            Some(paths) => paths.clone(),
        }
    }

    fn resolve_default(&self, path: &Path) -> Pooled<Vec<(SocketAddr, Bytes)>> {
        let default = self
            .defaults
            .range::<str, (Bound<&str>, Bound<&str>)>((
                Unbounded,
                Included(path.as_ref()),
            ))
            .next_back();
        match default {
            None => SIGNED_ADDRS_POOL.take(),
            Some(p) if path.starts_with(p.as_ref()) => {
                match self.by_path.get(p.as_ref()) {
                    None => SIGNED_ADDRS_POOL.take(),
                    Some(a) => {
                        let mut addrs = SIGNED_ADDRS_POOL.take();
                        addrs.extend(a.into_iter().map(|a| (a.0, Bytes::new())));
                        addrs
                    }
                }
            }
            Some(_) => SIGNED_ADDRS_POOL.take(),
        }
    }

    pub(crate) fn resolve(&self, path: &Path) -> Pooled<Vec<(SocketAddr, Bytes)>> {
        match self.by_path.get(path.as_ref()) {
            None => self.resolve_default(path),
            Some(a) => {
                let mut addrs = SIGNED_ADDRS_POOL.take();
                addrs.extend(a.into_iter().map(|addr| (addr.0, Bytes::new())));
                addrs
            }
        }
    }

    pub(crate) fn resolve_and_sign(
        &self,
        sec: &SecStoreInner,
        now: u64,
        perm: Permissions,
        path: &Path,
    ) -> (
        Pooled<HashMap<SocketAddr, Chars, FxBuildHasher>>,
        Pooled<Vec<(SocketAddr, Bytes)>>,
    ) {
        let mut krb5_spns = SPN_POOL.take();
        let mut sign_addr = |addr: &SocketAddr| match sec.get(addr) {
            None => (*addr, Bytes::new()),
            Some((spn, secret, _)) => {
                if !krb5_spns.contains_key(addr) {
                    krb5_spns.insert(*addr, spn.clone());
                }
                (
                    *addr,
                    utils::make_sha3_token(
                        None,
                        &[
                            &secret.to_be_bytes(),
                            &now.to_be_bytes(),
                            &perm.bits().to_be_bytes(),
                            path.as_bytes(),
                        ],
                    ),
                )
            }
        };
        let mut signed_addrs = SIGNED_ADDRS_POOL.take();
        match self.by_path.get(&*path) {
            None => signed_addrs
                .extend(self.resolve_default(path).drain(..).map(|(a, _)| sign_addr(&a))),
            Some(addrs) => {
                signed_addrs.extend(addrs.into_iter().map(|a| sign_addr(&a.0)))
            }
        }
        (krb5_spns, signed_addrs)
    }

    pub(crate) fn list(&self, parent: &Path) -> Pooled<Vec<Path>> {
        let parent = if parent == &Path::root() {
            parent.as_ref()
        } else {
            parent.trim_end_matches(path::SEP)
        };
        let n = Path::levels(parent);
        let mut parent = String::from(parent);
        if !parent.ends_with(path::SEP) {
            parent.push(path::SEP);
        }
        let mut paths = PATH_POOL.take();
        if let Some(l) = self.by_level.get(&(n + 1)) {
            paths.extend(
                l.range::<str, (Bound<&str>, Bound<&str>)>((
                    Excluded(parent.as_str()),
                    Unbounded,
                ))
                .take_while(|p| p.as_ref().starts_with(parent.as_str()))
                .cloned(),
            )
        }
        paths
    }

    pub(crate) fn columns(&self, root: &Path) -> Pooled<Vec<(Path, Z64)>> {
        let mut cols = COLS_POOL.take();
        if let Some(c) = self.columns.get(root) {
            cols.extend(c.iter().map(|(name, cnt)| (name.clone(), *cnt)));
        }
        cols
    }

    #[allow(dead_code)]
    pub(crate) fn invariant(&self) {
        debug!("resolver_store: checking invariants");
        for (addr, paths) in self.by_addr.iter() {
            for path in paths.iter() {
                match self.by_path.get(path) {
                    None => panic!("path {} in by_addr but not in by_path", path),
                    Some(addrs) => {
                        if !addrs.into_iter().any(|a| &a.0 == addr) {
                            panic!(
                                "path {} in {} by_addr, but {} not present in addrs",
                                path, addr, addr
                            )
                        }
                    }
                }
            }
        }
    }
}

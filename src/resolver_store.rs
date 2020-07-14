use crate::{
    auth::Permissions,
    chars::Chars,
    pack::Z64,
    path::{self, Path},
    pool::{Pool, Pooled},
    protocol::resolver::v1::Referral,
    secstore::SecStoreInner,
    utils,
};
use bytes::Bytes;
use fxhash::FxBuildHasher;
use parking_lot::RwLock;
use std::{
    clone::Clone,
    collections::{
        BTreeMap, BTreeSet, Bound,
        Bound::{Excluded, Included, Unbounded},
        HashMap, HashSet,
    },
    convert::AsRef,
    iter::{self, FromIterator},
    net::SocketAddr,
    ops::Deref,
    sync::{Arc, Weak},
};

lazy_static! {
    static ref EMPTY: Addrs = Arc::new(Vec::new());
}

type Addrs = Arc<Vec<SocketAddr>>;

// We hashcons the address sets. On average, a publisher should publish many paths.
// for each published value we only store the path once, since it's an Arc<str>,
// and a pointer to the set of addresses it is published by.
#[derive(Debug)]
struct HCAddrs(HashMap<Vec<SocketAddr>, Weak<Vec<SocketAddr>>, FxBuildHasher>);

impl HCAddrs {
    fn new() -> HCAddrs {
        HCAddrs(HashMap::with_hasher(FxBuildHasher::default()))
    }

    fn hashcons(&mut self, set: Vec<SocketAddr>) -> Addrs {
        match self.0.get(&set).and_then(|v| v.upgrade()) {
            Some(addrs) => addrs,
            None => {
                let addrs = Arc::new(set.clone());
                self.0.insert(set, Arc::downgrade(&addrs));
                addrs
            }
        }
    }

    fn add_address(&mut self, current: &Addrs, addr: SocketAddr) -> Addrs {
        let mut set = current.iter().copied().chain(iter::once(addr)).collect::<Vec<_>>();
        set.sort_by_key(|a| (a.ip(), a.port()));
        set.dedup();
        self.hashcons(set)
    }

    fn remove_address(&mut self, current: &Addrs, addr: SocketAddr) -> Option<Addrs> {
        if current.len() == 1 && current[0] == addr {
            None
        } else {
            let s = current.iter().copied().filter(|a| a != &addr);
            Some(self.hashcons(s.collect()))
        }
    }

    fn gc(&mut self) {
        let mut dead = Vec::new();
        for (k, v) in self.0.iter() {
            if v.upgrade().is_none() {
                dead.push(k.clone());
            }
        }
        for k in dead {
            self.0.remove(&k);
        }
    }
}

fn column_path_parts<S: AsRef<str>>(path: &S) -> Option<(&str, &str)> {
    let name = Path::basename(path)?;
    let root = Path::dirname(Path::dirname(path)?)?;
    Some((root, name))
}

#[derive(Debug)]
pub(crate) struct StoreInner<T> {
    by_path: HashMap<Path, Addrs>,
    by_addr: HashMap<SocketAddr, HashSet<Path>, FxBuildHasher>,
    by_level: HashMap<usize, BTreeSet<Path>, FxBuildHasher>,
    columns: HashMap<Path, HashMap<Path, Z64>>,
    defaults: BTreeSet<Path>,
    parent: Option<Referral>,
    children: BTreeMap<Path, Referral>,
    addrs: HCAddrs,
    clinfos: HashMap<SocketAddr, T, FxBuildHasher>,
    spn_pool: Pool<HashMap<SocketAddr, Chars, FxBuildHasher>>,
    signed_addrs_pool: Pool<Vec<(SocketAddr, Bytes)>>,
    addrs_pool: Pool<Vec<SocketAddr>>,
    paths_pool: Pool<Vec<Path>>,
    cols_pool: Pool<Vec<(Path, Z64)>>,
}

impl<T> StoreInner<T> {
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
        let addrs = self.by_path.entry(path.clone()).or_insert_with(|| EMPTY.clone());
        *addrs = self.addrs.add_address(addrs, addr);
        self.add_parents(path.as_ref());
        self.add_column(&path);
        let n = Path::levels(path.as_ref());
        self.by_level.entry(n).or_insert_with(BTreeSet::new).insert(path.clone());
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
            Some(addrs) => match self.addrs.remove_address(addrs, addr) {
                Some(new_addrs) => {
                    *addrs = new_addrs;
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
            },
        }
    }

    pub(crate) fn unpublish_addr(&mut self, addr: SocketAddr) {
        self.by_addr.remove(&addr).into_iter().for_each(|paths| {
            for path in paths {
                self.unpublish(path, addr);
            }
        })
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
            None => self.signed_addrs_pool.take(),
            Some(p) if path.starts_with(p.as_ref()) => {
                match self.by_path.get(p.as_ref()) {
                    None => self.signed_addrs_pool.take(),
                    Some(a) => {
                        let mut addrs = self.signed_addrs_pool.take();
                        addrs.extend(a.iter().map(|a| (*a, Bytes::new())));
                        addrs
                    }
                }
            }
            Some(_) => self.signed_addrs_pool.take(),
        }
    }

    pub(crate) fn resolve(&self, path: &Path) -> Pooled<Vec<(SocketAddr, Bytes)>> {
        match self.by_path.get(path.as_ref()) {
            None => self.resolve_default(path),
            Some(a) => {
                let mut addrs = self.signed_addrs_pool.take();
                addrs.extend(a.iter().map(|addr| (*addr, Bytes::new())));
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
        let mut krb5_spns = self.spn_pool.take();
        let mut sign_addr = |addr: &SocketAddr| match sec.get_write(addr) {
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
        let mut signed_addrs = self.signed_addrs_pool.take();
        match self.by_path.get(&*path) {
            None => signed_addrs
                .extend(self.resolve_default(path).drain(..).map(|(a, _)| sign_addr(&a))),
            Some(addrs) => signed_addrs.extend(addrs.iter().map(&mut sign_addr)),
        }
        (krb5_spns, signed_addrs)
    }

    pub(crate) fn list(&self, parent: &Path) -> Pooled<Vec<Path>> {
        let n = Path::levels(&**parent);
        let mut parent = String::from(&**parent);
        if !parent.ends_with(path::SEP) {
            parent.push(path::SEP);
        }
        let mut paths = self.paths_pool.take();
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
        let mut cols = self.cols_pool.take();
        if let Some(c) = self.columns.get(root) {
            cols.extend(c.iter().map(|(name, cnt)| (name.clone(), *cnt)));
        }
        cols
    }

    pub(crate) fn gc(&mut self) {
        self.addrs.gc();
    }

    pub(crate) fn clinfo(&self) -> &HashMap<SocketAddr, T, FxBuildHasher> {
        &self.clinfos
    }

    pub(crate) fn clinfo_mut(&mut self) -> &mut HashMap<SocketAddr, T, FxBuildHasher> {
        &mut self.clinfos
    }
}

pub(crate) struct Store<T>(Arc<RwLock<StoreInner<T>>>);

impl<T> Clone for Store<T> {
    fn clone(&self) -> Self {
        Store(Arc::clone(&self.0))
    }
}

impl<T> Deref for Store<T> {
    type Target = RwLock<StoreInner<T>>;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl<T> Store<T> {
    pub(crate) fn new(
        parent: Option<Referral>,
        children: BTreeMap<Path, Referral>,
    ) -> Self {
        let mut inner = StoreInner {
            by_path: HashMap::new(),
            by_addr: HashMap::with_hasher(FxBuildHasher::default()),
            by_level: HashMap::with_hasher(FxBuildHasher::default()),
            columns: HashMap::new(),
            defaults: BTreeSet::new(),
            parent,
            children,
            addrs: HCAddrs::new(),
            clinfos: HashMap::with_hasher(FxBuildHasher::default()),
            spn_pool: Pool::new(100),
            signed_addrs_pool: Pool::new(100),
            addrs_pool: Pool::new(100),
            paths_pool: Pool::new(100),
            cols_pool: Pool::new(100),
        };
        let children = inner.children.keys().cloned().collect::<Vec<_>>();
        for child in children {
            // since we want child to be in levels as well as
            // dirname(child) we add a fake level below child. This
            // will never be seen, since all requests for any path
            // under child result in a referral, and anyway it won't
            // even be added anywhere.
            inner.add_parents(child.append("z").as_ref());
        }
        Store(Arc::new(RwLock::new(inner)))
    }
}

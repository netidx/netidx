use crate::{
    chars::Chars, os::Krb5Ctx, path::Path, protocol::resolver::v1::PermissionToken,
    secstore::SecStoreInner, utils,
};
use anyhow::Result;
use bytes::Bytes;
use fxhash::FxBuildHasher;
use parking_lot::RwLock;
use std::{
    clone::Clone,
    collections::{
        BTreeSet, Bound,
        Bound::{Excluded, Included, Unbounded},
        HashMap, HashSet,
    },
    convert::AsRef,
    iter,
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

#[derive(Debug)]
pub(crate) struct StoreInner<T> {
    by_path: HashMap<Path, Addrs>,
    by_addr: HashMap<SocketAddr, HashSet<Path>, FxBuildHasher>,
    by_level: HashMap<usize, BTreeSet<Path>, FxBuildHasher>,
    addrs: HCAddrs,
    clinfos: HashMap<SocketAddr, T, FxBuildHasher>,
}

impl<T> StoreInner<T> {
    fn remove_parents(&mut self, mut p: &str) {
        loop {
            match Path::dirname(p) {
                None => break,
                Some(parent) => p = parent,
            }
            let n = Path::levels(p) + 1;
            let save = self.by_path.contains_key(p)
                || self
                    .by_level
                    .get(&n)
                    .map(|l| {
                        let mut r = l.range::<str, (Bound<&str>, Bound<&str>)>((
                            Included(p),
                            Unbounded,
                        ));
                        r.next().map(|o| o.as_ref().starts_with(p)).unwrap_or(false)
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
                l.insert(Path::from(p));
            }
        }
    }

    pub(crate) fn publish(&mut self, path: Path, addr: SocketAddr) {
        self.by_addr.entry(addr).or_insert_with(HashSet::new).insert(path.clone());
        let addrs = self.by_path.entry(path.clone()).or_insert_with(|| EMPTY.clone());
        *addrs = self.addrs.add_address(addrs, addr);
        self.add_parents(path.as_ref());
        let n = Path::levels(path.as_ref());
        self.by_level.entry(n).or_insert_with(BTreeSet::new).insert(path);
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
                    let n = Path::levels(path.as_ref());
                    self.by_level.get_mut(&n).into_iter().for_each(|s| {
                        s.remove(&path);
                    });
                    self.remove_parents(path.as_ref())
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

    pub(crate) fn resolve(&self, path: &Path) -> Vec<(SocketAddr, Bytes)> {
        self.by_path
            .get(&**path)
            .map(|a| a.iter().map(|addr| (*addr, Bytes::new())).collect())
            .unwrap_or_else(|| Vec::new())
    }

    pub(crate) fn resolve_and_sign(
        &self,
        sec: &SecStoreInner,
        krb5_spns: &mut HashMap<SocketAddr, Chars, FxBuildHasher>,
        now: u64,
        path: &Path,
    ) -> Result<Vec<(SocketAddr, Bytes)>> {
        self.by_path
            .get(&*path)
            .map(|addrs| {
                Ok(addrs
                    .iter()
                    .map(|addr| match sec.get_write(&addr) {
                        None => Ok((*addr, Bytes::new())),
                        Some((spn, ctx)) => {
                            if !krb5_spns.contains_key(addr) {
                                krb5_spns.insert(*addr, spn.clone());
                            }
                            let msg = utils::pack(&PermissionToken(path.clone(), now))?;
                            let tok = utils::bytes(&*ctx.wrap(true, &*msg)?);
                            Ok((*addr, tok))
                        }
                    })
                    .collect::<Result<Vec<(SocketAddr, Bytes)>>>()?)
            })
            .unwrap_or_else(|| Ok(vec![]))
    }

    pub(crate) fn list(&self, parent: &Path) -> Vec<Path> {
        self.by_level
            .get(&(Path::levels(&**parent) + 1))
            .map(|l| {
                l.range::<str, (Bound<&str>, Bound<&str>)>((Excluded(parent), Unbounded))
                    .take_while(|p| p.as_ref().starts_with(&**parent))
                    .cloned()
                    .collect()
            })
            .unwrap_or_else(Vec::new)
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
    pub(crate) fn new() -> Self {
        Store(Arc::new(RwLock::new(StoreInner {
            by_path: HashMap::new(),
            by_addr: HashMap::with_hasher(FxBuildHasher::default()),
            by_level: HashMap::with_hasher(FxBuildHasher::default()),
            addrs: HCAddrs::new(),
            clinfos: HashMap::with_hasher(FxBuildHasher::default()),
        })))
    }
}

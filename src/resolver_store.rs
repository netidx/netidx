use crate::{
    auth::Krb5Ctx,
    path::Path,
    protocol::resolver::{
        ReadServerResponse_AddrAndAuthToken as AddrAndAuthToken,
        ReadServerResponse_Resolution as Resolution,
    },
    protocol::shared::PermissionToken,
    secstore::SecStoreInner,
    utils::saddr_to_protobuf,
};
use anyhow::Result;
use bytes::Bytes;
use fxhash::FxBuildHasher;
use parking_lot::RwLock;
use protobuf::{Chars, Message, RepeatedField, SingularPtrField};
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
        let mut set = current
            .iter()
            .copied()
            .chain(iter::once(addr))
            .collect::<Vec<_>>();
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
        self.by_addr
            .entry(addr)
            .or_insert_with(HashSet::new)
            .insert(path.clone());
        let addrs = self
            .by_path
            .entry(path.clone())
            .or_insert_with(|| EMPTY.clone());
        *addrs = self.addrs.add_address(addrs, addr);
        self.add_parents(path.as_ref());
        let n = Path::levels(path.as_ref());
        self.by_level
            .entry(n)
            .or_insert_with(BTreeSet::new)
            .insert(path);
    }

    pub(crate) fn unpublish<P: AsRef<str>>(&mut self, path: P, addr: SocketAddr) {
        let client_gone = self
            .by_addr
            .get_mut(&addr)
            .map(|s| {
                s.remove(path.as_ref());
                s.is_empty()
            })
            .unwrap_or(true);
        if client_gone {
            self.by_addr.remove(&addr);
        }
        match self.by_path.get_mut(path.as_ref()) {
            None => (),
            Some(addrs) => match self.addrs.remove_address(addrs, addr) {
                Some(new_addrs) => {
                    *addrs = new_addrs;
                }
                None => {
                    self.by_path.remove(path.as_ref());
                    let n = Path::levels(path.as_ref());
                    self.by_level.get_mut(&n).into_iter().for_each(|s| {
                        s.remove(path.as_ref());
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

    pub(crate) fn resolve<S: AsRef<str>>(&self, path: S) -> Resolution {
        Resolution {
            addrs: self
                .by_path
                .get(path.as_ref())
                .map(|a| {
                    a.iter()
                        .map(|addr| AddrAndAuthToken {
                            addr: SingularPtrField::some(saddr_to_protobuf(*addr)),
                            token: Bytes::new(),
                            ..AddrAndAuthToken::default()
                        })
                        .collect()
                })
                .unwrap_or_else(|| RepeatedField::from_vec(vec![])),
            ..Resolution::default()
        }
    }

    pub(crate) fn resolve_and_sign<S: AsRef<str>>(
        &self,
        sec: &SecStoreInner,
        krb5_spns: &mut HashMap<SocketAddr, Chars, FxBuildHasher>,
        now: u64,
        path: S,
    ) -> Result<Resolution> {
        let sa = saddr_to_protobuf;
        let addrs: RepeatedField<AddrAndAuthToken> = self
            .by_path
            .get(path.as_ref())
            .map(|addrs| {
                addrs
                    .iter()
                    .map(|addr| match sec.get_write(&addr) {
                        None => Ok(AddrAndAuthToken {
                            addr: SingularPtrField::some(sa(*addr)),
                            token: Bytes::new(),
                            ..AddrAndAuthToken::default()
                        }),
                        Some((spn, ctx)) => {
                            if !krb5_spns.contains_key(addr) {
                                krb5_spns.insert(*addr, spn.clone());
                            }
                            let msg = PermissionToken {
                                path: Chars::from(path.as_ref()),
                                timestamp: now,
                                ..PermissionToken::default()
                            }
                            .write_to_bytes()?;
                            let token = Bytes::copy_from_slice(&*ctx.wrap(true, &msg)?);
                            Ok(AddrAndAuthToken {
                                addr: SingularPtrField::some(sa(*addr)),
                                token,
                                ..AddrAndAuthToken::default()
                            })
                        }
                    })
                    .collect::<Result<RepeatedField<AddrAndAuthToken>>>()
            })
            .unwrap_or_else(|| Ok(RepeatedField::from_vec(vec![])))?;
        Ok(Resolution {
            addrs,
            ..Resolution::default()
        })
    }

    pub(crate) fn list(&self, parent: &Chars) -> Vec<Chars> {
        let parent = parent.as_ref();
        self.by_level
            .get(&(Path::levels(parent) + 1))
            .map(|l| {
                l.range::<str, (Bound<&str>, Bound<&str>)>((Excluded(parent), Unbounded))
                    .take_while(|p| p.as_ref().starts_with(parent))
                    .cloned()
                    .map(|p| p.as_chars())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let apps = vec![
            (
                vec!["/app/test/app0/v0", "/app/test/app0/v1"],
                "127.0.0.1:100",
            ),
            (
                vec!["/app/test/app0/v0", "/app/test/app0/v1"],
                "127.0.0.1:101",
            ),
            (
                vec![
                    "/app/test/app1/v2",
                    "/app/test/app1/v3",
                    "/app/test/app1/v4",
                ],
                "127.0.0.1:105",
            ),
        ];
        let store = Store::<()>::new();
        {
            let mut store = store.write();
            for (paths, addr) in &apps {
                let parsed = paths.iter().map(|p| Path::from(*p)).collect::<Vec<_>>();
                let addr = addr.parse::<SocketAddr>().unwrap();
                for path in parsed.clone() {
                    store.publish(path.clone(), addr);
                    if !store.resolve(&path).contains(&addr) {
                        panic!()
                    }
                }
            }
        }
        {
            let store = store.read();
            let paths = store.list(&Path::from("/"));
            assert_eq!(paths.len(), 1);
            assert_eq!(paths[0].as_ref(), "/app");
            let paths = store.list(&Path::from("/app"));
            assert_eq!(paths.len(), 1);
            assert_eq!(paths[0].as_ref(), "/app/test");
            let paths = store.list(&Path::from("/app/test"));
            assert_eq!(paths.len(), 2);
            assert_eq!(paths[0].as_ref(), "/app/test/app0");
            assert_eq!(paths[1].as_ref(), "/app/test/app1");
            let paths = store.list(&Path::from("/app/test/app0"));
            assert_eq!(paths.len(), 2);
            assert_eq!(paths[0].as_ref(), "/app/test/app0/v0");
            assert_eq!(paths[1].as_ref(), "/app/test/app0/v1");
            let paths = store.list(&Path::from("/app/test/app1"));
            assert_eq!(paths.len(), 3);
            assert_eq!(paths[0].as_ref(), "/app/test/app1/v2");
            assert_eq!(paths[1].as_ref(), "/app/test/app1/v3");
            assert_eq!(paths[2].as_ref(), "/app/test/app1/v4");
        }
        let (ref paths, ref addr) = apps[2];
        let addr = addr.parse::<SocketAddr>().unwrap();
        let parsed = paths.iter().map(|p| Path::from(*p)).collect::<Vec<_>>();
        {
            let mut store = store.write();
            for path in parsed.clone() {
                store.unpublish(path.clone(), addr);
                if store.resolve(&path).contains(&addr) {
                    panic!()
                }
            }
        }
        {
            let store = store.read();
            let paths = store.list(&Path::from("/"));
            assert_eq!(paths.len(), 1);
            assert_eq!(paths[0].as_ref(), "/app");
            let paths = store.list(&Path::from("/app"));
            assert_eq!(paths.len(), 1);
            assert_eq!(paths[0].as_ref(), "/app/test");
            let paths = store.list(&Path::from("/app/test"));
            assert_eq!(paths.len(), 1);
            assert_eq!(paths[0].as_ref(), "/app/test/app0");
            let paths = store.list(&Path::from("/app/test/app1"));
            assert_eq!(paths.len(), 0);
            let paths = store.list(&Path::from("/app/test/app0"));
            assert_eq!(paths.len(), 2);
            assert_eq!(paths[0].as_ref(), "/app/test/app0/v0");
            assert_eq!(paths[1].as_ref(), "/app/test/app0/v1");
        }
    }
}

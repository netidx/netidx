use crate::path::Path;
use std::{
    iter::{self, FromIterator},
    net::SocketAddr, sync::{Arc, Weak, RwLock},
    collections::{
        Bound, Bound::{Included, Excluded, Unbounded},
        HashSet, HashMap, BTreeMap
    },
    ops::{Deref, DerefMut},
};

lazy_static! {
    static ref EMPTY: Addrs = Arc::new(Vec::new());
}

type Addrs = Arc<Vec<SocketAddr>>;

// We hashcons the address sets. On average, a publisher should publish many paths.
// for each published value we only store the path once, since it's an Arc<str>,
// and a pointer to the set of addresses it is published by. 
#[derive(Debug)]
struct HCAddrs(HashMap<Vec<SocketAddr>, Weak<Vec<SocketAddr>>>);

impl HCAddrs {
    fn new() -> HCAddrs {
        HCAddrs(HashMap::new())
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
        let mut set =
            current.iter().copied().chain(iter::once(addr)).collect::<Vec<_>>();
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
struct StoreInner {
    by_path: BTreeMap<Path, Addrs>,
    by_addr: HashMap<SocketAddr, HashSet<Path>>,
    addrs: HCAddrs,
}

impl StoreInner {
    fn remove_path(&mut self, mut p: &str) {
        loop {
            let mut r = self.by_path.range::<str, (Bound<&str>, Bound<&str>)>(
                (Included(p), Unbounded)
            );
            if let Some((_, pv)) = r.next() {
                if pv.len() != 0 { break }
                else {
                    match r.next() {
                        Some((sib, _)) if sib.starts_with(p) => break,
                        None | Some(_) => { self.by_path.remove(p); },
                    }
                }
            }
            match Path::dirname(p) {
                Some(parent) => p = parent,
                None => break
            }
        }
    }

    fn add_path(&mut self, mut p: &str) {
        loop {
            match Path::dirname(p) {
                None => break,
                Some(dirname) => match self.by_path.get(dirname) {
                    Some(_) => break,
                    None => {
                        self.by_path.insert(Path::from(dirname), EMPTY.clone());
                        p = dirname;
                    }
                }
            }
        }
    }

    fn publish(&mut self, path: Path, addr: SocketAddr) {
        let addrs = self.by_path.entry(path.clone()).or_insert_with(|| EMPTY.clone());
        *addrs = self.addrs.add_address(addrs, addr);
        self.add_path(path.as_ref());
        self.by_addr.entry(addr).or_insert_with(HashSet::new).insert(path);
    }

    fn unpublish(&mut self, path: Path, addr: SocketAddr) {
        self.by_addr.get_mut(&addr).into_iter().for_each(|s| { s.remove(&path); });
        match self.by_path.get_mut(&path) {
            None => (),
            Some(addrs) => match self.addrs.remove_address(addrs, addr) {
                Some(new_addrs) => { *addrs = new_addrs; }
                None => {
                    self.by_path.remove(&path);
                    self.remove_path(path.as_ref())
                }
            }
        }
    }

    fn unpublish_addr(&mut self, addr: SocketAddr) {
        self.by_addr.remove(&addr).into_iter().for_each(|paths| {
            for path in paths {
                self.unpublish(path, addr);
            }
        })
    }

    fn resolve(&self, path: &str) -> Vec<SocketAddr> {
        self.by_path.get(path)
            .map(|a| a.iter().copied().collect())
            .unwrap_or_else(|| Vec::new())
    }

    fn list(&self, parent: &str) -> Vec<Path> {
        self.by_path
            .range::<str, (Bound<&str>, Bound<&str>)>((Excluded(parent), Unbounded))
            .take_while(|(p, _)| parent == Path::dirname(p).unwrap_or("/"))
            .map(|(p, _)| p.clone())
            .collect()
    }
}

#[derive(Clone)]
pub(crate) struct Store(Arc<RwLock<StoreInner>>);

impl Store {
    pub(crate) fn new() -> Self {
        Store(Arc::new(RwLock::new(StoreInner {
            by_path: BTreeMap::new(),
            by_addr: HashMap::new(),
            addrs: HCAddrs::new(),
        })))
    }

    pub(crate) fn publish(&self, paths: Vec<Path>, addr: SocketAddr) {
        let mut inner = self.0.write().unwrap();
        for path in paths {
            inner.publish(path, addr);
        }
    }

    pub(crate) fn unpublish(&self, paths: Vec<Path>, addr: SocketAddr) {
        let mut inner = self.0.write().unwrap();
        for path in paths {
            inner.unpublish(path, addr);
        }
        inner.addrs.gc();
    }

    pub(crate) fn unpublish_addr(&self, addr: SocketAddr) {
        let mut inner = self.0.write().unwrap();
        inner.unpublish_addr(addr);
        inner.addrs.gc();
    }

    pub(crate) fn resolve(&self, paths: &Vec<Path>) -> Vec<Vec<SocketAddr>> {
        let inner = self.0.read().unwrap();
        paths.iter().map(|p| inner.resolve(p.as_ref())).collect()
    }

    pub(crate) fn list(&self, parent: &Path) -> Vec<Path> {
        let inner = self.0.read().unwrap();
        inner.list(parent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let apps = vec![
            (vec!["/app/test/app0/v0", "/app/test/app0/v1"], "127.0.0.1:100"),
            (vec!["/app/test/app0/v0", "/app/test/app0/v1"], "127.0.0.1:101"),
            (vec!["/app/test/app1/v2", "/app/test/app1/v3", "/app/test/app1/v4"],
             "127.0.0.1:105"),
        ];
        let mut store = Store::new();
        let addrs =
            vec!["127.0.0.1:100".parse::<SocketAddr>().unwrap(),
                 "127.0.0.1:101".parse::<SocketAddr>().unwrap()];
        for (paths, addr) in &apps {
            let parsed = paths.iter().map(|p| Path::from(*p)).collect::<Vec<_>>();
            let addr = addr.parse::<SocketAddr>().unwrap();
            store.publish(parsed.clone(), addr);
            if !store.resolve(&parsed).iter().all(|s| s.contains(&addr)) {
                panic!()
            }
        }
        dbg!(store.0.read().unwrap());
        let paths = store.list(&Path::from("/"));
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].as_ref(), "/app");
        let paths = store.list(&Path::from("/app"));
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].as_ref(), "/app/test");
        let paths = dbg!(store.list(&Path::from("/app/test")));
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
        let (ref paths, ref addr) = apps[2];
        let addr = addr.parse::<SocketAddr>().unwrap();
        let parsed = paths.iter().map(|p| Path::from(*p)).collect::<Vec<_>>();
        store.unpublish(parsed.clone(), addr);
        if store.resolve(&parsed).len() != 0 { panic!() }
        let paths = store.list(&Path::from("/"));
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].as_ref(), "/app");
        let paths = store.list(&Path::from("/app"));
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].as_ref(), "/app/test");
        let paths = store.list(&Path::from("/app/test"));
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[1].as_ref(), "/app/test/app1");
        let paths = store.list(&Path::from("/app/test/app0"));
        assert_eq!(paths.len(), 0);
        let paths = store.list(&Path::from("/app/test/app1"));
        assert_eq!(paths.len(), 3);
        assert_eq!(paths[0].as_ref(), "/app/test/app1/v2");
        assert_eq!(paths[1].as_ref(), "/app/test/app1/v3");
        assert_eq!(paths[2].as_ref(), "/app/test/app1/v4");
    }
}

use crate::path::Path;
use std::{
    iter::{self, FromIterator},
    net::SocketAddr, sync::{Arc, Weak, Mutex, RwLock},
    collections::{
        Bound, Bound::{Included, Excluded, Unbounded},
        HashSet, HashMap, BTreeMap
    },
    ops::{Deref, DerefMut, RangeFrom},
};

// We hashcons the address sets. On average, a publisher should publish many paths.
// for each published value we only store the path once, since it's an Arc<str>,
// and a pointer to the set of addresses it is published by. 
struct HCAddrs(HashMap<Vec<SocketAddr>, Weak<AddrsInner>>);

impl Deref for HCAddrs {
    type Target = HashMap<Vec<SocketAddr>, Weak<AddrsInner>>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

// CR estokes: limit the number of publishers that can publish the
// same path to a reasonable number.
impl DerefMut for HCAddrs {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 }
}

impl HCAddrs {
    fn new() -> HCAddrs {
        HCAddrs(HashMap::new())
    }

    fn hashcons(&mut self, set: Vec<SocketAddr>) -> Addrs {
        match self.get(&set).and_then(|v| v.upgrade()) {
            Some(addrs) => addrs,
            None => {
                let addrs = Arc::new(AddrsInner(set.clone()));
                self.insert(set, Arc::downgrade(&addrs));
                addrs
            }
        }
    }

    fn add_address(&mut self, current: &Addrs, addr: SocketAddr) -> Addrs {
        let set: HashSet<SocketAddr> =
            HashSet::from_iter(current.into_iter().chain(iter::once(addr)));
        self.hashcons(set.into_iter().collect())
    }

    fn remove_address(&mut self, current: &Addrs, addr: SocketAddr) -> Option<Addrs> {
        if current.len() == 1 && current[0] == addr {
            None
        } else {
            let s = current.into_iter().filter(|a| a != &addr);
            Some(self.hashcons(s.collect()))
        }
    }
}

lazy_static! {
    static ref EMPTY: Addrs = Arc::new(Vec::new());
    static ref ADDRS: Arc<Mutex<HCAddrs>> = Arc::new(Mutex::new(HashMap::new()));
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub(crate) struct AddrsInner(Vec<SocketAddr>);

impl Deref for AddrsInner {
    type Target = Vec<SocketAddr>;
    fn deref(&self) -> &Vec<SocketAddr> { &self.0 }
}

impl Drop for AddrsInner {
    fn drop(&mut self) {
        let addrs = ADDRS.lock().unwrap();
        addrs.remove(&self.0);
    }
}

pub(crate) type Addrs = Arc<AddrsInner>;

struct StoreInner {
    by_path: BTreeMap<Path, Addrs>,
    by_addr: HashMap<SocketAddr, HashSet<Path>>,
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

    fn publish(&mut self, hc: &mut HCAddrs, path: Path, addr: SocketAddr) {
        let addrs = self.by_path.entry(path.clone()).or_insert_with(|| EMPTY.clone());
        *addrs = hc.add_address(addrs, addr);
        self.add_path(path.as_ref());
        self.by_addr.entry(addr).or_insert_with(HashSet::new).insert(path);
    }

    fn unpublish(&mut self, hc: &mut HCAddrs, path: Path, addr: SocketAddr) {
        self.by_addr.get_mut(&addr).into_iter().for_each(|s| s.remove(&path));
        self.by_path.get_mut(&path).and_then(|addrs| {
            match hc.remove_address(addrs, addr) {
                Some(new_addrs) => { *addrs = new_addrs; }
                None => {
                    self.by_path.remove(&path);
                    self.remove_path(path.as_ref())
                }
            }
        })
    }

    fn unpublish_addr(&mut self, hc: &mut HCAddrs, addr: SocketAddr) {
        self.by_addr.remove(&addr).and_then(|paths| {
            for path in paths {
                self.unpublish(hc, path, *addr);
            }
        })
    }

    fn resolve(&self, path: &str) -> Vec<SocketAddr> {
        self.by_path.get(path)
            .and_then(|a| a.iter().copied().collect())
            .unwrap_or_else(|| Vec::new())
    }

    fn list(&self, parent: &str) -> Vec<Path> {
        self.by_path.range(Excluded(parent), Unbounded)
            .take_while(|(p, _)| parent == Path::dirname(p).unwrap_or("/"))
            .map(|(p, v)| p.clone())
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
        })))
    }

    pub(crate) fn publish(&self, paths: Vec<Path>, addr: SocketAddr) {
        let inner = self.0.write().unwrap();
        let hc = ADDRS.lock().unwrap();
        for path in paths {
            inner.publish(&mut *hc, path, addr);
        }
    }

    pub(crate) fn unpublish(&self, paths: Vec<Path>, addr: SocketAddr) {
        let inner = self.0.write().unwrap();
        let hc = ADDRS.lock().unwrap();
        for path in paths {
            inner.unpublish(&mut *hc, path, addr);
        }
    }

    pub(crate) fn unpublish_addr(&self, addr: SocketAddr) {
        let inner = self.0.write().unwrap();
        let hc = ADDRS.lock().unwrap();
        inner.unpublish_addr(&mut *hc, addr);
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
            let parsed = paths.iter().map(Path::from).collect();
            let addr = addr.parse::<SocketAddr>().unwrap();
            store.publish(parsed.clone(), addr);
            let expected = paths.iter().map(|_| vec![*addr]).collect::<Vec<_>>();
            if store.resolve(&parsed) != expected {
                panic!()
            }
        }
        let paths = store.list(&Path::from("/"));
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].as_ref(), "app");
        let paths = store.list(&Path::from("/app"));
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].as_ref(), "test");
        let paths = store.list(&Path::from("/app/test"));
        assert_eq!(paths.len(), 2);
        assert_eq!(paths[0], "app0");
        assert_eq!(paths[1].as_ref(), "app1");
        let paths = store.list(&Path::from("/app/test/app0"));
        assert_eq!(paths.len(), 2);
        assert_eq!(paths[0].as_ref(), "v0");
        assert_eq!(paths[1].as_ref(), "v1");
        let paths = store.list(&Path::from("/app/test/app1"));
        assert_eq!(paths.len(), 3);
        assert_eq!(paths[0].as_ref(), "v2");
        assert_eq!(paths[1].as_ref(), "v3");
        assert_eq!(paths[2].as_ref(), "v4");
        let (paths, addr) = apps[2];
        let addr = addr.parse::<SocketAddr>().unwrap();
        let parsed = paths.iter().map(Path::from).collect();
        store.unpublish(parsed.clone(), *addr);
        if store.resolve(&parsed) != vec![] { panic!() }
        let paths = store.list(&Path::from("/"));
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].as_ref(), "app");
        let paths = store.list(&Path::from("/app"));
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].as_ref(), "test");
        let paths = store.list(&Path::from("/app/test"));
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[1].as_ref(), "app1");
        let paths = store.list(&Path::from("/app/test/app0"));
        assert_eq!(paths.len(), 0);
        let paths = store.list(&Path::from("/app/test/app1"));
        assert_eq!(paths.len(), 3);
        assert_eq!(paths[0].as_ref(), "v2");
        assert_eq!(paths[1].as_ref(), "v3");
        assert_eq!(paths[2].as_ref(), "v4");
    }
}

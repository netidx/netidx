use std::{
    iter::{self, FromIterator},
    net::SocketAddr, sync::{Arc, Weak, Mutex},
    collections::{Bound::{Included, Excluded, Unbounded}, HashSet, HashMap, BTreeMap},
    ops::{Deref, DerefMut},
};
use path::Path;

// We hashcons the address sets. On average, a publisher should publish many paths.
// for each published value we only store the path once, since it's an Arc<str>,
// and a pointer to the set of addresses it is published by. 
struct HCAddrs(HashMap<HashSet<SocketAddr>, Weak<AddrsInner>>);

impl Deref for HCAddrs {
    type Target = HashMap<HashSet<SocketAddr>, Weak<AddrsInner>>;
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

    fn hashcons(&mut self, set: HashSet<SocketAddr>) -> Addrs {
        match self.get(&set).and_then(|v| v.upgrade()) {
            Some(addrs) => Some(addrs),
            None => {
                let addrs = Addrs(Arc::new(AddrsInner(set.clone())));
                self.insert(set, addrs.downgrade());
                Some(addrs)
            }
        }
    }

    fn add_address(&mut self, current: &Addrs, addr: SocketAddr) -> Addrs {
        let set = HashSet::from_iter(current.iter().copied().chain(iter::once(addr)));
        self.hashcons(set)
    }

    fn remove_address(&mut self, current: &Addrs, addr: SocketAddr) -> Option<Addrs> {
        if current.contains_key(&addr) && current.len() == 1 {
            None
        } else {
            let mut set = HashSet::from(current.iter().copied());
            set.remove(&addr);
            Some(self.hashcons(set))
        }
    }
}

lazy_static! {
    static ref EMPTY: Addrs = Arc::new(HashSet::new());
    static ref ADDRS: Arc<Mutex<HCAddrs>> = Arc::new(Mutex::new(HashMap::new()));
}

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct AddrsInner(HashSet<SocketAddr>);

impl Deref for AddrsInner {
    type Target = HashSet<SocketAddr>;
    fn deref(&self) -> &HashSet<SocketAddr> { &self.0 }
}

impl Drop for AddrsInner {
    fn drop(&mut self) {
        let addrs = ADDRS.lock().unwrap();
        addrs.remove(&self.0)
    }
}

pub(crate) type Addrs = Arc<AddrsInner>;

struct StoreInner {
    by_path: BTreeMap<Path, Addrs>,
    by_addr: HashMap<SocketAddr, HashSet<Path>>,
}

impl StoreInner {
    fn rm_path(&mut self, mut p: &str) {
        loop {
            let mut r = self.by_path.range(Included(p), Unbounded);
            if let Some((_, pv)) = r.next() {
                if pv.len() != 0 { break }
                else {
                    match r.next() {
                        Some((sib, _)) if sib.starts_with(p) => break,
                        None | Some(_) => self.by_path.remove(p),
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
        self.by_addr.entry(addr).insert(path);
    }

    fn unpublish(&mut self, hc: &mut HCAddrs, path: Path, addr: SocketAddr) {
        self.by_path.get_mut(&path).and_then(|addrs| {
            match hc.remove_address(addrs, *addr) {
                None => {
                    self.by_path.remove(&path);
                    
                }
            }
        })
    }
}

// updates are serialized by the ADDRS lock, however reads may
// continue while an update is in progress.
#[derive(Clone)]
pub(crate) struct Store(Arc<RwLock<StoreInner>>);

impl Store {
    pub(crate) fn new() -> Self {
        Store(ArcCell::new(Map::new()))
    }

    pub(crate) fn change<E>(&self, ops: E)
    where E: IntoIterator<Item=(&Path, (Action, SocketAddr))>
    {
        // this lock ensures that only one write can happen at a time
        let addrs = ADDRS.lock().unwrap();
        let mut added = Vec::new();
        let mut removed = Vec::new();
        let up = self.0.load().update_many(ops, &mut |p, (action, addr), cur| {
            let (p, cur) = match cur {
                None => (p, &*EMPTY),
                Some((p, cur)) => (p.clone(), cur)
            };
            let (s, changed) = match action {
                Action::Publish => {
                    let (s, present) = cur.insert(addr);
                    if !present && s.len() == 1 { added.push(p.clone()) }
                    (s, !present)
                },
                Action::Unpublish => {
                    let (s, present) = cur.remove(&addr);
                    if present && s.len() == 0 { removed.push(p.clone()) }
                    (s, present)
                }
            };
            if !changed { Some((p, cur.clone())) }
            else {
                if s.len() == 0 { Some((p, EMPTY.clone())) }
                else {
                    match addrs.get(&s) {
                        Some(s) => Some((p, s.upgrade().unwrap())),
                        None => {
                            let a = Arc::new(AddrsInner(s.clone()));
                            addrs.insert(s, a.downgrade());
                            Some((p, a))
                        }
                    }
                }
            }
        });
        let parent_ops =
            parents_to_rm(&up, removed).into_iter().map(|p| (p, false)).chain(
                parents_to_add(&up, added).iter().map(|p| (p, true))
            );
        let up = up.update_many(parent_ops, &mut |p, add, _| {
            if add { Some(p, EMPTY.clone()) }
            else { None }
        });
        // this is safe because writes are serialized by the ADDRS lock
        self.0.update(move |_| up);
    }

    pub(crate) fn read(&self) -> ArcCellGuard<'_, Map<Path, Addrs>> { self.0.load() }
}

pub(crate) fn resolve(t: &Map<Path, Addrs>, path: &Path) -> Vec<SocketAddr> {
    match t.get(path) {
        None => Vec::new(),
        Some(s) => s.into_iter().map(|a| *a).collect()
    }
}

pub(crate) fn list(t: &Map<Path, Addrs>, parent: &Path) -> Vec<Path> {
    let parent : &str = &*parent;
    let mut res = Vec::new();
    let paths = t.range(Excluded(parent), Unbounded);
    for (p, _) in paths {
        let d =
            match Path::dirname(p) {
                None => "/",
                Some(d) => d
            };
        if parent != d { break }
        else { Path::basename(p).map(|p| res.push(Path::from(p))); }
    }
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    static ADDRS: [(&str, &str)] = [
        ("/app/test/app0/v0", "127.0.0.1:100"),
        ("/app/test/app0/v1", "127.0.0.1:100"),
        ("/app/test/app0/v0", "127.0.0.1:101"),
        ("/app/test/app0/v1", "127.0.0.1:101"),
        ("/app/test/app1/v2", "127.0.0.1:105"),
        ("/app/test/app1/v3", "127.0.0.1:105"),
        ("/app/test/app1/v4", "127.0.0.1:105")
    ];

    #[test]
    fn test() {
        let mut store = Store::new();
        let mut n = 0;
        let addrs =
            vec!["127.0.0.1:100".parse::<SocketAddr>().unwrap(),
                 "127.0.0.1:101".parse::<SocketAddr>().unwrap()];
        for (path, addr) in &ADDRS {
            let addr = addr.parse::<SocketAddr>().unwrap();
            let path = Path::from(path);
            store.publish(path.clone(), addr);
            if n < 2 && store.resolve(&path) != vec![addr] { panic!() }
            else if n < 4 && store.resolve(&path) != addrs { panic!() }
            else if n >= 4 && store.resolve(&path) != vec![addr] { panic!() }
            n += 1
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
        n = 0;
        for (path, addr) in ADDRS[0..4] {
            let addr = addr.parse::<SocketAddr>().unwrap();
            let path = Path::from(path);
            store.unpublish(&path, &addr);
            if n < 2 && store.resolve(&path) != vec![addrs[1]] { panic!() }
            else if n < 4 && store.resolve(&path) != vec![] { panic!() }
            n += 1;
        }
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

    #[test]
    fn test_refcnt() {
        let mut store = Store::new();
        let path = Path::from("/foo/bar/baz");
        let addr = "127.0.0.1:111".parse::<SocketAddr>().unwrap();
        store.publish(path.clone(), addr);
        store.publish(path.clone(), addr);
        store.publish(path, addr);
        assert_eq!(store.resolve(&path), vec![addr]);
        store.unpublish(&path, &addr);
        assert_eq!(store.resolve(&path), vec![addr]);
        store.unpublish(&path, &addr);
        assert_eq!(store.resolve(&path), vec![addr]);
        store.unpublish(&path, &addr);
        assert_eq!(store.resolve(&path), vec![]);
    }
}

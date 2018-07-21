use std::{
    net::SocketAddr, iter::Iterator, sync::{Arc, Weak, RwLock, Mutex},
    collections::{Bound::{Included, Excluded, Unbounded}, HashSet, HashMap},
    ops::Deref,
};
use immutable_chunkmap::{map::Map, set::Set};
use path::Path;

lazy_static! {
    static ref EMPTY: Addrs = Arc::new(Set::new());
    static ref ADDRS: Mutex<HashMap<Set<SocketAddr>, Weak<AddrsInner>>> =
        Mutex::new(HashMap::new());
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Action { Publish, Unpublish }

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct AddrsInner(Set<SocketAddr>);

impl Deref for AddrsInner {
    type Target = Set<SocketAddr>;
    fn deref(&self) -> &Set<SocketAddr> { &self.0 }
}

impl Drop for AddrsInner {
    fn drop(&mut self) {
        let mut addrs = ADDRS.lock();
        addrs.remove(&self.0)
    }
}

pub(crate) type Addrs = Arc<AddrsInner>;

#[derive(Clone)]
pub(crate) struct Store {
    write: Arc<Mutex<Map<Path, Addrs>>>,
    read: Arc<RwLock<Map<Path, Addrs>>>
}

fn parents_to_rm(s: &Map<Path, Addrs>, removed: HashSet<Path>) -> HashSet<Path> {
    let mut to_rm = HashSet::new();
    for p in removed.drain() {
        let mut p : &str = p.as_ref();
        loop {
            let mut r = s.range(Included(p), Unbounded);
            if let Some((_, pv)) = r.next() {
                if pv.len() != 0 { break }
                else {
                    match r.next() {
                        Some((sib, _)) if sib.starts_with(p) => break,
                        None | Some(_) => to_rm.insert(Path::from(p))
                    }
                }
            }
            match Path::dirname(p) {
                Some(parent) => p = parent,
                None => break
            }
        }
    }
    to_rm
}

fn parents_to_add(s: &Map<Path, Addrs>, children: HashSet<Path>) -> HashSet<Path> {
    let mut to_add = HashSet::new();
    for child in children.into_iter() {
        let mut child : &str = child.as_ref();
        loop {
            match Path::dirname(child) {
                None => break,
                Some(dirname) =>
                    match s.get(dirname) {
                        Some(_) => break,
                        None => {
                            if to_add.contains(dirname) { break }
                            else {
                                to_add.insert(Path::from(dirname));
                                child = dirname;
                            }
                        }
                    }
            }
        }
    }
    to_add
}

impl Store {
    pub(crate) fn new() -> Self {
        Store {
            write: Arc::new(Mutex::new(Map::new())),
            read: Arc::new(RwLock::new(Map::new()))
        }
    }

    pub(crate) fn change<E>(&self, ops: E) -> Vec<Path>
    where E: IntoIterator<Item=(Path, (Action, SocketAddr))>
    {
        let mut paths = Vec::new();
        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut write = self.write.lock().unwrap();
        let up = {
            let mut addrs = ADDRS.lock();
            write.update_many(ops, &mut |p, (action, addr), cur| {
                let (p, cur) = match cur {
                    None => (p, &*EMPTY),
                    Some((p, cur)) => {
                        paths.push(p.clone());
                        (p.clone(), cur)
                    }
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
            })
        };
        let parent_ops =
            parents_to_rm(&up, removed).into_iter().map(|p| (p, false)).chain(
                parents_to_add(&up, added).iter().map(|p| (p, true))
            );
        let up =
            up.update_many(parent_ops, &mut |p, add, _| {
                if add { Some(p, EMPTY.clone()) }
                else { None }
            });
        *write = up.clone();
        let old = {
            /* overwriting read could result in a LOT of Arcs getting
            dropped, which could take a long time. We must not hold
            the write lock on read (thereby blocking all reads) while
            that happens, so we clone the old thing and only drop it
            after releasing the lock. */
            let mut read = self.read.write().unwrap();
            let old = read.clone();
            *read = up;
            old
        };
        drop(old);
        paths
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item=(&Path, &Addrs)> {
        self.read.read().unwrap().clone().into_iter()
    }

    pub(crate) fn read(&self) -> Map<Path, Addrs> { self.read.read().unwrap().clone() }
}

pub(crate) fn resolve(t: &Map<Path, Addrs>, path: &Path) -> Vec<Addrs> {
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
p        let mut store = Store::new();
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

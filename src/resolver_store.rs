use std::{
    net::SocketAddr, iter::Iterator, sync::Arc,
    collections::{Bound, Bound::{Included, Excluded, Unbounded}},
};
use immutable_chunkmap::{map::Map, set::Set};
use path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Action { Publish, Unpublish }

type Addrs = Arc<Set<SocketAddr>>;

pub(crate) struct Store {
    published: Map<Path, Addrs>,
    addrs: Map<Addrs, usize>,
    empty: Addrs
}

fn cleanup_rm(s: Map<Path, Addrs>, p: &str) -> Map<Path, Addrs> {
    let mut r = s.range(Included(p), Unbounded);
    match r.next() {
        None =>
            match Path::dirname(p) {
                None => s,
                Some(parent) => cleanup_rm(s, parent)
            },
        Some((_, pv)) => {
            if pv.len() != 0  { s }
            else {
                match r.next() {
                    None => {
                        let s = s.remove(p).0;
                        match Path::dirname(p) {
                            None => s,
                            Some(parent) => cleanup_rm(s, parent)
                        }
                    },
                    Some((sib, _)) => {
                        if sib.starts_with(p) { s }
                        else {
                            let s = s.remove(p).0;
                            match Path::dirname(p) {
                                None => s,
                                Some(parent) => cleanup_rm(s, parent)
                            }
                        }
                    }
                }
            }
        }
    }
}

fn parents(
    s: Map<Path, Addrs>, child: &str, empty: &Addrs
) -> Map<Path, Addrs> {
    match Path::dirname(child) {
        None => s,
        Some(dirname) =>
            match s.get(dirname) {
                Some(_) => s,
                None => {
                    let s = s.insert(Path::from(dirname), empty.clone());
                    parents(s, dirname, empty)
                }
            }
    }
}

impl Store {
    pub(crate) fn new() -> Self {
        Store { published: Map::new(), addrs: Map::new(), empty: Arc::new(Set::new()) }
    }

    pub(crate) fn change<E>(
        &self, ops: E
    ) -> Self where E: IntoIterator<Item=(Path, (Action, SocketAddr))> {
        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut addrs = self.addrs.clone();
        let published =
            self.published.update_many(ops, &mut |p, (action, addr), current| {
                let current = current.unwrap_or_else(|| &self.empty);
                let (s, changed) = match action {
                    Action::Publish => {
                        let (s, present) = current.insert(addr);
                        if !present && s.len() == 1 { added.push(p.clone()) }
                        (s, !present)
                    },
                    Action::Unpublish => {
                        let (s, present) = current.remove(&addr);
                        if present && s.len() == 0 { removed.push(p.clone()) }
                        (s, present)
                    }
                };
                if !changed { Some(current.clone()) }
                else {
                    let mut res = None;
                    addrs = addrs.update_many(
                        &[(&s, ()), (&current, ())], &mut |k, (), cur| {
                            if res.is_none() {
                                match cur {
                                    Some((a, i)) => {
                                        res = Some(a.clone());
                                        Some((a.clone(), i + 1))
                                    },
                                    None => {
                                        if s.len() == 0 {
                                            res = Some(self.empty.clone());
                                            None
                                        } else {
                                            let s = Arc::new(s);
                                            res = Some(s.clone());
                                            Some((s, 1))
                                        }
                                    }
                                }
                            } else {
                                match cur {
                                    None => None,
                                    Some((_, i)) => {
                                        if i <= 1 { None }
                                        else { Some((current.clone(), i - 1)) }
                                    }
                                }
                            }
                        });
                    res
                }
            });
        let published = added.iter().fold(published, |s, p| parents(s, p, &self.empty));
        let published = removed.iter().fold(published, &cleanup_rm);
        Store { published, addrs, empty: self.empty.clone() }
    }

    pub(crate) fn resolve(&self, path: &Path) -> Resolve {
        match self.0.get(path) {
            None | Some(Published::Empty) => Resolve::Empty,
            Some(Published::One(a, _)) => Resolve::One(a),
            Some(Published::Many(a)) => {
                if a.len() == 2 {
                    let mut i = a.into_iter();
                    Resolve::Two(*i.next().unwrap().0, i.next().unwrap().0)
                } else {
                    Resolve::Many(a.into_iter().map(|(a, _)| *a).collect::<Vec<_>>())
                }
            }
        }
    }

    pub(crate) fn list(&self, parent: &Path) -> Vec<Path> {
        let parent : &str = &*parent;
        let mut res = Vec::new();
        let paths =
            self.0.range::<str, (Bound<&str>, Bound<&str>)>(
                (Excluded(parent), Unbounded)
            );
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

    pub(crate) fn iter(&self) -> impl Iterator<Item=(&Path, &Published)> {
        self.0.iter()
    }
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

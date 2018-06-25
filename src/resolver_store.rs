use std::{
  net::SocketAddr, iter::Iterator,
  collections::{BTreeMap, HashMap, Bound, Bound::{Included, Excluded, Unbounded}},
};
use path::{self, Path};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Published {
  Empty,
  One(SocketAddr, usize),
  Many(HashMap<SocketAddr, usize>)
}

pub(crate) struct Store(BTreeMap<Path, Published>);

impl Store {
  pub(crate) fn new() -> Self { Store(BTreeMap::new()) }

  pub(crate) fn publish(&mut self, path: Path, addr: SocketAddr) {
    let v = self.0.entry(path).or_insert(Published::One(addr, 1));
    match *v {
      Published::Empty => { *v = Published::One(addr, 1) },
      Published::Many(ref mut set) => *set.entry(addr).or_insert(0) += 1,
      Published::One(cur, i) =>
        if cur == addr {
          *v = Published::One(cur, i + 1)
        } else {
          let s =
            [(addr, 1), (cur, i)].iter().map(|&(a, i)| (a, i))
            .collect::<HashMap<_, _>>();
          *v = Published::Many(s)
        }
    }
  }

  pub(crate) fn unpublish(&mut self, path: &Path, addr: &SocketAddr) {
    let remove =
      match self.0.get_mut(path) {
        None => false,
        Some(mut v) => {
          match *v {
            Published::Empty => false,
            Published::One(a, ref mut i) => a == *addr && { *i -= 1; *i <= 0 },
            Published::Many(ref mut set) => {
              let remove = 
                match set.get_mut(addr) {
                  None => false,
                  Some(i) => {
                    *i -= 1;
                    *i <= 0
                  }
                };
              if remove { set.remove(&addr); }
              set.is_empty()
            }
          }
        }
      };
    if remove {
      self.0.remove(path);
      // remove parents that have no further children
      let mut p : &str = path.as_ref();
      loop {
        match path::dirname(p) {
          None => break,
          Some(parent) => {
            let remove = {
              let mut r =
                self.0.range::<str, (Bound<&str>, Bound<&str>)>(
                  (Included(parent), Unbounded)
                );
              match r.next() {
                None => false, // parent doesn't exist, probably a bug
                Some((_, parent_v)) => {
                  if parent_v != &Published::Empty { break; }
                  else {
                    match r.next() {
                      None => true,
                      Some((sib, _)) => !sib.starts_with(parent)
                    }
                  }
                }
              }
            };
            if remove { self.0.remove(parent); }
            p = parent;
          }
        }
      }
    }
  }

  pub(crate) fn resolve(&self, path: &Path) -> Vec<SocketAddr> {
    match self.0.get(path) {
      None | Some(&Published::Empty) => vec![],
      Some(&Published::One(a, _)) => vec![a],
      Some(&Published::Many(ref a)) => {
        let s = a.iter().map(|(a, _)| *a).collect::<Vec<_>>();
        s
      }
    }
  }

  pub(crate) fn list(&self, parent: &Path) -> Vec<Path> {
    let parent : &str = parent.as_ref();
    let mut res = Vec::new();
    let paths =
      self.0.range::<str, (Bound<&str>, Bound<&str>)>(
        (Excluded(parent), Unbounded)
      );
    for (p, _) in paths {
      let d =
        match path::dirname(p) {
          None => "/",
          Some(d) => d
        };
      if parent != d { break }
      else { path::basename(p).map(|p| res.push(Path::from(p))); }
    }
    res
  }

  pub(crate) fn iter(&self) -> impl Iterator<Item=(&Path, &Published)> {
    self.0.iter()
  }
}

use std::{
    borrow::Borrow,
    cmp::Ordering,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

pub(super) struct Range<'a, R, K, V, Q> {
    index: &'a Index<K, V>,
    range: R,
    cur: usize,
    phantom: PhantomData<Q>,
}

impl<'a, R, K, V, Q> Iterator for Range<'a, R, K, V, Q>
where
    R: RangeBounds<Q>,
    K: Ord + Borrow<Q>,
    Q: Ord,
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        match self.index.get_kv_at(self.cur) {
            None => None,
            Some((k, v)) => {
                if self.range.contains(k.borrow()) {
                    self.cur += 1;
                    Some((k, v))
                } else {
                    None
                }
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct Index<K, V> {
    keys: Vec<K>,
    vals: Vec<V>,
}

impl<K, V> Index<K, V>
where
    K: Ord,
{
    pub(super) fn new() -> Self {
        Self { keys: vec![], vals: vec![] }
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.keys.iter().zip(self.vals.iter())
    }

    pub(super) fn range<R, Q>(&self, range: R) -> Range<R, K, V, Q>
    where
        R: RangeBounds<Q>,
        Q: Ord,
        K: Borrow<Q>,
    {
        let cur = match range.start_bound() {
            Bound::Unbounded => 0,
            Bound::Excluded(k) => {
                match self.keys.binary_search_by(|key| key.borrow().cmp(k)) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                }
            }
            Bound::Included(k) => {
                match self.keys.binary_search_by(|key| key.borrow().cmp(k)) {
                    Ok(i) | Err(i) => i,
                }
            }
        };
        Range { index: self, range, cur, phantom: PhantomData }
    }

    pub(super) fn reserve(&mut self, i: usize) {
        self.keys.reserve(i);
        self.vals.reserve(i);
    }

    pub(super) fn shrink_to_fit(&mut self) {
        self.keys.shrink_to_fit();
        self.vals.shrink_to_fit();
    }

    pub(super) fn get_at(&self, i: usize) -> Option<&V> {
        self.vals.get(i)
    }

    pub(super) fn get_kv_at(&self, i: usize) -> Option<(&K, &V)> {
        self.keys.get(i).map(|k| (k, &self.vals[i]))
    }

    pub(super) fn insert(&mut self, k: K, v: V) {
        match self.keys.last() {
            Some(last_k) => match k.cmp(last_k) {
                Ordering::Greater | Ordering::Equal => {
                    self.keys.push(k);
                    self.vals.push(v);
                }
                Ordering::Less => match self.keys.binary_search(&k) {
                    Ok(i) => {
                        self.keys[i] = k;
                        self.vals[i] = v;
                    }
                    Err(i) => {
                        self.keys.insert(i, k);
                        self.vals.insert(i, v);
                    }
                },
            },
            None => {
                self.keys.push(k);
                self.vals.push(v);
            }
        }
    }

    pub(super) fn remove<Q>(&mut self, k: &Q)
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        match self.keys.binary_search_by(|key| key.borrow().cmp(k)) {
            Ok(i) => {
                self.keys.remove(i);
                self.vals.remove(i);
            }
            Err(_) => (),
        }
    }

    pub(super) fn get<Q>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        match self.keys.binary_search_by(|cur| cur.borrow().cmp(k)) {
            Ok(i) => Some(&self.vals[i]),
            Err(_) => None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::{seq::SliceRandom, thread_rng, Rng};
    use std::collections::BTreeMap;

    #[test]
    fn test_insert_lookup_remove() {
        let mut model: BTreeMap<u32, u32> = BTreeMap::new();
        let mut index: Index<u32, u32> = Index::new();
        let mut rng = thread_rng();
        for _ in 0..10_000 {
            let kv = rng.gen();
            model.insert(kv, kv);
            index.insert(kv, kv);
            for (k, v) in model.iter() {
                assert_eq!(index.get(&k), Some(v))
            }
            for (k, v) in index.iter() {
                assert_eq!(model.get(&k), Some(v));
            }
        }
        let mut keys: Vec<u32> = model.keys().copied().collect();
        keys.shuffle(&mut rng);
        for k in &keys {
            model.remove(k);
            index.remove(k);
            for (k, v) in model.iter() {
                assert_eq!(index.get(&k), Some(v));
            }
            for (k, v) in index.iter() {
                assert_eq!(model.get(&k), Some(v));
            }
        }
    }

    #[test]
    fn test_range() {
        let mut model: BTreeMap<u32, u32> = BTreeMap::new();
        let mut index: Index<u32, u32> = Index::new();
        let mut keys = vec![];
        let mut rng = thread_rng();
        for _ in 0..10_000 {
            let kv = rng.gen();
            model.insert(kv, kv);
            index.insert(kv, kv);
            keys.push(kv);
        }
        keys.sort();
        while keys.len() > 0 {
            let mid = keys.len() / 2;
            let li = rng.gen_range(0..=mid);
            let ui = rng.gen_range(mid..keys.len());
            let mut le = false;
            let lower = if rng.gen_bool(0.1) {
                Bound::Unbounded
            } else if rng.gen_bool(0.5) {
                Bound::Included(keys.remove(li))
            } else {
                le = true;
                Bound::Excluded(keys.remove(li))
            };
            let upper = if rng.gen_bool(0.1) {
                Bound::Unbounded
            } else {
                let c = if le || rng.gen_bool(0.5) {
                    Bound::Included
                } else {
                    Bound::Excluded
                };
                match keys.get(ui) {
                    None => Bound::Unbounded,
                    Some(k) => {
                        let r = c(*k);
                        keys.remove(ui);
                        r
                    }
                }
            };
            let mut irange = index.range((lower, upper));
            for (km, vm) in model.range((lower, upper)) {
                match irange.next() {
                    None => panic!("expected elements"),
                    Some((ki, vi)) => {
                        assert_eq!(km, ki);
                        assert_eq!(vm, vi);
                    }
                }
            }
            assert_eq!(irange.next(), None);
        }
    }
}

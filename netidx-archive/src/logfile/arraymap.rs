use std::{
    borrow::Borrow,
    cmp::Ordering,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

#[derive(Clone, Copy)]
enum Dir {
    Fwd,
    Bwd,
}

pub struct Range<'a, R, K, V, Q> {
    map: &'a ArrayMap<K, V>,
    range: R,
    cur_fwd: usize,
    cur_bwd: usize,
    phantom: PhantomData<Q>,
}

impl<'a, R, K, V, Q> Range<'a, R, K, V, Q>
where
    R: RangeBounds<Q>,
    K: Ord + Borrow<Q>,
    Q: Ord,
{
    fn step(&mut self, dir: Dir) -> Option<(&'a K, &'a V)> {
        if self.cur_fwd <= self.cur_bwd {
            let idx = match dir {
                Dir::Fwd => self.cur_fwd,
                Dir::Bwd => self.cur_bwd,
            };
            match self.map.get_kv_at(idx) {
                Some((k, v)) => {
                    if self.range.contains(k.borrow()) {
                        match dir {
                            Dir::Fwd => {
                                self.cur_fwd += 1;
                            }
                            Dir::Bwd => {
                                self.cur_bwd -= 1;
                            }
                        }
                        Some((k, v))
                    } else {
                        None
                    }
                }
                None => None,
            }
        } else {
            None
        }
    }
}

impl<'a, R, K, V, Q> Iterator for Range<'a, R, K, V, Q>
where
    R: RangeBounds<Q>,
    K: Ord + Borrow<Q>,
    Q: Ord,
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.step(Dir::Fwd)
    }
}

impl<'a, R, K, V, Q> DoubleEndedIterator for Range<'a, R, K, V, Q>
where
    R: RangeBounds<Q>,
    K: Ord + Borrow<Q>,
    Q: Ord,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.step(Dir::Bwd)
    }
}

#[derive(Debug)]
pub struct ArrayMap<K, V> {
    keys: Vec<K>,
    vals: Vec<V>,
}

impl<K, V> ArrayMap<K, V>
where
    K: Ord,
{
    pub fn new() -> Self {
        Self { keys: vec![], vals: vec![] }
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&K, &V)> {
        self.keys.iter().zip(self.vals.iter())
    }

    pub fn keys(&self) -> impl DoubleEndedIterator<Item = &K> {
        self.keys.iter()
    }

    pub fn values(&self) -> impl DoubleEndedIterator<Item = &V> {
        self.vals.iter()
    }

    fn pos_for_bound<Q>(&self, bound: Bound<&Q>, dir: Dir) -> usize
    where
        Q: Ord,
        K: Borrow<Q>,
    {
        match bound {
            Bound::Unbounded => match dir {
                Dir::Fwd => 0,
                Dir::Bwd => self.keys.len().saturating_sub(1),
            },
            Bound::Excluded(k) => {
                match self.keys.binary_search_by(|key| key.borrow().cmp(k)) {
                    Err(i) => i,
                    Ok(i) => match dir {
                        Dir::Fwd => i + 1,
                        Dir::Bwd => i - 1,
                    },
                }
            }
            Bound::Included(k) => {
                match self.keys.binary_search_by(|key| key.borrow().cmp(k)) {
                    Ok(i) | Err(i) => i,
                }
            }
        }
    }

    pub fn range<R, Q>(&self, range: R) -> Range<R, K, V, Q>
    where
        R: RangeBounds<Q>,
        Q: Ord,
        K: Borrow<Q>,
    {
        let cur_fwd = self.pos_for_bound(range.start_bound(), Dir::Fwd);
        let cur_bwd = self.pos_for_bound(range.end_bound(), Dir::Bwd);
        Range { map: self, range, cur_fwd, cur_bwd, phantom: PhantomData }
    }

    pub fn reserve(&mut self, i: usize) {
        self.keys.reserve(i);
        self.vals.reserve(i);
    }

    pub fn shrink_to_fit(&mut self) {
        self.keys.shrink_to_fit();
        self.vals.shrink_to_fit();
    }

    pub fn get_at(&self, i: usize) -> Option<&V> {
        self.vals.get(i)
    }

    pub fn get_kv_at(&self, i: usize) -> Option<(&K, &V)> {
        self.keys.get(i).map(|k| (k, &self.vals[i]))
    }

    pub fn insert(&mut self, k: K, v: V) {
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

    pub fn remove<Q>(&mut self, k: &Q)
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

    pub fn get<Q>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        match self.keys.binary_search_by(|cur| cur.borrow().cmp(k)) {
            Ok(i) => Some(&self.vals[i]),
            Err(_) => None,
        }
    }

    pub fn first_key_value(&self) -> Option<(&K, &V)> {
        self.keys.get(0).map(|k| (k, &self.vals[0]))
    }

    pub fn last_key_value(&self) -> Option<(&K, &V)> {
        self.keys.last().map(|k| (k, self.vals.last().unwrap()))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::{rng, seq::SliceRandom, Rng};
    use std::collections::BTreeMap;

    #[test]
    fn test_insert_lookup_remove() {
        let mut model: BTreeMap<u32, u32> = BTreeMap::new();
        let mut index: ArrayMap<u32, u32> = ArrayMap::new();
        let mut rng = rng();
        for _ in 0..10_000 {
            let kv = rng.random();
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
        let mut index: ArrayMap<u32, u32> = ArrayMap::new();
        let mut keys = vec![];
        let mut rng = rng();
        for _ in 0..10_000 {
            let kv = rng.random();
            model.insert(kv, kv);
            index.insert(kv, kv);
            keys.push(kv);
        }
        keys.sort();
        while keys.len() > 0 {
            let mid = keys.len() / 2;
            let li = rng.random_range(0..=mid);
            let ui = rng.random_range(mid..keys.len());
            let mut le = false;
            let lower = if rng.random_bool(0.1) {
                Bound::Unbounded
            } else if rng.random_bool(0.5) {
                Bound::Included(keys.remove(li))
            } else {
                le = true;
                Bound::Excluded(keys.remove(li))
            };
            let upper = if rng.random_bool(0.1) {
                Bound::Unbounded
            } else {
                let c = if le || rng.random_bool(0.5) {
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
            let mut mrange = model.range((lower, upper));
            loop {
                let (mval, ival) = if rng.random_bool(0.1) {
                    match mrange.next_back() {
                        Some(mval) => (Some(mval), irange.next_back()),
                        None => break,
                    }
                } else {
                    match mrange.next() {
                        Some(mval) => (Some(mval), irange.next()),
                        None => break,
                    }
                };
                assert_eq!(mval, ival)
            }
            assert_eq!(irange.next(), None);
        }
    }
}

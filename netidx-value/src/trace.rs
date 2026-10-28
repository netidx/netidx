//! The shared allocations a value holds, for an embedder that must tell
//! which of them only its own values own (a cycle collector).
//!
//! [`Value::edges`] reports the allocations directly under a value, and
//! [`Edge::children`] those directly under an allocation, so a walk keeps
//! its own worklist and its depth costs no stack.

use crate::{
    Abstract, Map, Value,
    array::{ValArrayBase, ValArrayInner},
};
use immutable_chunkmap::map::NodeRef;
use triomphe::Arc;

/// A shared allocation reached from a value.
pub struct Edge<'a>(Kind<'a>);

enum Kind<'a> {
    Array(&'a Arc<ValArrayInner>),
    Base(&'a ValArrayBase),
    Error(&'a Arc<Value>),
    MapRoot(&'a Map, NodeRef<'a, Value, Value, 32>),
    MapNode(NodeRef<'a, Value, Value, 32>),
    Abstract(&'a Abstract),
}

impl<'a> Edge<'a> {
    /// The allocation's address: equal for every edge to it.
    pub fn id(&self) -> usize {
        match &self.0 {
            Kind::Array(a) => Arc::as_ptr(*a) as usize,
            Kind::Base(b) => b.identity(),
            Kind::Error(e) => Arc::as_ptr(*e) as usize,
            Kind::MapRoot(_, n) | Kind::MapNode(n) => n.identity(),
            Kind::Abstract(a) => a.identity(),
        }
    }

    /// How many owners the allocation has in all, or None where that
    /// cannot be known (a map's inner node), which a collector must read
    /// as held from outside.
    pub fn strong(&self) -> Option<usize> {
        match &self.0 {
            Kind::Array(a) => Some(Arc::strong_count(*a)),
            Kind::Base(b) => Some(b.strong_count()),
            Kind::Error(e) => Some(Arc::strong_count(*e)),
            Kind::MapRoot(m, _) => Some(m.strong_count()),
            Kind::MapNode(_) => None,
            Kind::Abstract(a) => Some(a.strong_count()),
        }
    }

    /// The abstract value this allocation is: its payload is the
    /// embedder's to walk, so [`Edge::children`] reports nothing under it.
    pub fn as_abstract(&self) -> Option<&'a Abstract> {
        match &self.0 {
            Kind::Abstract(a) => Some(*a),
            _ => None,
        }
    }

    /// The allocations directly under this one.
    pub fn children(&self, mut f: impl FnMut(Edge<'a>)) {
        match &self.0 {
            Kind::Array(a) => match &***a {
                ValArrayInner::Base(b) => f(Edge(Kind::Base(b))),
                ValArrayInner::Slice(s) => f(Edge(Kind::Base(s.base()))),
            },
            Kind::Base(b) => b.iter().for_each(|v| v.edges(&mut f)),
            Kind::Error(e) => e.edges(f),
            Kind::MapRoot(_, n) | Kind::MapNode(n) => {
                for (k, v) in n.pairs() {
                    k.edges(&mut f);
                    v.edges(&mut f);
                }
                n.left()
                    .into_iter()
                    .chain(n.right())
                    .for_each(|n| f(Edge(Kind::MapNode(n))))
            }
            Kind::Abstract(_) => (),
        }
    }
}

impl Value {
    /// The shared allocations directly under this value.
    pub fn edges<'a>(&'a self, mut f: impl FnMut(Edge<'a>)) {
        match self {
            Value::Array(a) => f(Edge(Kind::Array(a.inner()))),
            Value::Error(e) => f(Edge(Kind::Error(e.as_arc()))),
            Value::Map(m) => {
                if let Some(root) = m.root() {
                    f(Edge(Kind::MapRoot(m, root)))
                }
            }
            Value::Abstract(a) => f(Edge(Kind::Abstract(a))),
            _ => (),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{ValArray, Value};

    fn edges(v: &Value) -> Vec<(usize, Option<usize>)> {
        let mut out = vec![];
        v.edges(|e| out.push((e.id(), e.strong())));
        out
    }

    #[test]
    fn edges_count_every_owner() {
        let inner = Value::Array(ValArray::from_iter_exact(
            [1i64, 2].into_iter().map(Value::from),
        ));
        let outer = Value::Array(ValArray::from_iter_exact(
            [inner.clone(), inner.clone()].into_iter(),
        ));
        let [(a, Some(1))] = edges(&outer)[..] else { panic!("{:?}", edges(&outer)) };
        let mut bases = vec![];
        outer.edges(|e| e.children(|b| bases.push((b.id(), b.strong()))));
        assert_eq!(bases.len(), 1);
        let mut elements = vec![];
        outer.edges(|e| {
            e.children(|b| b.children(|x| elements.push((x.id(), x.strong()))))
        });
        // the two elements are one allocation, held by them and by `inner`
        assert_eq!(elements.len(), 2);
        assert_eq!(elements[0], elements[1]);
        assert_eq!(elements[0].1, Some(3));
        assert_ne!(elements[0].0, a);
        assert!(edges(&Value::I64(3)).is_empty());
    }
}

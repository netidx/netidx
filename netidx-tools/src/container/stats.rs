use anyhow::Result;
use netidx::{
    path::Path,
    publisher::{Publisher, UpdateBatch, Val, Value, DefaultHandle},
};
use std::collections::BTreeMap;

pub(super) struct Stats {
    publisher: Publisher,
    base_path: Path,
    roots: Vec<Val>,
    locked: Vec<(Val, Val)>,
}

impl Stats {
    pub(super) fn new(publisher: Publisher, base_path: Path) -> Self {
        Stats { publisher, base_path, roots: Vec::new(), locked: Vec::new() }
    }

    pub(super) fn set_roots(
        &mut self,
        batch: &mut UpdateBatch,
        roots: &BTreeMap<Path, DefaultHandle>,
    ) -> Result<()> {
        while self.roots.len() > roots.len() {
            self.roots.pop();
        }
        while self.roots.len() < roots.len() {
            let p = self.base_path.append(&format!("{0:6}", self.roots.len()));
            self.roots.push(self.publisher.publish(p, Value::Null)?);
        }
        for (path, p) in roots.keys().zip(self.roots.iter()) {
            p.update(batch, String::from(path.as_ref()).into());
        }
        Ok(())
    }

    pub(super) fn set_locked(
        &mut self,
        batch: &mut UpdateBatch,
        locked: &BTreeMap<Path, bool>,
    ) -> Result<()> {
        while self.locked.len() > locked.len() {
            self.locked.pop();
        }
        while self.locked.len() < locked.len() {
            let p = self.base_path.append(&format!("{0:6}", self.locked.len()));
            let pk = self.publisher.publish(p.append("path"), Value::Null)?;
            let pv = self.publisher.publish(p.append("locked"), Value::Null)?;
            self.locked.push((pk, pv));
        }
        for ((path, locked), (pv, lv)) in locked.iter().zip(self.locked.iter()) {
            pv.update(batch, String::from(path.as_ref()).into());
            lv.update(batch, locked.into());
        }
        Ok(())
    }
}

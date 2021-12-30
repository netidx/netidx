use anyhow::{bail, Result};

pub(crate) struct Mapper;

impl Mapper {
    pub(crate) fn new() -> Result<Mapper> {
        Ok(Mapper)
    }

    pub(crate) fn groups(&mut self, _user: &str) -> Result<Vec<String>> {
        bail!("group listing is not implemented on windows")
    }
}

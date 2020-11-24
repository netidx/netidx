//! Unix style file glob evaluation against the resolver
//!
//! e.g.
//! `/solar/{stats,config}/*` -> everything under /solar/stats, and /solar/config
//! `/**/foo/bar/* -> every subtree with foo/bar in it and one level under it
//! `/**/foo/bar/**` -> every subtree with foo/bar anywhere in it
//! ``
use anyhow::Result;
use globset::{self, GlobBuilder};
use netidx::{path::Path, resolver::ResolverRead, utils};

#[derive(Debug)]
enum Scope {
    OneLevel,
    Subtree,
}

#[derive(Debug)]
pub struct Glob {
    raw: Path,
    base: Path,
    globs: Vec<(Scope, globset::Glob)>,
}

impl Glob {
    pub fn new(raw: Path) -> Result<Glob> {
        if !Path::is_absolute(&raw) {
            bail!("glob paths must be absolute")
        }
        let (lvl, base) =
            Path::dirnames(&raw).enumerate().fold((0, "/"), |cur, (lvl, p)| {
                let mut s = p;
                let is_glob = loop {
                    if s.is_empty() {
                        break false;
                    } else {
                        match s.find("?*{[") {
                            None => break false,
                            Some(i) => {
                                if s.is_escaped(s, '\\', i) {
                                    s = &s[i + 1..];
                                } else {
                                    break true;
                                }
                            }
                        }
                    }
                };
                if !is_glob {
                    (lvl, p)
                } else {
                    cur
                }
            });
        let base = Path::from(String::from(base));
        let globs = Path::dirnames(&raw)
            .skip(lvl)
            .map(|p| {
                let glob = GlobBuilder::new(p).backslash_escape(true).build()?;
                let scope = if Path::basename(p) == "**" {
                    Scope::Subtree
                } else {
                    Scope::OneLevel
                };
                Ok((scope, glob))
            })
            .collect::<Result<Vec<globset::Glob>>>()?;
        Ok(Glob { raw, base, globs })
    }

    /// Evaluate the glob using the specified resolver, return a list
    /// of matching paths.
    pub async fn eval(&self, resolver: &ResolverRead) -> Result<Vec<Path>> {
        let mut matches = Vec::new();
        let base = resolver.list(self.base.clone()).await?;
    }
}

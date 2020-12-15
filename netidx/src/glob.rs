use crate::{
    auth::Scope,
    pack::{Pack, PackError},
    path::Path,
    pool::{Pool, Pooled},
    utils,
};
use anyhow::Result;
use bytes::{Buf, BufMut};
use globset;
use std::{ops::Deref, result, cmp::{PartialEq, Eq}, sync::Arc};

/// Unix style globs for matching paths in the resolver. All common
/// unix globing features are supported.
/// * ? matches any character except the path separator
/// * \* matches zero or more characters, but not the path separator
/// * \** recursively matches containers. It's only legal uses are
/// /**/foo, /foo/**/bar, and /foo/bar/**, which match respectively,
/// any path ending in foo, any path starting with /foo and ending in
/// bar, and any path starting with /foo/bar.
/// * {a, b}, matches a or b where a and b are glob patterns, {} can't be nested however.
/// * [ab], [!ab], matches respectively the char a or b, and any char but a or b.
/// * any of the above metacharacters can be escaped with a \, a
/// literal \ may be produced with \\.
///
/// e.g.
/// `/solar/{stats,settings}/*` -> all leaf paths under /solar/stats or /solar/settings.
/// `/s*/s*/**` -> any path who's first two levels start with s.
/// `/**/?` -> any path who's final component is a single character
/// `/marketdata/{IBM,MSFT,AMZN}/last`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Glob {
    raw: Path,
    base: String,
    scope: Scope,
    glob: globset::Glob,
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
                                if utils::is_escaped(s, '\\', i) {
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
        let base = Path::to_btnf(base).into_owned();
        let scope =
            if Path::dirnames(&raw).skip(lvl).any(|p| Path::basename(p) == Some("**")) {
                Scope::Subtree
            } else {
                Scope::Finite(lvl)
            };
        let glob = globset::Glob::new(&*raw)?;
        Ok(Glob { raw, base, scope, glob })
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    pub fn glob(&self) -> &globset::Glob {
        &self.glob
    }

    pub fn into_glob(self) -> globset::Glob {
        self.glob
    }
}

impl Pack for Glob {
    fn len(&self) -> usize {
        <Path as Pack>::len(&self.raw)
    }

    fn encode(&self, buf: &mut impl BufMut) -> result::Result<(), PackError> {
        <Path as Pack>::encode(&self.raw, buf)
    }

    fn decode(buf: &mut impl Buf) -> result::Result<Self, PackError> {
        Glob::new(<Path as Pack>::decode(buf)?).map_err(|_| PackError::InvalidFormat)
    }
}

#[derive(Debug)]
struct GlobSetInner {
    raw: Pooled<Vec<Glob>>,
    glob: globset::GlobSet,
}

#[derive(Debug, Clone)]
pub struct GlobSet(Arc<GlobSetInner>);

impl PartialEq for GlobSet {
    fn eq(&self, other: &Self) -> bool {
        &self.0.raw == &other.0.raw
    }
}

impl Eq for GlobSet {}

impl GlobSet {
    fn new(globs: impl IntoIterator<Item = Glob>) -> Result<GlobSet> {
        lazy_static! {
            static ref GLOB: Pool<Vec<Glob>> = Pool::new(10, 100);
        }
        let mut builder = globset::GlobSetBuilder::new();
        let mut raw = GLOB.take();
        for glob in globs {
            builder.add(glob.glob.clone());
            raw.push(glob);
        }
        Ok(GlobSet(Arc::new(GlobSetInner { raw, glob: builder.build()? })))
    }

    fn is_match(&self, path: &Path) -> bool {
        self.0.glob.is_match(path.as_ref())
    }
}

impl Deref for GlobSet {
    type Target = Vec<Glob>;

    fn deref(&self) -> &Self::Target {
        &*self.0.raw
    }
}

impl Pack for GlobSet {
    fn len(&self) -> usize {
        <Pooled<Vec<Glob>> as Pack>::len(&self.0.raw)
    }

    fn encode(&self, buf: &mut impl BufMut) -> result::Result<(), PackError> {
        <Pooled<Vec<Glob>> as Pack>::encode(&self.0.raw, buf)
    }

    fn decode(buf: &mut impl Buf) -> result::Result<Self, PackError> {
        let raw = <Pooled<Vec<Glob>> as Pack>::decode(buf)?;
        let mut builder = globset::GlobSetBuilder::new();
        for glob in raw.iter() {
            builder.add(glob.glob.clone());
        }
        let glob = builder.build().map_err(|_| PackError::InvalidFormat)?;
        Ok(GlobSet(Arc::new(GlobSetInner { raw, glob })))
    }
}

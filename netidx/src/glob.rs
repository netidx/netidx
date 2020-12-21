use crate::{
    auth::Scope,
    chars::Chars,
    pack::{Pack, PackError},
    path::Path,
    pool::{Pool, Pooled},
    utils,
};
use anyhow::Result;
use bytes::{Buf, BufMut};
use globset;
use std::{
    cmp::{Eq, PartialEq},
    ops::Deref,
    result,
    sync::Arc,
};

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
    raw: Chars,
    base: Path,
    scope: Scope,
    glob: globset::Glob,
}

impl Glob {
    pub fn new(raw: Chars) -> Result<Glob> {
        if !Path::is_absolute(&raw) {
            bail!("glob paths must be absolute")
        }
        let base = {
            let mut cur = "/";
            let mut iter = Path::dirnames(&raw);
            loop {
                match iter.next() {
                    None => break cur,
                    Some(p) => {
                        let mut s = p;
                        let is_glob = loop {
                            if s.is_empty() {
                                break false;
                            } else {
                                match s.find(&['?', '*', '{', '['][..]) {
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
                        if is_glob {
                            break cur;
                        } else {
                            cur = p;
                        }
                    }
                }
            }
        };
        let lvl = Path::levels(base);
        let base = Path::from(Arc::from(base));
        let scope =
            if Path::dirnames(&raw).skip(lvl).any(|p| Path::basename(p) == Some("**")) {
                Scope::Subtree
            } else {
                Scope::Finite(Path::levels(&raw))
            };
        let glob = globset::Glob::new(&*raw)?;
        Ok(Glob { raw, base, scope, glob })
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub(crate) fn scope(&self) -> &Scope {
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
    fn encoded_len(&self) -> usize {
        <Chars as Pack>::encoded_len(&self.raw)
    }

    fn encode(&self, buf: &mut impl BufMut) -> result::Result<(), PackError> {
        <Chars as Pack>::encode(&self.raw, buf)
    }

    fn decode(buf: &mut impl Buf) -> result::Result<Self, PackError> {
        Glob::new(<Chars as Pack>::decode(buf)?).map_err(|_| PackError::InvalidFormat)
    }
}

#[derive(Debug)]
struct GlobSetInner {
    raw: Pooled<Vec<Glob>>,
    published_only: bool,
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
    /// create a new globset from the specified globs. if
    /// published_only is true, then the globset will only match
    /// published paths, otherwise it will match both structural and
    /// published paths.
    pub fn new(
        published_only: bool,
        globs: impl IntoIterator<Item = Glob>,
    ) -> Result<GlobSet> {
        lazy_static! {
            static ref GLOB: Pool<Vec<Glob>> = Pool::new(10, 100);
        }
        let mut builder = globset::GlobSetBuilder::new();
        let mut raw = GLOB.take();
        for glob in globs {
            builder.add(glob.glob.clone());
            raw.push(glob);
        }
        raw.sort_unstable_by(|g0, g1| g0.base().cmp(g1.base()));
        Ok(GlobSet(Arc::new(GlobSetInner {
            raw,
            published_only,
            glob: builder.build()?,
        })))
    }

    pub fn is_match(&self, path: &Path) -> bool {
        self.0.glob.is_match(path.as_ref())
    }

    pub fn published_only(&self) -> bool {
        self.0.published_only
    }
}

impl Deref for GlobSet {
    type Target = Vec<Glob>;

    fn deref(&self) -> &Self::Target {
        &*self.0.raw
    }
}

impl Pack for GlobSet {
    fn encoded_len(&self) -> usize {
        <bool as Pack>::encoded_len(&self.0.published_only)
            + <Pooled<Vec<Glob>> as Pack>::encoded_len(&self.0.raw)
    }

    fn encode(&self, buf: &mut impl BufMut) -> result::Result<(), PackError> {
        <bool as Pack>::encode(&self.0.published_only, buf)?;
        <Pooled<Vec<Glob>> as Pack>::encode(&self.0.raw, buf)
    }

    fn decode(buf: &mut impl Buf) -> result::Result<Self, PackError> {
        let published_only = <bool as Pack>::decode(buf)?;
        let mut raw = <Pooled<Vec<Glob>> as Pack>::decode(buf)?;
        let mut builder = globset::GlobSetBuilder::new();
        for glob in raw.iter() {
            builder.add(glob.glob.clone());
        }
        raw.sort_unstable_by(|g0, g1| g0.base().cmp(g1.base()));
        let glob = builder.build().map_err(|_| PackError::InvalidFormat)?;
        Ok(GlobSet(Arc::new(GlobSetInner { raw, published_only, glob })))
    }
}

use anyhow::Result;
use arcstr::ArcStr;
use bytes::{Buf, BufMut};
use globset;
use netidx_core::{
    pack::{Pack, PackError},
    path::Path,
    pool::{Pool, Pooled},
};
use smallvec::SmallVec;
use std::{
    cmp::{Eq, PartialEq},
    ops::Deref,
    result,
    sync::{Arc, LazyLock},
};

use crate::value::{FromValue, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Subtree,
    Finite(usize),
}

impl Scope {
    pub fn contains(&self, levels: usize) -> bool {
        match self {
            Scope::Subtree => true,
            Scope::Finite(n) => levels <= *n,
        }
    }
}

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
    raw: ArcStr,
    base: Path,
    scope: Scope,
    glob: globset::Glob,
}

impl Glob {
    /// return the longest plain string before the first glob char
    pub fn plain(&self) -> &str {
        match Self::first_glob_char(&*self.raw) {
            None => &*self.raw,
            Some(i) => &self.raw[0..i],
        }
    }

    /// returns true if the specified string contains any non escaped
    /// glob meta chars.
    pub fn is_glob(s: &str) -> bool {
        Self::first_glob_char(s).is_some()
    }

    /// returns the index of the first glob special char or None if raw is a plain string
    pub fn first_glob_char(mut s: &str) -> Option<usize> {
        loop {
            if s.is_empty() {
                break None;
            } else {
                match s.find(&['?', '*', '{', '['][..]) {
                    None => break None,
                    Some(i) => {
                        if escaping::is_escaped(s, '\\', i) {
                            s = &s[i + 1..];
                        } else {
                            break Some(i);
                        }
                    }
                }
            }
        }
    }

    pub fn new(raw: ArcStr) -> Result<Glob> {
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
                        if Glob::is_glob(p) {
                            break cur;
                        } else {
                            cur = p;
                        }
                    }
                }
            }
        };
        let lvl = Path::levels(base);
        let base = Path::from(ArcStr::from(base));
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

    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    pub fn glob(&self) -> &globset::Glob {
        &self.glob
    }

    pub fn into_glob(self) -> globset::Glob {
        self.glob
    }

    pub fn raw(&self) -> &ArcStr {
        &self.raw
    }
}

impl Pack for Glob {
    fn encoded_len(&self) -> usize {
        Pack::encoded_len(&self.raw)
    }

    fn encode(&self, buf: &mut impl BufMut) -> result::Result<(), PackError> {
        Pack::encode(&self.raw, buf)
    }

    fn decode(buf: &mut impl Buf) -> result::Result<Self, PackError> {
        Glob::new(Pack::decode(buf)?).map_err(|_| PackError::InvalidFormat)
    }
}

impl Into<Value> for Glob {
    fn into(self) -> Value {
        self.raw.into()
    }
}

impl FromValue for Glob {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::String(s) => Self::new(s),
            x => bail!("Glob::from_value expected String got {x:?}"),
        }
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

impl Into<Value> for GlobSet {
    fn into(self) -> Value {
        (self.0.published_only, self.0.raw.clone()).into()
    }
}

impl FromValue for GlobSet {
    fn from_value(v: Value) -> Result<Self> {
        let (published_only, globs): (bool, Vec<Glob>) = FromValue::from_value(v)?;
        Self::new(published_only, globs)
    }
}

impl<'a> TryFrom<&'a [String]> for GlobSet {
    type Error = anyhow::Error;

    fn try_from(value: &[String]) -> result::Result<Self, Self::Error> {
        let v: SmallVec<[Glob; 8]> =
            value.iter().map(|c| Glob::new(ArcStr::from(&**c))).collect::<Result<_>>()?;
        GlobSet::new(false, v)
    }
}

impl<'a> TryFrom<&'a [ArcStr]> for GlobSet {
    type Error = anyhow::Error;

    fn try_from(value: &[ArcStr]) -> result::Result<Self, Self::Error> {
        let v: SmallVec<[Glob; 8]> = value
            .iter()
            .map(|c| Glob::new(ArcStr::from(c.clone())))
            .collect::<Result<_>>()?;
        GlobSet::new(false, v)
    }
}

impl TryFrom<Vec<String>> for GlobSet {
    type Error = anyhow::Error;

    fn try_from(value: Vec<String>) -> result::Result<Self, Self::Error> {
        let v: SmallVec<[Glob; 8]> = value
            .into_iter()
            .map(|c| Glob::new(ArcStr::from(&*c)))
            .collect::<Result<_>>()?;
        GlobSet::new(false, v)
    }
}

impl TryFrom<Vec<ArcStr>> for GlobSet {
    type Error = anyhow::Error;

    fn try_from(value: Vec<ArcStr>) -> result::Result<Self, Self::Error> {
        let v: SmallVec<[Glob; 8]> = value
            .into_iter()
            .map(|c| Glob::new(ArcStr::from(c.clone())))
            .collect::<Result<_>>()?;
        GlobSet::new(false, v)
    }
}

impl GlobSet {
    /// create a new globset from the specified globs. if
    /// published_only is true, then the globset will only match
    /// published paths, otherwise it will match both structural and
    /// published paths.
    pub fn new(
        published_only: bool,
        globs: impl IntoIterator<Item = Glob>,
    ) -> Result<GlobSet> {
        static GLOB: LazyLock<Pool<Vec<Glob>>> = LazyLock::new(|| Pool::new(10, 100));
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

    /// return the raw glob strings
    pub fn raw(&self) -> Vec<ArcStr> {
        self.0.raw.iter().map(|g| g.raw.clone()).collect()
    }

    /// disjoint globsets will never both match a given path. However
    /// non disjoint globsets might not match the same paths. So in
    /// other words this will only return turn if the two globsets
    /// definitely will not match any of the same paths. It is
    /// possible that this returns true and the globsets are in fact
    /// disjoint.
    pub fn disjoint(&self, other: &Self) -> bool {
        for g0 in self.0.raw.iter() {
            let plain0 = g0.plain();
            for g1 in other.0.raw.iter() {
                let plain1 = g1.plain();
                if plain0.starts_with(plain1)
                    || plain1.starts_with(plain0)
                    || plain0 == plain1
                {
                    return false;
                }
            }
        }
        return true;
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

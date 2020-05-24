use crate::{
    chars::Chars,
    pack::{Pack, PackError},
    utils,
};
use std::{
    borrow::Borrow,
    borrow::Cow,
    cmp::{Eq, Ord, PartialEq, PartialOrd},
    convert::{AsRef, From},
    fmt,
    iter::Iterator,
    ops::Deref,
    result::Result,
    str::FromStr,
};

pub static ESC: char = '\\';
pub static SEP: char = '/';

/// A path in the namespace. Paths are immutable and reference
/// counted.  Path components are seperated by /, which may be escaped
/// with \. / and \ are the only special characters in path, any other
/// unicode character may be used. Path lengths are not limited on the
/// local machine, but may be restricted by maximum message size on
/// the wire.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Path(Chars);

impl Pack for Path {
    fn len(&self) -> usize {
        <Chars as Pack>::len(&self.0)
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<(), PackError> {
        Ok(<Chars as Pack>::encode(&self.0, buf)?)
    }

    fn decode(buf: &mut bytes::BytesMut) -> Result<Self, PackError> {
        Ok(Path(<Chars as Pack>::decode(buf)?))
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl AsRef<str> for Path {
    fn as_ref(&self) -> &str {
        &*self.0
    }
}

impl Borrow<str> for Path {
    fn borrow(&self) -> &str {
        &*self.0
    }
}

impl Deref for Path {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Chars> for Path {
    fn from(c: Chars) -> Path {
        if is_canonical(&c) {
            Path(c)
        } else {
            Path(Chars::from(canonize(&c)))
        }
    }
}

impl From<String> for Path {
    fn from(s: String) -> Path {
        if is_canonical(&s) {
            Path(Chars::from(s))
        } else {
            Path(Chars::from(canonize(&s)))
        }
    }
}

impl From<&'static str> for Path {
    fn from(s: &'static str) -> Path {
        if is_canonical(s) {
            Path(Chars::from(s))
        } else {
            Path(Chars::from(canonize(s)))
        }
    }
}

impl<'a> From<&'a String> for Path {
    fn from(s: &String) -> Path {
        if is_canonical(s.as_str()) {
            Path(Chars::from(s.clone()))
        } else {
            Path(Chars::from(canonize(s.as_str())))
        }
    }
}

impl FromStr for Path {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Path(Chars::from(String::from(s))))
    }
}

fn is_canonical(s: &str) -> bool {
    for _ in Path::parts(s).filter(|p| *p == "") {
        return false;
    }
    true
}

fn canonize(s: &str) -> String {
    let mut res = String::with_capacity(s.len());
    if s.len() > 0 {
        if s.starts_with(SEP) {
            res.push(SEP)
        }
        let mut first = true;
        for p in Path::parts(s).filter(|p| *p != "") {
            if first {
                first = false;
            } else {
                res.push(SEP)
            }
            res.push_str(p);
        }
    }
    res
}

fn is_escaped(s: &str, i: usize) -> bool {
    let b = s.as_bytes();
    !s.is_char_boundary(i)
        || (b[i] == (SEP as u8) && {
            let mut res = false;
            for j in (0..i).rev() {
                if s.is_char_boundary(j) && b[j] == (ESC as u8) {
                    res = !res;
                } else {
                    break;
                }
            }
            res
        })
}

enum BaseNames<'a> {
    Root(bool),
    Path { cur: &'a str, all: &'a str, base: usize },
}

impl<'a> Iterator for BaseNames<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BaseNames::Root(false) => None,
            BaseNames::Root(true) => {
                *self = BaseNames::Root(false);
                Some("/")
            }
            BaseNames::Path { ref mut cur, ref all, ref mut base } => {
                if *base == all.len() {
                    None
                } else {
                    match Path::find_sep(cur) {
                        None => {
                            *base = all.len();
                            Some(all)
                        }
                        Some(p) => {
                            *base += p + 1;
                            *cur = &all[*base..];
                            Some(&all[0..*base - 1])
                        }
                    }
                }
            }
        }
    }
}

impl Path {
    pub fn as_chars(self) -> Chars {
        self.0
    }

    /// returns /
    pub fn root() -> Path {
        Path::from("/")
    }

    /// returns true if the path starts with /, false otherwise
    pub fn is_absolute<T: AsRef<str> + ?Sized>(p: &T) -> bool {
        p.as_ref().starts_with(SEP)
    }

    /// This will escape the path seperator and the escape character
    /// in a path part. If you want to be sure that e.g. `append` will
    /// only append 1 level, then you should call this function on
    /// your part before appending it.
    ///
    /// # Examples
    /// ```
    /// use netidx::path::Path;
    /// assert_eq!("foo\\/bar", &*Path::escape("foo/bar"));
    /// assert_eq!("\\\\hello world", &*Path::escape("\\hello world"));
    /// ```
    pub fn escape<T: AsRef<str> + ?Sized>(s: &T) -> Cow<str> {
        let s = s.as_ref();
        if s.find(|c: char| c == SEP || c == ESC).is_none() {
            Cow::Borrowed(s.as_ref())
        } else {
            let mut out = String::with_capacity(s.len());
            for c in s.chars() {
                if c == SEP {
                    out.push(ESC);
                    out.push(c);
                } else if c == ESC {
                    out.push(ESC);
                    out.push(c);
                } else {
                    out.push(c);
                }
            }
            Cow::Owned(out)
        }
    }

    /// return a new path with the specified string appended as a new
    /// part separated by the pathsep char.
    ///
    /// # Examples
    /// ```
    /// use netidx::path::Path;
    /// let p = Path::root().append("bar").append("baz");
    /// assert_eq!(&*p, "/bar/baz");
    ///
    /// let p = Path::root().append("/bar").append("//baz//////foo/");
    /// assert_eq!(&*p, "/bar/baz/foo");
    /// ```
    pub fn append<T: AsRef<str> + ?Sized>(&self, other: &T) -> Self {
        let other = other.as_ref();
        if other.len() == 0 {
            self.clone()
        } else {
            let mut res = String::with_capacity(self.as_ref().len() + other.len());
            res.push_str(self.as_ref());
            res.push(SEP);
            res.push_str(other);
            Path::from(res)
        }
    }

    /// return an iterator over the parts of the path. The path
    /// separator may be escaped with \. and a literal \ may be
    /// represented as \\.
    ///
    /// # Examples
    /// ```
    /// use netidx::path::Path;
    /// let p = Path::from("/foo/bar/baz");
    /// assert_eq!(Path::parts(&p).collect::<Vec<_>>(), vec!["foo", "bar", "baz"]);
    ///
    /// let p = Path::from(r"/foo\/bar/baz");
    /// assert_eq!(Path::parts(&p).collect::<Vec<_>>(), vec![r"foo\/bar", "baz"]);
    ///
    /// let p = Path::from(r"/foo\\/bar/baz");
    /// assert_eq!(Path::parts(&p).collect::<Vec<_>>(), vec![r"foo\\", "bar", "baz"]);
    ///
    /// let p = Path::from(r"/foo\\\/bar/baz");
    /// assert_eq!(Path::parts(&p).collect::<Vec<_>>(), vec![r"foo\\\/bar", "baz"]);
    /// ```
    pub fn parts<T: AsRef<str> + ?Sized>(s: &T) -> impl Iterator<Item = &str> {
        let s = s.as_ref();
        let skip = if s == "/" {
            2
        } else if s.starts_with("/") {
            1
        } else {
            0
        };
        utils::split_escaped(s, ESC, SEP).skip(skip)
    }

    /// Return an iterator over all the basenames in the path starting
    /// from the root and ending with the entire path.
    ///
    /// # Examples
    /// ```
    /// use netidx::path::Path;
    /// let p = Path::from("/some/path/ending/in/foo");
    /// let mut bn = Path::basenames(&p);
    /// assert_eq!(bn.next(), Some("/"));
    /// assert_eq!(bn.next(), Some("/some"));
    /// assert_eq!(bn.next(), Some("/some/path"));
    /// assert_eq!(bn.next(), Some("/some/path/ending"));
    /// assert_eq!(bn.next(), Some("/some/path/ending/in"));
    /// assert_eq!(bn.next(), Some("/some/path/ending/in/foo"));
    /// assert_eq!(bn.next(), None);
    /// ```
    pub fn basenames<T: AsRef<str> + ?Sized>(s: &T) -> impl Iterator<Item = &str> {
        let s = s.as_ref();
        if s == "/" {
            BaseNames::Root(true)
        } else {
            BaseNames::Path { cur: s, all: s, base: 1 }
        }
    }

    /// Return the number of levels in the path.
    ///
    /// # Examples
    /// ```
    /// use netidx::path::Path;
    /// let p = Path::from("/foo/bar/baz");
    /// assert_eq!(Path::levels(&p), 3);
    /// ```
    pub fn levels<T: AsRef<str> + ?Sized>(s: &T) -> usize {
        let mut p = 0;
        for _ in Path::parts(s) {
            p += 1
        }
        p
    }

    /// return the path without the last part, or return None if the
    /// path is empty or /.
    ///
    /// # Examples
    /// ```
    /// use netidx::path::Path;
    /// let p = Path::from("/foo/bar/baz");
    /// assert_eq!(Path::dirname(&p), Some("/foo/bar"));
    ///
    /// let p = Path::root();
    /// assert_eq!(Path::dirname(&p), None);
    ///
    /// let p = Path::from("/foo");
    /// assert_eq!(Path::dirname(&p), None);
    /// ```
    pub fn dirname<T: AsRef<str> + ?Sized>(s: &T) -> Option<&str> {
        let s = s.as_ref();
        Path::rfind_sep(s).and_then(|i| if i == 0 { None } else { Some(&s[0..i]) })
    }

    /// return the last part of the path, or return None if the path
    /// is empty.
    ///
    /// # Examples
    /// ```
    /// use netidx::path::Path;
    /// let p = Path::from("/foo/bar/baz");
    /// assert_eq!(Path::basename(&p), Some("baz"));
    ///
    /// let p = Path::from("foo");
    /// assert_eq!(Path::basename(&p), Some("foo"));
    ///
    /// let p = Path::from("foo/bar");
    /// assert_eq!(Path::basename(&p), Some("bar"));
    ///
    /// let p = Path::from("");
    /// assert_eq!(Path::basename(&p), None);
    ///
    /// let p = Path::from("/");
    /// assert_eq!(Path::basename(&p), None);
    /// ```
    pub fn basename<T: AsRef<str> + ?Sized>(s: &T) -> Option<&str> {
        let s = s.as_ref();
        match Path::rfind_sep(s) {
            None => {
                if s.len() > 0 {
                    Some(s)
                } else {
                    None
                }
            }
            Some(i) => {
                if s.len() <= 1 {
                    None
                } else {
                    Some(&s[i + 1..s.len()])
                }
            }
        }
    }

    fn find_sep_int<F: Fn(&str) -> Option<usize>>(mut s: &str, f: F) -> Option<usize> {
        if s.len() == 0 {
            None
        } else {
            loop {
                match f(s) {
                    None => return None,
                    Some(i) => {
                        if !is_escaped(s, i) {
                            return Some(i);
                        } else {
                            s = &s[0..i];
                        }
                    }
                }
            }
        }
    }

    /// return the position of the last path separator in the path, or
    /// None if there isn't one.
    ///
    /// # Examples
    /// ```
    /// use netidx::path::Path;
    /// let p = Path::from("/foo/bar/baz");
    /// assert_eq!(Path::rfind_sep(&p), Some(8));
    ///
    /// let p = Path::from("");
    /// assert_eq!(Path::rfind_sep(&p), None);
    /// ```
    pub fn rfind_sep<T: AsRef<str> + ?Sized>(s: &T) -> Option<usize> {
        let s = s.as_ref();
        Path::find_sep_int(s, |s| s.rfind(SEP))
    }

    /// return the position of the first path separator in the path, or
    /// None if there isn't one.
    ///
    /// # Examples
    /// ```
    /// use netidx::path::Path;
    /// let p = Path::from("foo/bar/baz");
    /// assert_eq!(Path::find_sep(&p), Some(3));
    ///
    /// let p = Path::from("");
    /// assert_eq!(Path::find_sep(&p), None);
    /// ```
    pub fn find_sep<T: AsRef<str> + ?Sized>(s: &T) -> Option<usize> {
        let s = s.as_ref();
        Path::find_sep_int(s, |s| s.find(SEP))
    }
}

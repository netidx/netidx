use arccstr::ArcCStr;
use std::{
    borrow::Borrow, ops::Deref, convert::{AsRef, From},
    ffi::CStr, str::from_utf8_unchecked
};

pub static ESC: char = '\\';
pub static SEP: char = '/';

// invariant: The contents of the ArcCstr are always valid UTF8.
// invariant: The only valid UTF8 code point containing NULL is the code point for NULL
// null characters aren't really useful in path anyway, so it's not a
// big problem to disallow them (filter them out).

/// A path in the json-pubsub namespace. This is stored as an ArcCstr,
/// so clone is virtually free, and the representation is very
/// compact. The only restiction is that NULL characters are not
/// allowed in the path, they will be removed on creation. Since the
/// only inputs are valid utf8, it can be derefed to a str at no cost.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Path(ArcCStr);

impl Borrow<str> for Path {
    fn borrow(&self) -> &str { &*self }
}

impl Deref for Path {
    type Target = str;

    fn deref(&self) -> &str {
        unsafe { from_utf8_unchecked(self.0.to_bytes()) }
    }
}

impl From<String> for Path {
    fn from(s: String) -> Path {
        Path(ArcCStr::from(
            CStr::from_bytes_with_nul(
                canonize(&s).as_bytes()
            ).unwrap()
        ))
    }
}

impl<'a> From<&'a str> for Path {
    fn from(s: &str) -> Path {
        Path(ArcCStr::from(
            CStr::from_bytes_with_nul(
                canonize(s).as_bytes()
            ).unwrap()
        ))
    }
}

impl<'a> From<&'a String> for Path {
    fn from(s: &String) -> Path {        
        Path(ArcCStr::from(
            CStr::from_bytes_with_nul(
                canonize(&*s).as_bytes()
            ).unwrap()
        ))
    }
}

fn canonize(s: &str) -> String {
    let mut res = s.replace("\0", "");
    if s.len() > 0 {
        if s.starts_with(SEP) { res.push(SEP) }
        let mut first = true;
        for p in Path::parts(s).filter(|p| *p != "") {
            if first { first = false; }
            else { res.push(SEP) }
            res.push_str(p);
        }
    }
    res.push('\0');
    res
}

fn is_escaped(s: &str, i: usize) -> bool {
    let b = s.as_bytes();
    !s.is_char_boundary(i) || (b[i] == (SEP as u8) && {
        let mut res = false;
        for j in (0..i).rev() {
            if s.is_char_boundary(j) && b[j] == (ESC as u8) { res = !res; }
            else { break }
        }
        res
    })
}

impl Path {
    /// returns /
    pub fn root() -> Path { Path::from("/") }

    /// returns true if the path starts with /, false otherwise
    pub fn is_absolute(&self) -> bool { self.starts_with(SEP) }

    /// return a new path with the specified string appended as a new
    /// part separated by the pathsep char.
    ///
    /// # Examples
    /// ```
    /// let p = Path::root().append("bar").append("baz");
    /// assert_eq!(&p, "/bar/baz");
    ///
    /// let p = Path::root().append("/bar").append("//baz//////foo/");
    /// assert_eq!(&p, "/bar/baz/foo");
    /// ```
    pub fn append<T: AsRef<str>>(&self, other: &T) -> Self {
        let other = other.as_ref();
        if other.len() == 0 { self.clone() }
        else {
            let mut res = String::with_capacity(self.len() + other.len() + 1);
            res.push_str(&self);
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
    /// let p = Path::from("/foo/bar/baz");
    /// assert_eq!(Path::parts(&p).collect::<Vec<_>>(), vec!["foo", "bar", "baz"]);
    ///
    /// let p = Path::from("/foo\/bar/baz");
    /// assert_eq!(Path::parts(&p).collect::<Vec<_>>(), vec!["foo\/bar", "baz"]);
    ///
    /// let p = Path::from("/foo\\/bar/baz");
    /// assert_eq!(Path::parts(&p).collect::<Vec<_>>(), vec!["foo\\", "bar", "baz"]);
    ///
    /// let p = Path::from("/foo\\\/bar/baz");
    /// assert_eq!(Path::parts(&p).collect::<Vec<_>>(), vec!["foo\\\/bar", "baz"]);
    /// ```
    pub fn parts(s: &str) -> impl Iterator<Item=&str> {
        s.split({
            let mut esc = false;
            move |c| {
                if c == SEP { !esc }
                else {
                    esc = c == ESC && !esc;
                    false
                }
            }
        })
    }

    /// return the path without the last part, or return None if the
    /// path is empty or /.
    ///
    /// # Examples
    /// ```
    /// let p = Path::from("/foo/bar/baz");
    /// assert_eq!(Path::dirname(&p), Some("/foo/bar"));
    ///
    /// let p = Path::root();
    /// assert_eq!(Path::dirname(&p), None);
    ///
    /// let p = Path::from("/foo");
    /// assert_eq!(Path::dirname(&p), None);
    /// ```
    pub fn dirname(s: &str) -> Option<&str> {
        Path::rfind_sep(s).and_then(|i| {
            if i == 0 { None } 
            else { Some(&s[0..i]) }
        })
    }

    /// return the last part of the path, or return None if the path
    /// is empty.
    ///
    /// # Examples
    /// ```
    /// let p = Path::from("/foo/bar/baz");
    /// assert_eq!(Path::basename(&p), Some("foo"));
    ///
    /// let p = Path::from("foo");
    /// assert_eq!(Path::basename(&p), Some("foo");
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
    pub fn basename(s: &str) -> Option<&str> {
        match Path::rfind_sep(s) {
            None => if s.len() > 0 { Some(s) } else { None },
            Some(i) => {
                if s.len() <= 1 { None }
                else { Some(&s[i+1..s.len()]) }
            }
        }
    }

    /// return the position of the last path separator in the path, or
    /// None if there isn't one.
    ///
    /// # Examples
    /// ```
    /// let p = Path::from("/foo/bar/baz");
    /// assert_eq!(Path::rfind_sep(&p), Some(8));
    ///
    /// let p = Path::from("");
    /// assert_eq!(Path::rfind_sep(&p), None);
    /// ```
    pub fn rfind_sep(mut s: &str) -> Option<usize> {
        if s.len() == 0 { None }
        else {
            loop {
                match s.rfind(SEP) {
                    Some(i) =>
                        if !is_escaped(s, i) { return Some(i) } else { s = &s[0..i] }
                    None => return None,
                }
            }
        }
    }
}

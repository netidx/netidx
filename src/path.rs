use std::{
  borrow::Borrow,
  ops::Deref,
  convert::{AsRef, From},
};

pub static ESC: char = '\\';
pub static SEP: char = '/';

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Path(String);

impl Borrow<str> for Path {
  fn borrow(&self) -> &str { self.0.borrow() }
}

impl Deref for Path {
  type Target = str;

  fn deref(&self) -> &str { self.0.deref() }
}

impl AsRef<[u8]> for Path {
  fn as_ref(&self) -> &[u8] { self.0.as_ref() }
}

impl AsRef<str> for Path {
  fn as_ref(&self) -> &str { self.0.as_ref() }
}

impl From<String> for Path {
  fn from(mut s: String) -> Path {
    let s =
      if !s.contains("//") {
        if s.ends_with('/') { s.pop(); }
        s
      } else {
        let mut res = String::with_capacity(s.len());
        canonize(&mut res, s.as_ref());
        res
      };
    Path(s)
  }
}

impl<'a> From<&'a str> for Path {
  fn from(s: &str) -> Path {
    let mut res = String::with_capacity(s.len());
    canonize(&mut res, s);
    Path(res)
  }
}

impl<'a> From<&'a String> for Path {
  fn from(s: &String) -> Path {
    let mut res = String::with_capacity(s.len());
    canonize(&mut res, s.as_ref());
    Path(res)
  }
}

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

fn canonize(res: &mut String, s: &str) {
  if s.len() > 0 {
    if s.starts_with(SEP) { res.push(SEP) }
    let mut first = true;
    for p in parts(s.as_ref()).filter(|p| *p != "") {
      if first { first = false; } 
      else { res.push(SEP) }
      res.push_str(p);
    }
  }
}

fn is_escaped(s: &str, i: usize) -> bool {
  let s = s.as_bytes();
  i < s.len() && s[i] == (SEP as u8) && {
    let mut res = false;
    for j in (0..i).rev() {
      if s[j] == (ESC as u8) { res = !res; }
      else { break }
    }
    res
  }
}

pub fn rfind_sep(mut s: &str) -> Option<usize> {
  if s.len() == 0 { None }
  else {
    loop {
      match s.rfind(SEP) {
        Some(i) => if !is_escaped(s, i) { return Some(i) } else { s = &s[0..i] }
        None => return None,
      }
    }
  }
}

pub fn dirname(s: &str) -> Option<&str> { 
  rfind_sep(s).and_then(|i| { 
    if i == 0 { None } 
    else { Some(&s[0..i]) }
  })
}

pub fn basename(s: &str) -> Option<&str> {
  match rfind_sep(s) {
    None => if s.len() > 0 { Some(s) } else { None },
    Some(i) => {
      if s.len() <= 1 { None }
      else { Some(&s[i+1..s.len()]) }
    }
  }
}

impl Path {
  pub fn root() -> Path { Path::from("/") }

  pub fn is_absolute(&self) -> bool { self.0.starts_with(SEP) }

  pub fn push<T: AsRef<str>>(&mut self, other: &T) {
    let other = other.as_ref();
    if other.len() > 0 {
      if other.starts_with(SEP) {
        self.0.clear();
        canonize(&mut self.0, other)
      } else {
        self.0.push(SEP);
        canonize(&mut self.0, other)
      }
    }
  }

  pub fn parts(&self) -> impl Iterator<Item=&str> { parts(self.0.as_ref()) }

  pub fn dirname(&self) -> Option<Path> {
    dirname(self.as_ref()).map(|s| Path(String::from(s)))
  }

  pub fn basename(&self) -> Option<Path> {
    basename(self.as_ref()).map(|s| Path(String::from(s)))
  }
}

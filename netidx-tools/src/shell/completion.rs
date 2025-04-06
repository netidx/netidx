use super::ReplCtx;
use anyhow::Result;
use netidx_bscript::{env::Env, expr::ModPath, typ::Type, NoUserEvent};
use reedline::{Completer, Span, Suggestion};
use std::marker::PhantomData;

enum CompletionContext<'a> {
    Bind(Span, &'a str),
    ArgLbl { span: Span, function: &'a str, arg: &'a str },
}

impl<'a> CompletionContext<'a> {
    fn from_str(s: &'a str) -> Result<Self> {
        let mut arg_lbl = 0;
        let mut fend = 0;
        let mut prev = 0;
        for (i, c) in s.char_indices().rev() {
            if c == '#' {
                arg_lbl = prev;
            }
            if c == '(' && arg_lbl != 0 {
                fend = i;
            }
            if c.is_whitespace() || c == '(' || c == '{' {
                if arg_lbl > 0 && fend > 0 {
                    let function =
                        s.get(prev..fend).ok_or_else(|| anyhow!("invalid function"))?;
                    let arg = s.get(arg_lbl..).ok_or_else(|| anyhow!("invalid arg"))?;
                    return Ok(Self::ArgLbl {
                        span: Span { start: arg_lbl, end: s.len() },
                        function,
                        arg,
                    });
                } else {
                    return Ok(Self::Bind(
                        Span { start: prev, end: s.len() },
                        s.get(prev..).ok_or_else(|| anyhow!("invalid bind"))?,
                    ));
                }
            }
            prev = i;
        }
        Ok(Self::Bind(Span { start: 0, end: s.len() }, s))
    }
}

pub(super) struct BComplete(pub Env<ReplCtx, NoUserEvent>);

impl Completer for BComplete {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        dbg!((line, pos));
        let mut res = vec![];
        if let Some(s) = line.get(0..=pos) {
            if let Ok(cc) = CompletionContext::from_str(s) {
                match cc {
                    CompletionContext::Bind(span, s) => {
                        let part = ModPath::from_iter(s.split("::"));
                        for (value, id) in self.0.lookup_matching(&ModPath::root(), &part)
                        {
                            let typ = match self.0.by_id.get(&id) {
                                None => &Type::Bottom(PhantomData),
                                Some(b) => &b.typ,
                            };
                            let description = Some(format!("{typ}"));
                            res.push(Suggestion {
                                span,
                                value: value.into(),
                                description,
                                style: None,
                                extra: None,
                                append_whitespace: false,
                            })
                        }
                    }
                    CompletionContext::ArgLbl { span, function, arg: part } => {
                        let function = ModPath::from_iter(function.split("::"));
                        if let Some((_, b)) =
                            self.0.lookup_bind(&ModPath::root(), &function)
                        {
                            if let Type::Fn(ft) = &b.typ {
                                for arg in ft.args.iter() {
                                    if let Some((lbl, _)) = &arg.label {
                                        if lbl.starts_with(part) {
                                            let description =
                                                Some(format!("{}", arg.typ));
                                            res.push(Suggestion {
                                                span,
                                                value: lbl.as_str().into(),
                                                description,
                                                style: None,
                                                extra: None,
                                                append_whitespace: false,
                                            })
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        res
    }
}

use super::{parser, Bind, Expr, ExprKind, Lambda, ModuleKind};
use compact_str::{format_compact, CompactString};
use netidx::{
    path::Path,
    utils::{self, Either},
};
use netidx_value::Value;
use std::fmt::{self, Write};

impl ExprKind {
    pub fn to_string_pretty(&self, col_limit: usize) -> String {
        let mut buf = String::new();
        self.pretty_print(0, col_limit, true, &mut buf).unwrap();
        buf
    }

    fn pretty_print(
        &self,
        indent: usize,
        limit: usize,
        newline: bool,
        buf: &mut String,
    ) -> fmt::Result {
        macro_rules! kill_newline {
            ($buf:expr) => {
                if let Some('\n') = $buf.chars().next_back() {
                    $buf.pop();
                }
            };
        }
        macro_rules! try_single_line {
            ($trunc:ident) => {{
                let len = buf.len();
                let (start, indent) = if newline {
                    push_indent(indent, buf);
                    (len, indent)
                } else {
                    (buf.rfind('\n').unwrap_or(0), 0)
                };
                writeln!(buf, "{}", self)?;
                if buf.len() - start <= limit {
                    return Ok(());
                } else {
                    if $trunc {
                        buf.truncate(len + indent)
                    }
                    len + indent
                }
            }};
        }
        macro_rules! binop {
            ($sep:literal, $lhs:expr, $rhs:expr) => {{
                try_single_line!(true);
                write!(buf, "(")?;
                writeln!(buf, "{} {}", $lhs, $sep)?;
                $rhs.kind.pretty_print(indent, limit, true, buf)?;
                write!(buf, ")")
            }};
        }
        let mut tbuf = CompactString::new("");
        macro_rules! typ {
            ($typ:expr) => {{
                match $typ {
                    None => "",
                    Some(typ) => {
                        tbuf.clear();
                        write!(tbuf, ": {typ}")?;
                        tbuf.as_str()
                    }
                }
            }};
        }
        fn push_indent(indent: usize, buf: &mut String) {
            buf.extend((0..indent).into_iter().map(|_| ' '));
        }
        fn pretty_print_exprs_int<'a, A, F: Fn(&'a A) -> &'a Expr>(
            indent: usize,
            limit: usize,
            buf: &mut String,
            exprs: &'a [A],
            open: &str,
            close: &str,
            sep: &str,
            f: F,
        ) -> fmt::Result {
            writeln!(buf, "{}", open)?;
            for i in 0..exprs.len() {
                f(&exprs[i]).kind.pretty_print(indent + 2, limit, true, buf)?;
                if i < exprs.len() - 1 {
                    kill_newline!(buf);
                    writeln!(buf, "{}", sep)?
                }
            }
            push_indent(indent, buf);
            writeln!(buf, "{}", close)
        }
        fn pretty_print_exprs(
            indent: usize,
            limit: usize,
            buf: &mut String,
            exprs: &[Expr],
            open: &str,
            close: &str,
            sep: &str,
        ) -> fmt::Result {
            pretty_print_exprs_int(indent, limit, buf, exprs, open, close, sep, |a| a)
        }
        let exp = |export| if export { "pub " } else { "" };
        match self {
            ExprKind::Constant(_)
            | ExprKind::Use { .. }
            | ExprKind::Ref { .. }
            | ExprKind::StructRef { .. }
            | ExprKind::TupleRef { .. }
            | ExprKind::TypeDef { .. }
            | ExprKind::ArrayRef { .. }
            | ExprKind::ArraySlice { .. }
            | ExprKind::StringInterpolate { .. }
            | ExprKind::Module {
                name: _,
                export: _,
                value: ModuleKind::Unresolved | ModuleKind::Resolved(_),
            } => {
                if newline {
                    push_indent(indent, buf);
                }
                writeln!(buf, "{self}")
            }
            ExprKind::Bind(b) => {
                let Bind { doc, pattern, typ, export, value } = &**b;
                try_single_line!(true);
                if let Some(doc) = doc {
                    if doc == "" {
                        writeln!(buf, "///")?;
                    } else {
                        for line in doc.lines() {
                            writeln!(buf, "///{line}")?;
                        }
                    }
                }
                writeln!(buf, "{}let {pattern}{} = ", exp(*export), typ!(typ))?;
                value.kind.pretty_print(indent + 2, limit, false, buf)
            }
            ExprKind::StructWith { source, replace } => {
                try_single_line!(true);
                match &source.kind {
                    ExprKind::Ref { .. } => writeln!(buf, "{{ {source} with")?,
                    _ => writeln!(buf, "{{ ({source}) with")?,
                }
                let indent = indent + 2;
                for (i, (name, e)) in replace.iter().enumerate() {
                    push_indent(indent, buf);
                    match &e.kind {
                        ExprKind::Ref { name: n }
                            if Path::dirname(&**n).is_none()
                                && Path::basename(&**n) == Some(&**name) =>
                        {
                            write!(buf, "{name}")?
                        }
                        e => {
                            write!(buf, "{name}: ")?;
                            e.pretty_print(indent + 2, limit, false, buf)?;
                        }
                    }
                    if i < replace.len() - 1 {
                        kill_newline!(buf);
                        writeln!(buf, ",")?
                    }
                }
                writeln!(buf, "}}")
            }
            ExprKind::Module { name, export, value: ModuleKind::Inline(exprs) } => {
                try_single_line!(true);
                write!(buf, "{}mod {name} ", exp(*export))?;
                pretty_print_exprs(indent, limit, buf, exprs, "{", "}", ";")
            }
            ExprKind::Do { exprs } => {
                try_single_line!(true);
                pretty_print_exprs(indent, limit, buf, exprs, "{", "}", ";")
            }
            ExprKind::Connect { name, value, deref } => {
                try_single_line!(true);
                let deref = if *deref { "*" } else { "" };
                writeln!(buf, "{deref}{name} <- ")?;
                value.kind.pretty_print(indent + 2, limit, false, buf)
            }
            ExprKind::TypeCast { expr, typ } => {
                try_single_line!(true);
                writeln!(buf, "cast<{typ}>(")?;
                expr.kind.pretty_print(indent + 2, limit, true, buf)?;
                writeln!(buf, ")")
            }
            ExprKind::Array { args } => {
                try_single_line!(true);
                pretty_print_exprs(indent, limit, buf, args, "[", "]", ",")
            }
            ExprKind::Any { args } => {
                try_single_line!(true);
                write!(buf, "any")?;
                pretty_print_exprs(indent, limit, buf, args, "(", ")", ",")
            }
            ExprKind::Tuple { args } => {
                try_single_line!(true);
                pretty_print_exprs(indent, limit, buf, args, "(", ")", ",")
            }
            ExprKind::Variant { tag: _, args } if args.len() == 0 => {
                if newline {
                    push_indent(indent, buf)
                }
                write!(buf, "{self}")
            }
            ExprKind::Variant { tag, args } => {
                try_single_line!(true);
                write!(buf, "`{tag}")?;
                pretty_print_exprs(indent, limit, buf, args, "(", ")", ",")
            }
            ExprKind::Struct { args } => {
                try_single_line!(true);
                writeln!(buf, "{{")?;
                for (i, (n, e)) in args.iter().enumerate() {
                    push_indent(indent + 2, buf);
                    match &e.kind {
                        ExprKind::Ref { name }
                            if Path::dirname(&**name).is_none()
                                && Path::basename(&**name) == Some(&**n) =>
                        {
                            write!(buf, "{n}")?
                        }
                        _ => {
                            write!(buf, "{n}: ")?;
                            e.kind.pretty_print(indent + 2, limit, false, buf)?;
                        }
                    }
                    if i < args.len() - 1 {
                        kill_newline!(buf);
                        writeln!(buf, ", ")?
                    }
                }
                push_indent(indent, buf);
                writeln!(buf, "}}")
            }
            ExprKind::Qop(e) => {
                try_single_line!(true);
                e.kind.pretty_print(indent, limit, true, buf)?;
                kill_newline!(buf);
                writeln!(buf, "?")
            }
            ExprKind::Apply { function, args } => {
                try_single_line!(true);
                match &function.kind {
                    ExprKind::Ref { .. } => {
                        function.kind.pretty_print(indent, limit, true, buf)?
                    }
                    e => {
                        write!(buf, "(")?;
                        e.pretty_print(indent, limit, true, buf)?;
                        kill_newline!(buf);
                        write!(buf, ")")?;
                    }
                }
                kill_newline!(buf);
                writeln!(buf, "(")?;
                for i in 0..args.len() {
                    match &args[i].0 {
                        None => {
                            args[i].1.kind.pretty_print(indent + 2, limit, true, buf)?
                        }
                        Some(name) => match &args[i].1.kind {
                            ExprKind::Ref { name: n }
                                if Path::dirname(&n.0).is_none()
                                    && Path::basename(&n.0) == Some(name.as_str()) =>
                            {
                                writeln!(buf, "#{name}")?
                            }
                            _ => {
                                write!(buf, "#{name}: ")?;
                                args[i].1.kind.pretty_print(
                                    indent + 2,
                                    limit,
                                    false,
                                    buf,
                                )?
                            }
                        },
                    }
                    if i < args.len() - 1 {
                        kill_newline!(buf);
                        writeln!(buf, ",")?
                    }
                }
                writeln!(buf, ")")
            }
            ExprKind::Lambda(l) => {
                let Lambda { args, vargs, rtype, constraints, body } = &**l;
                try_single_line!(true);
                for (i, (tvar, typ)) in constraints.iter().enumerate() {
                    write!(buf, "{tvar}: {typ}")?;
                    if i < constraints.len() - 1 {
                        write!(buf, ", ")?;
                    }
                }
                write!(buf, "|")?;
                for (i, a) in args.iter().enumerate() {
                    match &a.labeled {
                        None => {
                            write!(buf, "{}", a.pattern)?;
                            buf.push_str(typ!(&a.constraint));
                        }
                        Some(def) => {
                            write!(buf, "#{}", a.pattern)?;
                            buf.push_str(typ!(&a.constraint));
                            if let Some(def) = def {
                                write!(buf, " = {def}")?;
                            }
                        }
                    }
                    if vargs.is_some() || i < args.len() - 1 {
                        write!(buf, ", ")?
                    }
                }
                if let Some(typ) = vargs {
                    write!(buf, "@args{}", typ!(typ))?;
                }
                write!(buf, "| ")?;
                if let Some(t) = rtype {
                    write!(buf, "-> {t} ")?
                }
                match body {
                    Either::Right(builtin) => {
                        writeln!(buf, "'{builtin}")
                    }
                    Either::Left(body) => match &body.kind {
                        ExprKind::Do { exprs } => {
                            pretty_print_exprs(indent, limit, buf, exprs, "{", "}", ";")
                        }
                        _ => body.kind.pretty_print(indent, limit, false, buf),
                    },
                }
            }
            ExprKind::Eq { lhs, rhs } => binop!("==", lhs, rhs),
            ExprKind::Ne { lhs, rhs } => binop!("!=", lhs, rhs),
            ExprKind::Lt { lhs, rhs } => binop!("<", lhs, rhs),
            ExprKind::Gt { lhs, rhs } => binop!(">", lhs, rhs),
            ExprKind::Lte { lhs, rhs } => binop!("<=", lhs, rhs),
            ExprKind::Gte { lhs, rhs } => binop!(">=", lhs, rhs),
            ExprKind::And { lhs, rhs } => binop!("&&", lhs, rhs),
            ExprKind::Or { lhs, rhs } => binop!("||", lhs, rhs),
            ExprKind::Add { lhs, rhs } => binop!("+", lhs, rhs),
            ExprKind::Sub { lhs, rhs } => binop!("-", lhs, rhs),
            ExprKind::Mul { lhs, rhs } => binop!("*", lhs, rhs),
            ExprKind::Div { lhs, rhs } => binop!("/", lhs, rhs),
            ExprKind::Mod { lhs, rhs } => binop!("%", lhs, rhs),
            ExprKind::Sample { lhs, rhs } => binop!("~", lhs, rhs),
            ExprKind::Not { expr } => {
                try_single_line!(true);
                match &expr.kind {
                    ExprKind::Do { exprs } => {
                        pretty_print_exprs(indent, limit, buf, exprs, "!{", "}", ";")
                    }
                    _ => {
                        writeln!(buf, "!(")?;
                        expr.kind.pretty_print(indent + 2, limit, true, buf)?;
                        push_indent(indent, buf);
                        writeln!(buf, ")")
                    }
                }
            }
            ExprKind::ByRef(e) => {
                try_single_line!(true);
                write!(buf, "&")?;
                e.kind.pretty_print(indent + 2, limit, false, buf)
            }
            ExprKind::Deref(e) => match &e.kind {
                ExprKind::Connect { .. } | ExprKind::Qop(_) => {
                    try_single_line!(true);
                    writeln!(buf, "*(")?;
                    e.kind.pretty_print(indent + 2, limit, newline, buf)?;
                    writeln!(buf, ")")
                }
                _ => {
                    try_single_line!(true);
                    write!(buf, "*")?;
                    e.kind.pretty_print(indent + 2, limit, false, buf)
                }
            },
            ExprKind::Select { arg, arms } => {
                try_single_line!(true);
                write!(buf, "select ")?;
                arg.kind.pretty_print(indent, limit, false, buf)?;
                kill_newline!(buf);
                writeln!(buf, " {{")?;
                for (i, (pat, expr)) in arms.iter().enumerate() {
                    if let Some(tp) = &pat.type_predicate {
                        write!(buf, "{tp} as ")?;
                    }
                    write!(buf, "{} ", pat.structure_predicate)?;
                    if let Some(guard) = &pat.guard {
                        write!(buf, "if ")?;
                        guard.kind.pretty_print(indent + 2, limit, false, buf)?;
                        kill_newline!(buf);
                        write!(buf, " ")?;
                    }
                    write!(buf, "=> ")?;
                    if let ExprKind::Do { exprs } = &expr.kind {
                        let term = if i < arms.len() - 1 { "}," } else { "}" };
                        pretty_print_exprs(
                            indent + 2,
                            limit,
                            buf,
                            exprs,
                            "{",
                            term,
                            ";",
                        )?;
                    } else if i < arms.len() - 1 {
                        expr.kind.pretty_print(indent + 2, limit, false, buf)?;
                        kill_newline!(buf);
                        writeln!(buf, ",")?
                    } else {
                        expr.kind.pretty_print(indent, limit, false, buf)?;
                    }
                }
                push_indent(indent, buf);
                writeln!(buf, "}}")
            }
        }
    }
}

impl fmt::Display for ExprKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fn write_binop(
            f: &mut fmt::Formatter,
            op: &str,
            lhs: &Expr,
            rhs: &Expr,
        ) -> fmt::Result {
            write!(f, "(")?;
            write!(f, "{lhs} {op} {rhs}")?;
            write!(f, ")")
        }
        fn print_exprs(
            f: &mut fmt::Formatter,
            exprs: &[Expr],
            open: &str,
            close: &str,
            sep: &str,
        ) -> fmt::Result {
            write!(f, "{open}")?;
            for i in 0..exprs.len() {
                write!(f, "{}", &exprs[i])?;
                if i < exprs.len() - 1 {
                    write!(f, "{sep}")?
                }
            }
            write!(f, "{close}")
        }
        let mut tbuf = CompactString::new("");
        macro_rules! typ {
            ($typ:expr) => {{
                match $typ {
                    None => "",
                    Some(typ) => {
                        tbuf.clear();
                        write!(tbuf, ": {typ}")?;
                        tbuf.as_str()
                    }
                }
            }};
        }
        let exp = |export| if export { "pub " } else { "" };
        match self {
            ExprKind::Constant(v) => v.fmt_ext(f, &parser::BSCRIPT_ESC, true),
            ExprKind::Bind(b) => {
                let Bind { doc, pattern, typ, export, value } = &**b;
                if let Some(doc) = doc {
                    if doc == "" {
                        writeln!(f, "///")?
                    } else {
                        for line in doc.lines() {
                            writeln!(f, "///{line}")?
                        }
                    }
                }
                write!(f, "{}let {pattern}{} = {value}", exp(*export), typ!(typ))
            }
            ExprKind::StructWith { source, replace } => {
                match &source.kind {
                    ExprKind::Ref { .. } => write!(f, "{{ {source} with ")?,
                    _ => write!(f, "{{ ({source}) with ")?,
                }
                for (i, (name, e)) in replace.iter().enumerate() {
                    match &e.kind {
                        ExprKind::Ref { name: n }
                            if Path::dirname(&**n).is_none()
                                && Path::basename(&**n) == Some(&**name) =>
                        {
                            write!(f, "{name}")?
                        }
                        _ => write!(f, "{name}: {e}")?,
                    }
                    if i < replace.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, " }}")
            }
            ExprKind::Connect { name, value, deref } => {
                let deref = if *deref { "*" } else { "" };
                write!(f, "{deref}{name} <- {value}")
            }
            ExprKind::Use { name } => {
                write!(f, "use {name}")
            }
            ExprKind::Ref { name } => {
                write!(f, "{name}")
            }
            ExprKind::StructRef { source, field } => match &source.kind {
                ExprKind::Ref { .. } => {
                    write!(f, "{source}.{field}")
                }
                source => write!(f, "({source}).{field}"),
            },
            ExprKind::TupleRef { source, field } => match &source.kind {
                ExprKind::Ref { .. } => {
                    write!(f, "{source}.{field}")
                }
                source => write!(f, "({source}).{field}"),
            },
            ExprKind::Module { name, export, value } => {
                write!(f, "{}mod {name}", exp(*export))?;
                match value {
                    ModuleKind::Resolved(_) | ModuleKind::Unresolved => Ok(()),
                    ModuleKind::Inline(exprs) => print_exprs(f, &**exprs, "{", "}", "; "),
                }
            }
            ExprKind::TypeCast { expr, typ } => write!(f, "cast<{typ}>({expr})"),
            ExprKind::TypeDef { name, params, typ } => {
                write!(f, "type {name}")?;
                if !params.is_empty() {
                    write!(f, "<")?;
                    for (i, (tv, ct)) in params.iter().enumerate() {
                        write!(f, "{tv}")?;
                        if let Some(ct) = ct {
                            write!(f, ": {ct}")?;
                        }
                        if i < params.len() - 1 {
                            write!(f, ", ")?;
                        }
                    }
                    write!(f, ">")?;
                }
                write!(f, " = {typ}")
            }
            ExprKind::Do { exprs } => print_exprs(f, &**exprs, "{", "}", "; "),
            ExprKind::Lambda(l) => {
                let Lambda { args, vargs, rtype, constraints, body } = &**l;
                for (i, (tvar, typ)) in constraints.iter().enumerate() {
                    write!(f, "{tvar}: {typ}")?;
                    if i < constraints.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, "|")?;
                for (i, a) in args.iter().enumerate() {
                    match &a.labeled {
                        None => {
                            write!(f, "{}", a.pattern)?;
                            write!(f, "{}", typ!(&a.constraint))?;
                        }
                        Some(def) => {
                            write!(f, "#{}", a.pattern)?;
                            write!(f, "{}", typ!(&a.constraint))?;
                            if let Some(def) = def {
                                write!(f, " = {def}")?;
                            }
                        }
                    }
                    if vargs.is_some() || i < args.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                if let Some(typ) = vargs {
                    write!(f, "@args{}", typ!(typ))?;
                }
                write!(f, "| ")?;
                if let Some(t) = rtype {
                    write!(f, "-> {t} ")?
                }
                match body {
                    Either::Right(builtin) => write!(f, "'{builtin}"),
                    Either::Left(body) => write!(f, "{body}"),
                }
            }
            ExprKind::Array { args } => print_exprs(f, args, "[", "]", ", "),
            ExprKind::Any { args } => {
                write!(f, "any")?;
                print_exprs(f, args, "(", ")", ", ")
            }
            ExprKind::Tuple { args } => print_exprs(f, args, "(", ")", ", "),
            ExprKind::Variant { tag, args } if args.len() == 0 => {
                write!(f, "`{tag}")
            }
            ExprKind::Variant { tag, args } => {
                write!(f, "`{tag}")?;
                print_exprs(f, args, "(", ")", ", ")
            }
            ExprKind::Struct { args } => {
                write!(f, "{{ ")?;
                for (i, (n, e)) in args.iter().enumerate() {
                    match &e.kind {
                        ExprKind::Ref { name }
                            if Path::dirname(&**name).is_none()
                                && Path::basename(&**name) == Some(&**n) =>
                        {
                            write!(f, "{n}")?
                        }
                        _ => write!(f, "{n}: {e}")?,
                    }
                    if i < args.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, " }}")
            }
            ExprKind::Qop(e) => write!(f, "{}?", e),
            ExprKind::StringInterpolate { args } => {
                write!(f, "\"")?;
                for s in args.iter() {
                    match &s.kind {
                        ExprKind::Constant(Value::String(s)) if s.len() > 0 => {
                            let es = utils::escape(&*s, '\\', &parser::BSCRIPT_ESC);
                            write!(f, "{es}",)?;
                        }
                        s => {
                            write!(f, "[{s}]")?;
                        }
                    }
                }
                write!(f, "\"")
            }
            ExprKind::ArrayRef { source, i } => match &source.kind {
                ExprKind::Ref { .. } => {
                    write!(f, "{}[{}]", source, i)
                }
                _ => write!(f, "({})[{}]", &source, &i),
            },
            ExprKind::ArraySlice { source, start, end } => {
                let s = match start.as_ref() {
                    None => "",
                    Some(e) => &format_compact!("{e}"),
                };
                let e = match &end.as_ref() {
                    None => "",
                    Some(e) => &format_compact!("{e}"),
                };
                match &source.kind {
                    ExprKind::Ref { .. } => {
                        write!(f, "{}[{}..{}]", source, s, e)
                    }
                    _ => write!(f, "({})[{}..{}]", source, s, e),
                }
            }
            ExprKind::Apply { args, function } => {
                match &function.kind {
                    ExprKind::Ref { name: _ } => write!(f, "{function}")?,
                    function => write!(f, "({function})")?,
                }
                write!(f, "(")?;
                for i in 0..args.len() {
                    match &args[i].0 {
                        None => write!(f, "{}", &args[i].1)?,
                        Some(name) => match &args[i].1.kind {
                            ExprKind::Ref { name: n }
                                if Path::dirname(&n.0).is_none()
                                    && Path::basename(&n.0) == Some(name.as_str()) =>
                            {
                                write!(f, "#{name}")?
                            }
                            _ => write!(f, "#{name}: {}", &args[i].1)?,
                        },
                    }
                    if i < args.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, ")")
            }
            ExprKind::Select { arg, arms } => {
                write!(f, "select {arg} {{")?;
                for (i, (pat, rhs)) in arms.iter().enumerate() {
                    if let Some(tp) = &pat.type_predicate {
                        write!(f, "{tp} as ")?;
                    }
                    write!(f, "{} ", pat.structure_predicate)?;
                    if let Some(guard) = &pat.guard {
                        write!(f, "if {guard} ")?;
                    }
                    write!(f, "=> {rhs}")?;
                    if i < arms.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, "}}")
            }
            ExprKind::Eq { lhs, rhs } => write_binop(f, "==", lhs, rhs),
            ExprKind::Ne { lhs, rhs } => write_binop(f, "!=", lhs, rhs),
            ExprKind::Gt { lhs, rhs } => write_binop(f, ">", lhs, rhs),
            ExprKind::Lt { lhs, rhs } => write_binop(f, "<", lhs, rhs),
            ExprKind::Gte { lhs, rhs } => write_binop(f, ">=", lhs, rhs),
            ExprKind::Lte { lhs, rhs } => write_binop(f, "<=", lhs, rhs),
            ExprKind::And { lhs, rhs } => write_binop(f, "&&", lhs, rhs),
            ExprKind::Or { lhs, rhs } => write_binop(f, "||", lhs, rhs),
            ExprKind::Add { lhs, rhs } => write_binop(f, "+", lhs, rhs),
            ExprKind::Sub { lhs, rhs } => write_binop(f, "-", lhs, rhs),
            ExprKind::Mul { lhs, rhs } => write_binop(f, "*", lhs, rhs),
            ExprKind::Div { lhs, rhs } => write_binop(f, "/", lhs, rhs),
            ExprKind::Mod { lhs, rhs } => write_binop(f, "%", lhs, rhs),
            ExprKind::Sample { lhs, rhs } => write_binop(f, "~", lhs, rhs),
            ExprKind::ByRef(e) => write!(f, "&{e}"),
            ExprKind::Deref(e) => match &e.kind {
                ExprKind::Qop(e) => write!(f, "*({e}?)"),
                ExprKind::Connect { .. } => write!(f, "*({e})"),
                _ => write!(f, "*{e}"),
            },
            ExprKind::Not { expr } => {
                write!(f, "(!{expr})")
            }
        }
    }
}

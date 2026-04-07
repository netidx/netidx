// Expression manipulation helpers, type introspection, and defaults.

use arcstr::ArcStr;
use graphix_compiler::{
    env::Env,
    expr::{
        self, ApplyExpr, Expr, ExprKind, LambdaExpr, ModPath, Origin,
        Source as GxSource, StructExpr,
    },
    typ::{FnType, Type},
};
use netidx::{publisher::{Typ, Value}, utils::Either};
use triomphe::Arc;

use super::tree::ChildSlot;

// ---- Type helpers ----

/// Check if a type (deeply) contains Widget.
pub(super) fn type_contains_widget(typ: &Type) -> bool {
    match typ {
        Type::ByRef(inner) => type_contains_widget(inner),
        Type::Array(inner) => type_contains_widget(inner),
        Type::Set(variants) => {
            variants.iter().any(|v| matches!(v, Type::Variant(tag, _)
                if tag.as_str() == "Label" || tag.as_str() == "VBox" || tag.as_str() == "Table"))
            || variants.iter().any(|v| type_contains_widget(v))
        }
        Type::Ref { name, .. } => {
            let s = name.0.as_ref();
            s == "/Widget" || s.ends_with("/Widget")
        }
        _ => false,
    }
}

/// Determine child slots for a widget type from its FnType.
pub(super) fn child_slots_for_fntype(fn_type: &FnType) -> Vec<ChildSlot> {
    let mut slots = Vec::new();
    for arg in fn_type.args.iter() {
        if let Some((name, _)) = &arg.label {
            if type_contains_widget(&arg.typ) {
                match &arg.typ {
                    Type::ByRef(inner) => match inner.as_ref() {
                        Type::Array(_) => slots.push(ChildSlot::Array(name.clone())),
                        _ => slots.push(ChildSlot::Single(name.clone())),
                    },
                    Type::Array(_) => slots.push(ChildSlot::Array(name.clone())),
                    _ => slots.push(ChildSlot::Single(name.clone())),
                }
            }
        }
    }
    slots
}

/// Check if a constructor function exists for a named type.
/// Convention: type `Foo` → constructor `browser::foo`.
/// Returns the constructor name if found as a Fn bind in env.
pub(super) fn lookup_constructor(env: Option<&Env>, type_name: &ModPath) -> Option<ArcStr> {
    let basename = netidx::path::Path::basename(&type_name.0)?;
    let ctor_name = basename.to_lowercase();
    let func_path = format!("/browser/{}", ctor_name);
    let mod_path = ModPath(netidx::path::Path::from(ArcStr::from(func_path.as_str())));
    let scope = ModPath::root();
    env?.lookup_bind(&scope, &mod_path)
        .and_then(|(_, bind)| {
            if matches!(&bind.typ, Type::Fn(_)) {
                Some(ArcStr::from(ctor_name.as_str()))
            } else {
                None
            }
        })
}

// ---- Parse / reformat ----

/// Parse a small graphix expression string.
pub(super) fn parse_expr(s: &str) -> Option<Expr> {
    let ori = Origin {
        parent: None,
        source: GxSource::Unspecified,
        text: ArcStr::from(s),
    };
    expr::parser::parse(ori).ok()?.last().cloned()
}

/// Re-parse and pretty-print source text
pub(super) fn reformat_source(src: &str) -> String {
    use graphix_compiler::expr::print::PrettyDisplay;
    let ori = Origin {
        parent: None,
        source: GxSource::Unspecified,
        text: ArcStr::from(src),
    };
    match expr::parser::parse(ori) {
        Ok(exprs) => {
            let mut parts = Vec::new();
            for e in &*exprs {
                let s = e.to_string_pretty(80).to_string();
                parts.push(s.trim_end().to_string());
            }
            parts.join(";\n")
        }
        Err(_) => src.to_string(),
    }
}

// ---- Defaults ----

/// Generate a concrete default expression for a type. Always produces a non-null
/// value — recursively fills in all nested structures with sensible defaults.
pub(super) fn default_expr_for_type(typ: &Type, env: Option<&Env>) -> Expr {
    default_expr_inner(typ, env, 0)
}

fn default_expr_inner(typ: &Type, env: Option<&Env>, depth: u32) -> Expr {
    if depth > 8 {
        return ExprKind::Constant(Value::Null).to_expr_nopos();
    }
    match typ {
        Type::ByRef(inner) => {
            let inner_expr = default_expr_inner(inner, env, depth + 1);
            ExprKind::ByRef(Arc::new(inner_expr)).to_expr_nopos()
        }
        Type::Set(variants) if is_nullable_set(variants) => {
            default_expr_inner(non_null_type(variants), env, depth + 1)
        }
        Type::Set(variants) if all_argless_variants(variants) => {
            if let Some(Type::Variant(tag, _)) = variants.first() {
                ExprKind::Variant { tag: tag.clone(), args: Arc::from(Vec::<Expr>::new()) }.to_expr_nopos()
            } else {
                ExprKind::Constant(Value::Null).to_expr_nopos()
            }
        }
        Type::Set(variants) => {
            if let Some(Type::Variant(tag, args)) = variants.first() {
                let default_args: Vec<Expr> = args.iter()
                    .map(|t| default_expr_inner(t, env, depth + 1))
                    .collect();
                ExprKind::Variant {
                    tag: tag.clone(),
                    args: Arc::from(default_args),
                }.to_expr_nopos()
            } else {
                ExprKind::Constant(Value::Null).to_expr_nopos()
            }
        }
        Type::Ref { name, .. } => {
            let resolved = env.and_then(|e| typ.lookup_ref(e).ok());
            match resolved {
                Some(Type::Struct(fields)) => {
                    match lookup_constructor(env, name) {
                        Some(ctor_name) => {
                            let func_path = format!("/browser/{}", ctor_name);
                            let func = ExprKind::Ref {
                                name: ModPath(netidx::path::Path::from(
                                    ArcStr::from(func_path.as_str()),
                                )),
                            }.to_expr_nopos();
                            let args: Vec<(Option<ArcStr>, Expr)> = fields.iter()
                                .map(|(n, t)| {
                                    (Some(n.clone()), default_expr_inner(t, env, depth + 1))
                                })
                                .collect();
                            ExprKind::Apply(ApplyExpr {
                                function: Arc::new(func),
                                args: Arc::from(args),
                            }).to_expr_nopos()
                        }
                        None => {
                            // No constructor — use struct literal
                            let args: Vec<(ArcStr, Expr)> = fields.iter()
                                .map(|(n, t)| (n.clone(), default_expr_inner(t, env, depth + 1)))
                                .collect();
                            ExprKind::Struct(StructExpr { args: Arc::from(args) }).to_expr_nopos()
                        }
                    }
                }
                Some(resolved) => default_expr_inner(&resolved, env, depth + 1),
                None => ExprKind::Constant(Value::Null).to_expr_nopos(),
            }
        }
        Type::Struct(fields) => {
            let args: Vec<(ArcStr, Expr)> = fields.iter()
                .map(|(n, t)| (n.clone(), default_expr_inner(t, env, depth + 1)))
                .collect();
            ExprKind::Struct(StructExpr { args: Arc::from(args) }).to_expr_nopos()
        }
        Type::Primitive(flags) => {
            if flags.contains(Typ::String) {
                ExprKind::Constant(Value::String(ArcStr::from(""))).to_expr_nopos()
            } else if flags.contains(Typ::Bool) {
                ExprKind::Constant(Value::Bool(false)).to_expr_nopos()
            } else if flags.contains(Typ::F64) {
                ExprKind::Constant(Value::F64(0.0)).to_expr_nopos()
            } else if flags.contains(Typ::F32) {
                ExprKind::Constant(Value::F32(0.0)).to_expr_nopos()
            } else if flags.contains(Typ::Null) {
                ExprKind::Constant(Value::Null).to_expr_nopos()
            } else {
                ExprKind::Constant(Value::I64(0)).to_expr_nopos()
            }
        }
        Type::Array(_) => {
            ExprKind::Array { args: Arc::from(Vec::<Expr>::new()) }.to_expr_nopos()
        }
        Type::Map { .. } => {
            ExprKind::Map { args: Arc::from(Vec::<(Expr, Expr)>::new()) }.to_expr_nopos()
        }
        Type::Tuple(elems) => {
            let args: Vec<Expr> = elems.iter()
                .map(|t| default_expr_inner(t, env, depth + 1))
                .collect();
            ExprKind::Tuple { args: Arc::from(args) }.to_expr_nopos()
        }
        Type::Variant(tag, args) => {
            let default_args: Vec<Expr> = args.iter()
                .map(|t| default_expr_inner(t, env, depth + 1))
                .collect();
            ExprKind::Variant {
                tag: tag.clone(),
                args: Arc::from(default_args),
            }.to_expr_nopos()
        }
        Type::Fn(ft) => {
            let sig = fntype_sig(ft);
            let src = format!("{} null", sig);
            parse_expr(&src).unwrap_or_else(||
                ExprKind::Constant(Value::Null).to_expr_nopos()
            )
        }
        _ => ExprKind::Constant(Value::Null).to_expr_nopos(),
    }
}

// ---- Expression manipulation ----

/// Extract labeled args from an Apply expression (e.g., `common(#halign: &\`Center)`).
pub(super) fn extract_apply_args(expr: &Expr) -> Vec<(ArcStr, Expr)> {
    let inner = match &expr.kind {
        ExprKind::ByRef(i) => i.as_ref(),
        _ => expr,
    };
    match &inner.kind {
        ExprKind::Apply(ApplyExpr { args, .. }) => {
            args.iter().filter_map(|(label, e)| {
                label.as_ref().map(|l| (l.clone(), e.clone()))
            }).collect()
        }
        _ => vec![],
    }
}

/// Rebuild a constructor call Expr from its name and a set of (label, value) args.
/// Omits args whose value is None. Returns None if all args are None
/// (caller should remove the parent arg entirely).
pub(super) fn rebuild_constructor_expr(
    constructor: &str,
    args: &[(ArcStr, Option<Expr>)],
) -> Option<Expr> {
    let labeled: Vec<(Option<ArcStr>, Expr)> = args.iter()
        .filter_map(|(name, expr)| {
            expr.as_ref().map(|e| (Some(name.clone()), e.clone()))
        })
        .collect();
    if labeled.is_empty() {
        None
    } else {
        let func_path = format!("/browser/{}", constructor);
        let func = ExprKind::Ref {
            name: ModPath(netidx::path::Path::from(ArcStr::from(func_path.as_str()))),
        }.to_expr_nopos();
        Some(ExprKind::Apply(ApplyExpr {
            function: Arc::new(func),
            args: Arc::from(labeled),
        }).to_expr_nopos())
    }
}

// ---- Lambda helpers ----

pub(super) fn lambda_sig(lambda: &LambdaExpr) -> String {
    let mut s = String::from("|");
    for (i, a) in lambda.args.iter().enumerate() {
        match &a.labeled {
            None => {
                s.push_str(&format!("{}", a.pattern));
                if let Some(t) = &a.constraint {
                    s.push_str(&format!(": {}", t));
                }
            }
            Some(_) => {
                s.push_str(&format!("#{}", a.pattern));
                if let Some(t) = &a.constraint {
                    s.push_str(&format!(": {}", t));
                }
            }
        }
        if i < lambda.args.len() - 1 { s.push_str(", "); }
    }
    s.push('|');
    s
}

pub(super) fn fntype_sig(ft: &FnType) -> String {
    let mut s = String::from("|");
    for (i, a) in ft.args.iter().enumerate() {
        match &a.label {
            Some((name, _)) => s.push_str(&format!("#{}: {}", name, a.typ)),
            None => s.push_str(&format!("{}", a.typ)),
        }
        if i < ft.args.len() - 1 { s.push_str(", "); }
    }
    s.push('|');
    s
}

pub(super) fn lambda_body(lambda: &LambdaExpr) -> String {
    match &lambda.body {
        Either::Left(expr) => format!("{}", expr),
        Either::Right(name) => format!("'{}", name),
    }
}

// ---- Source text manipulation ----

pub(super) fn splice_arg(src: &str, name: &ArcStr, value: &str) -> String {
    let search = format!("#{}: ", name);
    let spliced = if let Some(pos) = src.find(&search) {
        let after_label = pos + search.len();
        let rest = &src[after_label..];
        let val_end = find_arg_end(rest) + after_label;
        let mut new = String::new();
        new.push_str(&src[..after_label]);
        new.push_str(value);
        new.push_str(&src[val_end..]);
        new
    } else {
        if let Some(close) = src.rfind(')') {
            let mut new = String::new();
            new.push_str(&src[..close]);
            let before = src[..close].trim_end();
            if !before.ends_with(',') && !before.ends_with('(') {
                new.push(',');
            }
            new.push_str(&format!("\n    #{}: {}", name, value));
            new.push_str(&src[close..]);
            new
        } else {
            src.to_string()
        }
    };
    reformat_source(&spliced)
}

fn find_arg_end(s: &str) -> usize {
    let mut depth = 0i32;
    let bytes = s.as_bytes();
    for i in 0..bytes.len() {
        match bytes[i] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => { if depth == 0 { return i; } depth -= 1; }
            b',' if depth == 0 => return i,
            _ => {}
        }
    }
    s.len()
}

/// Check if an Expr can be meaningfully edited by a structured GUI editor.
/// Constants, variants, constructor calls, struct/array/tuple literals all
/// have dedicated editors. Arbitrary complex expressions (variables, binary
/// ops, selects, etc.) need a raw text Entry.
pub(super) fn is_gui_editable(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::Constant(_) => true,
        ExprKind::Variant { .. } => true,
        ExprKind::ByRef(inner) => is_gui_editable(inner),
        ExprKind::Struct(_) => true,
        ExprKind::Array { .. } => true,
        ExprKind::Tuple { .. } => true,
        ExprKind::Apply(ApplyExpr { function, .. }) => {
            // Constructor calls (browser::foo(...)) are editable via struct_editor
            matches!(&function.kind, ExprKind::Ref { name }
                if name.0.as_ref().starts_with("/browser/"))
        }
        _ => false,
    }
}

// ---- Type check helpers ----

pub(super) fn is_nullable_set(variants: &[Type]) -> bool {
    variants.len() == 2
        && variants.iter().any(|t| matches!(t, Type::Primitive(p) if p.contains(Typ::Null)))
}

pub(super) fn non_null_type<'a>(variants: &'a [Type]) -> &'a Type {
    variants.iter()
        .find(|t| !matches!(t, Type::Primitive(p) if p.contains(Typ::Null)))
        .unwrap_or(&variants[0])
}

pub(super) fn all_argless_variants(variants: &[Type]) -> bool {
    !variants.is_empty()
        && variants.iter().all(|t| matches!(t, Type::Variant(_, args) if args.is_empty()))
}

pub(super) fn all_variants(variants: &[Type]) -> bool {
    !variants.is_empty()
        && variants.iter().all(|t| matches!(t, Type::Variant(_, _)))
}

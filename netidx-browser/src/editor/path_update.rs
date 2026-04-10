//! Path-based expression update mechanism.
//!
//! Replaces GTK's `Rc<dyn Fn(Option<Expr>)>` closure chains with a
//! flat message containing a path through the expression tree. The
//! `apply_at_path` function walks the path, reconstructing each level
//! with the modification.

use arcstr::ArcStr;
use fxhash::{FxHashMap, FxHashSet};
use graphix_compiler::expr::{Expr, ExprKind, StructExpr};
use netidx::utils::Either;
use triomphe::Arc;

// ---- Path segments ----

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum PathSegment {
    /// Unwrap/wrap `&expr`
    ByRef,
    /// Navigate into a struct field by name
    StructField(ArcStr),
    /// Navigate into an array element by index
    ArrayIndex(usize),
    /// Navigate into a tuple element by index
    TupleIndex(usize),
    /// Navigate into a variant argument by index
    VariantArg(usize),
    /// Navigate into a lambda body
    LambdaBody,
    /// Navigate into a map entry's value by key index
    MapValueIndex(usize),
    /// Navigate into a constructor's labeled argument
    Constructor(ArcStr),
}

// ---- Actions ----

#[derive(Debug, Clone)]
pub(crate) enum PropAction {
    /// Text input content changed (keystroke-by-keystroke, NOT committed)
    TextChanged(String),
    /// Text input committed (Enter pressed) — parse and update Expr
    TextCommit,
    /// Boolean toggled
    BoolSet(bool),
    /// Variant with args — tag changed, generate defaults for args
    VariantSelected(ArcStr),
    /// Argless variant selected — direct set
    ArglessVariantSelected(ArcStr),
    /// Array: append default element at end
    ArrayAdd,
    /// Array: remove element at index
    ArrayRemove(usize),
    /// Map: add a new key-value pair
    MapAdd,
    /// Map: remove entry at index
    MapRemove(usize),
    /// Map: key text changed at index (keystroke)
    MapKeyChanged(usize, String),
    /// Map: key text committed at index (Enter)
    MapKeyCommit(usize),
    /// Toggle section collapsed/expanded
    ToggleSection,
    /// Clear (remove arg / set to None)
    Clear,
}

// ---- Editor UI state ----

/// Lightweight UI-only state per tree node. Does NOT store expression
/// values — those live in TreeNodeData.args.
#[derive(Default)]
pub(crate) struct EditorUiState {
    /// Paths of collapsed sections (default is expanded)
    pub collapsed: FxHashSet<Vec<PathSegment>>,
    /// In-progress text edits before commit (keyed by path)
    pub text_inputs: FxHashMap<Vec<PathSegment>, String>,
}

// ---- Path-based Expr update ----

/// Top-level: apply a change at the given path within a node's args.
pub(crate) fn apply_at_path(
    args: &mut Vec<(ArcStr, Expr)>,
    arg_label: &ArcStr,
    path: &[PathSegment],
    new_value: Option<Expr>,
) {
    if path.is_empty() {
        // Base case: replace or remove the top-level arg
        match new_value {
            Some(expr) => {
                if let Some(entry) = args.iter_mut().find(|(l, _)| l == arg_label) {
                    entry.1 = expr;
                } else {
                    args.push((arg_label.clone(), expr));
                }
            }
            None => args.retain(|(l, _)| l != arg_label),
        }
        return;
    }

    let current = args.iter().find(|(l, _)| l == arg_label).map(|(_, e)| e.clone());
    let updated = update_expr_at_path(current, path, new_value);

    match updated {
        Some(expr) => {
            if let Some(entry) = args.iter_mut().find(|(l, _)| l == arg_label) {
                entry.1 = expr;
            } else {
                args.push((arg_label.clone(), expr));
            }
        }
        None => args.retain(|(l, _)| l != arg_label),
    }
}

/// Recursive: walk the path through an expression tree, reconstruct
/// each level with the modification.
pub(crate) fn update_expr_at_path(
    current: Option<Expr>,
    path: &[PathSegment],
    new_value: Option<Expr>,
) -> Option<Expr> {
    if path.is_empty() {
        return new_value;
    }

    match &path[0] {
        PathSegment::ByRef => {
            let inner = current.and_then(|e| match e.kind {
                ExprKind::ByRef(ref inner) => Some(inner.as_ref().clone()),
                _ => Some(e),
            });
            update_expr_at_path(inner, &path[1..], new_value)
                .map(|e| ExprKind::ByRef(Arc::new(e)).to_expr_nopos())
        }

        PathSegment::StructField(name) => {
            let mut fields = extract_struct_fields(current.as_ref());
            let sub = fields.iter()
                .find(|(n, _)| n == name)
                .map(|(_, e)| e.clone());
            let updated = update_expr_at_path(sub, &path[1..], new_value);
            match updated {
                Some(expr) => {
                    if let Some(entry) = fields.iter_mut().find(|(n, _)| n == name) {
                        entry.1 = expr;
                    } else {
                        fields.push((name.clone(), expr));
                    }
                }
                None => fields.retain(|(n, _)| n != name),
            }
            if fields.is_empty() {
                None
            } else {
                Some(ExprKind::Struct(StructExpr {
                    args: Arc::from(fields),
                }).to_expr_nopos())
            }
        }

        PathSegment::ArrayIndex(i) => {
            let mut elems = extract_array_elems(current.as_ref());
            if *i < elems.len() {
                let sub = Some(elems[*i].clone());
                match update_expr_at_path(sub, &path[1..], new_value) {
                    Some(expr) => elems[*i] = expr,
                    None => { elems.remove(*i); }
                }
            }
            if elems.is_empty() {
                None
            } else {
                Some(ExprKind::Array { args: Arc::from(elems) }.to_expr_nopos())
            }
        }

        PathSegment::TupleIndex(i) => {
            let mut elems = extract_tuple_elems(current.as_ref());
            while elems.len() <= *i {
                elems.push(ExprKind::Constant(netidx::publisher::Value::Null).to_expr_nopos());
            }
            if let Some(expr) = update_expr_at_path(
                Some(elems[*i].clone()), &path[1..], new_value,
            ) {
                elems[*i] = expr;
            }
            Some(ExprKind::Tuple { args: Arc::from(elems) }.to_expr_nopos())
        }

        PathSegment::VariantArg(i) => {
            let (tag, mut args) = extract_variant(current.as_ref());
            if let Some(tag) = tag {
                while args.len() <= *i {
                    args.push(ExprKind::Constant(netidx::publisher::Value::Null).to_expr_nopos());
                }
                if let Some(expr) = update_expr_at_path(
                    Some(args[*i].clone()), &path[1..], new_value,
                ) {
                    args[*i] = expr;
                }
                Some(ExprKind::Variant {
                    tag,
                    args: Arc::from(args),
                }.to_expr_nopos())
            } else {
                None
            }
        }

        PathSegment::MapValueIndex(i) => {
            let mut entries = extract_map_entries(current.as_ref());
            if *i < entries.len() {
                let sub = Some(entries[*i].1.clone());
                match update_expr_at_path(sub, &path[1..], new_value) {
                    Some(expr) => entries[*i].1 = expr,
                    None => { entries.remove(*i); }
                }
            }
            if entries.is_empty() {
                None
            } else {
                Some(ExprKind::Map {
                    args: Arc::from(entries),
                }.to_expr_nopos())
            }
        }

        PathSegment::LambdaBody => {
            // For lambda body editing, we reconstruct the full lambda
            // with the new body expression
            match current {
                Some(e) => match &e.kind {
                    ExprKind::Lambda(lambda) => {
                        let current_body = match &lambda.body {
                            Either::Left(expr) => Some(expr.clone()),
                            Either::Right(_) => None,
                        };
                        let new_body = match update_expr_at_path(
                            current_body, &path[1..], new_value,
                        ) {
                            Some(b) => b,
                            None => return None,
                        };
                        let mut new_lambda = lambda.as_ref().clone();
                        new_lambda.body = Either::Left(new_body);
                        Some(ExprKind::Lambda(Arc::new(new_lambda)).to_expr_nopos())
                    }
                    _ => new_value,
                },
                None => new_value,
            }
        }

        PathSegment::Constructor(field_name) => {
            // Navigate into a constructor call's labeled argument
            let mut labeled_args = extract_apply_args(current.as_ref());
            let sub = labeled_args.iter()
                .find(|(n, _)| n == field_name)
                .map(|(_, e)| e.clone());
            let updated = update_expr_at_path(sub, &path[1..], new_value);
            match updated {
                Some(expr) => {
                    if let Some(entry) = labeled_args.iter_mut().find(|(n, _)| n == field_name) {
                        entry.1 = expr;
                    } else {
                        labeled_args.push((field_name.clone(), expr));
                    }
                }
                None => labeled_args.retain(|(n, _)| n != field_name),
            }
            // Reconstruct the Apply expression
            // For now, we can't fully reconstruct without the function ref,
            // so fall back to building a struct if we lost the function
            if labeled_args.is_empty() {
                None
            } else {
                // Try to preserve the original Apply if possible
                match current {
                    Some(e) => match &e.kind {
                        ExprKind::Apply(apply) => {
                            let new_args: Vec<(Option<ArcStr>, Expr)> = labeled_args
                                .into_iter()
                                .map(|(l, e)| (Some(l), e))
                                .collect();
                            Some(ExprKind::Apply(graphix_compiler::expr::ApplyExpr {
                                function: apply.function.clone(),
                                args: Arc::from(new_args),
                            }).to_expr_nopos())
                        }
                        _ => Some(ExprKind::Struct(StructExpr {
                            args: Arc::from(labeled_args),
                        }).to_expr_nopos()),
                    },
                    None => Some(ExprKind::Struct(StructExpr {
                        args: Arc::from(labeled_args),
                    }).to_expr_nopos()),
                }
            }
        }
    }
}

// ---- Helper extractors ----

pub(crate) fn extract_struct_fields(expr: Option<&Expr>) -> Vec<(ArcStr, Expr)> {
    match expr {
        Some(e) => match &e.kind {
            ExprKind::Struct(se) => se.args.iter()
                .map(|(n, e)| (n.clone(), e.clone()))
                .collect(),
            ExprKind::ByRef(inner) => extract_struct_fields(Some(inner)),
            _ => vec![],
        },
        None => vec![],
    }
}

pub(crate) fn extract_array_elems(expr: Option<&Expr>) -> Vec<Expr> {
    match expr {
        Some(e) => match &e.kind {
            ExprKind::Array { args } => args.iter().cloned().collect(),
            ExprKind::ByRef(inner) => extract_array_elems(Some(inner)),
            _ => vec![],
        },
        None => vec![],
    }
}

pub(crate) fn extract_tuple_elems(expr: Option<&Expr>) -> Vec<Expr> {
    match expr {
        Some(e) => match &e.kind {
            ExprKind::Tuple { args } => args.iter().cloned().collect(),
            ExprKind::ByRef(inner) => extract_tuple_elems(Some(inner)),
            _ => vec![],
        },
        None => vec![],
    }
}

pub(crate) fn extract_variant(expr: Option<&Expr>) -> (Option<ArcStr>, Vec<Expr>) {
    match expr {
        Some(e) => match &e.kind {
            ExprKind::Variant { tag, args } => {
                (Some(tag.clone()), args.iter().cloned().collect())
            }
            ExprKind::ByRef(inner) => extract_variant(Some(inner)),
            _ => (None, vec![]),
        },
        None => (None, vec![]),
    }
}

pub(crate) fn extract_map_entries(expr: Option<&Expr>) -> Vec<(Expr, Expr)> {
    match expr {
        Some(e) => match &e.kind {
            ExprKind::Map { args } => args.iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            ExprKind::ByRef(inner) => extract_map_entries(Some(inner)),
            _ => vec![],
        },
        None => vec![],
    }
}

pub(crate) fn extract_apply_args(expr: Option<&Expr>) -> Vec<(ArcStr, Expr)> {
    match expr {
        Some(e) => match &e.kind {
            ExprKind::Apply(apply) => apply.args.iter()
                .filter_map(|(label, e)| label.as_ref().map(|l| (l.clone(), e.clone())))
                .collect(),
            ExprKind::ByRef(inner) => extract_apply_args(Some(inner)),
            _ => vec![],
        },
        None => vec![],
    }
}

//! Type-driven property panel using recursive type dispatch.
//!
//! Each editor emits `DesignMsg::PropEdit` with a path encoding the
//! nesting location in the expression tree. The centralized handler
//! in main.rs uses `apply_at_path` to update the Expr.

use super::path_update::{EditorUiState, PathSegment, PropAction};
use super::tree_model::{TreeNodeData, TreeNodeId};
use crate::DesignMsg;
use arcstr::ArcStr;
use graphix_compiler::{
    env::Env,
    expr::{Expr, ExprKind},
    typ::{FnType, Type},
};
use graphix_package_gui::{theme::GraphixTheme, widgets::Renderer};
use iced_core::Color;
use netidx::publisher::Value;

type Element<'a> = iced_core::Element<'a, DesignMsg, GraphixTheme, Renderer>;

/// Build the property panel for the selected node.
pub(crate) fn view<'a>(
    node_id: TreeNodeId,
    data: &'a TreeNodeData,
    env: Option<&'a Env>,
    ui_state: &'a EditorUiState,
) -> Element<'a> {
    let header: Element<'a> = iced_widget::text(format!("{}", data.kind))
        .size(16)
        .into();

    let mut col = iced_widget::Column::new()
        .push(header)
        .push(iced_widget::rule::horizontal::<'_, GraphixTheme>(1))
        .spacing(4)
        .padding(iced_core::Padding::from([4, 8]));

    match &data.fn_type {
        Some(ft) => {
            for farg in ft.args.iter() {
                let (label, optional) = match &farg.label {
                    Some((l, opt)) => (l.clone(), *opt),
                    None => continue,
                };
                if type_contains_widget(&farg.typ) {
                    continue;
                }
                let arg_expr = data.args.iter()
                    .find(|(l, _)| *l == label)
                    .map(|(_, e)| e);
                let path = vec![];
                col = col.push(
                    arg_row(node_id, &label, &path, &farg.typ, arg_expr, env, ui_state, optional)
                );
            }
        }
        None => {
            for (label, expr) in &data.args {
                let path = vec![];
                col = col.push(
                    arg_row(node_id, label, &path, &Type::Any, Some(expr), env, ui_state, false)
                );
            }
        }
    }

    col.into()
}

/// Build a single arg row: label + type-dispatched editor.
fn arg_row<'a>(
    node_id: TreeNodeId,
    label: &ArcStr,
    path: &[PathSegment],
    typ: &Type,
    expr: Option<&Expr>,
    env: Option<&'a Env>,
    ui_state: &'a EditorUiState,
    optional: bool,
) -> Element<'a> {
    let type_str = format!("{}", typ);

    let label_el: Element<'a> = iced_widget::Tooltip::new(
        iced_widget::text(format!("#{label}"))
            .size(12)
            .color(Color::from_rgb(0.8, 0.5, 0.9))
            .width(130.0),
        iced_widget::text(type_str).size(11),
        iced_widget::tooltip::Position::Top,
    ).into();

    let editor = type_editor(node_id, label, path, typ, expr, env, ui_state, optional);

    // Edit button — navigates to the expression in the source editor.
    // If the arg doesn't exist yet, clicking creates a default first.
    let edit_msg = match expr {
        Some(e) => DesignMsg::EditInSource {
            line: e.pos.line,
            column: e.pos.column,
            expr_text: format!("{}", e),
        },
        None => DesignMsg::EditInSourceWithDefault {
            node_id,
            arg: label.clone(),
            typ: typ.clone(),
        },
    };
    let edit_btn: Element<'a> = iced_widget::Button::new(
        iced_widget::text("Edit").size(10)
    )
    .on_press(edit_msg)
    .padding(iced_core::Padding::from([1, 6]))
    .style(|_t: &GraphixTheme, _s| iced_widget::button::Style {
        background: None,
        text_color: Color::from_rgb(0.5, 0.6, 0.8),
        border: iced_core::Border::default(),
        shadow: iced_core::Shadow::default(),
        ..Default::default()
    })
    .into();

    iced_widget::Row::new()
        .push(label_el)
        .push(editor)
        .push(edit_btn)
        .spacing(4)
        .align_y(iced_core::Alignment::Center)
        .into()
}

// ---- Recursive type dispatch ----

fn type_editor<'a>(
    node_id: TreeNodeId,
    arg: &ArcStr,
    path: &[PathSegment],
    typ: &Type,
    expr: Option<&Expr>,
    env: Option<&'a Env>,
    ui_state: &'a EditorUiState,
    optional: bool,
) -> Element<'a> {
    match typ {
        // ByRef: unwrap and recurse
        Type::ByRef(inner) => {
            let inner_expr = expr.and_then(|e| match &e.kind {
                ExprKind::ByRef(i) => Some(i.as_ref()),
                _ => Some(e),
            });
            let mut p = path.to_vec();
            p.push(PathSegment::ByRef);
            type_editor(node_id, arg, &p, inner, inner_expr, env, ui_state, optional)
        }

        // Nullable set: unwrap and recurse with optional=true
        Type::Set(variants) if is_nullable_set(variants) => {
            let inner_expr = expr.and_then(|e| {
                if matches!(&e.kind, ExprKind::Constant(Value::Null)) {
                    None
                } else {
                    Some(e)
                }
            });
            let inner_type = non_null_type(variants);
            type_editor(node_id, arg, path, &inner_type, inner_expr, env, ui_state, true)
        }

        // Expression exists but is a complex expression (function call, binary
        // op, etc.) — show as text entry. When no expression exists, we always
        // build from the type so the user can fill fields via GUI.
        _ if expr.map_or(false, |e| !is_gui_editable(e)) => {
            text_entry(node_id, arg, path, expr, ui_state, optional)
        }

        // Function type: callback editor (body only)
        Type::Fn(ft) => {
            callback_editor(node_id, arg, path, ft, expr, ui_state)
        }

        // Argless variants: pick_list
        Type::Set(variants) if all_argless_variants(variants) => {
            argless_variant_editor(node_id, arg, path, variants, expr, optional)
        }

        // Variants with args: pick_list + sub-editors
        Type::Set(variants) if all_variants(variants) => {
            variant_editor(node_id, arg, path, variants, expr, env, ui_state, optional)
        }

        // Bool
        _ if is_bool_type(typ) => {
            bool_editor(node_id, arg, path, expr, optional)
        }

        // Struct (inline)
        Type::Struct(fields) => {
            struct_editor(node_id, arg, path, fields, expr, env, ui_state)
        }

        // Array
        Type::Array(elem) => {
            array_editor(node_id, arg, path, elem, expr, env, ui_state)
        }

        // Map
        Type::Map { key, value } => {
            map_editor(node_id, arg, path, key, value, expr, env, ui_state)
        }

        // Tuple
        Type::Tuple(elems) => {
            tuple_editor(node_id, arg, path, elems, expr, env, ui_state)
        }

        // Ref: resolve and recurse
        Type::Ref { .. } => {
            if let Some(env) = env {
                if let Ok(resolved) = typ.lookup_ref(env) {
                    return type_editor(node_id, arg, path, &resolved, expr, env.into(), ui_state, optional);
                }
            }
            text_entry(node_id, arg, path, expr, ui_state, optional)
        }

        // Fallback: text entry
        _ => text_entry(node_id, arg, path, expr, ui_state, optional),
    }
}

// ---- Specific editors ----

fn text_entry<'a>(
    node_id: TreeNodeId,
    arg: &ArcStr,
    path: &[PathSegment],
    expr: Option<&Expr>,
    ui_state: &'a EditorUiState,
    optional: bool,
) -> Element<'a> {
    let path_vec = path.to_vec();
    let field_key = (arg.clone(), path_vec.clone());
    // Use in-progress text if exists, otherwise format from Expr
    let display = ui_state.text_inputs.get(&field_key)
        .cloned()
        .unwrap_or_else(|| expr.map(|e| format!("{e}")).unwrap_or_default());
    let placeholder = if optional { "optional" } else { "" };
    let has_error = ui_state.parse_errors.contains_key(&field_key);
    let error_msg = ui_state.parse_errors.get(&field_key).cloned();
    let arg_c = arg.clone();
    let path_for_input = path_vec.clone();
    let path_for_submit = path_vec;
    let arg_submit = arg.clone();

    let input: Element<'a> = iced_widget::TextInput::new(placeholder, &display)
        .on_input(move |text| DesignMsg::PropEdit {
            node_id,
            arg: arg_c.clone(),
            path: path_for_input.clone(),
            action: PropAction::TextChanged(text),
        })
        .on_submit(DesignMsg::PropEdit {
            node_id,
            arg: arg_submit.clone(),
            path: path_for_submit.clone(),
            action: PropAction::TextCommit,
        })
        .size(12)
        .padding(iced_core::Padding::from([3, 6]))
        .width(iced_core::Length::Fill)
        .style(move |theme: &GraphixTheme, status| {
            let mut style = iced_widget::text_input::default(&theme.inner, status);
            style.value = if has_error {
                Color::from_rgb(1.0, 0.4, 0.4)
            } else {
                Color::WHITE
            };
            style
        })
        .into();

    // Wrap in tooltip showing parse error if present
    if let Some(err) = error_msg {
        iced_widget::Tooltip::new(
            input,
            iced_widget::text(err).size(11),
            iced_widget::tooltip::Position::Top,
        ).into()
    } else {
        input
    }
}

fn bool_editor<'a>(
    node_id: TreeNodeId,
    arg: &ArcStr,
    path: &[PathSegment],
    expr: Option<&Expr>,
    optional: bool,
) -> Element<'a> {
    let path_vec = path.to_vec();
    if optional {
        let options = vec!["".to_string(), "true".to_string(), "false".to_string()];
        let selected = expr.and_then(|e| match &e.kind {
            ExprKind::Constant(Value::Bool(true)) => Some("true".to_string()),
            ExprKind::Constant(Value::Bool(false)) => Some("false".to_string()),
            _ => None,
        });
        let arg_c = arg.clone();
        iced_widget::PickList::new(options, selected, move |v| {
            let action = match v.as_str() {
                "true" => PropAction::BoolSet(true),
                "false" => PropAction::BoolSet(false),
                _ => PropAction::Clear,
            };
            DesignMsg::PropEdit { node_id, arg: arg_c.clone(), path: path_vec.clone(), action }
        })
        .text_size(12)
        .width(iced_core::Length::Fill)
        .into()
    } else {
        let checked = matches!(expr, Some(e) if matches!(&e.kind, ExprKind::Constant(Value::Bool(true))));
        let arg_c = arg.clone();
        iced_widget::Checkbox::new(checked)
            .on_toggle(move |v| DesignMsg::PropEdit {
                node_id,
                arg: arg_c.clone(),
                path: path_vec.clone(),
                action: PropAction::BoolSet(v),
            })
            .size(16)
            .into()
    }
}

fn callback_editor<'a>(
    node_id: TreeNodeId,
    arg: &ArcStr,
    path: &[PathSegment],
    ft: &triomphe::Arc<FnType>,
    expr: Option<&Expr>,
    ui_state: &'a EditorUiState,
) -> Element<'a> {
    let sig = build_lambda_sig(ft);
    let body = extract_lambda_body(expr);
    let mut body_path = path.to_vec();
    body_path.push(PathSegment::LambdaBody);

    let body_key = (arg.clone(), body_path.clone());
    let display = ui_state.text_inputs.get(&body_key)
        .cloned()
        .unwrap_or(body);

    let sig_owned = sig.clone();
    let sig_label: Element<'a> = iced_widget::text(sig_owned)
        .size(11)
        .color(Color::from_rgb(0.5, 0.6, 0.7))
        .into();

    let arg_c = arg.clone();
    let path_input = body_path.clone();
    let arg_submit = arg.clone();
    let path_submit = body_path;

    let body_input: Element<'a> = iced_widget::TextInput::new("body expression", &display)
        .on_input(move |text| DesignMsg::PropEdit {
            node_id,
            arg: arg_c.clone(),
            path: path_input.clone(),
            action: PropAction::TextChanged(text),
        })
        .on_submit(DesignMsg::PropEdit {
            node_id,
            arg: arg_submit.clone(),
            path: path_submit.clone(),
            action: PropAction::TextCommit,
        })
        .size(12)
        .padding(iced_core::Padding::from([3, 6]))
        .width(iced_core::Length::Fill)
        .style(|theme: &GraphixTheme, status| {
            let mut style = iced_widget::text_input::default(&theme.inner, status);
            style.value = Color::WHITE;
            style
        })
        .into();

    iced_widget::Column::new()
        .push(sig_label)
        .push(body_input)
        .spacing(2)
        .width(iced_core::Length::Fill)
        .into()
}

fn argless_variant_editor<'a>(
    node_id: TreeNodeId,
    arg: &ArcStr,
    path: &[PathSegment],
    variants: &[Type],
    expr: Option<&Expr>,
    optional: bool,
) -> Element<'a> {
    let path_vec = path.to_vec();
    let mut options: Vec<String> = Vec::new();
    if optional { options.push(String::new()); }
    for v in variants {
        if let Type::Variant(tag, _) = v {
            options.push(format!("`{tag}"));
        }
    }
    let selected = expr.and_then(|e| match &e.kind {
        ExprKind::Variant { tag, .. } => Some(format!("`{tag}")),
        _ => None,
    });
    let arg_c = arg.clone();
    iced_widget::PickList::new(options, selected, move |v| {
        let action = if v.is_empty() {
            PropAction::Clear
        } else if v.starts_with('`') {
            PropAction::ArglessVariantSelected(ArcStr::from(&v[1..]))
        } else {
            PropAction::Clear
        };
        DesignMsg::PropEdit { node_id, arg: arg_c.clone(), path: path_vec.clone(), action }
    })
    .text_size(12)
    .width(iced_core::Length::Fill)
    .into()
}

fn variant_editor<'a>(
    node_id: TreeNodeId,
    arg: &ArcStr,
    path: &[PathSegment],
    variants: &[Type],
    expr: Option<&Expr>,
    env: Option<&'a Env>,
    ui_state: &'a EditorUiState,
    optional: bool,
) -> Element<'a> {
    let path_vec = path.to_vec();
    // Tag dropdown
    let mut options: Vec<String> = Vec::new();
    if optional { options.push(String::new()); }
    for v in variants {
        if let Type::Variant(tag, _) = v {
            options.push(format!("`{tag}"));
        }
    }
    let (current_tag, current_args) = match expr {
        Some(e) => match &e.kind {
            ExprKind::Variant { tag, args } => {
                (Some(format!("`{tag}")), args.iter().collect::<Vec<_>>())
            }
            _ => (None, vec![]),
        },
        None => (None, vec![]),
    };
    let arg_c = arg.clone();
    let path_c = path_vec.clone();
    let tag_picker: Element<'a> = iced_widget::PickList::new(
        options, current_tag, move |v| {
            let action = if v.is_empty() {
                PropAction::Clear
            } else if v.starts_with('`') {
                PropAction::VariantSelected(ArcStr::from(&v[1..]))
            } else {
                PropAction::Clear
            };
            DesignMsg::PropEdit { node_id, arg: arg_c.clone(), path: path_c.clone(), action }
        },
    )
    .text_size(12)
    .width(iced_core::Length::Fill)
    .into();

    let mut col = iced_widget::Column::new()
        .push(tag_picker)
        .spacing(4)
        .width(iced_core::Length::Fill);

    // Sub-editors for variant args
    if let Some(e) = expr {
        if let ExprKind::Variant { tag, args } = &e.kind {
            let variant_type = variants.iter()
                .find(|v| matches!(v, Type::Variant(t, _) if t == tag));
            if let Some(Type::Variant(_, arg_types)) = variant_type {
                for (i, arg_type) in arg_types.iter().enumerate() {
                    let sub_expr = args.get(i);
                    let mut sub_path = path_vec.clone();
                    sub_path.push(PathSegment::VariantArg(i));
                    let label: Element<'a> = iced_widget::text(format!("{i}:"))
                        .size(11)
                        .color(Color::from_rgb(0.6, 0.6, 0.6))
                        .width(25.0)
                        .into();
                    let sub_ed = type_editor(
                        node_id, arg, &sub_path, arg_type, sub_expr, env, ui_state, false,
                    );
                    let row: Element<'a> = iced_widget::Row::new()
                        .push(label)
                        .push(sub_ed)
                        .spacing(4)
                        .into();
                    col = col.push(row);
                }
            }
        }
    }

    col.into()
}

fn struct_editor<'a>(
    node_id: TreeNodeId,
    arg: &ArcStr,
    path: &[PathSegment],
    fields: &[(ArcStr, Type)],
    expr: Option<&Expr>,
    env: Option<&'a Env>,
    ui_state: &'a EditorUiState,
) -> Element<'a> {
    let current_fields: Vec<(ArcStr, Expr)> = super::path_update::extract_struct_fields(expr);
    let collapsed = ui_state.collapsed.contains(&(arg.clone(), path.to_vec()));

    let arg_c = arg.clone();
    let path_c = path.to_vec();
    let toggle_label = if collapsed { "\u{25B8} fields" } else { "\u{25BE} fields" };
    let toggle: Element<'a> = iced_widget::Button::new(
        iced_widget::text(toggle_label).size(11)
    )
    .on_press(DesignMsg::PropEdit {
        node_id, arg: arg_c, path: path_c, action: PropAction::ToggleSection,
    })
    .padding(iced_core::Padding::from([1, 4]))
    .style(|_t, _s| iced_widget::button::Style {
        background: None,
        text_color: Color::from_rgb(0.6, 0.7, 0.8),
        border: iced_core::Border::default(),
        shadow: iced_core::Shadow::default(),
        ..Default::default()
    })
    .into();

    let mut col = iced_widget::Column::new()
        .push(toggle)
        .spacing(3)
        .width(iced_core::Length::Fill);

    if !collapsed {
        for (name, typ) in fields {
            let sub_expr = current_fields.iter()
                .find(|(n, _)| n == name)
                .map(|(_, e)| e);
            let mut sub_path = path.to_vec();
            sub_path.push(PathSegment::StructField(name.clone()));
            let label: Element<'a> = iced_widget::Tooltip::new(
                iced_widget::text(format!("{name}:"))
                    .size(11)
                    .color(Color::from_rgb(0.6, 0.6, 0.6))
                    .width(80.0),
                iced_widget::text(format!("{typ}")).size(11),
                iced_widget::tooltip::Position::Top,
            ).into();
            let sub_ed = type_editor(
                node_id, arg, &sub_path, typ, sub_expr, env, ui_state, true,
            );
            let row: Element<'a> = iced_widget::Row::new()
                .push(label)
                .push(sub_ed)
                .spacing(4)
                .into();
            col = col.push(row);
        }
    }

    col.into()
}

fn array_editor<'a>(
    node_id: TreeNodeId,
    arg: &ArcStr,
    path: &[PathSegment],
    elem_type: &Type,
    expr: Option<&Expr>,
    env: Option<&'a Env>,
    ui_state: &'a EditorUiState,
) -> Element<'a> {
    let elems = super::path_update::extract_array_elems(expr);
    let path_vec = path.to_vec();

    let arg_add = arg.clone();
    let path_add = path_vec.clone();
    let add_btn: Element<'a> = iced_widget::Button::new(
        iced_widget::text("+").size(12)
    )
    .on_press(DesignMsg::PropEdit {
        node_id, arg: arg_add, path: path_add, action: PropAction::ArrayAdd,
    })
    .padding(iced_core::Padding::from([1, 8]))
    .into();

    let mut col = iced_widget::Column::new()
        .spacing(3)
        .width(iced_core::Length::Fill);

    for (i, elem) in elems.iter().enumerate() {
        let mut sub_path = path_vec.clone();
        sub_path.push(PathSegment::ArrayIndex(i));
        let sub_ed = type_editor(
            node_id, arg, &sub_path, elem_type, Some(elem), env, ui_state, false,
        );
        let arg_rm = arg.clone();
        let path_rm = path_vec.clone();
        let remove_btn: Element<'a> = iced_widget::Button::new(
            iced_widget::text("-").size(12)
        )
        .on_press(DesignMsg::PropEdit {
            node_id, arg: arg_rm, path: path_rm, action: PropAction::ArrayRemove(i),
        })
        .padding(iced_core::Padding::from([1, 6]))
        .into();
        let row: Element<'a> = iced_widget::Row::new()
            .push(sub_ed)
            .push(remove_btn)
            .spacing(4)
            .into();
        col = col.push(row);
    }
    col = col.push(add_btn);

    col.into()
}

fn map_editor<'a>(
    node_id: TreeNodeId,
    arg: &ArcStr,
    path: &[PathSegment],
    _key_type: &Type,
    value_type: &Type,
    expr: Option<&Expr>,
    env: Option<&'a Env>,
    ui_state: &'a EditorUiState,
) -> Element<'a> {
    let entries = super::path_update::extract_map_entries(expr);
    let path_vec = path.to_vec();

    let arg_add = arg.clone();
    let path_add = path_vec.clone();
    let add_btn: Element<'a> = iced_widget::Button::new(
        iced_widget::text("+").size(12)
    )
    .on_press(DesignMsg::PropEdit {
        node_id, arg: arg_add, path: path_add, action: PropAction::MapAdd,
    })
    .padding(iced_core::Padding::from([1, 8]))
    .into();

    let mut col = iced_widget::Column::new()
        .spacing(3)
        .width(iced_core::Length::Fill);

    for (i, (key_expr, _val_expr)) in entries.iter().enumerate() {
        // Key input
        let key_display = ui_state.text_inputs
            .get(&{
                let mut p = path_vec.clone();
                p.push(PathSegment::MapValueIndex(i));
                p.push(PathSegment::StructField(arcstr::literal!("__key__")));
                (arg.clone(), p)
            })
            .cloned()
            .unwrap_or_else(|| format!("{}", key_expr));

        let arg_k = arg.clone();
        let path_k = path_vec.clone();
        let arg_kc = arg.clone();
        let path_kc = path_vec.clone();
        let key_input: Element<'a> = iced_widget::TextInput::new("key", &key_display)
            .on_input(move |text| DesignMsg::PropEdit {
                node_id, arg: arg_k.clone(), path: path_k.clone(),
                action: PropAction::MapKeyChanged(i, text),
            })
            .on_submit(DesignMsg::PropEdit {
                node_id, arg: arg_kc.clone(), path: path_kc.clone(),
                action: PropAction::MapKeyCommit(i),
            })
            .size(11)
            .padding(iced_core::Padding::from([2, 4]))
            .width(iced_core::Length::FillPortion(1))
            .style(|theme: &GraphixTheme, status| {
                let mut style = iced_widget::text_input::default(&theme.inner, status);
                style.value = Color::WHITE;
                style
            })
            .into();

        // Value editor
        let mut val_path = path_vec.clone();
        val_path.push(PathSegment::MapValueIndex(i));
        let val_ed = type_editor(
            node_id, arg, &val_path, value_type, Some(&entries[i].1), env, ui_state, false,
        );

        // Remove button
        let arg_rm = arg.clone();
        let path_rm = path_vec.clone();
        let remove_btn: Element<'a> = iced_widget::Button::new(
            iced_widget::text("-").size(12)
        )
        .on_press(DesignMsg::PropEdit {
            node_id, arg: arg_rm, path: path_rm, action: PropAction::MapRemove(i),
        })
        .padding(iced_core::Padding::from([1, 6]))
        .into();

        let row: Element<'a> = iced_widget::Row::new()
            .push(key_input)
            .push(iced_widget::text("=>").size(11))
            .push(iced_widget::container(val_ed).width(iced_core::Length::FillPortion(2)))
            .push(remove_btn)
            .spacing(4)
            .align_y(iced_core::Alignment::Center)
            .into();
        col = col.push(row);
    }
    col = col.push(add_btn);

    col.into()
}

fn tuple_editor<'a>(
    node_id: TreeNodeId,
    arg: &ArcStr,
    path: &[PathSegment],
    elem_types: &[Type],
    expr: Option<&Expr>,
    env: Option<&'a Env>,
    ui_state: &'a EditorUiState,
) -> Element<'a> {
    let elems = super::path_update::extract_tuple_elems(expr);

    let mut col = iced_widget::Column::new()
        .spacing(3)
        .width(iced_core::Length::Fill);

    for (i, typ) in elem_types.iter().enumerate() {
        let sub_expr = elems.get(i);
        let mut sub_path = path.to_vec();
        sub_path.push(PathSegment::TupleIndex(i));
        let label: Element<'a> = iced_widget::text(format!("{i}:"))
            .size(11)
            .color(Color::from_rgb(0.6, 0.6, 0.6))
            .width(25.0)
            .into();
        let sub_ed = type_editor(
            node_id, arg, &sub_path, typ, sub_expr, env, ui_state, false,
        );
        let row: Element<'a> = iced_widget::Row::new()
            .push(label)
            .push(sub_ed)
            .spacing(4)
            .into();
        col = col.push(row);
    }

    col.into()
}

// ---- Type helpers ----

fn type_contains_widget(typ: &Type) -> bool {
    match typ {
        Type::ByRef(inner) => type_contains_widget(inner),
        Type::Array(inner) => type_contains_widget(inner),
        Type::Ref { name, .. } => {
            let s = name.0.as_ref();
            s.ends_with("Widget") || s.ends_with("/Widget")
        }
        Type::Set(variants) => variants.iter().any(|v| type_contains_widget(v)),
        _ => false,
    }
}

/// Generate a default expression for a given type.
/// Used by ArrayAdd, MapAdd, and variant sub-editor initialization.
pub(crate) fn default_expr_for_type(typ: &Type, env: Option<&Env>) -> Expr {
    match typ {
        Type::ByRef(inner) => {
            let inner_default = default_expr_for_type(inner, env);
            ExprKind::ByRef(triomphe::Arc::new(inner_default)).to_expr_nopos()
        }
        Type::Primitive(flags) if flags.contains(netidx::publisher::Typ::Bool) => {
            ExprKind::Constant(Value::Bool(false)).to_expr_nopos()
        }
        Type::Primitive(flags) if flags.contains(netidx::publisher::Typ::String) => {
            ExprKind::Constant(Value::String(arcstr::literal!(""))).to_expr_nopos()
        }
        Type::Primitive(_) => {
            ExprKind::Constant(Value::Null).to_expr_nopos()
        }
        Type::Set(variants) if is_nullable_set(variants) => {
            ExprKind::Constant(Value::Null).to_expr_nopos()
        }
        Type::Set(variants) if !variants.is_empty() => {
            // Pick the first variant
            if let Some(Type::Variant(tag, args)) = variants.first() {
                let default_args: Vec<Expr> = args.iter()
                    .map(|t| default_expr_for_type(t, env))
                    .collect();
                ExprKind::Variant {
                    tag: tag.clone(),
                    args: triomphe::Arc::from(default_args),
                }.to_expr_nopos()
            } else {
                ExprKind::Constant(Value::Null).to_expr_nopos()
            }
        }
        Type::Struct(fields) => {
            let field_exprs: Vec<(ArcStr, Expr)> = fields.iter()
                .map(|(name, typ)| (name.clone(), default_expr_for_type(typ, env)))
                .collect();
            ExprKind::Struct(graphix_compiler::expr::StructExpr {
                args: triomphe::Arc::from(field_exprs),
            }).to_expr_nopos()
        }
        Type::Array(_) => {
            ExprKind::Array { args: triomphe::Arc::from(Vec::<Expr>::new()) }.to_expr_nopos()
        }
        Type::Map { .. } => {
            ExprKind::Map { args: triomphe::Arc::from(Vec::<(Expr, Expr)>::new()) }.to_expr_nopos()
        }
        Type::Tuple(elems) => {
            let elem_defaults: Vec<Expr> = elems.iter()
                .map(|t| default_expr_for_type(t, env))
                .collect();
            ExprKind::Tuple { args: triomphe::Arc::from(elem_defaults) }.to_expr_nopos()
        }
        Type::Ref { .. } => {
            // Try to resolve and generate default for the resolved type
            if let Some(env) = env {
                if let Ok(resolved) = typ.lookup_ref(env) {
                    return default_expr_for_type(&resolved, Some(env));
                }
            }
            ExprKind::Constant(Value::Null).to_expr_nopos()
        }
        _ => ExprKind::Constant(Value::Null).to_expr_nopos(),
    }
}

/// Check if an expression is simple enough for structured GUI editing.
/// Complex expressions (function calls, binary ops, etc.) should be
/// shown as text entries since the GUI can't decompose them.
fn is_gui_editable(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::Constant(_) => true,
        ExprKind::Variant { .. } => true,
        ExprKind::ByRef(inner) => is_gui_editable(inner),
        ExprKind::Struct(_) => true,
        ExprKind::Array { .. } => true,
        ExprKind::Tuple { .. } => true,
        ExprKind::Map { .. } => true,
        ExprKind::Lambda(_) => true,
        _ => false,
    }
}

fn is_bool_type(typ: &Type) -> bool {
    matches!(typ, Type::Primitive(flags) if flags.contains(netidx::publisher::Typ::Bool))
}

fn all_argless_variants(variants: &[Type]) -> bool {
    !variants.is_empty()
        && variants.iter().all(|v| matches!(v, Type::Variant(_, args) if args.is_empty()))
}

fn all_variants(variants: &[Type]) -> bool {
    !variants.is_empty()
        && variants.iter().all(|v| matches!(v, Type::Variant(_, _)))
}

fn is_null_type(typ: &Type) -> bool {
    match typ {
        Type::Primitive(flags) => flags.contains(netidx::publisher::Typ::Null),
        Type::Variant(tag, args) => tag.as_str() == "Null" && args.is_empty(),
        _ => false,
    }
}

fn is_nullable_set(variants: &[Type]) -> bool {
    variants.len() == 2
        && variants.iter().any(|v| is_null_type(v))
        && variants.iter().any(|v| !is_null_type(v))
}

fn non_null_type(variants: &[Type]) -> Type {
    variants.iter()
        .find(|v| !is_null_type(v))
        .cloned()
        .unwrap_or(Type::Any)
}

// ---- Lambda helpers ----

fn build_lambda_sig(ft: &FnType) -> String {
    let mut parts = Vec::new();
    for arg in ft.args.iter() {
        let name = match &arg.label {
            Some((l, _)) => format!("#{l}"),
            None => "_".to_string(),
        };
        parts.push(format!("{name}: {}", arg.typ));
    }
    format!("|{}|", parts.join(", "))
}

fn extract_lambda_body(expr: Option<&Expr>) -> String {
    match expr {
        Some(e) => match &e.kind {
            ExprKind::Lambda(lambda) => match &lambda.body {
                netidx::utils::Either::Left(body) => format!("{body}"),
                netidx::utils::Either::Right(s) => s.to_string(),
            },
            _ => format!("{e}"),
        },
        None => String::new(),
    }
}

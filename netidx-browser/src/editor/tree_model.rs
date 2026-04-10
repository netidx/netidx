//! Pure tree data model for the widget hierarchy.
//!
//! Stores a tree of widget nodes parsed from graphix source. Each node
//! has a widget kind (e.g. "column", "text"), property args, and child
//! slots describing how children map back to source args during
//! reconstruction.

use arcstr::ArcStr;
use graphix_compiler::{
    env::Env,
    expr::{ApplyExpr, Expr, ExprKind, ModPath},
    typ::{FnType, Type},
};
use std::sync::atomic::{AtomicU64, Ordering};
use triomphe::Arc as TArc;

// ---- IDs ----

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TreeNodeId(u64);

impl TreeNodeId {
    fn new() -> Self {
        Self(NEXT_ID.fetch_add(1, Ordering::Relaxed))
    }
}

// ---- Widget kinds (from graphix-package-gui) ----

/// Widget names as they appear in graphix source (lowercase).
/// Must match the gui package's compile() dispatch in widgets/mod.rs.
pub(crate) static GUI_WIDGET_KINDS: &[&str] = &[
    "text", "column", "row", "container", "grid", "button", "space",
    "text_input", "checkbox", "toggler", "slider", "progress_bar",
    "scrollable", "horizontal_rule", "vertical_rule", "tooltip",
    "pick_list", "stack", "radio", "vertical_slider", "combo_box",
    "text_editor", "keyboard_area", "mouse_area", "image", "canvas",
    "context_menu", "chart", "markdown", "menu_bar", "qr_code",
    "table", "data_table",
];

// ---- Tree node data ----

/// How children map back to labeled args during source reconstruction.
#[derive(Clone, Debug)]
pub(crate) enum ChildSlot {
    /// Children came from an Array-typed arg: `#label: &[child, ...]`
    Array(ArcStr),
    /// A single Widget-typed arg: `#label: &child`
    Single(ArcStr),
}

/// Per-node data in the widget tree.
#[derive(Clone, Debug)]
pub(crate) struct TreeNodeData {
    /// Short widget kind name (e.g. "row", "data_table")
    pub kind: ArcStr,
    /// Full constructor path as it appeared in the AST (e.g. "data_table",
    /// "gui::row::row"). Used for source reconstruction.
    pub constructor_path: ArcStr,
    /// Non-child labeled args (properties for the inspector).
    /// Stores (label, AST expression) for each provided arg.
    pub args: Vec<(ArcStr, Expr)>,
    /// How tree children map back to source args
    pub child_slots: Vec<ChildSlot>,
    /// The widget constructor's FnType (if resolved from Env).
    /// Used by the property panel to show all possible args.
    pub fn_type: Option<TArc<FnType>>,
}

// ---- Tree node ----

#[derive(Clone, Debug)]
pub(crate) struct TreeNode {
    pub id: TreeNodeId,
    pub data: TreeNodeData,
    pub children: Vec<TreeNodeId>,
    pub parent: Option<TreeNodeId>,
    pub expanded: bool,
}

// ---- Tree model ----

pub(crate) struct TreeModel {
    nodes: Vec<TreeNode>,
    pub roots: Vec<TreeNodeId>,
    pub selected: Option<TreeNodeId>,
    /// Non-widget source lines (use statements, let bindings, etc.)
    /// that appear before the widget tree.
    pub preamble: String,
    /// Per-node UI state for the property editor
    pub editor_ui: fxhash::FxHashMap<TreeNodeId, super::path_update::EditorUiState>,
}

impl TreeModel {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            roots: Vec::new(),
            selected: None,
            preamble: String::new(),
            editor_ui: fxhash::FxHashMap::default(),
        }
    }

    pub fn get(&self, id: TreeNodeId) -> Option<&TreeNode> {
        self.nodes.iter().find(|n| n.id == id)
    }

    pub fn get_mut(&mut self, id: TreeNodeId) -> Option<&mut TreeNode> {
        self.nodes.iter_mut().find(|n| n.id == id)
    }

    /// Iterate nodes depth-first for rendering.
    pub fn walk(&self) -> Vec<(TreeNodeId, usize)> {
        let mut result = Vec::new();
        for &root_id in &self.roots {
            self.walk_inner(root_id, 0, &mut result);
        }
        result
    }

    fn walk_inner(&self, id: TreeNodeId, depth: usize, out: &mut Vec<(TreeNodeId, usize)>) {
        out.push((id, depth));
        if let Some(node) = self.get(id) {
            if node.expanded {
                for &child_id in &node.children {
                    self.walk_inner(child_id, depth + 1, out);
                }
            }
        }
    }

    pub fn has_children(&self, id: TreeNodeId) -> bool {
        self.get(id).map_or(false, |n| !n.children.is_empty())
    }

    /// Remove a node and all its descendants.
    pub fn remove(&mut self, id: TreeNodeId) {
        // Remove from parent's children list
        if let Some(parent_id) = self.get(id).and_then(|n| n.parent) {
            if let Some(parent) = self.get_mut(parent_id) {
                parent.children.retain(|&c| c != id);
            }
        }
        // Remove from roots
        self.roots.retain(|&r| r != id);
        // Collect all descendant IDs
        let mut to_remove = vec![id];
        let mut i = 0;
        while i < to_remove.len() {
            let cur = to_remove[i];
            if let Some(node) = self.get(cur) {
                to_remove.extend_from_slice(&node.children);
            }
            i += 1;
        }
        self.nodes.retain(|n| !to_remove.contains(&n.id));
        if self.selected == Some(id) {
            self.selected = None;
        }
    }

    /// Move a node up among its siblings.
    pub fn move_up(&mut self, id: TreeNodeId) {
        let parent_id = self.get(id).and_then(|n| n.parent);
        let list = match parent_id {
            Some(pid) => &self.get(pid).unwrap().children.clone(),
            None => &self.roots.clone(),
        };
        if let Some(pos) = list.iter().position(|&c| c == id) {
            if pos > 0 {
                let list = match parent_id {
                    Some(pid) => &mut self.get_mut(pid).unwrap().children,
                    None => &mut self.roots,
                };
                list.swap(pos, pos - 1);
            }
        }
    }

    /// Move a node down among its siblings.
    pub fn move_down(&mut self, id: TreeNodeId) {
        let parent_id = self.get(id).and_then(|n| n.parent);
        let list = match parent_id {
            Some(pid) => &self.get(pid).unwrap().children.clone(),
            None => &self.roots.clone(),
        };
        let len = list.len();
        if let Some(pos) = list.iter().position(|&c| c == id) {
            if pos + 1 < len {
                let list = match parent_id {
                    Some(pid) => &mut self.get_mut(pid).unwrap().children,
                    None => &mut self.roots,
                };
                list.swap(pos, pos + 1);
            }
        }
    }

    /// Indent: make the node a child of its previous sibling.
    /// (Move right in the tree — "become a child")
    pub fn indent(&mut self, id: TreeNodeId) {
        let parent_id = self.get(id).and_then(|n| n.parent);
        let siblings = match parent_id {
            Some(pid) => self.get(pid).map(|p| p.children.clone()),
            None => Some(self.roots.clone()),
        };
        let Some(sibs) = siblings else { return };
        let Some(pos) = sibs.iter().position(|&c| c == id) else { return };
        if pos == 0 { return; } // no previous sibling to become child of
        let new_parent_id = sibs[pos - 1];

        // Remove from current parent's children
        let list = match parent_id {
            Some(pid) => &mut self.get_mut(pid).unwrap().children,
            None => &mut self.roots,
        };
        list.retain(|&c| c != id);

        // Add as last child of new parent
        self.get_mut(new_parent_id).unwrap().children.push(id);
        self.get_mut(new_parent_id).unwrap().expanded = true;
        self.get_mut(id).unwrap().parent = Some(new_parent_id);
    }

    /// Outdent: make the node a sibling of its parent.
    /// (Move left in the tree — "become a sibling")
    pub fn outdent(&mut self, id: TreeNodeId) {
        let Some(parent_id) = self.get(id).and_then(|n| n.parent) else {
            return; // already at root
        };
        let grandparent_id = self.get(parent_id).and_then(|n| n.parent);

        // Find position of parent among its siblings
        let parent_pos = match grandparent_id {
            Some(gp) => self.get(gp).unwrap().children.iter()
                .position(|&c| c == parent_id),
            None => self.roots.iter().position(|&c| c == parent_id),
        };
        let Some(parent_pos) = parent_pos else { return };

        // Remove from parent's children
        self.get_mut(parent_id).unwrap().children.retain(|&c| c != id);

        // Insert after parent in grandparent's children (or roots)
        let list = match grandparent_id {
            Some(gp) => &mut self.get_mut(gp).unwrap().children,
            None => &mut self.roots,
        };
        list.insert(parent_pos + 1, id);
        self.get_mut(id).unwrap().parent = grandparent_id;
    }

    /// Change the kind of a node, preserving compatible args.
    pub fn change_kind(&mut self, id: TreeNodeId, new_kind: &str, env: &Env) {
        let Some(node) = self.get_mut(id) else { return };
        if node.data.kind.as_str() == new_kind {
            return;
        }

        let old_args = std::mem::take(&mut node.data.args);
        node.data.kind = ArcStr::from(new_kind);
        let ctor = match new_kind {
            "horizontal_rule" | "vertical_rule" => format!("gui::rule::{new_kind}"),
            "context_menu" | "menu_bar" => format!("gui::menu::{new_kind}"),
            _ => format!("gui::{new_kind}::{new_kind}"),
        };
        node.data.constructor_path = ArcStr::from(ctor.as_str());

        // Look up new widget's FnType
        let paths = [
            ModPath::from(["gui", new_kind]),
            ModPath::from([new_kind]),
        ];
        let new_ft = paths.iter().find_map(|p| {
            env.lookup_bind(&ModPath::root(), p).and_then(|(_, bind)| {
                match &bind.typ {
                    Type::Fn(ft) => Some(ft.clone()),
                    _ => None,
                }
            })
        });

        // Migrate compatible args: keep old args that exist in the new type
        let mut new_args = Vec::new();
        if let Some(ft) = &new_ft {
            for farg in ft.args.iter() {
                if let Some((lbl, _)) = &farg.label {
                    if type_contains_widget(&farg.typ) {
                        continue;
                    }
                    // Check if old args had this label
                    if let Some((_, val)) = old_args.iter().find(|(l, _)| l == lbl) {
                        new_args.push((lbl.clone(), val.clone()));
                    }
                }
            }
        }

        let node = self.get_mut(id).unwrap();
        node.data.args = new_args;
        node.data.fn_type = new_ft;
    }

    /// Add a new widget node as child of the selected node (or as root).
    pub fn add_widget(&mut self, kind: &str) -> TreeNodeId {
        let id = TreeNodeId::new();
        // Default constructor path: gui::{kind}::{kind}
        // Special cases for widgets whose module differs from their name
        let ctor = match kind {
            "horizontal_rule" | "vertical_rule" => format!("gui::rule::{kind}"),
            "context_menu" | "menu_bar" => format!("gui::menu::{kind}"),
            _ => format!("gui::{kind}::{kind}"),
        };
        let node = TreeNode {
            id,
            data: TreeNodeData {
                kind: ArcStr::from(kind),
                constructor_path: ArcStr::from(ctor.as_str()),
                args: Vec::new(),
                child_slots: Vec::new(),
                fn_type: None,
            },
            children: Vec::new(),
            parent: self.selected,
            expanded: true,
        };
        self.nodes.push(node);

        if let Some(parent_id) = self.selected {
            if let Some(parent) = self.get_mut(parent_id) {
                parent.children.push(id);
            }
        } else {
            self.roots.push(id);
        }
        id
    }

    // ---- Populate from parsed source ----

    /// Parse source and populate the tree.
    pub fn populate_from_source(&mut self, source: &str, env: &Env) {
        self.nodes.clear();
        self.roots.clear();
        self.selected = None;

        let origin = graphix_compiler::expr::Origin {
            parent: None,
            source: graphix_compiler::expr::Source::Unspecified,
            text: ArcStr::from(source),
        };
        let exprs = match graphix_compiler::expr::parser::parse(origin) {
            Ok(e) => e,
            Err(_) => return,
        };

        // Split expressions into preamble (use, let, mod, type) and widget exprs
        let mut preamble_lines = Vec::new();
        let mut widget_exprs = Vec::new();

        for expr in exprs.iter() {
            match &expr.kind {
                ExprKind::Use { .. }
                | ExprKind::Module { .. }
                | ExprKind::TypeDef(_) => {
                    preamble_lines.push(expr_to_source(expr, source));
                }
                ExprKind::Bind(_) => {
                    // Let bindings go to preamble unless their value is a widget
                    if classify_widget_expr(expr, env).is_some() {
                        widget_exprs.push(expr.clone());
                    } else {
                        preamble_lines.push(expr_to_source(expr, source));
                    }
                }
                _ => {
                    widget_exprs.push(expr.clone());
                }
            }
        }

        self.preamble = preamble_lines.join("\n");

        for expr in &widget_exprs {
            if let Some(node_id) = self.populate_expr(expr, None, env) {
                self.roots.push(node_id);
            }
        }
    }

    fn populate_expr(
        &mut self,
        expr: &Expr,
        parent: Option<TreeNodeId>,
        env: &Env,
    ) -> Option<TreeNodeId> {
        let (kind, constructor_path, prop_args, child_slots, child_exprs, fn_type) =
            classify_widget_expr(expr, env)?;

        let id = TreeNodeId::new();
        let data = TreeNodeData {
            kind,
            constructor_path,
            args: prop_args,
            child_slots,
            fn_type,
        };
        let node = TreeNode {
            id,
            data,
            children: Vec::new(),
            parent,
            expanded: true,
        };
        self.nodes.push(node);

        // Recursively populate children
        let mut child_ids = Vec::new();
        for child_expr in &child_exprs {
            if let Some(child_id) = self.populate_expr(child_expr, Some(id), env) {
                child_ids.push(child_id);
            }
        }
        if let Some(node) = self.get_mut(id) {
            node.children = child_ids;
        }

        Some(id)
    }

    // ---- Source reconstruction ----

    /// Reconstruct full source from preamble + tree.
    pub fn to_source(&self) -> String {
        let mut out = String::new();
        if !self.preamble.is_empty() {
            out.push_str(&self.preamble);
            out.push('\n');
        }
        for &root_id in &self.roots {
            self.node_to_source(root_id, &mut out, 0);
            out.push('\n');
        }
        out
    }

    fn node_to_source(&self, id: TreeNodeId, out: &mut String, depth: usize) {
        let Some(node) = self.get(id) else { return };
        let indent: String = "  ".repeat(depth);

        out.push_str(&indent);
        out.push_str(&node.data.constructor_path);
        out.push('(');

        let mut first = true;
        // Write property args
        for (label, expr) in &node.data.args {
            if !first { out.push_str(", "); }
            first = false;
            out.push('\n');
            out.push_str(&indent);
            out.push_str("  #");
            out.push_str(label);
            out.push_str(": ");
            out.push_str(&format!("{}", expr));
        }

        // Write children based on child_slots
        if !node.children.is_empty() {
            if let Some(slot) = node.data.child_slots.first() {
                if !first { out.push_str(","); }
                first = false;
                match slot {
                    ChildSlot::Array(label) => {
                        out.push('\n');
                        out.push_str(&indent);
                        out.push_str("  #");
                        out.push_str(label);
                        out.push_str(": &[\n");
                        for &child_id in &node.children {
                            self.node_to_source(child_id, out, depth + 2);
                            out.push_str(",\n");
                        }
                        out.push_str(&indent);
                        out.push_str("  ]");
                    }
                    ChildSlot::Single(label) => {
                        for &child_id in &node.children {
                            out.push('\n');
                            out.push_str(&indent);
                            out.push_str("  #");
                            out.push_str(label);
                            out.push_str(": &");
                            self.node_to_source(child_id, out, depth + 1);
                        }
                    }
                }
            } else {
                // No labeled child slot — positional children
                if !first { out.push_str(","); }
                out.push_str(" &[\n");
                for &child_id in &node.children {
                    self.node_to_source(child_id, out, depth + 2);
                    out.push_str(",\n");
                }
                out.push_str(&indent);
                out.push(']');
            }
        }

        out.push(')');
    }
}

// ---- AST classification ----

/// Extract the source text corresponding to an expression (approximate).
fn expr_to_source(expr: &Expr, full_source: &str) -> String {
    // Use position info to extract the line
    let line = expr.pos.line.saturating_sub(1) as usize;
    full_source
        .lines()
        .nth(line)
        .unwrap_or("")
        .to_string()
}

/// Check if a ModPath refers to a gui:: widget.
/// Handles both qualified (`gui::text`) and unqualified (`text` after
/// `use gui::text`) forms by checking the last path component.
fn is_gui_widget(name: &ModPath) -> Option<ArcStr> {
    let s = name.0.as_ref();
    // Try explicit /gui/ prefix first
    if let Some(rest) = s.strip_prefix("/gui/")
        .or_else(|| s.strip_prefix("gui/"))
    {
        if GUI_WIDGET_KINDS.contains(&rest) {
            return Some(ArcStr::from(rest));
        }
    }
    // Fall back to checking the last path component (handles `use gui::xxx`)
    let last = match s.rfind('/') {
        Some(i) => &s[i + 1..],
        None => s,
    };
    if GUI_WIDGET_KINDS.contains(&last) {
        Some(ArcStr::from(last))
    } else {
        None
    }
}

/// Check if a type (deeply) refers to Widget.
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

/// Try to interpret an expression as a widget constructor call.
/// Returns (kind, constructor_path, property_args, child_slots, child_exprs, fn_type).
pub(crate) fn classify_widget_expr(
    expr: &Expr,
    env: &Env,
) -> Option<(ArcStr, ArcStr, Vec<(ArcStr, Expr)>, Vec<ChildSlot>, Vec<Expr>, Option<TArc<FnType>>)> {
    // Unwrap ByRef
    let expr = match &expr.kind {
        ExprKind::ByRef(inner) => inner.as_ref(),
        _ => expr,
    };

    let apply = match &expr.kind {
        ExprKind::Apply(a) => a,
        _ => return None,
    };

    // Check if the function is a gui:: widget
    let (widget_kind, constructor_path) = match &apply.function.kind {
        ExprKind::Ref { name } => {
            let kind = is_gui_widget(name)?;
            // Store the original path as it appeared in the source
            // (e.g. "data_table" or "gui::row::row")
            let path_str = format!("{}", name);
            (kind, ArcStr::from(path_str.as_str()))
        }
        _ => return None,
    };

    // Look up the function type from the env to determine which args
    // are widget-bearing (children) vs properties.
    // Try both qualified (gui::xxx) and unqualified (xxx) paths.
    let fn_type = {
        let paths = [
            ModPath::from(["gui", &widget_kind]),
            ModPath::from([widget_kind.as_str()]),
        ];
        paths.iter().find_map(|p| {
            env.lookup_bind(&ModPath::root(), p).and_then(|(_, bind)| {
                match &bind.typ {
                    Type::Fn(ft) => Some(ft.clone()),
                    _ => None,
                }
            })
        })
    };

    let mut prop_args: Vec<(ArcStr, Expr)> = Vec::new();
    let mut child_slots = Vec::new();
    let mut child_exprs = Vec::new();

    for (label_opt, arg_expr) in apply.args.iter() {
        let is_child = if let Some(label) = label_opt {
            // Check if this labeled arg's type contains Widget
            if let Some(ft) = &fn_type {
                ft.args.iter().any(|a| {
                    a.label.as_ref().map_or(false, |(l, _)| l == label)
                        && type_contains_widget(&a.typ)
                })
            } else {
                false
            }
        } else {
            // Positional arg — check if it contains widget content
            // (usually the last positional arg is children)
            is_widget_content(arg_expr, env)
        };

        if is_child {
            // Extract child expressions
            let label = label_opt.clone().unwrap_or_else(|| ArcStr::from("children"));
            match &arg_expr.kind {
                ExprKind::ByRef(inner) => match &inner.kind {
                    ExprKind::Array { args } => {
                        child_slots.push(ChildSlot::Array(label));
                        child_exprs.extend(args.iter().cloned());
                    }
                    _ => {
                        child_slots.push(ChildSlot::Single(label));
                        child_exprs.push(inner.as_ref().clone());
                    }
                },
                ExprKind::Array { args } => {
                    child_slots.push(ChildSlot::Array(label));
                    child_exprs.extend(args.iter().cloned());
                }
                _ => {
                    child_slots.push(ChildSlot::Single(label));
                    child_exprs.push(arg_expr.clone());
                }
            }
        } else if let Some(label) = label_opt {
            // Property arg — store as label + Expr
            prop_args.push((label.clone(), arg_expr.clone()));
        }
        // Unlabeled non-child args are uncommon; skip for now
    }

    Some((widget_kind, constructor_path, prop_args, child_slots, child_exprs, fn_type))
}

/// Check if an expression looks like widget content (array of widgets or single widget).
fn is_widget_content(expr: &Expr, env: &Env) -> bool {
    match &expr.kind {
        ExprKind::ByRef(inner) => is_widget_content(inner, env),
        ExprKind::Array { args } => {
            args.iter().any(|a| classify_widget_expr(a, env).is_some())
        }
        _ => classify_widget_expr(expr, env).is_some(),
    }
}

/// Convert an expression to source text using the compiler's Display impl.
fn expr_to_text(expr: &Expr) -> String {
    format!("{}", expr)
}

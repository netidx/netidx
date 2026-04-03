use crate::view;
use super::OnChange;
use glib::clone;
use gtk::{self, prelude::*};
use std::{
    cell::RefCell,
    rc::Rc,
};

/// Helper: create a label + entry pair for a GxExpr property.
/// When the user activates (presses Enter) the entry, the on_change
/// callback fires with the new source text.
fn gx_entry(
    grid: &gtk::Grid,
    row: &mut i32,
    label_text: &str,
    initial: &view::GxExpr,
    on_change: impl Fn(view::GxExpr) + 'static,
) {
    let lbl = gtk::Label::new(Some(label_text));
    lbl.set_halign(gtk::Align::End);
    let entry = gtk::Entry::new();
    entry.set_text(&initial.0);
    entry.set_hexpand(true);
    entry.set_icon_activatable(gtk::EntryIconPosition::Secondary, true);
    entry.connect_changed(|e| {
        e.set_icon_from_icon_name(
            gtk::EntryIconPosition::Secondary,
            Some("media-floppy"),
        );
    });
    entry.connect_icon_press(|e, _, _| e.emit_activate());
    entry.connect_activate(move |e| {
        e.set_icon_from_icon_name(gtk::EntryIconPosition::Secondary, None);
        on_change(view::GxExpr(e.text().to_string()));
    });
    grid.attach(&lbl, 0, *row, 1, 1);
    grid.attach(&entry, 1, *row, 1, 1);
    *row += 1;
}

/// Helper: create a label + entry pair for a callback GxExpr property.
/// Shows a placeholder with the expected lambda signature.
fn gx_callback_entry(
    grid: &gtk::Grid,
    row: &mut i32,
    label_text: &str,
    placeholder: &str,
    initial: &view::GxExpr,
    on_change: impl Fn(view::GxExpr) + 'static,
) {
    let lbl = gtk::Label::new(Some(label_text));
    lbl.set_halign(gtk::Align::End);
    let entry = gtk::Entry::new();
    entry.set_text(&initial.0);
    entry.set_placeholder_text(Some(placeholder));
    entry.set_hexpand(true);
    entry.set_icon_activatable(gtk::EntryIconPosition::Secondary, true);
    entry.connect_changed(|e| {
        e.set_icon_from_icon_name(
            gtk::EntryIconPosition::Secondary,
            Some("media-floppy"),
        );
    });
    entry.connect_icon_press(|e, _, _| e.emit_activate());
    entry.connect_activate(move |e| {
        e.set_icon_from_icon_name(gtk::EntryIconPosition::Secondary, None);
        on_change(view::GxExpr(e.text().to_string()));
    });
    grid.attach(&lbl, 0, *row, 1, 1);
    grid.attach(&entry, 1, *row, 1, 1);
    *row += 1;
}

/// Helper: create a label + text entry pair for a plain string field.
fn str_entry(
    grid: &gtk::Grid,
    row: &mut i32,
    label_text: &str,
    initial: &str,
    on_change: impl Fn(String) + 'static,
) {
    let lbl = gtk::Label::new(Some(label_text));
    lbl.set_halign(gtk::Align::End);
    let entry = gtk::Entry::new();
    entry.set_text(initial);
    entry.set_hexpand(true);
    entry.set_icon_activatable(gtk::EntryIconPosition::Secondary, true);
    entry.connect_changed(|e| {
        e.set_icon_from_icon_name(
            gtk::EntryIconPosition::Secondary,
            Some("media-floppy"),
        );
    });
    entry.connect_icon_press(|e, _, _| e.emit_activate());
    entry.connect_activate(move |e| {
        e.set_icon_from_icon_name(gtk::EntryIconPosition::Secondary, None);
        on_change(e.text().to_string());
    });
    grid.attach(&lbl, 0, *row, 1, 1);
    grid.attach(&entry, 1, *row, 1, 1);
    *row += 1;
}

/// Helper: create a label + spin button pair for a u32 field.
fn u32_entry(
    grid: &gtk::Grid,
    row: &mut i32,
    label_text: &str,
    initial: u32,
    on_change: impl Fn(u32) + 'static,
) {
    let lbl = gtk::Label::new(Some(label_text));
    lbl.set_halign(gtk::Align::End);
    let adj = gtk::Adjustment::new(
        initial as f64, 0.0, 1_000_000.0, 1.0, 10.0, 0.0,
    );
    let spin = gtk::SpinButton::new(Some(&adj), 1.0, 0);
    spin.set_hexpand(true);
    spin.connect_value_changed(move |s| {
        on_change(s.value_as_int() as u32);
    });
    grid.attach(&lbl, 0, *row, 1, 1);
    grid.attach(&spin, 1, *row, 1, 1);
    *row += 1;
}

/// Helper: create a label + spin button pair for a u64 field.
fn u64_entry(
    grid: &gtk::Grid,
    row: &mut i32,
    label_text: &str,
    initial: u64,
    on_change: impl Fn(u64) + 'static,
) {
    let lbl = gtk::Label::new(Some(label_text));
    lbl.set_halign(gtk::Align::End);
    let adj = gtk::Adjustment::new(
        initial as f64, 0.0, 1e18, 1.0, 10.0, 0.0,
    );
    let spin = gtk::SpinButton::new(Some(&adj), 1.0, 0);
    spin.set_hexpand(true);
    spin.connect_value_changed(move |s| {
        on_change(s.value_as_int() as u64);
    });
    grid.attach(&lbl, 0, *row, 1, 1);
    grid.attach(&spin, 1, *row, 1, 1);
    *row += 1;
}

/// Helper: create a label + usize spin button pair.
fn usize_entry(
    grid: &gtk::Grid,
    row: &mut i32,
    label_text: &str,
    initial: usize,
    on_change: impl Fn(usize) + 'static,
) {
    let lbl = gtk::Label::new(Some(label_text));
    lbl.set_halign(gtk::Align::End);
    let adj = gtk::Adjustment::new(
        initial as f64, 0.0, 1e18, 1.0, 10.0, 0.0,
    );
    let spin = gtk::SpinButton::new(Some(&adj), 1.0, 0);
    spin.set_hexpand(true);
    spin.connect_value_changed(move |s| {
        on_change(s.value_as_int() as usize);
    });
    grid.attach(&lbl, 0, *row, 1, 1);
    grid.attach(&spin, 1, *row, 1, 1);
    *row += 1;
}

/// Helper: create a label + checkbox for a bool field.
fn bool_check(
    grid: &gtk::Grid,
    row: &mut i32,
    label_text: &str,
    initial: bool,
    on_change: impl Fn(bool) + 'static,
) {
    let check = gtk::CheckButton::with_label(label_text);
    check.set_active(initial);
    check.connect_toggled(move |b| on_change(b.is_active()));
    grid.attach(&check, 0, *row, 2, 1);
    *row += 1;
}

/// Helper: create a label + combo box for a direction field.
fn direction_combo(
    grid: &gtk::Grid,
    row: &mut i32,
    label_text: &str,
    initial: view::Direction,
    on_change: impl Fn(view::Direction) + 'static,
) {
    let lbl = gtk::Label::new(Some(label_text));
    lbl.set_halign(gtk::Align::End);
    let combo = gtk::ComboBoxText::new();
    combo.append(Some("Horizontal"), "Horizontal");
    combo.append(Some("Vertical"), "Vertical");
    combo.set_hexpand(true);
    match initial {
        view::Direction::Horizontal => combo.set_active_id(Some("Horizontal")),
        view::Direction::Vertical => combo.set_active_id(Some("Vertical")),
    };
    combo.connect_changed(move |c| {
        if let Some(id) = c.active_id() {
            let d = match &*id {
                "Horizontal" => view::Direction::Horizontal,
                _ => view::Direction::Vertical,
            };
            on_change(d);
        }
    });
    grid.attach(&lbl, 0, *row, 1, 1);
    grid.attach(&combo, 1, *row, 1, 1);
    *row += 1;
}

/// Helper: create align combo
fn align_combo(
    grid: &gtk::Grid,
    row: &mut i32,
    label_text: &str,
    initial: view::Align,
    on_change: impl Fn(view::Align) + 'static,
) {
    let lbl = gtk::Label::new(Some(label_text));
    lbl.set_halign(gtk::Align::End);
    let combo = gtk::ComboBoxText::new();
    combo.set_hexpand(true);
    for a in &["Fill", "Start", "End", "Center", "Baseline"] {
        combo.append(Some(a), a);
    }
    let id = match initial {
        view::Align::Fill => "Fill",
        view::Align::Start => "Start",
        view::Align::End => "End",
        view::Align::Center => "Center",
        view::Align::Baseline => "Baseline",
    };
    combo.set_active_id(Some(id));
    combo.connect_changed(move |c| {
        if let Some(id) = c.active_id() {
            let a = match &*id {
                "Start" => view::Align::Start,
                "End" => view::Align::End,
                "Center" => view::Align::Center,
                "Baseline" => view::Align::Baseline,
                _ => view::Align::Fill,
            };
            on_change(a);
        }
    });
    grid.attach(&lbl, 0, *row, 1, 1);
    grid.attach(&combo, 1, *row, 1, 1);
    *row += 1;
}

/// Helper: create pack combo
fn pack_combo(
    grid: &gtk::Grid,
    row: &mut i32,
    label_text: &str,
    initial: view::Pack,
    on_change: impl Fn(view::Pack) + 'static,
) {
    let lbl = gtk::Label::new(Some(label_text));
    lbl.set_halign(gtk::Align::End);
    let combo = gtk::ComboBoxText::new();
    combo.set_hexpand(true);
    combo.append(Some("Start"), "Start");
    combo.append(Some("End"), "End");
    let id = match initial {
        view::Pack::Start => "Start",
        view::Pack::End => "End",
    };
    combo.set_active_id(Some(id));
    combo.connect_changed(move |c| {
        if let Some(id) = c.active_id() {
            let p = match &*id {
                "End" => view::Pack::End,
                _ => view::Pack::Start,
            };
            on_change(p);
        }
    });
    grid.attach(&lbl, 0, *row, 1, 1);
    grid.attach(&combo, 1, *row, 1, 1);
    *row += 1;
}

/// Helper: create tab position combo
fn tab_position_combo(
    grid: &gtk::Grid,
    row: &mut i32,
    label_text: &str,
    initial: view::TabPosition,
    on_change: impl Fn(view::TabPosition) + 'static,
) {
    let lbl = gtk::Label::new(Some(label_text));
    lbl.set_halign(gtk::Align::End);
    let combo = gtk::ComboBoxText::new();
    combo.set_hexpand(true);
    for t in &["Left", "Right", "Top", "Bottom"] {
        combo.append(Some(t), t);
    }
    let id = match initial {
        view::TabPosition::Left => "Left",
        view::TabPosition::Right => "Right",
        view::TabPosition::Top => "Top",
        view::TabPosition::Bottom => "Bottom",
    };
    combo.set_active_id(Some(id));
    combo.connect_changed(move |c| {
        if let Some(id) = c.active_id() {
            let t = match &*id {
                "Left" => view::TabPosition::Left,
                "Right" => view::TabPosition::Right,
                "Bottom" => view::TabPosition::Bottom,
                _ => view::TabPosition::Top,
            };
            on_change(t);
        }
    });
    grid.attach(&lbl, 0, *row, 1, 1);
    grid.attach(&combo, 1, *row, 1, 1);
    *row += 1;
}

/// Helper: f32 entry
fn f32_entry(
    grid: &gtk::Grid,
    row: &mut i32,
    label_text: &str,
    initial: f32,
    on_change: impl Fn(f32) + 'static,
) {
    let lbl = gtk::Label::new(Some(label_text));
    lbl.set_halign(gtk::Align::End);
    let adj = gtk::Adjustment::new(
        initial as f64, -1e6, 1e6, 0.01, 0.1, 0.0,
    );
    let spin = gtk::SpinButton::new(Some(&adj), 0.01, 3);
    spin.set_hexpand(true);
    spin.connect_value_changed(move |s| on_change(s.value() as f32));
    grid.attach(&lbl, 0, *row, 1, 1);
    grid.attach(&spin, 1, *row, 1, 1);
    *row += 1;
}

/// Build the common WidgetProps property editors into the grid.
pub(super) fn build_common_props(
    grid: &gtk::Grid,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
) {
    let mut row = 0i32;
    let props = spec.borrow().props.clone().unwrap_or_default();
    align_combo(grid, &mut row, "H Align:", props.halign, clone!(
        @strong spec, @strong on_change => move |a| {
            spec.borrow_mut().props.get_or_insert_with(Default::default).halign = a;
            on_change();
        }
    ));
    align_combo(grid, &mut row, "V Align:", props.valign, clone!(
        @strong spec, @strong on_change => move |a| {
            spec.borrow_mut().props.get_or_insert_with(Default::default).valign = a;
            on_change();
        }
    ));
    bool_check(grid, &mut row, "H Expand", props.hexpand, clone!(
        @strong spec, @strong on_change => move |b| {
            spec.borrow_mut().props.get_or_insert_with(Default::default).hexpand = b;
            on_change();
        }
    ));
    bool_check(grid, &mut row, "V Expand", props.vexpand, clone!(
        @strong spec, @strong on_change => move |b| {
            spec.borrow_mut().props.get_or_insert_with(Default::default).vexpand = b;
            on_change();
        }
    ));
    u32_entry(grid, &mut row, "Margin Top:", props.margin_top, clone!(
        @strong spec, @strong on_change => move |v| {
            spec.borrow_mut().props.get_or_insert_with(Default::default).margin_top = v;
            on_change();
        }
    ));
    u32_entry(grid, &mut row, "Margin Bottom:", props.margin_bottom, clone!(
        @strong spec, @strong on_change => move |v| {
            spec.borrow_mut().props.get_or_insert_with(Default::default).margin_bottom = v;
            on_change();
        }
    ));
    u32_entry(grid, &mut row, "Margin Start:", props.margin_start, clone!(
        @strong spec, @strong on_change => move |v| {
            spec.borrow_mut().props.get_or_insert_with(Default::default).margin_start = v;
            on_change();
        }
    ));
    u32_entry(grid, &mut row, "Margin End:", props.margin_end, clone!(
        @strong spec, @strong on_change => move |v| {
            spec.borrow_mut().props.get_or_insert_with(Default::default).margin_end = v;
            on_change();
        }
    ));
    gx_entry(grid, &mut row, "Sensitive:", &props.sensitive, clone!(
        @strong spec, @strong on_change => move |e| {
            spec.borrow_mut().props.get_or_insert_with(Default::default).sensitive = e;
            on_change();
        }
    ));
    gx_entry(grid, &mut row, "Visible:", &props.visible, clone!(
        @strong spec, @strong on_change => move |e| {
            spec.borrow_mut().props.get_or_insert_with(Default::default).visible = e;
            on_change();
        }
    ));
}

/// Build the widget-kind-specific property editors into the grid.
pub(super) fn build_kind_props(
    grid: &gtk::Grid,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
) {
    let mut row = 0i32;
    let s = spec.borrow();
    match &s.kind {
        view::WidgetKind::BScript(expr) => {
            gx_entry(grid, &mut row, "Expression:", expr, clone!(
                @strong spec, @strong on_change => move |e| {
                    spec.borrow_mut().kind = view::WidgetKind::BScript(e);
                    on_change();
                }
            ));
        }
        view::WidgetKind::Table(t) => {
            build_table_props(grid, &mut row, spec, on_change, t);
        }
        view::WidgetKind::Label(l) => {
            build_label_props(grid, &mut row, spec, on_change, l);
        }
        view::WidgetKind::Button(b) => {
            build_button_props(grid, &mut row, spec, on_change, b);
        }
        view::WidgetKind::LinkButton(b) => {
            build_link_button_props(grid, &mut row, spec, on_change, b);
        }
        view::WidgetKind::Switch(sw) => {
            build_switch_props(grid, &mut row, spec, on_change, sw);
        }
        view::WidgetKind::ToggleButton(tb) => {
            build_toggle_button_props(
                grid, &mut row, spec, on_change, tb, false,
            );
        }
        view::WidgetKind::CheckButton(tb) => {
            build_toggle_button_props(
                grid, &mut row, spec, on_change, tb, true,
            );
        }
        view::WidgetKind::RadioButton(rb) => {
            build_radio_button_props(grid, &mut row, spec, on_change, rb);
        }
        view::WidgetKind::ComboBox(cb) => {
            build_combo_box_props(grid, &mut row, spec, on_change, cb);
        }
        view::WidgetKind::Entry(e) => {
            build_entry_props(grid, &mut row, spec, on_change, e);
        }
        view::WidgetKind::SearchEntry(se) => {
            build_search_entry_props(grid, &mut row, spec, on_change, se);
        }
        view::WidgetKind::ProgressBar(pb) => {
            build_progress_bar_props(grid, &mut row, spec, on_change, pb);
        }
        view::WidgetKind::Scale(sc) => {
            build_scale_props(grid, &mut row, spec, on_change, sc);
        }
        view::WidgetKind::Image(im) => {
            build_image_props(grid, &mut row, spec, on_change, im);
        }
        view::WidgetKind::Frame(f) => {
            build_frame_props(grid, &mut row, spec, on_change, f);
        }
        view::WidgetKind::Box(b) => {
            build_box_props(grid, &mut row, spec, on_change, b);
        }
        view::WidgetKind::BoxChild(bc) => {
            build_box_child_props(grid, &mut row, spec, on_change, bc);
        }
        view::WidgetKind::Grid(g) => {
            build_grid_props(grid, &mut row, spec, on_change, g);
        }
        view::WidgetKind::GridChild(gc) => {
            build_grid_child_props(grid, &mut row, spec, on_change, gc);
        }
        view::WidgetKind::GridRow(_) => {
            // GridRow has no editable properties of its own
        }
        view::WidgetKind::Paned(p) => {
            build_paned_props(grid, &mut row, spec, on_change, p);
        }
        view::WidgetKind::Notebook(n) => {
            build_notebook_props(grid, &mut row, spec, on_change, n);
        }
        view::WidgetKind::NotebookPage(np) => {
            build_notebook_page_props(grid, &mut row, spec, on_change, np);
        }
        view::WidgetKind::LinePlot(lp) => {
            build_line_plot_props(grid, &mut row, spec, on_change, lp);
        }
    }
}

fn build_table_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    t: &view::Table,
) {
    gx_entry(grid, row, "Path:", &t.path, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Table(ref mut t) = spec.borrow_mut().kind {
                t.path = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Sort Mode:", &t.sort_mode, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Table(ref mut t) = spec.borrow_mut().kind {
                t.sort_mode = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Column Filter:", &t.column_filter, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Table(ref mut t) = spec.borrow_mut().kind {
                t.column_filter = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Row Filter:", &t.row_filter, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Table(ref mut t) = spec.borrow_mut().kind {
                t.row_filter = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Column Editable:", &t.column_editable, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Table(ref mut t) = spec.borrow_mut().kind {
                t.column_editable = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Column Widths:", &t.column_widths, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Table(ref mut t) = spec.borrow_mut().kind {
                t.column_widths = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Columns Resizable:", &t.columns_resizable, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Table(ref mut t) = spec.borrow_mut().kind {
                t.columns_resizable = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Column Types:", &t.column_types, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Table(ref mut t) = spec.borrow_mut().kind {
                t.column_types = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Selection Mode:", &t.selection_mode, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Table(ref mut t) = spec.borrow_mut().kind {
                t.selection_mode = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Selection:", &t.selection, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Table(ref mut t) = spec.borrow_mut().kind {
                t.selection = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Show Row Name:", &t.show_row_name, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Table(ref mut t) = spec.borrow_mut().kind {
                t.show_row_name = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Refresh:", &t.refresh, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Table(ref mut t) = spec.borrow_mut().kind {
                t.refresh = e;
            }
            on_change();
        }
    ));
    gx_callback_entry(grid, row, "On Select:", "|paths: array<string>| ...", &t.on_select, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Table(ref mut t) = spec.borrow_mut().kind {
                t.on_select = e;
            }
            on_change();
        }
    ));
    gx_callback_entry(grid, row, "On Activate:", "|path: string| ...", &t.on_activate, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Table(ref mut t) = spec.borrow_mut().kind {
                t.on_activate = e;
            }
            on_change();
        }
    ));
    gx_callback_entry(grid, row, "On Edit:", "|e: {path: string, value: Any}| ...", &t.on_edit, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Table(ref mut t) = spec.borrow_mut().kind {
                t.on_edit = e;
            }
            on_change();
        }
    ));
    gx_callback_entry(grid, row, "On Header Click:", "|col: string| ...", &t.on_header_click, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Table(ref mut t) = spec.borrow_mut().kind {
                t.on_header_click = e;
            }
            on_change();
        }
    ));
}

fn build_label_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    l: &view::Label,
) {
    gx_entry(grid, row, "Text:", &l.text, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Label(ref mut l) = spec.borrow_mut().kind {
                l.text = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Ellipsize:", &l.ellipsize, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Label(ref mut l) = spec.borrow_mut().kind {
                l.ellipsize = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Width:", &l.width, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Label(ref mut l) = spec.borrow_mut().kind {
                l.width = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Single Line:", &l.single_line, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Label(ref mut l) = spec.borrow_mut().kind {
                l.single_line = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Selectable:", &l.selectable, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Label(ref mut l) = spec.borrow_mut().kind {
                l.selectable = e;
            }
            on_change();
        }
    ));
}

fn build_button_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    b: &view::Button,
) {
    gx_entry(grid, row, "Label:", &b.label, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Button(ref mut b) = spec.borrow_mut().kind {
                b.label = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Image:", &b.image, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Button(ref mut b) = spec.borrow_mut().kind {
                b.image = e;
            }
            on_change();
        }
    ));
    gx_callback_entry(grid, row, "On Click:", "|_| ...", &b.on_click, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Button(ref mut b) = spec.borrow_mut().kind {
                b.on_click = e;
            }
            on_change();
        }
    ));
}

fn build_link_button_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    b: &view::LinkButton,
) {
    gx_entry(grid, row, "URI:", &b.uri, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::LinkButton(ref mut b) = spec.borrow_mut().kind {
                b.uri = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Label:", &b.label, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::LinkButton(ref mut b) = spec.borrow_mut().kind {
                b.label = e;
            }
            on_change();
        }
    ));
    gx_callback_entry(grid, row, "On Activate:", "|uri: string| ...", &b.on_activate_link, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::LinkButton(ref mut b) = spec.borrow_mut().kind {
                b.on_activate_link = e;
            }
            on_change();
        }
    ));
}

fn build_switch_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    sw: &view::Switch,
) {
    gx_entry(grid, row, "Value:", &sw.value, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Switch(ref mut sw) = spec.borrow_mut().kind {
                sw.value = e;
            }
            on_change();
        }
    ));
    gx_callback_entry(grid, row, "On Change:", "|active: bool| ...", &sw.on_change, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Switch(ref mut sw) = spec.borrow_mut().kind {
                sw.on_change = e;
            }
            on_change();
        }
    ));
}

fn build_toggle_button_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    tb: &view::ToggleButton,
    is_check: bool,
) {
    gx_entry(grid, row, "Value:", &tb.toggle.value, clone!(
        @strong spec, @strong on_change => move |e| {
            let mut s = spec.borrow_mut();
            let tgt = if is_check {
                if let view::WidgetKind::CheckButton(ref mut tb) = s.kind {
                    Some(&mut tb.toggle)
                } else { None }
            } else {
                if let view::WidgetKind::ToggleButton(ref mut tb) = s.kind {
                    Some(&mut tb.toggle)
                } else { None }
            };
            if let Some(toggle) = tgt { toggle.value = e; }
            drop(s);
            on_change();
        }
    ));
    gx_callback_entry(grid, row, "On Change:", "|active: bool| ...", &tb.toggle.on_change, clone!(
        @strong spec, @strong on_change => move |e| {
            let mut s = spec.borrow_mut();
            let tgt = if is_check {
                if let view::WidgetKind::CheckButton(ref mut tb) = s.kind {
                    Some(&mut tb.toggle)
                } else { None }
            } else {
                if let view::WidgetKind::ToggleButton(ref mut tb) = s.kind {
                    Some(&mut tb.toggle)
                } else { None }
            };
            if let Some(toggle) = tgt { toggle.on_change = e; }
            drop(s);
            on_change();
        }
    ));
    gx_entry(grid, row, "Label:", &tb.label, clone!(
        @strong spec, @strong on_change => move |e| {
            let mut s = spec.borrow_mut();
            if is_check {
                if let view::WidgetKind::CheckButton(ref mut tb) = s.kind {
                    tb.label = e;
                }
            } else {
                if let view::WidgetKind::ToggleButton(ref mut tb) = s.kind {
                    tb.label = e;
                }
            }
            drop(s);
            on_change();
        }
    ));
    gx_entry(grid, row, "Image:", &tb.image, clone!(
        @strong spec, @strong on_change => move |e| {
            let mut s = spec.borrow_mut();
            if is_check {
                if let view::WidgetKind::CheckButton(ref mut tb) = s.kind {
                    tb.image = e;
                }
            } else {
                if let view::WidgetKind::ToggleButton(ref mut tb) = s.kind {
                    tb.image = e;
                }
            }
            drop(s);
            on_change();
        }
    ));
}

fn build_radio_button_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    rb: &view::RadioButton,
) {
    gx_entry(grid, row, "Label:", &rb.label, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::RadioButton(ref mut rb) = spec.borrow_mut().kind {
                rb.label = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Image:", &rb.image, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::RadioButton(ref mut rb) = spec.borrow_mut().kind {
                rb.image = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Group:", &rb.group, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::RadioButton(ref mut rb) = spec.borrow_mut().kind {
                rb.group = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Value:", &rb.value, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::RadioButton(ref mut rb) = spec.borrow_mut().kind {
                rb.value = e;
            }
            on_change();
        }
    ));
    gx_callback_entry(grid, row, "On Toggled:", "|active: bool| ...", &rb.on_toggled, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::RadioButton(ref mut rb) = spec.borrow_mut().kind {
                rb.on_toggled = e;
            }
            on_change();
        }
    ));
}

fn build_combo_box_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    cb: &view::ComboBox,
) {
    gx_entry(grid, row, "Choices:", &cb.choices, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::ComboBox(ref mut cb) = spec.borrow_mut().kind {
                cb.choices = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Selected:", &cb.selected, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::ComboBox(ref mut cb) = spec.borrow_mut().kind {
                cb.selected = e;
            }
            on_change();
        }
    ));
    gx_callback_entry(grid, row, "On Change:", "|id: string| ...", &cb.on_change, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::ComboBox(ref mut cb) = spec.borrow_mut().kind {
                cb.on_change = e;
            }
            on_change();
        }
    ));
}

fn build_entry_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    e: &view::Entry,
) {
    gx_entry(grid, row, "Text:", &e.text, clone!(
        @strong spec, @strong on_change => move |ex| {
            if let view::WidgetKind::Entry(ref mut e) = spec.borrow_mut().kind {
                e.text = ex;
            }
            on_change();
        }
    ));
    gx_callback_entry(grid, row, "On Change:", "|text: string| ...", &e.on_change, clone!(
        @strong spec, @strong on_change => move |ex| {
            if let view::WidgetKind::Entry(ref mut e) = spec.borrow_mut().kind {
                e.on_change = ex;
            }
            on_change();
        }
    ));
    gx_callback_entry(grid, row, "On Activate:", "|text: string| ...", &e.on_activate, clone!(
        @strong spec, @strong on_change => move |ex| {
            if let view::WidgetKind::Entry(ref mut e) = spec.borrow_mut().kind {
                e.on_activate = ex;
            }
            on_change();
        }
    ));
}

fn build_search_entry_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    se: &view::SearchEntry,
) {
    gx_entry(grid, row, "Text:", &se.text, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::SearchEntry(ref mut se) = spec.borrow_mut().kind {
                se.text = e;
            }
            on_change();
        }
    ));
    gx_callback_entry(grid, row, "On Search Changed:", "|text: string| ...", &se.on_search_changed, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::SearchEntry(ref mut se) = spec.borrow_mut().kind {
                se.on_search_changed = e;
            }
            on_change();
        }
    ));
    gx_callback_entry(grid, row, "On Activate:", "|text: string| ...", &se.on_activate, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::SearchEntry(ref mut se) = spec.borrow_mut().kind {
                se.on_activate = e;
            }
            on_change();
        }
    ));
}

fn build_progress_bar_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    pb: &view::ProgressBar,
) {
    gx_entry(grid, row, "Ellipsize:", &pb.ellipsize, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::ProgressBar(ref mut pb) = spec.borrow_mut().kind {
                pb.ellipsize = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Fraction:", &pb.fraction, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::ProgressBar(ref mut pb) = spec.borrow_mut().kind {
                pb.fraction = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Pulse:", &pb.pulse, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::ProgressBar(ref mut pb) = spec.borrow_mut().kind {
                pb.pulse = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Text:", &pb.text, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::ProgressBar(ref mut pb) = spec.borrow_mut().kind {
                pb.text = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Show Text:", &pb.show_text, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::ProgressBar(ref mut pb) = spec.borrow_mut().kind {
                pb.show_text = e;
            }
            on_change();
        }
    ));
}

fn build_scale_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    sc: &view::Scale,
) {
    direction_combo(grid, row, "Direction:", sc.direction, clone!(
        @strong spec, @strong on_change => move |d| {
            if let view::WidgetKind::Scale(ref mut sc) = spec.borrow_mut().kind {
                sc.direction = d;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Draw Value:", &sc.draw_value, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Scale(ref mut sc) = spec.borrow_mut().kind {
                sc.draw_value = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Marks:", &sc.marks, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Scale(ref mut sc) = spec.borrow_mut().kind {
                sc.marks = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Has Origin:", &sc.has_origin, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Scale(ref mut sc) = spec.borrow_mut().kind {
                sc.has_origin = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Value:", &sc.value, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Scale(ref mut sc) = spec.borrow_mut().kind {
                sc.value = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Min:", &sc.min, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Scale(ref mut sc) = spec.borrow_mut().kind {
                sc.min = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Max:", &sc.max, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Scale(ref mut sc) = spec.borrow_mut().kind {
                sc.max = e;
            }
            on_change();
        }
    ));
    gx_callback_entry(grid, row, "On Change:", "|value: f64| ...", &sc.on_change, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Scale(ref mut sc) = spec.borrow_mut().kind {
                sc.on_change = e;
            }
            on_change();
        }
    ));
}

fn build_image_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    im: &view::Image,
) {
    gx_entry(grid, row, "Spec:", &im.spec, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Image(ref mut im) = spec.borrow_mut().kind {
                im.spec = e;
            }
            on_change();
        }
    ));
    gx_callback_entry(grid, row, "On Click:", "|_| ...", &im.on_click, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Image(ref mut im) = spec.borrow_mut().kind {
                im.on_click = e;
            }
            on_change();
        }
    ));
}

fn build_frame_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    f: &view::Frame,
) {
    gx_entry(grid, row, "Label:", &f.label, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Frame(ref mut f) = spec.borrow_mut().kind {
                f.label = e;
            }
            on_change();
        }
    ));
    f32_entry(grid, row, "Label H Align:", f.label_align_horizontal, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::Frame(ref mut f) = spec.borrow_mut().kind {
                f.label_align_horizontal = v;
            }
            on_change();
        }
    ));
    f32_entry(grid, row, "Label V Align:", f.label_align_vertical, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::Frame(ref mut f) = spec.borrow_mut().kind {
                f.label_align_vertical = v;
            }
            on_change();
        }
    ));
}

fn build_box_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    b: &view::Box,
) {
    direction_combo(grid, row, "Direction:", b.direction, clone!(
        @strong spec, @strong on_change => move |d| {
            if let view::WidgetKind::Box(ref mut b) = spec.borrow_mut().kind {
                b.direction = d;
            }
            on_change();
        }
    ));
    bool_check(grid, row, "Homogeneous", b.homogeneous, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::Box(ref mut b) = spec.borrow_mut().kind {
                b.homogeneous = v;
            }
            on_change();
        }
    ));
    u32_entry(grid, row, "Spacing:", b.spacing, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::Box(ref mut b) = spec.borrow_mut().kind {
                b.spacing = v;
            }
            on_change();
        }
    ));
}

fn build_box_child_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    bc: &view::BoxChild,
) {
    pack_combo(grid, row, "Pack:", bc.pack, clone!(
        @strong spec, @strong on_change => move |p| {
            if let view::WidgetKind::BoxChild(ref mut bc) = spec.borrow_mut().kind {
                bc.pack = p;
            }
            on_change();
        }
    ));
    u64_entry(grid, row, "Padding:", bc.padding, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::BoxChild(ref mut bc) = spec.borrow_mut().kind {
                bc.padding = v;
            }
            on_change();
        }
    ));
}

fn build_grid_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    g: &view::Grid,
) {
    bool_check(grid, row, "Homogeneous Columns", g.homogeneous_columns, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::Grid(ref mut g) = spec.borrow_mut().kind {
                g.homogeneous_columns = v;
            }
            on_change();
        }
    ));
    bool_check(grid, row, "Homogeneous Rows", g.homogeneous_rows, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::Grid(ref mut g) = spec.borrow_mut().kind {
                g.homogeneous_rows = v;
            }
            on_change();
        }
    ));
    u32_entry(grid, row, "Column Spacing:", g.column_spacing, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::Grid(ref mut g) = spec.borrow_mut().kind {
                g.column_spacing = v;
            }
            on_change();
        }
    ));
    u32_entry(grid, row, "Row Spacing:", g.row_spacing, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::Grid(ref mut g) = spec.borrow_mut().kind {
                g.row_spacing = v;
            }
            on_change();
        }
    ));
}

fn build_grid_child_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    gc: &view::GridChild,
) {
    u32_entry(grid, row, "Width:", gc.width, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::GridChild(ref mut gc) = spec.borrow_mut().kind {
                gc.width = v;
            }
            on_change();
        }
    ));
    u32_entry(grid, row, "Height:", gc.height, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::GridChild(ref mut gc) = spec.borrow_mut().kind {
                gc.height = v;
            }
            on_change();
        }
    ));
}

fn build_paned_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    p: &view::Paned,
) {
    direction_combo(grid, row, "Direction:", p.direction, clone!(
        @strong spec, @strong on_change => move |d| {
            if let view::WidgetKind::Paned(ref mut p) = spec.borrow_mut().kind {
                p.direction = d;
            }
            on_change();
        }
    ));
    bool_check(grid, row, "Wide Handle", p.wide_handle, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::Paned(ref mut p) = spec.borrow_mut().kind {
                p.wide_handle = v;
            }
            on_change();
        }
    ));
}

fn build_notebook_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    n: &view::Notebook,
) {
    tab_position_combo(grid, row, "Tabs Position:", n.tabs_position, clone!(
        @strong spec, @strong on_change => move |t| {
            if let view::WidgetKind::Notebook(ref mut n) = spec.borrow_mut().kind {
                n.tabs_position = t;
            }
            on_change();
        }
    ));
    bool_check(grid, row, "Tabs Visible", n.tabs_visible, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::Notebook(ref mut n) = spec.borrow_mut().kind {
                n.tabs_visible = v;
            }
            on_change();
        }
    ));
    bool_check(grid, row, "Tabs Scrollable", n.tabs_scrollable, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::Notebook(ref mut n) = spec.borrow_mut().kind {
                n.tabs_scrollable = v;
            }
            on_change();
        }
    ));
    bool_check(grid, row, "Tabs Popup", n.tabs_popup, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::Notebook(ref mut n) = spec.borrow_mut().kind {
                n.tabs_popup = v;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Page:", &n.page, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Notebook(ref mut n) = spec.borrow_mut().kind {
                n.page = e;
            }
            on_change();
        }
    ));
    gx_callback_entry(grid, row, "On Switch Page:", "|page: i64| ...", &n.on_switch_page, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::Notebook(ref mut n) = spec.borrow_mut().kind {
                n.on_switch_page = e;
            }
            on_change();
        }
    ));
}

fn build_notebook_page_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    np: &view::NotebookPage,
) {
    str_entry(grid, row, "Label:", &np.label, clone!(
        @strong spec, @strong on_change => move |s| {
            if let view::WidgetKind::NotebookPage(ref mut np) = spec.borrow_mut().kind {
                np.label = s;
            }
            on_change();
        }
    ));
    bool_check(grid, row, "Reorderable", np.reorderable, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::NotebookPage(ref mut np) = spec.borrow_mut().kind {
                np.reorderable = v;
            }
            on_change();
        }
    ));
}

fn build_line_plot_props(
    grid: &gtk::Grid,
    row: &mut i32,
    spec: &Rc<RefCell<view::Widget>>,
    on_change: &OnChange,
    lp: &view::LinePlot,
) {
    str_entry(grid, row, "Title:", &lp.title, clone!(
        @strong spec, @strong on_change => move |s| {
            if let view::WidgetKind::LinePlot(ref mut lp) = spec.borrow_mut().kind {
                lp.title = s;
            }
            on_change();
        }
    ));
    str_entry(grid, row, "X Label:", &lp.x_label, clone!(
        @strong spec, @strong on_change => move |s| {
            if let view::WidgetKind::LinePlot(ref mut lp) = spec.borrow_mut().kind {
                lp.x_label = s;
            }
            on_change();
        }
    ));
    str_entry(grid, row, "Y Label:", &lp.y_label, clone!(
        @strong spec, @strong on_change => move |s| {
            if let view::WidgetKind::LinePlot(ref mut lp) = spec.borrow_mut().kind {
                lp.y_label = s;
            }
            on_change();
        }
    ));
    usize_entry(grid, row, "X Labels:", lp.x_labels, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::LinePlot(ref mut lp) = spec.borrow_mut().kind {
                lp.x_labels = v;
            }
            on_change();
        }
    ));
    usize_entry(grid, row, "Y Labels:", lp.y_labels, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::LinePlot(ref mut lp) = spec.borrow_mut().kind {
                lp.y_labels = v;
            }
            on_change();
        }
    ));
    bool_check(grid, row, "X Grid", lp.x_grid, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::LinePlot(ref mut lp) = spec.borrow_mut().kind {
                lp.x_grid = v;
            }
            on_change();
        }
    ));
    bool_check(grid, row, "Y Grid", lp.y_grid, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::LinePlot(ref mut lp) = spec.borrow_mut().kind {
                lp.y_grid = v;
            }
            on_change();
        }
    ));
    u32_entry(grid, row, "Margin:", lp.margin, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::LinePlot(ref mut lp) = spec.borrow_mut().kind {
                lp.margin = v;
            }
            on_change();
        }
    ));
    u32_entry(grid, row, "Label Area:", lp.label_area, clone!(
        @strong spec, @strong on_change => move |v| {
            if let view::WidgetKind::LinePlot(ref mut lp) = spec.borrow_mut().kind {
                lp.label_area = v;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "X Min:", &lp.x_min, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::LinePlot(ref mut lp) = spec.borrow_mut().kind {
                lp.x_min = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "X Max:", &lp.x_max, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::LinePlot(ref mut lp) = spec.borrow_mut().kind {
                lp.x_max = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Y Min:", &lp.y_min, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::LinePlot(ref mut lp) = spec.borrow_mut().kind {
                lp.y_min = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Y Max:", &lp.y_max, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::LinePlot(ref mut lp) = spec.borrow_mut().kind {
                lp.y_max = e;
            }
            on_change();
        }
    ));
    gx_entry(grid, row, "Keep Points:", &lp.keep_points, clone!(
        @strong spec, @strong on_change => move |e| {
            if let view::WidgetKind::LinePlot(ref mut lp) = spec.borrow_mut().kind {
                lp.keep_points = e;
            }
            on_change();
        }
    ));
}

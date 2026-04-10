//! Renders the widget tree as a scrollable column of indented rows
//! with a kind selector dropdown that syncs with the selected widget.

use super::tree_model::{TreeModel, TreeNodeId, GUI_WIDGET_KINDS};
use crate::DesignMsg;
use graphix_package_gui::{theme::GraphixTheme, widgets::Renderer};
use iced_core::Color;

type Element<'a> = iced_core::Element<'a, DesignMsg, GraphixTheme, Renderer>;

/// Build the full tree panel: kind selector + tree rows + toolbar.
pub(crate) fn view<'a>(model: &'a TreeModel) -> Element<'a> {
    let entries = model.walk();

    // Kind selector — shows/changes the selected widget's type
    let selected_kind = model.selected
        .and_then(|id| model.get(id))
        .map(|n| n.data.kind.to_string());
    let kind_options: Vec<String> = GUI_WIDGET_KINDS.iter().map(|s| s.to_string()).collect();
    let kind_selector: Element<'a> = iced_widget::PickList::new(
        kind_options.clone(),
        selected_kind.clone(),
        DesignMsg::TreeChangeKind,
    )
    .placeholder("Widget type")
    .text_size(12)
    .width(iced_core::Length::Fill)
    .into();

    // Tree rows
    let mut tree_col = iced_widget::Column::new().spacing(0);
    for (node_id, depth) in &entries {
        if let Some(node) = model.get(*node_id) {
            let selected = model.selected == Some(*node_id);
            let has_children = model.has_children(*node_id);
            let row = tree_row(
                *node_id,
                &node.data.kind,
                *depth,
                selected,
                has_children,
                node.expanded,
            );
            tree_col = tree_col.push(row);
        }
    }

    // Toolbar: Add + Remove + Move Up/Down
    let add_btn: Element<'a> = iced_widget::Button::new(
        iced_widget::text("+").size(14)
    )
    .on_press(DesignMsg::TreeAddWidget(
        selected_kind.as_deref().unwrap_or("text").to_string()
    ))
    .padding(iced_core::Padding::from([2, 8]))
    .into();

    let remove_btn: Element<'a> = iced_widget::Button::new(
        iced_widget::text("-").size(14)
    )
    .on_press(DesignMsg::TreeRemoveSelected)
    .padding(iced_core::Padding::from([2, 8]))
    .into();

    let up_btn: Element<'a> = iced_widget::Tooltip::new(
        iced_widget::Button::new(iced_widget::text("\u{25B2}").size(10))
            .on_press(DesignMsg::TreeMoveUp)
            .padding(iced_core::Padding::from([2, 6])),
        iced_widget::text("Move up").size(11),
        iced_widget::tooltip::Position::Top,
    ).into();

    let down_btn: Element<'a> = iced_widget::Tooltip::new(
        iced_widget::Button::new(iced_widget::text("\u{25BC}").size(10))
            .on_press(DesignMsg::TreeMoveDown)
            .padding(iced_core::Padding::from([2, 6])),
        iced_widget::text("Move down").size(11),
        iced_widget::tooltip::Position::Top,
    ).into();

    let indent_btn: Element<'a> = iced_widget::Tooltip::new(
        iced_widget::Button::new(iced_widget::text("\u{25B6}").size(10))
            .on_press(DesignMsg::TreeIndent)
            .padding(iced_core::Padding::from([2, 6])),
        iced_widget::text("Become child of previous sibling").size(11),
        iced_widget::tooltip::Position::Top,
    ).into();

    let outdent_btn: Element<'a> = iced_widget::Tooltip::new(
        iced_widget::Button::new(iced_widget::text("\u{25C0}").size(10))
            .on_press(DesignMsg::TreeOutdent)
            .padding(iced_core::Padding::from([2, 6])),
        iced_widget::text("Become sibling of parent").size(11),
        iced_widget::tooltip::Position::Top,
    ).into();

    let toolbar: Element<'a> = iced_widget::Row::new()
        .push(add_btn)
        .push(remove_btn)
        .push(outdent_btn)
        .push(indent_btn)
        .push(up_btn)
        .push(down_btn)
        .spacing(4)
        .padding(iced_core::Padding::from([4, 4]))
        .into();

    iced_widget::Column::new()
        .push(kind_selector)
        .push(iced_widget::rule::horizontal::<'_, GraphixTheme>(1))
        .push(
            iced_widget::Scrollable::new(tree_col)
                .height(iced_core::Length::Fill)
        )
        .push(iced_widget::rule::horizontal::<'_, GraphixTheme>(1))
        .push(toolbar)
        .into()
}

fn tree_row<'a>(
    id: TreeNodeId,
    kind: &str,
    depth: usize,
    selected: bool,
    has_children: bool,
    expanded: bool,
) -> Element<'a> {
    let indent_width = (depth as f32) * 16.0;
    let indent: Element<'a> = if indent_width > 0.0 {
        iced_widget::Space::new()
            .width(indent_width)
            .into()
    } else {
        iced_widget::Space::new().width(0).into()
    };

    let arrow: Element<'a> = if has_children {
        let arrow_text = if expanded { "\u{25BE}" } else { "\u{25B8}" };
        iced_widget::Button::new(
            iced_widget::text(arrow_text).size(12)
        )
        .on_press(DesignMsg::TreeToggleExpand(id))
        .padding(iced_core::Padding::from([0, 4]))
        .style(text_btn_style)
        .into()
    } else {
        iced_widget::Space::new().width(20.0).into()
    };

    let label: Element<'a> = iced_widget::text(kind.to_string())
        .size(13)
        .into();

    let bg = if selected {
        Color::from_rgb(0.2, 0.3, 0.5)
    } else {
        Color::TRANSPARENT
    };

    let row_content: Element<'a> = iced_widget::Row::new()
        .push(indent)
        .push(arrow)
        .push(label)
        .spacing(2)
        .align_y(iced_core::Alignment::Center)
        .into();

    iced_widget::Button::new(row_content)
        .on_press(DesignMsg::TreeSelect(id))
        .padding(iced_core::Padding::from([2, 4]))
        .width(iced_core::Length::Fill)
        .style(move |_theme, _status| {
            iced_widget::button::Style {
                background: Some(iced_core::Background::Color(bg)),
                text_color: Color::WHITE,
                border: iced_core::Border::default(),
                shadow: iced_core::Shadow::default(),
                ..Default::default()
            }
        })
        .into()
}

fn text_btn_style(
    _theme: &GraphixTheme,
    _status: iced_widget::button::Status,
) -> iced_widget::button::Style {
    iced_widget::button::Style {
        background: None,
        text_color: Color::WHITE,
        border: iced_core::Border::default(),
        shadow: iced_core::Shadow::default(),
        ..Default::default()
    }
}

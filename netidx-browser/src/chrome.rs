//! Browser chrome: breadcrumbs and dialogs.
//!
//! The menu bar is handled by the custom MenuBar widget (menu_bar.rs).
//! This module provides breadcrumbs navigation and inline dialogs
//! (Go, Save As).

use crate::{BrowserMsg, ViewLoc};
use graphix_package_gui::{theme::GraphixTheme, widgets::Renderer};
use iced_core::Color;
use netidx::path::Path;

type Element<'a> = iced_core::Element<'a, BrowserMsg, GraphixTheme, Renderer>;
type Row<'a> = iced_widget::Row<'a, BrowserMsg, GraphixTheme, Renderer>;
type Column<'a> = iced_widget::Column<'a, BrowserMsg, GraphixTheme, Renderer>;
type Button<'a> = iced_widget::Button<'a, BrowserMsg, GraphixTheme, Renderer>;
type TextInput<'a> = iced_widget::TextInput<'a, BrowserMsg, GraphixTheme, Renderer>;
type Text<'a> = iced_widget::Text<'a, GraphixTheme, Renderer>;
type Scrollable<'a> = iced_widget::Scrollable<'a, BrowserMsg, GraphixTheme, Renderer>;

fn text_button_style(
    theme: &GraphixTheme,
    status: iced_widget::button::Status,
) -> iced_widget::button::Style {
    let palette = theme.palette();
    let color = match status {
        iced_widget::button::Status::Hovered => palette.primary,
        _ => palette.text,
    };
    iced_widget::button::Style {
        text_color: color,
        background: None,
        border: iced_core::Border::default(),
        shadow: iced_core::Shadow::default(),
        ..Default::default()
    }
}

fn dialog_button_style(
    _theme: &GraphixTheme,
    status: iced_widget::button::Status,
) -> iced_widget::button::Style {
    let bg = match status {
        iced_widget::button::Status::Hovered => Color::from_rgb(0.3, 0.3, 0.35),
        iced_widget::button::Status::Pressed => Color::from_rgb(0.25, 0.25, 0.3),
        _ => Color::from_rgb(0.2, 0.2, 0.25),
    };
    iced_widget::button::Style {
        text_color: Color::WHITE,
        background: Some(iced_core::Background::Color(bg)),
        border: iced_core::Border {
            radius: 3.0.into(),
            ..Default::default()
        },
        shadow: iced_core::Shadow::default(),
        ..Default::default()
    }
}

fn dialog_container_style(
    _theme: &GraphixTheme,
) -> iced_widget::container::Style {
    iced_widget::container::Style {
        background: Some(iced_core::Background::Color(
            Color::from_rgb(0.15, 0.15, 0.2),
        )),
        border: iced_core::Border {
            color: Color::from_rgb(0.3, 0.3, 0.4),
            width: 1.0,
            radius: 4.0.into(),
        },
        ..Default::default()
    }
}

/// Browser chrome state.
pub(crate) struct Chrome {
    /// Whether Save is enabled (save_loc is Some)
    pub(crate) save_enabled: bool,
    /// Whether raw view mode is active
    pub(crate) raw_view: bool,
    /// Whether the save dialog is visible
    pub(crate) show_save_dialog: bool,
    /// Text input for the save dialog
    pub(crate) save_dialog_input: String,
    /// Whether the go dialog is visible
    pub(crate) show_go_dialog: bool,
    /// Text input for the go dialog
    pub(crate) go_dialog_input: String,
}

impl Chrome {
    pub(crate) fn new() -> Self {
        Self {
            save_enabled: false,
            raw_view: false,
            show_save_dialog: false,
            save_dialog_input: String::new(),
            show_go_dialog: false,
            go_dialog_input: String::new(),
        }
    }

    /// Build the full browser UI: menu bar + dialogs + breadcrumbs + content.
    pub(crate) fn view<'a>(
        &'a self,
        loc: &'a ViewLoc,
        content: Element<'a>,
    ) -> Element<'a> {
        let menu_bar: Element<'a> = crate::menu_bar::MenuBar::new(vec![
            crate::menu_bar::MenuGroup {
                label: "File",
                items: vec![
                    crate::menu_bar::MenuItem {
                        label: "Save",
                        shortcut: "Ctrl+S",
                        msg: if self.save_enabled { Some(BrowserMsg::Save) } else { None },
                    },
                    crate::menu_bar::MenuItem {
                        label: "Save As...",
                        shortcut: "Ctrl+Shift+S",
                        msg: Some(BrowserMsg::SaveAs),
                    },
                    crate::menu_bar::MenuItem {
                        label: "Open File...",
                        shortcut: "Ctrl+O",
                        msg: Some(BrowserMsg::OpenFile),
                    },
                    crate::menu_bar::MenuItem {
                        label: "Open Netidx...",
                        shortcut: "Ctrl+L",
                        msg: Some(BrowserMsg::ShowGoDialog),
                    },
                ],
            },
            crate::menu_bar::MenuGroup {
                label: "View",
                items: vec![
                    crate::menu_bar::MenuItem {
                        label: "Design Mode",
                        shortcut: "Ctrl+D",
                        msg: Some(BrowserMsg::ToggleDesignMode),
                    },
                    crate::menu_bar::MenuItem {
                        label: if self.raw_view { "Raw View  [on]" } else { "Raw View" },
                        shortcut: "",
                        msg: Some(BrowserMsg::ToggleRawView),
                    },
                ],
            },
        ]).into();
        let mut col = Column::new().push(menu_bar);
        if self.show_go_dialog {
            col = col.push(self.go_dialog());
        }
        if self.show_save_dialog {
            col = col.push(self.save_dialog());
        }
        let crumbs = self.breadcrumbs(loc);
        let rule: Element<'a> =
            iced_widget::rule::horizontal::<'a, GraphixTheme>(1).into();
        col.push(crumbs).push(rule).push(content).into()
    }

    fn go_dialog(&self) -> Element<'_> {
        let label: Element<'_> = Text::new("Navigate to path:")
            .size(12)
            .into();
        let input: Element<'_> =
            TextInput::new("/netidx/path or file:/path", &self.go_dialog_input)
                .on_input(BrowserMsg::GoDialogInput)
                .on_submit(BrowserMsg::GoDialogSubmit)
                .size(13)
                .padding(4)
                .into();
        let go_btn: Element<'_> = Button::new(Text::new("Go").size(12))
            .style(dialog_button_style)
            .padding(iced_core::Padding::from([3, 12]))
            .on_press(BrowserMsg::GoDialogSubmit)
            .into();
        let cancel_btn: Element<'_> = Button::new(Text::new("Cancel").size(12))
            .style(dialog_button_style)
            .padding(iced_core::Padding::from([3, 12]))
            .on_press(BrowserMsg::GoDialogCancel)
            .into();
        let buttons: Element<'_> = Row::new()
            .push(go_btn)
            .push(cancel_btn)
            .spacing(4)
            .into();
        iced_widget::container(
            Column::new()
                .push(label)
                .push(input)
                .push(buttons)
                .spacing(4)
                .padding(iced_core::Padding::from(8))
        )
            .style(dialog_container_style)
            .width(iced_core::Length::Fill)
            .into()
    }

    fn save_dialog(&self) -> Element<'_> {
        let label: Element<'_> = Text::new("Save to Netidx:")
            .size(12)
            .into();
        let input: Element<'_> =
            TextInput::new("e.g. /my/path or file:/path/to/view.gx", &self.save_dialog_input)
                .on_input(BrowserMsg::SaveDialogInput)
                .on_submit(BrowserMsg::SaveDialogSubmit)
                .size(13)
                .padding(4)
                .into();
        let save_btn: Element<'_> = Button::new(Text::new("Save").size(12))
            .style(dialog_button_style)
            .padding(iced_core::Padding::from([3, 10]))
            .on_press(BrowserMsg::SaveDialogSubmit)
            .into();
        let save_file_btn: Element<'_> = Button::new(Text::new("Save to File").size(12))
            .style(dialog_button_style)
            .padding(iced_core::Padding::from([3, 10]))
            .on_press(BrowserMsg::SaveDialogBrowse)
            .into();
        let cancel_btn: Element<'_> = Button::new(Text::new("Cancel").size(12))
            .style(dialog_button_style)
            .padding(iced_core::Padding::from([3, 10]))
            .on_press(BrowserMsg::SaveDialogCancel)
            .into();
        let buttons: Element<'_> = Row::new()
            .push(save_btn)
            .push(save_file_btn)
            .push(cancel_btn)
            .spacing(4)
            .into();
        iced_widget::container(
            Column::new()
                .push(label)
                .push(input)
                .push(buttons)
                .spacing(4)
                .padding(iced_core::Padding::from(8))
        )
            .style(dialog_container_style)
            .width(iced_core::Length::Fill)
            .into()
    }

    fn breadcrumbs<'a>(&'a self, loc: &'a ViewLoc) -> Element<'a> {
        let mut row = Row::new().spacing(2);
        match loc {
            ViewLoc::Netidx(path) => {
                for target in Path::dirnames(path) {
                    let name = match Path::basename(target) {
                        None => "/",
                        Some(n) => n,
                    };
                    if name != "/" {
                        let sep: Element<'a> = Text::new(" > ").size(14).into();
                        row = row.push(sep);
                    }
                    let target = target.to_string();
                    let label: Element<'a> = Text::new(name).size(14).into();
                    let btn: Element<'a> = Button::new(label)
                        .style(text_button_style)
                        .padding(iced_core::Padding::from([2, 4]))
                        .on_press(BrowserMsg::Navigate(
                            ViewLoc::Netidx(Path::from(target)),
                        ))
                        .into();
                    row = row.push(btn);
                }
            }
            ViewLoc::File(file) => {
                let root_label: Element<'a> = Text::new("/").size(14).into();
                let root_btn: Element<'a> = Button::new(root_label)
                    .style(text_button_style)
                    .padding(iced_core::Padding::from([2, 4]))
                    .on_press(BrowserMsg::Navigate(
                        ViewLoc::Netidx(Path::from("/")),
                    ))
                    .into();
                row = row.push(root_btn);
                let sep: Element<'a> = Text::new(" > ").size(14).into();
                row = row.push(sep);
                let file_label: Element<'a> =
                    Text::new(format!("file:{}", file.display()))
                        .size(14)
                        .into();
                row = row.push(file_label);
            }
        }
        Scrollable::new(row)
            .direction(iced_widget::scrollable::Direction::Horizontal(
                iced_widget::scrollable::Scrollbar::default(),
            ))
            .into()
    }
}

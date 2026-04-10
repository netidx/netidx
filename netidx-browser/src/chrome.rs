//! Browser chrome: breadcrumbs, navigation bar, toolbar.
//!
//! Built as native iced widgets (not graphix-authored). The chrome
//! wraps around the graphix-compiled content area.

use crate::{BrowserMsg, ViewLoc};
use graphix_package_gui::{theme::GraphixTheme, widgets::Renderer};
use netidx::path::Path;

type Element<'a> = iced_core::Element<'a, BrowserMsg, GraphixTheme, Renderer>;
type Row<'a> = iced_widget::Row<'a, BrowserMsg, GraphixTheme, Renderer>;
type Column<'a> = iced_widget::Column<'a, BrowserMsg, GraphixTheme, Renderer>;
type Button<'a> = iced_widget::Button<'a, BrowserMsg, GraphixTheme, Renderer>;
type TextInput<'a> = iced_widget::TextInput<'a, BrowserMsg, GraphixTheme, Renderer>;
type Text<'a> = iced_widget::Text<'a, GraphixTheme, Renderer>;
type Scrollable<'a> = iced_widget::Scrollable<'a, BrowserMsg, GraphixTheme, Renderer>;

/// Button style that looks like a text link (no background, no border).
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
        ..iced_widget::button::Style::default()
    }
}

/// Browser chrome state.
pub(crate) struct Chrome {
    /// Text in the navigation input box
    pub(crate) nav_input: String,
}

impl Chrome {
    pub(crate) fn new() -> Self {
        Self { nav_input: String::new() }
    }

    /// Build the full browser UI: chrome bar on top, content below.
    pub(crate) fn view<'a>(
        &'a self,
        loc: &'a ViewLoc,
        content: Element<'a>,
    ) -> Element<'a> {
        let crumbs = self.breadcrumbs(loc);
        let nav_row = self.nav_bar();
        let rule: Element<'a> =
            iced_widget::rule::horizontal::<'a, GraphixTheme>(1).into();
        Column::new()
            .push(crumbs)
            .push(nav_row)
            .push(rule)
            .push(content)
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

    fn nav_bar(&self) -> Element<'_> {
        let input: Element<'_> =
            TextInput::new("Navigate to path...", &self.nav_input)
                .on_input(BrowserMsg::NavInputChanged)
                .on_submit(BrowserMsg::NavSubmit)
                .size(14)
                .padding(4)
                .into();
        let go_label: Element<'_> = Text::new("Go").size(14).into();
        let go_btn: Element<'_> = Button::new(go_label)
            .padding(iced_core::Padding::from([4, 12]))
            .on_press(BrowserMsg::NavSubmit)
            .into();
        let design_label: Element<'_> = Text::new("Design").size(14).into();
        let design_btn: Element<'_> = Button::new(design_label)
            .padding(iced_core::Padding::from([4, 8]))
            .on_press(BrowserMsg::ToggleDesignMode)
            .into();
        Row::new()
            .push(input)
            .push(go_btn)
            .push(design_btn)
            .spacing(4)
            .padding(iced_core::Padding::from([2, 4]))
            .into()
    }
}

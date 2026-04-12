//! Custom menu bar widget with floating overlay dropdowns.
//!
//! Follows the same overlay pattern as graphix's OwnedMenuBar:
//! the widget renders menu header buttons, and when one is open,
//! `overlay()` returns a `MenuOverlay` that renders the dropdown
//! items floating on top of the window content.

use crate::BrowserMsg;
use graphix_package_gui::{theme::GraphixTheme, widgets::Renderer};
use iced_core::{
    alignment, layout, mouse, overlay, renderer, touch, widget, Clipboard,
    Element, Event, Layout, Length, Padding, Point, Rectangle, Shell, Size, Vector,
    Widget,
};

// ---- Data types ----

pub(crate) struct MenuItem {
    pub label: &'static str,
    pub shortcut: &'static str,
    pub msg: Option<BrowserMsg>,
}

pub(crate) struct MenuGroup {
    pub label: &'static str,
    pub items: Vec<MenuItem>,
}

// ---- Widget state ----

#[derive(Default)]
struct State {
    open_menu: Option<usize>,
}

// ---- Menu bar widget ----

pub(crate) struct MenuBar {
    menus: Vec<MenuGroup>,
}

impl MenuBar {
    pub(crate) fn new(menus: Vec<MenuGroup>) -> Self {
        Self { menus }
    }
}

const BAR_PADDING: Padding = Padding { top: 4.0, right: 10.0, bottom: 4.0, left: 10.0 };
const ITEM_PADDING: Padding = Padding { top: 5.0, right: 16.0, bottom: 5.0, left: 16.0 };
const MIN_ITEM_WIDTH: f32 = 220.0;

impl Widget<BrowserMsg, GraphixTheme, Renderer> for MenuBar {
    fn tag(&self) -> widget::tree::Tag {
        widget::tree::Tag::of::<State>()
    }

    fn state(&self) -> widget::tree::State {
        widget::tree::State::new(State::default())
    }

    fn size(&self) -> Size<Length> {
        Size::new(Length::Fill, Length::Shrink)
    }

    fn layout(
        &mut self,
        _tree: &mut widget::Tree,
        renderer: &Renderer,
        limits: &layout::Limits,
    ) -> layout::Node {
        let limits = limits.width(Length::Fill);
        let max = limits.max();
        let text_size = <Renderer as iced_core::text::Renderer>::default_size(renderer).0;
        let mut total_width: f32 = 0.0;
        let mut max_height: f32 = 0.0;
        let mut children = Vec::with_capacity(self.menus.len());
        for menu in &self.menus {
            let label_w = text_size * menu.label.len() as f32 * 0.6;
            let padded_w = label_w + BAR_PADDING.left + BAR_PADDING.right;
            let padded_h = text_size + BAR_PADDING.top + BAR_PADDING.bottom;
            children.push(
                layout::Node::new(Size::new(padded_w, padded_h))
                    .move_to(Point::new(total_width, 0.0)),
            );
            total_width += padded_w;
            max_height = max_height.max(padded_h);
        }
        for child in &mut children {
            let s = child.size();
            *child = layout::Node::new(Size::new(s.width, max_height))
                .move_to(child.bounds().position());
        }
        layout::Node::with_children(Size::new(max.width, max_height), children)
    }

    fn draw(
        &self,
        tree: &widget::Tree,
        renderer: &mut Renderer,
        theme: &GraphixTheme,
        _style: &renderer::Style,
        layout: Layout<'_>,
        cursor: mouse::Cursor,
        _viewport: &Rectangle,
    ) {
        let state = tree.state.downcast_ref::<State>();
        let palette = theme.palette();
        // Bar background
        let bar_bg = iced_core::Color::from_rgba(
            palette.background.r * 0.9,
            palette.background.g * 0.9,
            palette.background.b * 0.9,
            1.0,
        );
        <Renderer as renderer::Renderer>::fill_quad(
            renderer,
            renderer::Quad {
                bounds: layout.bounds(),
                border: Default::default(),
                shadow: Default::default(),
                snap: true,
            },
            bar_bg,
        );
        let text_size = <Renderer as iced_core::text::Renderer>::default_size(renderer);
        for (i, (menu, child_layout)) in
            self.menus.iter().zip(layout.children()).enumerate()
        {
            let bounds = child_layout.bounds();
            let is_open = state.open_menu == Some(i);
            let is_hovered = cursor.is_over(bounds);
            if is_open || is_hovered {
                let highlight = iced_core::Color::from_rgba(
                    palette.primary.r,
                    palette.primary.g,
                    palette.primary.b,
                    if is_open { 0.3 } else { 0.15 },
                );
                <Renderer as renderer::Renderer>::fill_quad(
                    renderer,
                    renderer::Quad {
                        bounds,
                        border: Default::default(),
                        shadow: Default::default(),
                        snap: true,
                    },
                    highlight,
                );
            }
            <Renderer as iced_core::text::Renderer>::fill_text(
                renderer,
                iced_core::Text {
                    content: menu.label.into(),
                    bounds: Size::new(bounds.width, bounds.height),
                    size: text_size,
                    line_height: iced_core::text::LineHeight::default(),
                    font: iced_core::Font::DEFAULT,
                    align_x: alignment::Horizontal::Center.into(),
                    align_y: alignment::Vertical::Center,
                    shaping: iced_core::text::Shaping::Basic,
                    wrapping: iced_core::text::Wrapping::None,
                },
                bounds.center(),
                palette.text,
                bounds,
            );
        }
    }

    fn update(
        &mut self,
        tree: &mut widget::Tree,
        event: &Event,
        layout: Layout<'_>,
        cursor: mouse::Cursor,
        _renderer: &Renderer,
        _clipboard: &mut dyn Clipboard,
        shell: &mut Shell<'_, BrowserMsg>,
        _viewport: &Rectangle,
    ) {
        let state = tree.state.downcast_mut::<State>();
        match event {
            Event::Mouse(mouse::Event::ButtonPressed(mouse::Button::Left))
            | Event::Touch(touch::Event::FingerPressed { .. }) => {
                for (i, child_layout) in layout.children().enumerate() {
                    if cursor.is_over(child_layout.bounds()) {
                        if state.open_menu == Some(i) {
                            state.open_menu = None;
                        } else {
                            state.open_menu = Some(i);
                        }
                        shell.capture_event();
                        return;
                    }
                }
                // Click outside menu headers while menu is open → close
                if state.open_menu.is_some() {
                    state.open_menu = None;
                    // Don't capture — let the click pass through
                }
            }
            Event::Mouse(mouse::Event::CursorMoved { .. }) => {
                // Hover-switch between menus while one is open
                if state.open_menu.is_some() {
                    for (i, child_layout) in layout.children().enumerate() {
                        if cursor.is_over(child_layout.bounds())
                            && state.open_menu != Some(i)
                        {
                            state.open_menu = Some(i);
                            return;
                        }
                    }
                }
            }
            Event::Keyboard(iced_core::keyboard::Event::KeyPressed {
                key: iced_core::keyboard::Key::Named(iced_core::keyboard::key::Named::Escape),
                ..
            }) => {
                if state.open_menu.is_some() {
                    state.open_menu = None;
                    shell.capture_event();
                }
            }
            _ => {}
        }
    }

    fn overlay<'b>(
        &'b mut self,
        tree: &'b mut widget::Tree,
        layout: Layout<'b>,
        _renderer: &Renderer,
        _viewport: &Rectangle,
        _translation: Vector,
    ) -> Option<overlay::Element<'b, BrowserMsg, GraphixTheme, Renderer>> {
        let state = tree.state.downcast_mut::<State>();
        let idx = state.open_menu?;
        if idx >= self.menus.len() {
            return None;
        }
        let label_bounds = layout.children().nth(idx)?.bounds();
        let position = Point::new(label_bounds.x, label_bounds.y + label_bounds.height);
        Some(overlay::Element::new(Box::new(MenuOverlay {
            menu: &self.menus[idx],
            position,
            state,
        })))
    }
}

impl<'a> From<MenuBar> for Element<'a, BrowserMsg, GraphixTheme, Renderer> {
    fn from(w: MenuBar) -> Self {
        Self::new(w)
    }
}

// ---- Floating overlay ----

struct MenuOverlay<'a> {
    menu: &'a MenuGroup,
    position: Point,
    state: &'a mut State,
}

impl overlay::Overlay<BrowserMsg, GraphixTheme, Renderer> for MenuOverlay<'_> {
    fn layout(&mut self, renderer: &Renderer, _bounds: Size) -> layout::Node {
        let text_size = <Renderer as iced_core::text::Renderer>::default_size(renderer).0;
        let mut max_width: f32 = MIN_ITEM_WIDTH;
        let mut total_height: f32 = 0.0;
        let mut child_sizes = Vec::with_capacity(self.menu.items.len());
        for item in &self.menu.items {
            let display_len = if item.shortcut.is_empty() {
                item.label.len()
            } else {
                item.label.len() + 3 + item.shortcut.len()
            };
            let item_w = text_size * display_len as f32 * 0.6
                + ITEM_PADDING.left
                + ITEM_PADDING.right;
            let item_h = text_size + ITEM_PADDING.top + ITEM_PADDING.bottom;
            max_width = max_width.max(item_w);
            child_sizes.push(item_h);
            total_height += item_h;
        }
        let mut y = 0.0f32;
        let nodes: Vec<_> = child_sizes
            .into_iter()
            .map(|h| {
                let node = layout::Node::new(Size::new(max_width, h))
                    .move_to(Point::new(0.0, y));
                y += h;
                node
            })
            .collect();
        layout::Node::with_children(Size::new(max_width, total_height), nodes)
            .move_to(self.position)
    }

    fn draw(
        &self,
        renderer: &mut Renderer,
        theme: &GraphixTheme,
        _style: &renderer::Style,
        layout: Layout<'_>,
        cursor: mouse::Cursor,
    ) {
        let palette = theme.palette();
        let bounds = layout.bounds();
        // Drop shadow
        <Renderer as renderer::Renderer>::fill_quad(
            renderer,
            renderer::Quad {
                bounds: Rectangle { x: bounds.x + 2.0, y: bounds.y + 2.0, ..bounds },
                border: Default::default(),
                shadow: Default::default(),
                snap: true,
            },
            iced_core::Color::from_rgba(0.0, 0.0, 0.0, 0.3),
        );
        // Background
        <Renderer as renderer::Renderer>::fill_quad(
            renderer,
            renderer::Quad {
                bounds,
                border: iced_core::Border {
                    color: iced_core::Color::from_rgba(0.5, 0.5, 0.5, 0.3),
                    width: 1.0,
                    radius: 4.0.into(),
                },
                shadow: Default::default(),
                snap: true,
            },
            palette.background,
        );
        // Items
        let text_size = <Renderer as iced_core::text::Renderer>::default_size(renderer);
        for (item, child_layout) in self.menu.items.iter().zip(layout.children()) {
            let item_bounds = child_layout.bounds();
            let disabled = item.msg.is_none();
            let is_hovered = !disabled && cursor.is_over(item_bounds);
            if is_hovered {
                <Renderer as renderer::Renderer>::fill_quad(
                    renderer,
                    renderer::Quad {
                        bounds: item_bounds,
                        border: Default::default(),
                        shadow: Default::default(),
                        snap: true,
                    },
                    iced_core::Color::from_rgba(
                        palette.primary.r,
                        palette.primary.g,
                        palette.primary.b,
                        0.25,
                    ),
                );
            }
            let text_color = if disabled {
                iced_core::Color::from_rgba(
                    palette.text.r,
                    palette.text.g,
                    palette.text.b,
                    0.4,
                )
            } else {
                palette.text
            };
            let text_bounds = Size::new(
                item_bounds.width - ITEM_PADDING.left - ITEM_PADDING.right,
                item_bounds.height,
            );
            // Label
            <Renderer as iced_core::text::Renderer>::fill_text(
                renderer,
                iced_core::Text {
                    content: item.label.into(),
                    bounds: text_bounds,
                    size: text_size,
                    line_height: iced_core::text::LineHeight::default(),
                    font: iced_core::Font::DEFAULT,
                    align_x: alignment::Horizontal::Left.into(),
                    align_y: alignment::Vertical::Center,
                    shaping: iced_core::text::Shaping::Basic,
                    wrapping: iced_core::text::Wrapping::None,
                },
                Point::new(
                    item_bounds.x + ITEM_PADDING.left,
                    item_bounds.center_y(),
                ),
                text_color,
                item_bounds,
            );
            // Shortcut
            if !item.shortcut.is_empty() {
                let dimmed = iced_core::Color::from_rgba(
                    text_color.r,
                    text_color.g,
                    text_color.b,
                    text_color.a * 0.5,
                );
                <Renderer as iced_core::text::Renderer>::fill_text(
                    renderer,
                    iced_core::Text {
                        content: item.shortcut.into(),
                        bounds: text_bounds,
                        size: text_size,
                        line_height: iced_core::text::LineHeight::default(),
                        font: iced_core::Font::DEFAULT,
                        align_x: alignment::Horizontal::Right.into(),
                        align_y: alignment::Vertical::Center,
                        shaping: iced_core::text::Shaping::Basic,
                        wrapping: iced_core::text::Wrapping::None,
                    },
                    Point::new(
                        item_bounds.x + item_bounds.width - ITEM_PADDING.right,
                        item_bounds.center_y(),
                    ),
                    dimmed,
                    item_bounds,
                );
            }
        }
    }

    fn update(
        &mut self,
        event: &Event,
        layout: Layout<'_>,
        cursor: mouse::Cursor,
        _renderer: &Renderer,
        _clipboard: &mut dyn Clipboard,
        shell: &mut Shell<'_, BrowserMsg>,
    ) {
        match event {
            Event::Mouse(mouse::Event::ButtonPressed(mouse::Button::Left))
            | Event::Touch(touch::Event::FingerPressed { .. }) => {
                for (item, child_layout) in self.menu.items.iter().zip(layout.children()) {
                    if cursor.is_over(child_layout.bounds()) {
                        if let Some(msg) = &item.msg {
                            self.state.open_menu = None;
                            shell.publish(msg.clone());
                            shell.capture_event();
                            return;
                        }
                    }
                }
                // Click outside the dropdown → close menu
                if !cursor.is_over(layout.bounds()) {
                    self.state.open_menu = None;
                }
            }
            _ => {}
        }
    }
}

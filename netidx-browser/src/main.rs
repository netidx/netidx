#![recursion_limit = "2048"]

mod backend;
mod builtins;
mod chrome;

use anyhow::Result;
use arcstr::ArcStr;
use graphix_compiler::expr::ExprId;
use graphix_package_gui::{
    convert,
    render::{GpuState, WindowSurface},
    theme::GraphixTheme,
    widgets::{self, GuiW, IcedElement, Message, Renderer},
};
use graphix_rt::NoExt;
use iced_core::{clipboard, mouse, renderer::Style};
use iced_runtime::user_interface::{self, UserInterface};
use iced_wgpu::wgpu;
use log::error;
use netidx::{
    config::Config,
    path::Path,
    publisher::Value,
    subscriber::DesiredAuth,
};
use std::{
    cell::RefCell,
    fmt, mem,
    path::PathBuf,
    str,
    sync::Arc,
    time::{Duration, Instant},
};
use winit::{
    application::ApplicationHandler,
    event::WindowEvent,
    event_loop::{ActiveEventLoop, ControlFlow, EventLoop},
    keyboard::ModifiersState,
    window::{CursorIcon, WindowId},
};

// ---- Browser UI message type ----

/// Unified message type for the browser UI. Wraps graphix widget
/// messages and adds browser-specific actions.
#[derive(Debug, Clone)]
pub(crate) enum BrowserMsg {
    /// Message from a graphix widget (button press, text input, etc.)
    Widget(Message),
    /// Navigate to a location (from breadcrumb click or Go button)
    Navigate(ViewLoc),
    /// Navigate to parent path (Backspace)
    NavigateUp,
    /// Navigation input text changed
    NavInputChanged(String),
    /// Navigation input submitted (Enter or Go button)
    NavSubmit,
    /// Toggle design mode (opens/closes design window)
    ToggleDesignMode,
}

/// Element type for the full browser UI (chrome + content).
type BrowserElement<'a> =
    iced_core::Element<'a, BrowserMsg, GraphixTheme, Renderer>;

// ---- ViewLoc ----

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ViewLoc {
    File(PathBuf),
    Netidx(Path),
}

impl str::FromStr for ViewLoc {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if Path::is_absolute(s) {
            Ok(ViewLoc::Netidx(Path::from(ArcStr::from(s))))
        } else if s.starts_with("netidx:") && Path::is_absolute(&s[7..]) {
            Ok(ViewLoc::Netidx(Path::from(ArcStr::from(&s[7..]))))
        } else if s.starts_with("file:") {
            let mut buf = PathBuf::new();
            buf.push(&s[5..]);
            Ok(ViewLoc::File(buf))
        } else {
            Err(())
        }
    }
}

impl fmt::Display for ViewLoc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ViewLoc::File(s) => write!(f, "file:{:?}", s),
            ViewLoc::Netidx(s) => write!(f, "netidx:{}", s),
        }
    }
}

// ---- Default view ----

fn default_view_source(path: &Path) -> ArcStr {
    ArcStr::from(format!(
        "use gui;\nuse gui::data_table;\nuse sys;\n\
         let sel = [];\n\
         data_table(\n\
         \x20 #on_activate: |#path: string| browser::navigate(path),\n\
         \x20 #on_select: |#path: string| sel <- [path],\n\
         \x20 #selection: &sel,\n\
         \x20 #table: &sys::net::list_table({:?})$\n\
         )",
        &**path
    ))
}

// ---- BrowserEvent ----

/// Messages delivered to the winit event loop via EventLoopProxy.
#[derive(Debug)]
pub(crate) enum BrowserEvent {
    /// A new view to display
    View {
        loc: Option<ViewLoc>,
        source: ArcStr,
        generated: bool,
    },
    /// Navigate to a location (from graphix builtin)
    Navigate(ViewLoc),
    /// Open location in a new window
    NavigateInWindow(ViewLoc),
    /// Batch of graphix expression updates
    Update(Vec<(ExprId, Value)>),
    /// Display an error
    ShowError(String),
    /// Request confirmation (reply via oneshot)
    Confirm {
        message: String,
        reply: Arc<std::sync::Mutex<Option<tokio::sync::oneshot::Sender<bool>>>>,
    },
    /// Shutdown
    Terminate,
}

// ---- Clipboard ----

struct Clipboard {
    state: RefCell<Option<arboard::Clipboard>>,
}

impl Clipboard {
    fn new() -> Self {
        Self { state: RefCell::new(arboard::Clipboard::new().ok()) }
    }
}

impl clipboard::Clipboard for Clipboard {
    fn read(&self, kind: clipboard::Kind) -> Option<String> {
        let mut cb = self.state.borrow_mut();
        let cb = cb.as_mut()?;
        match kind {
            clipboard::Kind::Standard => cb.get_text().ok(),
            clipboard::Kind::Primary => {
                #[cfg(target_os = "linux")]
                {
                    use arboard::GetExtLinux;
                    cb.get().clipboard(arboard::LinuxClipboardKind::Primary).text().ok()
                }
                #[cfg(not(target_os = "linux"))]
                None
            }
        }
    }

    fn write(&mut self, kind: clipboard::Kind, contents: String) {
        let mut cb = self.state.borrow_mut();
        let Some(cb) = cb.as_mut() else { return };
        match kind {
            clipboard::Kind::Standard => {
                let _ = cb.set_text(contents);
            }
            clipboard::Kind::Primary => {
                #[cfg(target_os = "linux")]
                {
                    use arboard::SetExtLinux;
                    let _ = cb
                        .set()
                        .clipboard(arboard::LinuxClipboardKind::Primary)
                        .text(contents);
                }
            }
        }
    }
}

// ---- Mouse interaction mapping ----

fn mouse_interaction_to_cursor(interaction: mouse::Interaction) -> CursorIcon {
    match interaction {
        mouse::Interaction::None | mouse::Interaction::Idle => CursorIcon::Default,
        mouse::Interaction::Hidden => CursorIcon::Default,
        mouse::Interaction::Pointer => CursorIcon::Pointer,
        mouse::Interaction::Grab => CursorIcon::Grab,
        mouse::Interaction::Grabbing => CursorIcon::Grabbing,
        mouse::Interaction::Text => CursorIcon::Text,
        mouse::Interaction::Crosshair => CursorIcon::Crosshair,
        mouse::Interaction::Cell => CursorIcon::Cell,
        mouse::Interaction::Help => CursorIcon::Help,
        mouse::Interaction::ContextMenu => CursorIcon::ContextMenu,
        mouse::Interaction::Progress => CursorIcon::Progress,
        mouse::Interaction::Wait => CursorIcon::Wait,
        mouse::Interaction::Alias => CursorIcon::Alias,
        mouse::Interaction::Copy => CursorIcon::Copy,
        mouse::Interaction::Move => CursorIcon::Move,
        mouse::Interaction::NoDrop => CursorIcon::NoDrop,
        mouse::Interaction::NotAllowed => CursorIcon::NotAllowed,
        mouse::Interaction::ResizingHorizontally => CursorIcon::EwResize,
        mouse::Interaction::ResizingVertically => CursorIcon::NsResize,
        mouse::Interaction::ResizingDiagonallyUp => CursorIcon::NeswResize,
        mouse::Interaction::ResizingDiagonallyDown => CursorIcon::NwseResize,
        mouse::Interaction::ResizingColumn => CursorIcon::ColResize,
        mouse::Interaction::ResizingRow => CursorIcon::RowResize,
        mouse::Interaction::AllScroll => CursorIcon::AllScroll,
        mouse::Interaction::ZoomIn => CursorIcon::ZoomIn,
        mouse::Interaction::ZoomOut => CursorIcon::ZoomOut,
    }
}

// ---- Compiled view ----

/// A compiled graphix view ready for rendering.
struct CompiledView {
    content: GuiW<NoExt>,
    gx: graphix_rt::GXHandle<NoExt>,
    /// If set, the first update matching this id triggers widget compilation
    /// from the graphix value. Once compiled, this is set to None.
    pending_top_id: Option<ExprId>,
    /// Keeps the CompRes alive so graphix expressions don't get dropped.
    _comp: Option<graphix_rt::CompRes<NoExt>>,
}

impl CompiledView {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        // If we're waiting for the top expression's first value, compile it
        // into an actual widget tree.
        if let Some(top_id) = self.pending_top_id {
            if id == top_id {
                match rt.block_on(widgets::compile(self.gx.clone(), v.clone())) {
                    Ok(w) => {
                        self.content = w;
                        self.pending_top_id = None;
                        return Ok(true);
                    }
                    Err(e) => {
                        log::warn!("failed to compile widget tree: {e:?}");
                        self.content = Box::new(ErrorView(format!("{e:?}")));
                        self.pending_top_id = None;
                        return Ok(true);
                    }
                }
            }
        }
        self.content.handle_update(rt, id, v)
    }

    fn view(&self) -> IcedElement<'_> {
        self.content.view()
    }
}

// ---- Browser window state ----

struct BrowserWindow {
    window: Arc<winit::window::Window>,
    pending_events: Vec<iced_core::Event>,
    cursor_position: iced_core::Point,
    needs_redraw: bool,
    last_render: Instant,
    pending_resize: Option<(u32, u32, f64)>,
    last_mouse_interaction: mouse::Interaction,
    /// The currently displayed compiled view, if any
    view: Option<CompiledView>,
    /// Current location
    current_loc: ViewLoc,
}

impl BrowserWindow {
    fn new(window: Arc<winit::window::Window>) -> Self {
        Self {
            window,
            pending_events: Vec::new(),
            cursor_position: iced_core::Point::ORIGIN,
            needs_redraw: true,
            last_render: Instant::now(),
            pending_resize: None,
            last_mouse_interaction: mouse::Interaction::Idle,
            view: None,
            current_loc: ViewLoc::Netidx(Path::from("/")),
        }
    }

    fn window_id(&self) -> WindowId {
        self.window.id()
    }

    fn push_event(&mut self, ev: iced_core::Event) {
        self.pending_events.push(ev);
        self.needs_redraw = true;
    }

    fn cursor(&self) -> mouse::Cursor {
        mouse::Cursor::Available(self.cursor_position)
    }

    fn content_element(&self) -> IcedElement<'_> {
        match &self.view {
            Some(v) => v.view(),
            None => iced_widget::text("Loading...").into(),
        }
    }
}

const RESIZE_RENDER_INTERVAL: Duration = Duration::from_millis(8);

// ---- Main handler ----

struct BrowserHandler {
    backend: backend::Ctx,
    gpu: Option<GpuState>,
    rt: tokio::runtime::Handle,
    window: Option<BrowserWindow>,
    surface: Option<WindowSurface>,
    ui_cache: user_interface::Cache,
    clipboard: Clipboard,
    chrome: chrome::Chrome,
    messages: Vec<BrowserMsg>,
    modifiers: ModifiersState,
    /// Initial location to navigate to on startup
    initial_loc: Option<ViewLoc>,
}

impl ApplicationHandler<BrowserEvent> for BrowserHandler {
    fn resumed(&mut self, event_loop: &ActiveEventLoop) {
        event_loop.set_control_flow(ControlFlow::Wait);
        // Create the initial window on first resume
        if self.window.is_none() {
            let attrs = winit::window::WindowAttributes::default()
                .with_title("Netidx Browser")
                .with_inner_size(winit::dpi::LogicalSize::new(1024.0, 768.0));
            match event_loop.create_window(attrs) {
                Ok(win) => {
                    let win = Arc::new(win);
                    // Initialize GPU state from this window
                    if self.gpu.is_none() {
                        match self.rt.block_on(GpuState::new(win.clone())) {
                            Ok(gpu) => {
                                match WindowSurface::new(&gpu, win.clone()) {
                                    Ok(ws) => {
                                        self.surface = Some(ws);
                                    }
                                    Err(e) => {
                                        error!("failed to create window surface: {e:?}");
                                    }
                                }
                                self.gpu = Some(gpu);
                            }
                            Err(e) => {
                                error!("failed to create GPU state: {e:?}");
                            }
                        }
                    }
                    self.window = Some(BrowserWindow::new(win));
                    // Navigate to initial location
                    if let Some(loc) = self.initial_loc.take() {
                        self.backend.navigate(loc);
                    }
                }
                Err(e) => {
                    error!("failed to create window: {e:?}");
                    event_loop.exit();
                }
            }
        }
    }

    fn window_event(
        &mut self,
        event_loop: &ActiveEventLoop,
        _window_id: WindowId,
        event: WindowEvent,
    ) {
        if let WindowEvent::ModifiersChanged(m) = &event {
            self.modifiers = m.state();
        }

        let Some(bw) = self.window.as_mut() else { return };

        match &event {
            WindowEvent::Resized(size) => {
                let scale = bw.window.scale_factor();
                bw.pending_resize = Some((size.width, size.height, scale));
                bw.needs_redraw = true;
                // Notify data table of new viewport size
                let logical = size.to_logical::<f32>(scale);
                if let Some(view) = bw.view.as_mut() {
                    view.content.handle_viewport_resize(logical.width, logical.height);
                }
            }
            WindowEvent::RedrawRequested => {
                bw.needs_redraw = true;
            }
            WindowEvent::CloseRequested => {
                self.backend.terminate();
                self.window = None;
                self.surface = None;
                self.gpu = None;
                event_loop.exit();
                return;
            }
            _ => {
                // Intercept Backspace for browser "navigate up".
                // Only fires if no text input widget is focused (iced handles
                // Backspace for text inputs before it reaches here).
                if let WindowEvent::KeyboardInput { event: ref key_event, .. } = event {
                    use winit::keyboard::{Key, NamedKey};
                    if key_event.state == winit::event::ElementState::Pressed
                        && key_event.logical_key == Key::Named(NamedKey::Backspace)
                    {
                        self.messages.push(BrowserMsg::NavigateUp);
                    }
                }
                let scale = bw.window.scale_factor();
                let mut iced_events = convert::window_event(&event, scale, self.modifiers);
                for ev in iced_events.drain(..) {
                    if let iced_core::Event::Mouse(mouse::Event::CursorMoved {
                        position,
                    }) = &ev
                    {
                        bw.cursor_position = *position;
                    }
                    bw.push_event(ev);
                }
            }
        }
    }

    fn user_event(&mut self, event_loop: &ActiveEventLoop, event: BrowserEvent) {
        match event {
            BrowserEvent::Terminate => {
                self.window = None;
                self.surface = None;
                self.gpu = None;
                event_loop.exit();
            }
            BrowserEvent::View { loc, source, generated: _ } => {
                if let Some(bw) = self.window.as_mut() {
                    if let Some(l) = &loc {
                        bw.current_loc = l.clone();
                        self.backend.set_current_path(l);
                    }
                    // Compile the view source
                    match self.rt.block_on(
                        self.backend.gx.compile(source.clone()),
                    ) {
                        Err(e) => {
                            error!("failed to compile view: {e:?}");
                            bw.view = None;
                        }
                        Ok(comp) => {
                            if comp.exprs.is_empty() {
                                bw.view = None;
                            } else {
                                // Use last expression (use statements may precede it)
                                let top_id = comp.exprs.last().unwrap().id;
                                bw.view = Some(CompiledView {
                                    content: Box::new(LoadingView),
                                    gx: self.backend.gx.clone(),
                                    pending_top_id: Some(top_id),
                                    _comp: Some(comp),
                                });
                            }
                        }
                    }
                    bw.needs_redraw = true;
                }
            }
            BrowserEvent::Navigate(loc) => {
                self.backend.navigate(loc);
            }
            BrowserEvent::NavigateInWindow(_loc) => {
                // TODO: Phase 6 multi-window support
            }
            BrowserEvent::Update(updates) => {
                if let Some(bw) = self.window.as_mut() {
                    for (id, v) in &updates {
                        if let Some(view) = bw.view.as_mut() {
                            match view.handle_update(&self.rt, *id, v) {
                                Ok(changed) => {
                                    if changed {
                                        bw.needs_redraw = true;
                                    }
                                }
                                Err(e) => {
                                    error!("widget update error: {e:?}");
                                }
                            }
                        }
                    }
                }
            }
            BrowserEvent::ShowError(msg) => {
                error!("browser error: {msg}");
                // TODO: show error overlay in Phase 2
            }
            BrowserEvent::Confirm { message: _, reply } => {
                // TODO: show confirmation overlay in Phase 6
                // For now, auto-confirm
                if let Some(tx) = reply.lock().unwrap().take() {
                    let _ = tx.send(true);
                }
            }
        }
    }

    fn about_to_wait(&mut self, event_loop: &ActiveEventLoop) {
        let Some(gpu) = self.gpu.as_ref() else { return };
        let Some(bw) = self.window.as_mut() else { return };
        let Some(ws) = self.surface.as_mut() else { return };

        if !bw.needs_redraw {
            return;
        }

        // During resize, throttle rendering
        if bw.pending_resize.is_some() {
            let elapsed = bw.last_render.elapsed();
            if elapsed < RESIZE_RENDER_INTERVAL {
                let wake = bw.last_render + RESIZE_RENDER_INTERVAL;
                event_loop.set_control_flow(ControlFlow::WaitUntil(wake));
                return;
            }
        }

        if let Some((pw, ph, scale)) = bw.pending_resize.take() {
            ws.resize(gpu, pw, ph, scale);
            bw.push_event(iced_core::Event::Window(
                iced_core::window::Event::Resized(ws.logical_size()),
            ));
        }

        // Build and render the UI.
        // We must extract values from `state` before touching `bw` again,
        // because `ui` borrows the events in `bw`.
        let theme = {
            use iced_core::theme::Base;
            GraphixTheme {
                inner: iced_core::Theme::default(iced_core::theme::Mode::Dark),
                overrides: None,
            }
        };
        // Compose: chrome wraps around graphix content.
        // Graphix content uses Message; map it to BrowserMsg::Widget.
        let content: BrowserElement<'_> =
            bw.content_element().map(BrowserMsg::Widget);
        let element = self.chrome.view(&bw.current_loc, content);
        let viewport_size = ws.logical_size();
        let cache = mem::take(&mut self.ui_cache);
        let cursor = bw.cursor();
        let mut ui = UserInterface::build(element, viewport_size, cache, &mut ws.renderer);
        let (state, _statuses) = ui.update(
            &bw.pending_events,
            cursor,
            &mut ws.renderer,
            &mut self.clipboard,
            &mut self.messages,
        );

        // Extract mouse interaction and redraw request from state before
        // consuming the UI (which releases borrows on bw).
        let new_mouse = match &state {
            user_interface::State::Updated { mouse_interaction, .. } => {
                Some(*mouse_interaction)
            }
            _ => None,
        };
        let redraw_request = match &state {
            user_interface::State::Outdated => {
                Some(iced_core::window::RedrawRequest::NextFrame)
            }
            user_interface::State::Updated { redraw_request, .. } => {
                match redraw_request {
                    iced_core::window::RedrawRequest::Wait => None,
                    r => Some(*r),
                }
            }
        };

        let style = Style { text_color: theme.palette().text };
        ui.draw(&mut ws.renderer, &theme, &style, cursor);
        self.ui_cache = ui.into_cache();
        bw.pending_events.clear();

        // Now safe to modify bw
        if let Some(mi) = new_mouse {
            if bw.last_mouse_interaction != mi {
                bw.last_mouse_interaction = mi;
                match mi {
                    mouse::Interaction::Hidden => {
                        bw.window.set_cursor_visible(false);
                    }
                    _ => {
                        bw.window.set_cursor_visible(true);
                        bw.window.set_cursor(mouse_interaction_to_cursor(mi));
                    }
                }
            }
        }

        match ws.surface.get_current_texture() {
            Ok(frame) => {
                let view = frame
                    .texture
                    .create_view(&wgpu::TextureViewDescriptor::default());
                ws.renderer.present(
                    Some(theme.palette().background),
                    gpu.format,
                    &view,
                    &ws.viewport,
                );
                frame.present();
                bw.last_render = Instant::now();
                // When a view with subscriptions is active, poll at ~20fps
                // to pick up cell data updates from netidx.
                let poll_interval = if bw.view.is_some() {
                    Some(Instant::now() + Duration::from_millis(100))
                } else {
                    None
                };
                let wake = match (redraw_request, poll_interval) {
                    (Some(r), Some(p)) => {
                        let t = match r {
                            iced_core::window::RedrawRequest::NextFrame => Instant::now(),
                            iced_core::window::RedrawRequest::At(t) => t,
                            iced_core::window::RedrawRequest::Wait => unreachable!(),
                        };
                        Some(t.min(p))
                    }
                    (Some(r), None) => Some(match r {
                        iced_core::window::RedrawRequest::NextFrame => Instant::now(),
                        iced_core::window::RedrawRequest::At(t) => t,
                        iced_core::window::RedrawRequest::Wait => unreachable!(),
                    }),
                    (None, Some(p)) => Some(p),
                    (None, None) => None,
                };
                bw.needs_redraw = wake.is_some();
                if let Some(t) = wake {
                    event_loop.set_control_flow(ControlFlow::WaitUntil(t));
                } else {
                    event_loop.set_control_flow(ControlFlow::Wait);
                }
            }
            Err(wgpu::SurfaceError::Lost | wgpu::SurfaceError::Outdated) => {
                ws.surface.configure(&gpu.device, &ws.config);
                bw.needs_redraw = true;
                event_loop.set_control_flow(ControlFlow::Poll);
            }
            Err(e) => {
                error!("surface frame error: {e:?}");
                bw.needs_redraw = false;
            }
        }

        // Handle messages from iced widgets and browser chrome
        for msg in self.messages.drain(..) {
            match msg {
                BrowserMsg::Widget(Message::Nop) => {}
                BrowserMsg::Widget(Message::CellClick(row, col)) => {
                    if let Some(bw) = self.window.as_mut() {
                        if let Some(view) = bw.view.as_mut() {
                            if view.content.handle_cell_click(row, col) {
                                bw.needs_redraw = true;
                            }
                        }
                    }
                }
                BrowserMsg::Widget(Message::CellEdit(row, col)) => {
                    if let Some(bw) = self.window.as_mut() {
                        if let Some(view) = bw.view.as_mut() {
                            if view.content.handle_cell_edit(row, col) {
                                bw.needs_redraw = true;
                            }
                        }
                    }
                }
                BrowserMsg::Widget(Message::CellEditInput(text)) => {
                    if let Some(bw) = self.window.as_mut() {
                        if let Some(view) = bw.view.as_mut() {
                            if view.content.handle_cell_edit_input(text) {
                                bw.needs_redraw = true;
                            }
                        }
                    }
                }
                BrowserMsg::Widget(Message::CellEditSubmit) => {
                    if let Some(bw) = self.window.as_mut() {
                        if let Some(view) = bw.view.as_mut() {
                            if view.content.handle_cell_edit_submit() {
                                bw.needs_redraw = true;
                            }
                        }
                    }
                }
                BrowserMsg::Widget(Message::TableKey(action)) => {
                    if let Some(bw) = self.window.as_mut() {
                        if let Some(view) = bw.view.as_mut() {
                            if view.content.handle_table_key(&action) {
                                bw.needs_redraw = true;
                            }
                        }
                    }
                }
                BrowserMsg::Widget(Message::CellEditCancel) => {
                    if let Some(bw) = self.window.as_mut() {
                        if let Some(view) = bw.view.as_mut() {
                            if view.content.handle_cell_edit_cancel() {
                                bw.needs_redraw = true;
                            }
                        }
                    }
                }
                BrowserMsg::Widget(Message::Scroll(v, h, vp_w, vp_h)) => {
                    if let Some(bw) = self.window.as_mut() {
                        if let Some(view) = bw.view.as_mut() {
                            if view.content.handle_scroll(v, h, vp_w, vp_h) {
                                bw.needs_redraw = true;
                            }
                        }
                    }
                }
                BrowserMsg::Widget(Message::Call(id, args)) => {
                    if let Err(e) = self.backend.gx.call(id, args) {
                        error!("failed to call: {e:?}");
                    }
                }
                BrowserMsg::Widget(Message::EditorAction(id, action)) => {
                    if let Some(view) = self.window.as_mut().and_then(|bw| bw.view.as_mut()) {
                        if let Some((callable_id, v)) =
                            view.content.editor_action(id, &action)
                        {
                            if let Err(e) = self.backend.gx.call(
                                callable_id,
                                netidx::protocol::valarray::ValArray::from_iter([v]),
                            ) {
                                error!("failed to call editor callback: {e:?}");
                            }
                        }
                    }
                }
                BrowserMsg::Navigate(loc) => {
                    self.backend.navigate(loc);
                }
                BrowserMsg::NavigateUp => {
                    if let Some(bw) = self.window.as_ref() {
                        let parent = match &bw.current_loc {
                            ViewLoc::Netidx(p) => {
                                Path::dirname(p).map(|d| {
                                    ViewLoc::Netidx(Path::from(ArcStr::from(d)))
                                })
                            }
                            ViewLoc::File(_) => {
                                Some(ViewLoc::Netidx(Path::from("/")))
                            }
                        };
                        if let Some(loc) = parent {
                            self.backend.navigate(loc);
                        }
                    }
                }
                BrowserMsg::NavInputChanged(text) => {
                    self.chrome.nav_input = text;
                    if let Some(bw) = self.window.as_mut() {
                        bw.needs_redraw = true;
                    }
                }
                BrowserMsg::NavSubmit => {
                    let input = self.chrome.nav_input.trim().to_string();
                    if !input.is_empty() {
                        if let Ok(loc) = input.parse::<ViewLoc>() {
                            self.backend.navigate(loc);
                            self.chrome.nav_input.clear();
                        }
                    }
                }
                BrowserMsg::ToggleDesignMode => {
                    // TODO: Phase 5 — open design window
                }
            }
        }
    }
}

// ---- Placeholder widgets ----

struct LoadingView;

impl widgets::GuiWidget<NoExt> for LoadingView {
    fn handle_update(
        &mut self,
        _rt: &tokio::runtime::Handle,
        _id: ExprId,
        _v: &Value,
    ) -> Result<bool> {
        Ok(false)
    }

    fn view(&self) -> IcedElement<'_> {
        iced_widget::text("Loading...").into()
    }
}

struct ErrorView(String);

impl widgets::GuiWidget<NoExt> for ErrorView {
    fn handle_update(
        &mut self,
        _rt: &tokio::runtime::Handle,
        _id: ExprId,
        _v: &Value,
    ) -> Result<bool> {
        Ok(false)
    }

    fn view(&self) -> IcedElement<'_> {
        iced_widget::text(format!("Error: {}", self.0)).into()
    }
}

// ---- Entry point ----

fn main() {
    env_logger::init();

    let cfg = match Config::load_default() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to load netidx config: {e}");
            std::process::exit(1);
        }
    };
    let auth = cfg.default_auth();

    // Determine initial location from CLI args
    let initial_loc = std::env::args().nth(1).and_then(|a| a.parse::<ViewLoc>().ok());
    let initial_loc = initial_loc.unwrap_or(ViewLoc::Netidx(Path::from("/")));

    // Create the winit event loop on the main thread
    let event_loop = match EventLoop::<BrowserEvent>::with_user_event().build() {
        Ok(el) => el,
        Err(e) => {
            eprintln!("failed to create event loop: {e}");
            std::process::exit(1);
        }
    };
    let proxy = event_loop.create_proxy();

    // Create backend context (starts tokio runtime, graphix, subscriber)
    let ctx = match backend::Ctx::new(cfg, auth, proxy) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to create backend: {e}");
            std::process::exit(1);
        }
    };

    let rt_handle = ctx.rt_handle.clone();
    let mut handler = BrowserHandler {
        backend: ctx,
        gpu: None,
        rt: rt_handle,
        window: None,
        surface: None,
        ui_cache: user_interface::Cache::default(),
        clipboard: Clipboard::new(),
        chrome: chrome::Chrome::new(),
        messages: Vec::new(),
        modifiers: ModifiersState::default(),
        initial_loc: Some(initial_loc),
    };

    if let Err(e) = event_loop.run_app(&mut handler) {
        error!("event loop error: {e:?}");
    }
}

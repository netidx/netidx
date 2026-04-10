#![recursion_limit = "2048"]

mod backend;
mod builtins;
mod chrome;
mod editor;

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
use iced_core::{clipboard, mouse, renderer::Style, Color};
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
    /// Updated Env from graphix runtime (for completions)
    EnvUpdate(graphix_compiler::env::Env),
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

// ---- Design window state ----

/// Message type for the design window.
#[derive(Debug, Clone)]
pub(crate) enum DesignMsg {
    // ---- Source editor ----
    /// Editor text action (keystroke, cursor move, etc.)
    EditorAction(iced_widget::text_editor::Action),
    /// Apply: recompile the source and update the main window
    EditorApply,
    /// Trigger completion at current cursor
    TriggerCompletion,
    /// Select a completion item by index
    CompletionSelect(usize),
    /// Dismiss the completion popup
    CompletionDismiss,
    /// Navigate completion list up
    CompletionUp,
    /// Navigate completion list down
    CompletionDown,
    // ---- Widget tree ----
    /// Select a node in the tree
    TreeSelect(editor::tree_model::TreeNodeId),
    /// Toggle expand/collapse of a node
    TreeToggleExpand(editor::tree_model::TreeNodeId),
    /// Add a new widget of the given kind
    TreeAddWidget(String),
    /// Remove the selected widget
    TreeRemoveSelected,
    /// Change the selected widget's kind (from kind dropdown)
    TreeChangeKind(String),
    /// Move selected widget up among siblings
    TreeMoveUp,
    /// Move selected widget down among siblings
    TreeMoveDown,
    /// Indent: make selected widget a child of its previous sibling
    TreeIndent,
    /// Outdent: make selected widget a sibling of its parent
    TreeOutdent,
    // ---- Property panel ----
    /// A property edit action (path-based)
    PropEdit {
        node_id: editor::tree_model::TreeNodeId,
        arg: ArcStr,
        path: Vec<editor::path_update::PathSegment>,
        action: editor::path_update::PropAction,
    },
}

/// Element type for the design window.
type DesignElement<'a> =
    iced_core::Element<'a, DesignMsg, GraphixTheme, Renderer>;

/// State for the design (GUI builder) window.
struct DesignWindow {
    window: Arc<winit::window::Window>,
    surface: WindowSurface,
    pending_events: Vec<iced_core::Event>,
    cursor_position: iced_core::Point,
    needs_redraw: bool,
    last_render: Instant,
    pending_resize: Option<(u32, u32, f64)>,
    last_mouse_interaction: mouse::Interaction,
    ui_cache: user_interface::Cache,
    /// Source editor content
    editor_content: iced_widget::text_editor::Content<Renderer>,
    /// Diagnostic message from last compilation attempt
    diagnostic: Option<String>,
    /// Whether the source has been modified since last apply
    dirty: bool,
    /// Completion popup state
    completion: editor::completion::CompletionState,
    /// Environment snapshot for completions/hover
    env: Option<graphix_compiler::env::Env>,
    /// Widget tree model
    tree: editor::tree_model::TreeModel,
    /// Highlighter version — bumped on content replacement to force re-highlight
    highlight_version: u64,
    /// Timestamp of last edit — for debounced auto-typecheck
    last_edit: Option<Instant>,
    /// Whether a typecheck is pending (waiting for debounce)
    typecheck_pending: bool,
}

impl DesignWindow {
    fn new(
        window: Arc<winit::window::Window>,
        surface: WindowSurface,
        source: &str,
        env: Option<graphix_compiler::env::Env>,
    ) -> Self {
        Self {
            window,
            surface,
            pending_events: Vec::new(),
            cursor_position: iced_core::Point::ORIGIN,
            needs_redraw: true,
            last_render: Instant::now(),
            pending_resize: None,
            last_mouse_interaction: mouse::Interaction::Idle,
            ui_cache: user_interface::Cache::default(),
            editor_content: iced_widget::text_editor::Content::with_text(source),
            diagnostic: None,
            dirty: false,
            completion: editor::completion::CompletionState::new(),
            env: env.clone(),
            highlight_version: 0,
            last_edit: None,
            typecheck_pending: false,
            tree: {
                let mut tree = editor::tree_model::TreeModel::new();
                if let Some(env) = &env {
                    tree.populate_from_source(source, env);
                }
                tree
            },
        }
    }

    fn push_event(&mut self, ev: iced_core::Event) {
        self.pending_events.push(ev);
        self.needs_redraw = true;
    }

    fn cursor(&self) -> mouse::Cursor {
        mouse::Cursor::Available(self.cursor_position)
    }

    fn source_text(&self) -> String {
        self.editor_content.text()
    }

    /// Replace the editor content and bump highlight version to force re-highlight.
    fn set_source(&mut self, source: &str) {
        self.editor_content = iced_widget::text_editor::Content::with_text(source);
        self.highlight_version += 1;
        self.dirty = true;
    }

    /// Trigger completion at the current cursor position.
    fn trigger_completion(&mut self) {
        let env = match &self.env {
            Some(e) => e,
            None => return,
        };
        let text = self.editor_content.text();
        let cursor = self.editor_content.cursor();
        self.completion = editor::completion::complete(
            env,
            &text,
            cursor.position.line,
            cursor.position.column,
        );
    }

    /// Accept the currently selected completion item.
    fn accept_completion(&mut self) {
        if !self.completion.active || self.completion.items.is_empty() {
            return;
        }
        let idx = self.completion.selected.min(self.completion.items.len() - 1);
        let label = self.completion.items[idx].label.clone();

        // We need to replace the prefix text with the completion label.
        // Strategy: select backward from cursor by (replace_end - replace_start)
        // chars, then paste the label.
        let prefix_len = self.completion.replace_end - self.completion.replace_start;
        use iced_widget::text_editor::{Action, Edit, Motion};
        for _ in 0..prefix_len {
            self.editor_content.perform(Action::Select(Motion::Left));
        }
        self.editor_content.perform(Action::Edit(Edit::Paste(
            std::sync::Arc::new(label),
        )));
        self.dirty = true;
        self.completion.dismiss();
    }
}

// ---- Main handler ----

struct BrowserHandler {
    backend: backend::Ctx,
    gpu: Option<GpuState>,
    rt: tokio::runtime::Handle,
    window: Option<BrowserWindow>,
    surface: Option<WindowSurface>,
    ui_cache: user_interface::Cache,
    design: Option<DesignWindow>,
    clipboard: Clipboard,
    chrome: chrome::Chrome,
    messages: Vec<BrowserMsg>,
    design_messages: Vec<DesignMsg>,
    modifiers: ModifiersState,
    /// Initial location to navigate to on startup
    initial_loc: Option<ViewLoc>,
    /// Current view source (retained for design mode)
    current_source: ArcStr,
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
        window_id: WindowId,
        event: WindowEvent,
    ) {
        if let WindowEvent::ModifiersChanged(m) = &event {
            self.modifiers = m.state();
        }

        // Route to design window if its ID matches
        if let Some(dw) = self.design.as_mut() {
            if dw.window.id() == window_id {
                match &event {
                    WindowEvent::Resized(size) => {
                        let scale = dw.window.scale_factor();
                        dw.pending_resize = Some((size.width, size.height, scale));
                        dw.needs_redraw = true;
                    }
                    WindowEvent::RedrawRequested => {
                        dw.needs_redraw = true;
                    }
                    WindowEvent::CloseRequested => {
                        self.design = None;
                    }
                    _ => {
                        let scale = dw.window.scale_factor();
                        let mut iced_events = convert::window_event(&event, scale, self.modifiers);
                        for ev in iced_events.drain(..) {
                            if let iced_core::Event::Mouse(mouse::Event::CursorMoved {
                                position,
                            }) = &ev
                            {
                                dw.cursor_position = *position;
                            }
                            dw.push_event(ev);
                        }
                    }
                }
                return;
            }
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
                self.current_source = source.clone();
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
            BrowserEvent::EnvUpdate(env) => {
                if let Some(dw) = self.design.as_mut() {
                    dw.env = Some(env);
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

        // ---- Debounced typecheck for design window ----
        if let Some(dw) = self.design.as_mut() {
            if dw.typecheck_pending {
                if let Some(last_edit) = dw.last_edit {
                    if last_edit.elapsed() >= Duration::from_secs(1) {
                        dw.typecheck_pending = false;
                        let source = dw.source_text();
                        let origin = graphix_compiler::expr::Origin {
                            parent: None,
                            source: graphix_compiler::expr::Source::Unspecified,
                            text: ArcStr::from(source.as_str()),
                        };
                        match graphix_compiler::expr::parser::parse(origin) {
                            Ok(_) => dw.diagnostic = None,
                            Err(e) => dw.diagnostic = Some(format!("{e}")),
                        }
                        dw.needs_redraw = true;
                    } else {
                        // Wake up to check again
                        event_loop.set_control_flow(ControlFlow::WaitUntil(
                            last_edit + Duration::from_secs(1),
                        ));
                    }
                }
            }
        }

        // ---- Render design window ----
        if let Some(dw) = self.design.as_mut() {
            if dw.needs_redraw {
                if let Some(gpu) = self.gpu.as_ref() {
                    if let Some((pw, ph, scale)) = dw.pending_resize.take() {
                        dw.surface.resize(gpu, pw, ph, scale);
                        dw.push_event(iced_core::Event::Window(
                            iced_core::window::Event::Resized(dw.surface.logical_size()),
                        ));
                    }

                    let theme = {
                        use iced_core::theme::Base;
                        GraphixTheme {
                            inner: iced_core::Theme::default(iced_core::theme::Mode::Dark),
                            overrides: None,
                        }
                    };
                    // Build the design window element
                    use editor::highlight::{GxHighlighter, GxSettings, to_format};

                    // Ensure editor_ui entry exists for selected node
                    // (must happen before any immutable borrow of dw.tree)
                    if let Some(sel_id) = dw.tree.selected {
                        dw.tree.editor_ui.entry(sel_id).or_default();
                    }

                    // -- Tree panel (left) --
                    let tree_panel: DesignElement<'_> = editor::tree_view::view(&dw.tree);

                    // -- Property panel (right) --
                    let prop_panel: DesignElement<'_> = if let Some(sel_id) = dw.tree.selected {
                        if let Some(node) = dw.tree.get(sel_id) {
                            let ui_state = dw.tree.editor_ui
                                .get(&sel_id)
                                .unwrap(); // safe: inserted above
                            editor::prop_panel::view(
                                sel_id,
                                &node.data,
                                dw.env.as_ref(),
                                ui_state,
                            )
                        } else {
                            iced_widget::text("Select a widget").size(13).into()
                        }
                    } else {
                        iced_widget::text("Select a widget").size(13).into()
                    };

                    // -- Top half: tree | rule | properties --
                    let top_half: DesignElement<'_> = iced_widget::Row::new()
                        .push(
                            iced_widget::container(tree_panel)
                                .width(iced_core::Length::FillPortion(2))
                                .height(iced_core::Length::Fill)
                        )
                        .push(iced_widget::rule::vertical::<'_, GraphixTheme>(1))
                        .push(
                            iced_widget::Scrollable::new(prop_panel)
                                .width(iced_core::Length::FillPortion(3))
                                .height(iced_core::Length::Fill)
                        )
                        .height(iced_core::Length::FillPortion(1))
                        .into();

                    // -- Bottom half: source editor --
                    let dirty_indicator: DesignElement<'_> = if dw.dirty {
                        iced_widget::text("*modified").size(12).into()
                    } else {
                        iced_widget::text("").size(12).into()
                    };
                    let apply_btn: DesignElement<'_> = iced_widget::Button::new(
                        iced_widget::text("Apply (Ctrl+Enter)").size(12)
                    )
                        .padding(iced_core::Padding::from([3, 10]))
                        .on_press(DesignMsg::EditorApply)
                        .into();
                    let toolbar: DesignElement<'_> = iced_widget::Row::new()
                        .push(iced_widget::text("Source").size(13))
                        .push(dirty_indicator)
                        .push(apply_btn)
                        .spacing(8)
                        .padding(iced_core::Padding::from([2, 8]))
                        .into();
                    let completion_active = dw.completion.active;
                    let te: DesignElement<'_> = iced_widget::TextEditor::new(&dw.editor_content)
                        .highlight_with::<GxHighlighter>(
                            GxSettings { version: dw.highlight_version },
                            to_format,
                        )
                        .on_action(DesignMsg::EditorAction)
                        .key_binding(move |key_press| {
                            use iced_core::keyboard::{Key, key::Named};
                            use iced_widget::text_editor::Binding;
                            let ctrl = key_press.modifiers.command();

                            // Ctrl+Enter → Apply
                            if ctrl && key_press.key == Key::Named(Named::Enter) {
                                return Some(Binding::Custom(DesignMsg::EditorApply));
                            }
                            // Ctrl+Space → Trigger completion
                            if ctrl && key_press.key == Key::Named(Named::Space) {
                                return Some(Binding::Custom(DesignMsg::TriggerCompletion));
                            }
                            // When completion popup is active, intercept nav keys
                            if completion_active {
                                match &key_press.key {
                                    Key::Named(Named::Escape) => {
                                        return Some(Binding::Custom(DesignMsg::CompletionDismiss));
                                    }
                                    Key::Named(Named::ArrowUp) => {
                                        return Some(Binding::Custom(DesignMsg::CompletionUp));
                                    }
                                    Key::Named(Named::ArrowDown) => {
                                        return Some(Binding::Custom(DesignMsg::CompletionDown));
                                    }
                                    Key::Named(Named::Tab) | Key::Named(Named::Enter) => {
                                        return Some(Binding::Custom(DesignMsg::CompletionSelect(0)));
                                    }
                                    _ => {}
                                }
                            }
                            Binding::from_key_press(key_press)
                        })
                        .height(iced_core::Length::Fill)
                        .padding(iced_core::Padding::from(6))
                        .into();
                    // Build right pane (completion + diagnostics)
                    let mut right_pane = iced_widget::Column::new()
                        .spacing(4)
                        .padding(iced_core::Padding::from([4, 4]));
                    let has_right_content = dw.diagnostic.is_some()
                        || (dw.completion.active && !dw.completion.items.is_empty());

                    // Diagnostics
                    if let Some(diag) = &dw.diagnostic {
                        let diag_header: DesignElement<'_> = iced_widget::text("Errors")
                            .size(12)
                            .color(Color::from_rgb(1.0, 0.5, 0.5))
                            .into();
                        let diag_text: DesignElement<'_> = iced_widget::text(diag.clone())
                            .size(11)
                            .color(Color::from_rgb(1.0, 0.4, 0.4))
                            .into();
                        right_pane = right_pane
                            .push(diag_header)
                            .push(diag_text)
                            .push(iced_widget::rule::horizontal::<'_, GraphixTheme>(1));
                    }

                    // Completion list
                    if dw.completion.active && !dw.completion.items.is_empty() {
                        let comp_header: DesignElement<'_> = iced_widget::text("Completions")
                            .size(12)
                            .color(Color::from_rgb(0.6, 0.7, 0.8))
                            .into();
                        right_pane = right_pane.push(comp_header);
                        let max_items = 12usize;
                        for (i, item) in dw.completion.items.iter().enumerate().take(max_items) {
                            let bg = if i == dw.completion.selected {
                                Color::from_rgb(0.2, 0.3, 0.5)
                            } else {
                                Color::TRANSPARENT
                            };
                            let label: DesignElement<'_> =
                                iced_widget::text(&item.label).size(12).into();
                            let detail: DesignElement<'_> =
                                iced_widget::text(&item.detail)
                                    .size(10)
                                    .color(Color::from_rgb(0.5, 0.5, 0.5))
                                    .into();
                            let item_col: DesignElement<'_> = iced_widget::Column::new()
                                .push(label)
                                .push(detail)
                                .spacing(1)
                                .into();
                            let idx = i;
                            let btn: DesignElement<'_> = iced_widget::Button::new(item_col)
                                .on_press(DesignMsg::CompletionSelect(idx))
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
                                .into();
                            right_pane = right_pane.push(btn);
                        }
                    }

                    // Bottom: editor (left) | completion+diagnostics (right)
                    let editor_area: DesignElement<'_> = if has_right_content {
                        iced_widget::Row::new()
                            .push(
                                iced_widget::Column::new()
                                    .push(toolbar)
                                    .push(te)
                                    .width(iced_core::Length::FillPortion(3))
                            )
                            .push(iced_widget::rule::vertical::<'_, GraphixTheme>(1))
                            .push(
                                iced_widget::Scrollable::new(right_pane)
                                    .width(iced_core::Length::FillPortion(1))
                                    .height(iced_core::Length::Fill)
                            )
                            .height(iced_core::Length::FillPortion(1))
                            .into()
                    } else {
                        iced_widget::Column::new()
                            .push(toolbar)
                            .push(te)
                            .height(iced_core::Length::FillPortion(1))
                            .into()
                    };

                    let element: DesignElement<'_> = iced_widget::Column::new()
                        .push(top_half)
                        .push(iced_widget::rule::horizontal::<'_, GraphixTheme>(1))
                        .push(editor_area)
                        .into();
                    let viewport_size = dw.surface.logical_size();
                    let cache = mem::take(&mut dw.ui_cache);
                    let cursor = dw.cursor();
                    let mut ui = UserInterface::build(
                        element,
                        viewport_size,
                        cache,
                        &mut dw.surface.renderer,
                    );
                    let (state, _statuses) = ui.update(
                        &dw.pending_events,
                        cursor,
                        &mut dw.surface.renderer,
                        &mut self.clipboard,
                        &mut self.design_messages,
                    );

                    let new_mouse = match &state {
                        user_interface::State::Updated { mouse_interaction, .. } => {
                            Some(*mouse_interaction)
                        }
                        _ => None,
                    };

                    let style = Style { text_color: theme.palette().text };
                    ui.draw(&mut dw.surface.renderer, &theme, &style, cursor);
                    dw.ui_cache = ui.into_cache();
                    dw.pending_events.clear();

                    if let Some(mi) = new_mouse {
                        if dw.last_mouse_interaction != mi {
                            dw.last_mouse_interaction = mi;
                            dw.window.set_cursor(mouse_interaction_to_cursor(mi));
                        }
                    }

                    match dw.surface.surface.get_current_texture() {
                        Ok(frame) => {
                            let view = frame
                                .texture
                                .create_view(&wgpu::TextureViewDescriptor::default());
                            dw.surface.renderer.present(
                                Some(theme.palette().background),
                                gpu.format,
                                &view,
                                &dw.surface.viewport,
                            );
                            frame.present();
                            dw.last_render = Instant::now();
                            dw.needs_redraw = false;
                        }
                        Err(wgpu::SurfaceError::Lost | wgpu::SurfaceError::Outdated) => {
                            dw.surface.surface.configure(&gpu.device, &dw.surface.config);
                            dw.needs_redraw = true;
                        }
                        Err(e) => {
                            error!("design surface error: {e:?}");
                            dw.needs_redraw = false;
                        }
                    }
                }
            }
        }

        // Handle design messages
        for msg in self.design_messages.drain(..) {
            match msg {
                DesignMsg::EditorAction(action) => {
                    if let Some(dw) = self.design.as_mut() {
                        let is_edit = action.is_edit();
                        dw.editor_content.perform(action);
                        if is_edit {
                            dw.dirty = true;
                            dw.diagnostic = None;
                            dw.last_edit = Some(Instant::now());
                            dw.typecheck_pending = true;
                            // Auto-trigger completion after typing :: or #
                            let text = dw.editor_content.text();
                            let cursor = dw.editor_content.cursor();
                            let offset = editor::completion::cursor_byte_offset(
                                &text,
                                cursor.position.line,
                                cursor.position.column,
                            );
                            if offset >= 2 {
                                let tail = &text[offset.saturating_sub(2)..offset];
                                if tail == "::" || tail.ends_with('#') {
                                    dw.trigger_completion();
                                } else if dw.completion.active {
                                    // Re-filter while typing
                                    dw.trigger_completion();
                                }
                            } else if dw.completion.active {
                                dw.completion.dismiss();
                            }
                        }
                        dw.needs_redraw = true;
                    }
                }
                DesignMsg::EditorApply => {
                    if let Some(dw) = self.design.as_mut() {
                        dw.completion.dismiss();
                        let source = ArcStr::from(dw.source_text());
                        match self.rt.block_on(
                            self.backend.gx.compile(source.clone()),
                        ) {
                            Err(e) => {
                                dw.diagnostic = Some(format!("{e:#}"));
                            }
                            Ok(comp) => {
                                dw.diagnostic = None;
                                dw.dirty = false;
                                // Update env for completions
                                dw.env = Some(comp.env.clone());
                                // Rebuild tree from source
                                dw.tree.populate_from_source(&source, &comp.env);
                                self.current_source = source.clone();
                                // Update the main window view
                                if let Some(bw) = self.window.as_mut() {
                                    if comp.exprs.is_empty() {
                                        bw.view = None;
                                    } else {
                                        let top_id =
                                            comp.exprs.last().unwrap().id;
                                        bw.view = Some(CompiledView {
                                            content: Box::new(LoadingView),
                                            gx: self.backend.gx.clone(),
                                            pending_top_id: Some(top_id),
                                            _comp: Some(comp),
                                        });
                                    }
                                    bw.needs_redraw = true;
                                }
                            }
                        }
                        dw.needs_redraw = true;
                    }
                }
                DesignMsg::TriggerCompletion => {
                    if let Some(dw) = self.design.as_mut() {
                        dw.trigger_completion();
                        dw.needs_redraw = true;
                    }
                }
                DesignMsg::CompletionSelect(_) => {
                    if let Some(dw) = self.design.as_mut() {
                        dw.accept_completion();
                        dw.needs_redraw = true;
                    }
                }
                DesignMsg::CompletionDismiss => {
                    if let Some(dw) = self.design.as_mut() {
                        dw.completion.dismiss();
                        dw.needs_redraw = true;
                    }
                }
                DesignMsg::CompletionUp => {
                    if let Some(dw) = self.design.as_mut() {
                        dw.completion.select_up();
                        dw.needs_redraw = true;
                    }
                }
                DesignMsg::CompletionDown => {
                    if let Some(dw) = self.design.as_mut() {
                        dw.completion.select_down();
                        dw.needs_redraw = true;
                    }
                }
                DesignMsg::TreeSelect(id) => {
                    if let Some(dw) = self.design.as_mut() {
                        dw.tree.selected = Some(id);
                        dw.needs_redraw = true;
                    }
                }
                DesignMsg::TreeToggleExpand(id) => {
                    if let Some(dw) = self.design.as_mut() {
                        if let Some(node) = dw.tree.get_mut(id) {
                            node.expanded = !node.expanded;
                        }
                        dw.needs_redraw = true;
                    }
                }
                DesignMsg::TreeAddWidget(kind) => {
                    if let Some(dw) = self.design.as_mut() {
                        let new_id = dw.tree.add_widget(&kind);
                        dw.tree.selected = Some(new_id);
                        // Sync tree → source → editor
                        let source = dw.tree.to_source();
                        dw.set_source(&source);
                        dw.needs_redraw = true;
                    }
                }
                DesignMsg::TreeRemoveSelected => {
                    if let Some(dw) = self.design.as_mut() {
                        if let Some(sel) = dw.tree.selected {
                            dw.tree.remove(sel);
                            let source = dw.tree.to_source();
                            dw.set_source(&source);
                        }
                        dw.needs_redraw = true;
                    }
                }
                DesignMsg::TreeChangeKind(new_kind) => {
                    if let Some(dw) = self.design.as_mut() {
                        if let Some(sel) = dw.tree.selected {
                            if let Some(env) = dw.env.clone() {
                                dw.tree.change_kind(sel, &new_kind, &env);
                                let source = dw.tree.to_source();
                                dw.set_source(&source);
                            }
                        }
                        dw.needs_redraw = true;
                    }
                }
                DesignMsg::TreeMoveUp => {
                    if let Some(dw) = self.design.as_mut() {
                        if let Some(sel) = dw.tree.selected {
                            dw.tree.move_up(sel);
                            let source = dw.tree.to_source();
                            dw.set_source(&source);
                        }
                        dw.needs_redraw = true;
                    }
                }
                DesignMsg::TreeMoveDown => {
                    if let Some(dw) = self.design.as_mut() {
                        if let Some(sel) = dw.tree.selected {
                            dw.tree.move_down(sel);
                            let source = dw.tree.to_source();
                            dw.set_source(&source);
                        }
                        dw.needs_redraw = true;
                    }
                }
                DesignMsg::TreeIndent => {
                    if let Some(dw) = self.design.as_mut() {
                        if let Some(sel) = dw.tree.selected {
                            dw.tree.indent(sel);
                            let source = dw.tree.to_source();
                            dw.set_source(&source);
                        }
                        dw.needs_redraw = true;
                    }
                }
                DesignMsg::TreeOutdent => {
                    if let Some(dw) = self.design.as_mut() {
                        if let Some(sel) = dw.tree.selected {
                            dw.tree.outdent(sel);
                            let source = dw.tree.to_source();
                            dw.set_source(&source);
                        }
                        dw.needs_redraw = true;
                    }
                }
                DesignMsg::PropEdit { node_id, arg, path, action } => {
                    if let Some(dw) = self.design.as_mut() {
                        use editor::path_update::{PropAction, apply_at_path};
                        match action {
                            PropAction::TextChanged(text) => {
                                let ui = dw.tree.editor_ui.entry(node_id).or_default();
                                ui.text_inputs.insert(path, text);
                                // No sync — just UI state update
                            }
                            PropAction::TextCommit => {
                                let ui = dw.tree.editor_ui.entry(node_id).or_default();
                                if let Some(text) = ui.text_inputs.remove(&path) {
                                    if let Some(node) = dw.tree.get_mut(node_id) {
                                        let new_expr = if text.is_empty() {
                                            None
                                        } else {
                                            graphix_compiler::expr::parser::parse_one(&text).ok()
                                        };
                                        apply_at_path(&mut node.data.args, &arg, &path, new_expr);
                                        let source = dw.tree.to_source();
                                        dw.set_source(&source);
                                    }
                                }
                            }
                            PropAction::BoolSet(v) => {
                                if let Some(node) = dw.tree.get_mut(node_id) {
                                    let expr = graphix_compiler::expr::ExprKind::Constant(
                                        netidx::publisher::Value::Bool(v)
                                    ).to_expr_nopos();
                                    apply_at_path(&mut node.data.args, &arg, &path, Some(expr));
                                    let source = dw.tree.to_source();
                                    dw.set_source(&source);
                                }
                            }
                            PropAction::ArglessVariantSelected(tag) => {
                                if let Some(node) = dw.tree.get_mut(node_id) {
                                    let expr = graphix_compiler::expr::ExprKind::Variant {
                                        tag,
                                        args: triomphe::Arc::from(Vec::<graphix_compiler::expr::Expr>::new()),
                                    }.to_expr_nopos();
                                    apply_at_path(&mut node.data.args, &arg, &path, Some(expr));
                                    let source = dw.tree.to_source();
                                    dw.set_source(&source);
                                }
                            }
                            PropAction::VariantSelected(tag) => {
                                // TODO: look up variant arg types, generate defaults
                                if let Some(node) = dw.tree.get_mut(node_id) {
                                    let expr = graphix_compiler::expr::ExprKind::Variant {
                                        tag,
                                        args: triomphe::Arc::from(Vec::<graphix_compiler::expr::Expr>::new()),
                                    }.to_expr_nopos();
                                    apply_at_path(&mut node.data.args, &arg, &path, Some(expr));
                                    let source = dw.tree.to_source();
                                    dw.set_source(&source);
                                }
                            }
                            PropAction::ArrayAdd => {
                                if let Some(node) = dw.tree.get_mut(node_id) {
                                    let mut elems = editor::path_update::extract_array_elems(
                                        node.data.args.iter().find(|(l,_)| *l == arg).map(|(_,e)| e)
                                    );
                                    // Add a null default
                                    elems.push(graphix_compiler::expr::ExprKind::Constant(
                                        netidx::publisher::Value::Null
                                    ).to_expr_nopos());
                                    let arr = graphix_compiler::expr::ExprKind::Array {
                                        args: triomphe::Arc::from(elems),
                                    }.to_expr_nopos();
                                    apply_at_path(&mut node.data.args, &arg, &path, Some(arr));
                                    let source = dw.tree.to_source();
                                    dw.set_source(&source);
                                }
                            }
                            PropAction::ArrayRemove(i) => {
                                if let Some(node) = dw.tree.get_mut(node_id) {
                                    let mut elems = editor::path_update::extract_array_elems(
                                        node.data.args.iter().find(|(l,_)| *l == arg).map(|(_,e)| e)
                                    );
                                    if i < elems.len() {
                                        elems.remove(i);
                                    }
                                    let new_expr = if elems.is_empty() {
                                        None
                                    } else {
                                        Some(graphix_compiler::expr::ExprKind::Array {
                                            args: triomphe::Arc::from(elems),
                                        }.to_expr_nopos())
                                    };
                                    apply_at_path(&mut node.data.args, &arg, &path, new_expr);
                                    let source = dw.tree.to_source();
                                    dw.set_source(&source);
                                }
                            }
                            PropAction::MapAdd => {
                                if let Some(node) = dw.tree.get_mut(node_id) {
                                    let mut entries = editor::path_update::extract_map_entries(
                                        node.data.args.iter().find(|(l,_)| *l == arg).map(|(_,e)| e)
                                    );
                                    let default_key = graphix_compiler::expr::ExprKind::Constant(
                                        netidx::publisher::Value::String(arcstr::literal!(""))
                                    ).to_expr_nopos();
                                    let default_val = graphix_compiler::expr::ExprKind::Constant(
                                        netidx::publisher::Value::Null
                                    ).to_expr_nopos();
                                    entries.push((default_key, default_val));
                                    let map_expr = graphix_compiler::expr::ExprKind::Map {
                                        args: triomphe::Arc::from(entries),
                                    }.to_expr_nopos();
                                    apply_at_path(&mut node.data.args, &arg, &path, Some(map_expr));
                                    let source = dw.tree.to_source();
                                    dw.set_source(&source);
                                }
                            }
                            PropAction::MapRemove(i) => {
                                if let Some(node) = dw.tree.get_mut(node_id) {
                                    let mut entries = editor::path_update::extract_map_entries(
                                        node.data.args.iter().find(|(l,_)| *l == arg).map(|(_,e)| e)
                                    );
                                    if i < entries.len() {
                                        entries.remove(i);
                                    }
                                    let new_expr = if entries.is_empty() {
                                        None
                                    } else {
                                        Some(graphix_compiler::expr::ExprKind::Map {
                                            args: triomphe::Arc::from(entries),
                                        }.to_expr_nopos())
                                    };
                                    apply_at_path(&mut node.data.args, &arg, &path, new_expr);
                                    let source = dw.tree.to_source();
                                    dw.set_source(&source);
                                }
                            }
                            PropAction::MapKeyChanged(i, text) => {
                                let ui = dw.tree.editor_ui.entry(node_id).or_default();
                                let mut key_path = path.clone();
                                key_path.push(editor::path_update::PathSegment::MapValueIndex(i));
                                key_path.push(editor::path_update::PathSegment::StructField(arcstr::literal!("__key__")));
                                ui.text_inputs.insert(key_path, text);
                            }
                            PropAction::MapKeyCommit(i) => {
                                let ui = dw.tree.editor_ui.entry(node_id).or_default();
                                let mut key_path = path.clone();
                                key_path.push(editor::path_update::PathSegment::MapValueIndex(i));
                                key_path.push(editor::path_update::PathSegment::StructField(arcstr::literal!("__key__")));
                                if let Some(text) = ui.text_inputs.remove(&key_path) {
                                    if let Some(node) = dw.tree.get_mut(node_id) {
                                        let new_key = graphix_compiler::expr::parser::parse_one(&text).ok();
                                        if let Some(new_key) = new_key {
                                            let mut entries = editor::path_update::extract_map_entries(
                                                node.data.args.iter().find(|(l,_)| *l == arg).map(|(_,e)| e)
                                            );
                                            if i < entries.len() {
                                                entries[i].0 = new_key;
                                                let map_expr = graphix_compiler::expr::ExprKind::Map {
                                                    args: triomphe::Arc::from(entries),
                                                }.to_expr_nopos();
                                                apply_at_path(&mut node.data.args, &arg, &path, Some(map_expr));
                                                let source = dw.tree.to_source();
                                                dw.set_source(&source);
                                            }
                                        }
                                    }
                                }
                            }
                            PropAction::ToggleSection => {
                                let ui = dw.tree.editor_ui.entry(node_id).or_default();
                                if !ui.collapsed.remove(&path) {
                                    ui.collapsed.insert(path);
                                }
                            }
                            PropAction::Clear => {
                                if let Some(node) = dw.tree.get_mut(node_id) {
                                    apply_at_path(&mut node.data.args, &arg, &path, None);
                                    let source = dw.tree.to_source();
                                    dw.set_source(&source);
                                }
                            }
                        }
                        dw.needs_redraw = true;
                    }
                }
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
                    if self.design.is_some() {
                        // Close design window
                        self.design = None;
                    } else if let Some(gpu) = self.gpu.as_ref() {
                        // Open design window
                        let attrs = winit::window::WindowAttributes::default()
                            .with_title("Netidx Browser — Design")
                            .with_inner_size(winit::dpi::LogicalSize::new(900.0, 700.0));
                        match event_loop.create_window(attrs) {
                            Ok(win) => {
                                let win = Arc::new(win);
                                match WindowSurface::new(gpu, win.clone()) {
                                    Ok(ws) => {
                                        // Seed the env for completions
                                        let env = self.rt.block_on(
                                            self.backend.gx.get_env()
                                        ).ok();
                                        self.design = Some(DesignWindow::new(
                                            win,
                                            ws,
                                            &self.current_source,
                                            env,
                                        ));
                                    }
                                    Err(e) => {
                                        error!("failed to create design surface: {e:?}");
                                    }
                                }
                            }
                            Err(e) => {
                                error!("failed to create design window: {e:?}");
                            }
                        }
                    }
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
        design: None,
        clipboard: Clipboard::new(),
        chrome: chrome::Chrome::new(),
        messages: Vec::new(),
        design_messages: Vec::new(),
        modifiers: ModifiersState::default(),
        initial_loc: Some(initial_loc),
        current_source: ArcStr::from(""),
    };

    if let Err(e) = event_loop.run_app(&mut handler) {
        error!("event loop error: {e:?}");
    }
}

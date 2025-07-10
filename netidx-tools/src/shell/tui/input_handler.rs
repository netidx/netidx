use super::{TRef, TuiW, TuiWidget};
use crate::shell::tui::{compile, EmptyW};
use anyhow::{Context, Result};
use arcstr::{literal, ArcStr};
use async_trait::async_trait;
use crossterm::event::{
    Event, KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers, MediaKeyCode,
    ModifierKeyCode, MouseButton, MouseEvent, MouseEventKind,
};
use log::debug;
use netidx::{protocol::valarray::ValArray, publisher::Value};
use netidx_bscript::{
    expr::ExprId,
    rt::{BSHandle, Callable, Ref},
};
use ratatui::{layout::Rect, Frame};
use smallvec::{smallvec, SmallVec};
use std::collections::VecDeque;
use tokio::try_join;

fn media_keycode_to_value(kc: &MediaKeyCode) -> Value {
    use MediaKeyCode::*;
    match kc {
        Play => literal!("Play"),
        Pause => literal!("Pause"),
        PlayPause => literal!("PlayPause"),
        Reverse => literal!("Reverse"),
        Stop => literal!("Stop"),
        FastForward => literal!("FastForward"),
        Rewind => literal!("Rewind"),
        TrackNext => literal!("TrackNext"),
        TrackPrevious => literal!("TrackPrevious"),
        Record => literal!("Record"),
        LowerVolume => literal!("LowerVolume"),
        RaiseVolume => literal!("RaiseVolume"),
        MuteVolume => literal!("MuteVolume"),
    }
    .into()
}

fn modifier_keycode_to_value(mc: &ModifierKeyCode) -> Value {
    use ModifierKeyCode::*;
    match mc {
        LeftShift => literal!("LeftShift"),
        LeftControl => literal!("LeftControl"),
        LeftAlt => literal!("LeftAlt"),
        LeftSuper => literal!("LeftSuper"),
        LeftHyper => literal!("LeftHyper"),
        LeftMeta => literal!("LeftMeta"),
        RightShift => literal!("RightShift"),
        RightControl => literal!("RightControl"),
        RightAlt => literal!("RightAlt"),
        RightSuper => literal!("RightSuper"),
        RightHyper => literal!("RightHyper"),
        RightMeta => literal!("RightMeta"),
        IsoLevel3Shift => literal!("IsoLevel3Shift"),
        IsoLevel5Shift => literal!("IsoLevel5Shift"),
    }
    .into()
}

fn keycode_to_value(kc: &KeyCode) -> Value {
    const ASCII: [ArcStr; 95] = [
        literal!(" "),
        literal!("!"),
        literal!("\""),
        literal!("#"),
        literal!("$"),
        literal!("%"),
        literal!("&"),
        literal!("'"),
        literal!("("),
        literal!(")"),
        literal!("*"),
        literal!("+"),
        literal!(","),
        literal!("-"),
        literal!("."),
        literal!("/"),
        literal!("0"),
        literal!("1"),
        literal!("2"),
        literal!("3"),
        literal!("4"),
        literal!("5"),
        literal!("6"),
        literal!("7"),
        literal!("8"),
        literal!("9"),
        literal!(":"),
        literal!(";"),
        literal!("<"),
        literal!("="),
        literal!(">"),
        literal!("?"),
        literal!("@"),
        literal!("A"),
        literal!("B"),
        literal!("C"),
        literal!("D"),
        literal!("E"),
        literal!("F"),
        literal!("G"),
        literal!("H"),
        literal!("I"),
        literal!("J"),
        literal!("K"),
        literal!("L"),
        literal!("M"),
        literal!("N"),
        literal!("O"),
        literal!("P"),
        literal!("Q"),
        literal!("R"),
        literal!("S"),
        literal!("T"),
        literal!("U"),
        literal!("V"),
        literal!("W"),
        literal!("X"),
        literal!("Y"),
        literal!("Z"),
        literal!("["),
        literal!("\\"),
        literal!("]"),
        literal!("^"),
        literal!("_"),
        literal!("`"),
        literal!("a"),
        literal!("b"),
        literal!("c"),
        literal!("d"),
        literal!("e"),
        literal!("f"),
        literal!("g"),
        literal!("h"),
        literal!("i"),
        literal!("j"),
        literal!("k"),
        literal!("l"),
        literal!("m"),
        literal!("n"),
        literal!("o"),
        literal!("p"),
        literal!("q"),
        literal!("r"),
        literal!("s"),
        literal!("t"),
        literal!("u"),
        literal!("v"),
        literal!("w"),
        literal!("x"),
        literal!("y"),
        literal!("z"),
        literal!("{"),
        literal!("|"),
        literal!("}"),
        literal!("~"),
    ];
    use KeyCode::*;
    match kc {
        Backspace => literal!("Backspace").into(),
        Enter => literal!("Enter").into(),
        Left => literal!("Left").into(),
        Right => literal!("Right").into(),
        Up => literal!("Up").into(),
        Down => literal!("Down").into(),
        Home => literal!("Home").into(),
        End => literal!("End").into(),
        PageUp => literal!("PageUp").into(),
        PageDown => literal!("PageDown").into(),
        Tab => literal!("Tab").into(),
        BackTab => literal!("BackTab").into(),
        Delete => literal!("Delete").into(),
        Insert => literal!("Insert").into(),
        F(n) => ValArray::from_iter_exact(
            [literal!("F").into(), (*n as i64).into()].into_iter(),
        )
        .into(),
        Char(c) => {
            if c.len_utf8() == 1 {
                let mut buf = [0u8];
                c.encode_utf8(&mut buf);
                if buf[0] >= 32 {
                    let i = (buf[0] - 32) as usize;
                    if i < ASCII.len() {
                        return ValArray::from_iter_exact(
                            [literal!("Char").into(), ASCII[i].clone().into()]
                                .into_iter(),
                        )
                        .into();
                    }
                }
            }
            let s = ArcStr::init_with(c.len_utf8(), |buf| {
                c.encode_utf8(buf);
            })
            .unwrap();
            ValArray::from_iter_exact([literal!("Char").into(), s.into()].into_iter())
                .into()
        }
        Null => literal!("Null").into(),
        Esc => literal!("Esc").into(),
        CapsLock => literal!("CapsLock").into(),
        ScrollLock => literal!("ScrollLock").into(),
        NumLock => literal!("NumLock").into(),
        PrintScreen => literal!("PrintScreen").into(),
        Pause => literal!("Pause").into(),
        Menu => literal!("Menu").into(),
        KeypadBegin => literal!("KeypadBegin").into(),
        Media(mc) => ValArray::from_iter_exact(
            [literal!("Media").into(), media_keycode_to_value(mc).into()].into_iter(),
        )
        .into(),
        Modifier(mc) => ValArray::from_iter_exact(
            [literal!("Modifier").into(), modifier_keycode_to_value(mc).into()]
                .into_iter(),
        )
        .into(),
    }
}

fn key_modifiers_to_value(k: &KeyModifiers) -> Value {
    let mut res: SmallVec<[Value; 6]> = smallvec![];
    if k.contains(KeyModifiers::SHIFT) {
        res.push(literal!("Shift").into());
    }
    if k.contains(KeyModifiers::CONTROL) {
        res.push(literal!("Control").into());
    }
    if k.contains(KeyModifiers::ALT) {
        res.push(literal!("Alt").into());
    }
    if k.contains(KeyModifiers::SUPER) {
        res.push(literal!("Super").into());
    }
    if k.contains(KeyModifiers::HYPER) {
        res.push(literal!("Hyper").into());
    }
    if k.contains(KeyModifiers::META) {
        res.push(literal!("Meta").into());
    }
    ValArray::from_iter_exact(res.into_iter()).into()
}

fn key_event_kind_to_value(k: &KeyEventKind) -> Value {
    match k {
        KeyEventKind::Press => literal!("Press").into(),
        KeyEventKind::Release => literal!("Release").into(),
        KeyEventKind::Repeat => literal!("Repeat").into(),
    }
}

fn key_event_states_to_value(s: &KeyEventState) -> Value {
    let mut res: SmallVec<[Value; 3]> = smallvec![];
    if s.contains(KeyEventState::KEYPAD) {
        res.push(literal!("Keypad").into());
    }
    if s.contains(KeyEventState::CAPS_LOCK) {
        res.push(literal!("CapsLock").into());
    }
    if s.contains(KeyEventState::NUM_LOCK) {
        res.push(literal!("NumLock").into());
    }
    ValArray::from_iter_exact(res.into_iter()).into()
}

fn key_event_to_value(e: &KeyEvent) -> Value {
    let code: Value = ValArray::from_iter_exact(
        [literal!("code").into(), keycode_to_value(&e.code).into()].into_iter(),
    )
    .into();
    let kind: Value = ValArray::from_iter_exact(
        [literal!("kind").into(), key_event_kind_to_value(&e.kind)].into_iter(),
    )
    .into();
    let modifiers: Value = ValArray::from_iter_exact(
        [literal!("modifiers").into(), key_modifiers_to_value(&e.modifiers)].into_iter(),
    )
    .into();
    let state: Value = ValArray::from_iter_exact(
        [literal!("state").into(), key_event_states_to_value(&e.state)].into_iter(),
    )
    .into();
    ValArray::from_iter_exact([code, kind, modifiers, state].into_iter()).into()
}

fn mouse_button_to_value(b: &MouseButton) -> Value {
    match b {
        MouseButton::Left => literal!("Left").into(),
        MouseButton::Right => literal!("Right").into(),
        MouseButton::Middle => literal!("Middle").into(),
    }
}

fn mouse_event_kind_to_value(k: &MouseEventKind) -> Value {
    match k {
        MouseEventKind::Down(b) => ValArray::from_iter_exact(
            [literal!("Down").into(), mouse_button_to_value(b).into()].into_iter(),
        )
        .into(),
        MouseEventKind::Up(b) => ValArray::from_iter_exact(
            [literal!("Up").into(), mouse_button_to_value(b).into()].into_iter(),
        )
        .into(),
        MouseEventKind::Drag(b) => ValArray::from_iter_exact(
            [literal!("Drag").into(), mouse_button_to_value(b).into()].into_iter(),
        )
        .into(),
        MouseEventKind::Moved => literal!("Moved").into(),
        MouseEventKind::ScrollDown => literal!("ScrollDown").into(),
        MouseEventKind::ScrollUp => literal!("ScrollUp").into(),
        MouseEventKind::ScrollLeft => literal!("ScrollLeft").into(),
        MouseEventKind::ScrollRight => literal!("ScrollRight").into(),
    }
}

fn mouse_event_to_value(e: &MouseEvent) -> Value {
    let column: Value = ValArray::from_iter_exact(
        [literal!("column").into(), (e.column as i64).into()].into_iter(),
    )
    .into();
    let kind: Value = ValArray::from_iter_exact(
        [literal!("kind").into(), mouse_event_kind_to_value(&e.kind)].into_iter(),
    )
    .into();
    let modifiers: Value = ValArray::from_iter_exact(
        [literal!("modifiers").into(), key_modifiers_to_value(&e.modifiers)].into_iter(),
    )
    .into();
    let row = ValArray::from_iter_exact(
        [literal!("row").into(), (e.row as i64).into()].into_iter(),
    )
    .into();
    ValArray::from_iter_exact([column, kind, modifiers, row].into_iter()).into()
}

pub(super) fn event_to_value(e: &Event) -> Value {
    match e {
        Event::FocusGained => literal!("FocusGained").into(),
        Event::FocusLost => literal!("FocusLost").into(),
        Event::Key(e) => ValArray::from_iter_exact(
            [literal!("Key").into(), key_event_to_value(e)].into_iter(),
        )
        .into(),
        Event::Mouse(e) => ValArray::from_iter_exact(
            [literal!("Mouse").into(), mouse_event_to_value(e)].into_iter(),
        )
        .into(),
        Event::Paste(s) => ValArray::from_iter_exact(
            [literal!("Paste").into(), ArcStr::from(s).into()].into_iter(),
        )
        .into(),
        Event::Resize(x, y) => ValArray::from_iter_exact(
            [literal!("Resize").into(), (*x as i64).into(), (*y as i64).into()]
                .into_iter(),
        )
        .into(),
    }
}

pub(super) struct InputHandlerW {
    bs: BSHandle,
    enabled: TRef<Option<bool>>,
    handle_ref: Ref,
    handle: Option<Callable>,
    child_ref: Ref,
    child: TuiW,
    queued: VecDeque<(Event, Value)>,
    pending: bool,
}

impl InputHandlerW {
    pub(crate) async fn compile(bs: BSHandle, v: Value) -> Result<TuiW> {
        let [(_, child), (_, enabled), (_, handle)] =
            v.cast_to::<[(ArcStr, u64); 3]>().context("input handler fields")?;
        let (child_ref, enabled, handle_ref) = try_join! {
            bs.compile_ref(child),
            bs.compile_ref(enabled),
            bs.compile_ref(handle)
        }?;
        let mut t = Self {
            bs,
            enabled: TRef::new(enabled).context("input handler tref enabled")?,
            handle_ref,
            handle: None,
            child_ref,
            child: Box::new(EmptyW),
            queued: VecDeque::new(),
            pending: false,
        };
        if let Some(v) = t.handle_ref.last.take() {
            t.set_handle(v).await?
        }
        if let Some(v) = t.child_ref.last.take() {
            t.child = compile(t.bs.clone(), v).await?;
        }
        Ok(Box::new(t))
    }

    async fn maybe_send_queued(&mut self) -> Result<()> {
        if !self.pending
            && let Some((e, v)) = self.queued.front()
            && let Some(h) = &self.handle
        {
            debug!("sending event: {e:?}");
            h.call(ValArray::from_iter_exact([v.clone()].into_iter())).await?;
            self.pending = true
        }
        Ok(())
    }

    async fn set_handle(&mut self, v: Value) -> Result<()> {
        let handle = self.bs.compile_callable(v).await?;
        self.handle = Some(handle);
        self.maybe_send_queued().await?;
        Ok(())
    }
}

#[async_trait]
impl TuiWidget for InputHandlerW {
    async fn handle_event(&mut self, e: Event, v: Value) -> Result<()> {
        if self.enabled.t.and_then(|b| b).unwrap_or(true) {
            self.queued.push_back((e, v));
            self.maybe_send_queued().await?
        }
        Ok(())
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()> {
        let Self {
            bs: _,
            enabled,
            handle_ref: _,
            handle,
            child_ref,
            child,
            queued,
            pending,
        } = self;
        if let Some(Some(false)) =
            enabled.update(id, &v).context("input handler enabled update")?
        {
            *pending = false;
            queued.clear();
        }
        if id == child_ref.id {
            *child = compile(self.bs.clone(), v.clone()).await?;
        }
        if let Some(h) = handle
            && id == h.expr
        {
            *pending = false;
            if let Some((e, ev)) = queued.pop_front() {
                match &*v.clone().cast_to::<ArcStr>()? {
                    "Stop" => (),
                    "Continue" => child.handle_event(e, ev).await?,
                    v => bail!("invalid respose from input handler {v}"),
                }
            }
            self.maybe_send_queued().await?
        }
        if id == self.handle_ref.id {
            self.set_handle(v.clone()).await?;
        }
        self.child.handle_update(id, v).await
    }

    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()> {
        self.child.draw(frame, rect)
    }
}

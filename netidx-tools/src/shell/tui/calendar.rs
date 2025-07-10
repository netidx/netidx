use super::{StyleV, TRef, TuiW, TuiWidget};
use anyhow::{bail, Context, Result};
use arcstr::ArcStr;
use async_trait::async_trait;
use crossterm::event::Event;
use netidx::publisher::{FromValue, Value};
use netidx_bscript::{
    expr::ExprId,
    rt::{BSHandle, Ref},
};
use ratatui::{
    layout::Rect,
    widgets::calendar::{CalendarEventStore, Monthly},
    Frame,
};
use time::{Date, Month};
use tokio::try_join;

#[derive(Clone, Copy)]
struct DateV(Date);

impl FromValue for DateV {
    fn from_value(v: Value) -> Result<Self> {
        // bscript structs are stored sorted by field name (day, month, year)
        let [(_, day), (_, month), (_, year)] = v.cast_to::<[(ArcStr, i64); 3]>()?;
        let month = Month::try_from(month as u8)
            .map_err(|_| anyhow::anyhow!("invalid month {month}"))?;
        let date = Date::from_calendar_date(year as i32, month, day as u8)?;
        Ok(Self(date))
    }
}

struct EventV {
    date: DateV,
    style: StyleV,
}

impl FromValue for EventV {
    fn from_value(v: Value) -> Result<Self> {
        let [(_, date), (_, style)] = v.cast_to::<[(ArcStr, Value); 2]>()?;
        Ok(Self { date: date.cast_to::<DateV>()?, style: style.cast_to::<StyleV>()? })
    }
}

pub(super) struct CalendarW {
    display_date: TRef<DateV>,
    events_ref: Ref,
    events: CalendarEventStore,
    show_month: TRef<Option<StyleV>>,
    show_surrounding: TRef<Option<StyleV>>,
    show_weekday: TRef<Option<StyleV>>,
    default_style: TRef<Option<StyleV>>,
}

impl CalendarW {
    pub(super) async fn compile(bs: BSHandle, v: Value) -> Result<TuiW> {
        let [(_, default_style), (_, display_date), (_, events), (_, show_month), (_, show_surrounding), (_, show_weekday)] =
            v.cast_to::<[(ArcStr, u64); 6]>().context("calendar fields")?;
        let (
            default_style,
            display_date,
            events_ref,
            show_month,
            show_surrounding,
            show_weekday,
        ) = try_join! {
            bs.compile_ref(default_style),
            bs.compile_ref(display_date),
            bs.compile_ref(events),
            bs.compile_ref(show_month),
            bs.compile_ref(show_surrounding),
            bs.compile_ref(show_weekday)
        }?;
        let mut t = Self {
            display_date: TRef::new(display_date)
                .context("calendar tref display_date")?,
            events_ref,
            events: CalendarEventStore::default(),
            show_month: TRef::new(show_month).context("calendar tref show_month")?,
            show_surrounding: TRef::new(show_surrounding)
                .context("calendar tref show_surrounding")?,
            show_weekday: TRef::new(show_weekday)
                .context("calendar tref show_weekday")?,
            default_style: TRef::new(default_style)
                .context("calendar tref default_style")?,
        };
        if let Some(v) = t.events_ref.last.take() {
            t.set_events(&v)?;
        }
        Ok(Box::new(t))
    }

    fn set_events(&mut self, v: &Value) -> Result<()> {
        self.events = CalendarEventStore::default();
        match v {
            Value::Null => return Ok(()),
            Value::Array(a) => {
                for ev in a {
                    let EventV { date, style } = ev.clone().cast_to::<EventV>()?;
                    self.events.add(date.0, style.0);
                }
            }
            v => bail!("invalid calendar events {v}"),
        }
        Ok(())
    }
}

#[async_trait]
impl TuiWidget for CalendarW {
    async fn handle_event(&mut self, _e: Event, _v: Value) -> Result<()> {
        Ok(())
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()> {
        let Self {
            display_date,
            events_ref,
            events: _,
            show_month,
            show_surrounding,
            show_weekday,
            default_style,
        } = self;
        display_date.update(id, &v).context("calendar update display_date")?;
        show_month.update(id, &v).context("calendar update show_month")?;
        show_surrounding.update(id, &v).context("calendar update show_surrounding")?;
        show_weekday.update(id, &v).context("calendar update show_weekday")?;
        default_style.update(id, &v).context("calendar update default_style")?;
        if events_ref.id == id {
            self.set_events(&v)?;
        }
        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()> {
        let Self {
            display_date,
            events_ref: _,
            events,
            show_month,
            show_surrounding,
            show_weekday,
            default_style,
        } = self;
        let mut cal = Monthly::new(display_date.t.unwrap().0, &*events);
        if let Some(Some(s)) = &show_surrounding.t {
            cal = cal.show_surrounding(s.0);
        }
        if let Some(Some(s)) = &show_weekday.t {
            cal = cal.show_weekdays_header(s.0);
        }
        if let Some(Some(s)) = &show_month.t {
            cal = cal.show_month_header(s.0);
        }
        if let Some(Some(s)) = &default_style.t {
            cal = cal.default_style(s.0);
        }
        frame.render_widget(cal, rect);
        Ok(())
    }
}

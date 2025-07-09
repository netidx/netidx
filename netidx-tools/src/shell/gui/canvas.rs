use super::{ColorV, GuiW, GuiWidget, LineV, TRef};
use anyhow::{bail, Context, Result};
use arcstr::ArcStr;
use async_trait::async_trait;
use crossterm::event::Event;
use futures::future::try_join_all;
use netidx::publisher::{FromValue, Value};
use netidx_bscript::{
    expr::ExprId,
    rt::{BSHandle, Ref},
};
use ratatui::{
    layout::Rect,
    style::Color,
    symbols,
    widgets::canvas::{
        Canvas, Circle, Context as CanvasContext, Line, Points, Rectangle,
    },
    Frame,
};
use smallvec::SmallVec;
use tokio::try_join;

#[derive(Clone, Copy)]
struct BoundsV([f64; 2]);

impl FromValue for BoundsV {
    fn from_value(v: Value) -> Result<Self> {
        let [(_, max), (_, min)] = v.cast_to::<[(ArcStr, f64); 2]>()?;
        Ok(Self([min, max]))
    }
}

#[derive(Clone)]
struct MarkerV(symbols::Marker);

impl FromValue for MarkerV {
    fn from_value(v: Value) -> Result<Self> {
        let m = match &*v.cast_to::<ArcStr>()? {
            "Dot" => symbols::Marker::Dot,
            "Block" => symbols::Marker::Block,
            "Bar" => symbols::Marker::Bar,
            "Braille" => symbols::Marker::Braille,
            "HalfBlock" => symbols::Marker::HalfBlock,
            s => bail!("invalid marker {s}"),
        };
        Ok(Self(m))
    }
}

#[derive(Clone)]
struct CanvasLineV(Line);

impl FromValue for CanvasLineV {
    fn from_value(v: Value) -> Result<Self> {
        let [(_, color), (_, x1), (_, x2), (_, y1), (_, y2)] =
            v.cast_to::<[(ArcStr, Value); 5]>()?;
        let color = color.cast_to::<ColorV>()?.0;
        Ok(Self(Line {
            x1: x1.cast_to()?,
            y1: y1.cast_to()?,
            x2: x2.cast_to()?,
            y2: y2.cast_to()?,
            color,
        }))
    }
}

#[derive(Clone)]
struct CanvasCircleV(Circle);

impl FromValue for CanvasCircleV {
    fn from_value(v: Value) -> Result<Self> {
        let [(_, color), (_, radius), (_, x), (_, y)] =
            v.cast_to::<[(ArcStr, Value); 4]>()?;
        let color = color.cast_to::<ColorV>()?.0;
        Ok(Self(Circle {
            x: x.cast_to()?,
            y: y.cast_to()?,
            radius: radius.cast_to()?,
            color,
        }))
    }
}

#[derive(Clone)]
struct CanvasRectangleV(Rectangle);

impl FromValue for CanvasRectangleV {
    fn from_value(v: Value) -> Result<Self> {
        let [(_, color), (_, height), (_, width), (_, x), (_, y)] =
            v.cast_to::<[(ArcStr, Value); 5]>()?;
        let color = color.cast_to::<ColorV>()?.0;
        Ok(Self(Rectangle {
            x: x.cast_to()?,
            y: y.cast_to()?,
            width: width.cast_to()?,
            height: height.cast_to()?,
            color,
        }))
    }
}

#[derive(Clone)]
struct CanvasPointsV {
    color: Color,
    coords: Vec<(f64, f64)>,
}

impl FromValue for CanvasPointsV {
    fn from_value(v: Value) -> Result<Self> {
        let [(_, color), (_, coords)] = v.cast_to::<[(ArcStr, Value); 2]>()?;
        let color = color.cast_to::<ColorV>()?.0;
        let coords = match coords {
            Value::Array(a) => {
                let mut c = Vec::with_capacity(a.len());
                for v in a {
                    c.push(v.cast_to::<(f64, f64)>()?);
                }
                c
            }
            Value::Null => Vec::new(),
            v => bail!("invalid points coords {v}"),
        };
        Ok(Self { color, coords })
    }
}

#[derive(Clone)]
struct CanvasLabelV {
    line: LineV,
    x: f64,
    y: f64,
}

impl FromValue for CanvasLabelV {
    fn from_value(v: Value) -> Result<Self> {
        let [(_, line), (_, x), (_, y)] = v.cast_to::<[(ArcStr, Value); 3]>()?;
        Ok(Self { line: line.cast_to()?, x: x.cast_to()?, y: y.cast_to()? })
    }
}

#[derive(Clone)]
enum ShapeV {
    Line(CanvasLineV),
    Circle(CanvasCircleV),
    Rectangle(CanvasRectangleV),
    Points(CanvasPointsV),
    Label(CanvasLabelV),
}

impl FromValue for ShapeV {
    fn from_value(v: Value) -> Result<Self> {
        match v.cast_to::<(ArcStr, Value)>()? {
            (s, v) if &s == "Line" => Ok(ShapeV::Line(v.cast_to()?)),
            (s, v) if &s == "Circle" => Ok(ShapeV::Circle(v.cast_to()?)),
            (s, v) if &s == "Rectangle" => Ok(ShapeV::Rectangle(v.cast_to()?)),
            (s, v) if &s == "Points" => Ok(ShapeV::Points(v.cast_to()?)),
            (s, v) if &s == "Label" => Ok(ShapeV::Label(v.cast_to()?)),
            (s, v) => bail!("invalid shape {s}({v})"),
        }
    }
}

impl ShapeV {
    fn draw(&self, ctx: &mut CanvasContext) {
        match self {
            ShapeV::Line(s) => {
                ctx.draw(&s.0);
            }
            ShapeV::Circle(s) => {
                ctx.draw(&s.0);
            }
            ShapeV::Rectangle(s) => {
                ctx.draw(&s.0);
            }
            ShapeV::Points(s) => {
                let points = Points { coords: &s.coords, color: s.color };
                ctx.draw(&points);
            }
            ShapeV::Label(s) => {
                ctx.print(s.x, s.y, s.line.0.clone());
            }
        }
    }
}

struct ShapeRef {
    r: Ref,
    shape: Option<ShapeV>,
}

impl ShapeRef {
    fn update(&mut self, id: ExprId, v: &Value) -> Result<()> {
        if self.r.id == id {
            self.shape = Some(v.clone().cast_to::<ShapeV>()?);
        }
        Ok(())
    }
}

pub(super) struct CanvasW {
    bs: BSHandle,
    shapes_ref: Ref,
    shapes: Vec<ShapeRef>,
    background_color: TRef<Option<ColorV>>,
    marker: TRef<Option<MarkerV>>,
    x_bounds: TRef<BoundsV>,
    y_bounds: TRef<BoundsV>,
}

impl CanvasW {
    pub(super) async fn compile(bs: BSHandle, v: Value) -> Result<GuiW> {
        let [(_, background_color), (_, marker), (_, shapes), (_, x_bounds), (_, y_bounds)] =
            v.cast_to::<[(ArcStr, u64); 5]>()?;
        let (background_color, marker, shapes_ref, x_bounds, y_bounds) = try_join! {
            bs.compile_ref(background_color),
            bs.compile_ref(marker),
            bs.compile_ref(shapes),
            bs.compile_ref(x_bounds),
            bs.compile_ref(y_bounds)
        }?;
        let mut t = Self {
            bs: bs.clone(),
            shapes_ref,
            shapes: Vec::new(),
            background_color: TRef::new(background_color)?,
            marker: TRef::new(marker)?,
            x_bounds: TRef::new(x_bounds)?,
            y_bounds: TRef::new(y_bounds)?,
        };
        if let Some(v) = t.shapes_ref.last.take() {
            t.set_shapes(v).await?;
        }
        Ok(Box::new(t))
    }

    async fn set_shapes(&mut self, v: Value) -> Result<()> {
        let ids = v.cast_to::<SmallVec<[u64; 8]>>()?;
        let refs = try_join_all(ids.into_iter().map(|id| {
            let bs = self.bs.clone();
            async move {
                let mut r = bs.compile_ref(id).await?;
                let shape = match r.last.take() {
                    Some(v) => Some(v.cast_to::<ShapeV>()?),
                    None => None,
                };
                Ok::<ShapeRef, anyhow::Error>(ShapeRef { r, shape })
            }
        }))
        .await?;
        self.shapes = refs;
        Ok(())
    }
}

#[async_trait]
impl GuiWidget for CanvasW {
    async fn handle_event(&mut self, _e: Event) -> Result<()> {
        Ok(())
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()> {
        self.background_color.update(id, &v).context("canvas background update")?;
        self.marker.update(id, &v).context("canvas marker update")?;
        self.x_bounds.update(id, &v).context("canvas x_bounds update")?;
        self.y_bounds.update(id, &v).context("canvas y_bounds update")?;
        if self.shapes_ref.id == id {
            self.set_shapes(v.clone()).await?;
        }
        for s in &mut self.shapes {
            s.update(id, &v)?;
        }
        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()> {
        let x_bounds = self.x_bounds.t.unwrap_or(BoundsV([0.0, 0.0])).0;
        let y_bounds = self.y_bounds.t.unwrap_or(BoundsV([0.0, 0.0])).0;
        let bg = self.background_color.t.as_ref().and_then(|c| c.as_ref()).cloned();
        let marker = self.marker.t.as_ref().and_then(|m| m.as_ref()).cloned();
        let shapes = &self.shapes;
        let mut canvas = Canvas::default().x_bounds(x_bounds).y_bounds(y_bounds);
        if let Some(c) = bg {
            canvas = canvas.background_color(c.0);
        }
        if let Some(m) = marker {
            canvas = canvas.marker(m.0);
        }
        canvas = canvas.paint(|ctx| {
            for s in shapes {
                if let Some(shape) = &s.shape {
                    shape.draw(ctx);
                }
            }
        });
        frame.render_widget(canvas, rect);
        Ok(())
    }
}

// estokes 2022-01-21, This code is copied from the plotters-cairo
// crate. The author is unresponsive, and cairo needed to be
// updated. If the crate is updated in the future then this code
// should be removed in favor of using the crate.

use gtk::cairo::{Context as CairoContext, Error as CairoError, FontSlant, FontWeight};
use plotters_backend::text_anchor::{HPos, VPos};
use plotters_backend::{
    BackendColor, BackendCoord, BackendStyle, BackendTextStyle, DrawingBackend, DrawingErrorKind,
    FontStyle, FontTransform,
};

/// The drawing backend that is backed with a Cairo context
pub struct CairoBackend<'a> {
    context: &'a CairoContext,
    width: u32,
    height: u32,
    init_flag: bool,
}

impl<'a> CairoBackend<'a> {
    /// Call cairo functions and verify the cairo status afterward.
    ///
    /// All major cairo objects retain an error status internally
    /// which can be queried anytime by the users using status() method.
    /// In the mean time, it is safe to call all cairo functions normally even
    /// if the underlying object is in an error status.
    /// This means that no error handling code is required before or after
    /// each individual cairo function call.
    ///
    /// - `f`: The function to call
    /// - *Returns* The wrapped result of the function
    fn call_cairo<T, F: Fn(&CairoContext) -> Result<T, CairoError>>(
        &self,
        f: F,
    ) -> Result<T, DrawingErrorKind<CairoError>> {
        f(self.context).map_err(DrawingErrorKind::DrawingError)
    }

    fn set_color(&self, color: &BackendColor) -> Result<(), DrawingErrorKind<CairoError>> {
        self.call_cairo(|c| {
            c.set_source_rgba(
                f64::from(color.rgb.0) / 255.0,
                f64::from(color.rgb.1) / 255.0,
                f64::from(color.rgb.2) / 255.0,
                color.alpha,
            );
            Ok(())
        })
    }

    fn set_stroke_width(&self, width: u32) -> Result<(), DrawingErrorKind<CairoError>> {
        self.call_cairo(|c| {
            c.set_line_width(f64::from(width));
            Ok(())
        })
    }

    fn set_font<S: BackendTextStyle>(&self, font: &S) -> Result<(), DrawingErrorKind<CairoError>> {
        let actual_size = font.size();
        self.call_cairo(|c| {
            match font.style() {
                FontStyle::Normal => c.select_font_face(
                    font.family().as_str(),
                    FontSlant::Normal,
                    FontWeight::Normal,
                ),
                FontStyle::Bold => {
                    c.select_font_face(font.family().as_str(), FontSlant::Normal, FontWeight::Bold)
                }
                FontStyle::Oblique => c.select_font_face(
                    font.family().as_str(),
                    FontSlant::Oblique,
                    FontWeight::Normal,
                ),
                FontStyle::Italic => c.select_font_face(
                    font.family().as_str(),
                    FontSlant::Italic,
                    FontWeight::Normal,
                ),
            };
            c.set_font_size(actual_size);
            Ok(())
        })
    }

    pub fn new(context: &'a CairoContext, (w, h): (u32, u32)) -> Result<Self, CairoError> {
        let ret = Self {
            context,
            width: w,
            height: h,
            init_flag: false,
        };
        Ok(ret)
    }
}

impl<'a> DrawingBackend for CairoBackend<'a> {
    type ErrorType = CairoError;

    fn get_size(&self) -> (u32, u32) {
        (self.width, self.height)
    }

    fn ensure_prepared(&mut self) -> Result<(), DrawingErrorKind<Self::ErrorType>> {
        if !self.init_flag {
            self.call_cairo(|c| {
                let (x0, y0, x1, y1) = c.clip_extents()?;
                c.scale(
                    (x1 - x0) / f64::from(self.width),
                    (y1 - y0) / f64::from(self.height),
                );
                Ok(())
            })?;
            self.init_flag = true;
        }
        Ok(())
    }

    fn present(&mut self) -> Result<(), DrawingErrorKind<Self::ErrorType>> {
        Ok(())
    }

    fn draw_pixel(
        &mut self,
        point: BackendCoord,
        color: BackendColor,
    ) -> Result<(), DrawingErrorKind<Self::ErrorType>> {
        self.call_cairo(|c| {
            c.rectangle(f64::from(point.0), f64::from(point.1), 1.0, 1.0);
            c.set_source_rgba(
                f64::from(color.rgb.0) / 255.0,
                f64::from(color.rgb.1) / 255.0,
                f64::from(color.rgb.2) / 255.0,
                color.alpha,
            );
            c.fill()
        })
    }

    fn draw_line<S: BackendStyle>(
        &mut self,
        from: BackendCoord,
        to: BackendCoord,
        style: &S,
    ) -> Result<(), DrawingErrorKind<Self::ErrorType>> {
        self.set_color(&style.color())?;
        self.set_stroke_width(style.stroke_width())?;

        self.call_cairo(|c| {
            c.move_to(f64::from(from.0), f64::from(from.1));
            c.line_to(f64::from(to.0), f64::from(to.1));
            c.stroke()
        })
    }

    fn draw_rect<S: BackendStyle>(
        &mut self,
        upper_left: BackendCoord,
        bottom_right: BackendCoord,
        style: &S,
        fill: bool,
    ) -> Result<(), DrawingErrorKind<Self::ErrorType>> {
        self.set_color(&style.color())?;
        self.set_stroke_width(style.stroke_width())?;

        self.call_cairo(|c| {
            c.rectangle(
                f64::from(upper_left.0),
                f64::from(upper_left.1),
                f64::from(bottom_right.0 - upper_left.0),
                f64::from(bottom_right.1 - upper_left.1),
            );
            if fill {
                c.fill()
            } else {
                c.stroke()
            }
        })
    }

    fn draw_path<S: BackendStyle, I: IntoIterator<Item = BackendCoord>>(
        &mut self,
        path: I,
        style: &S,
    ) -> Result<(), DrawingErrorKind<Self::ErrorType>> {
        self.set_color(&style.color())?;
        self.set_stroke_width(style.stroke_width())?;

        let mut path = path.into_iter();
        if let Some((x, y)) = path.next() {
            self.call_cairo(|c| {
                c.move_to(f64::from(x), f64::from(y));
                Ok(())
            })?;
        }

        for (x, y) in path {
            self.call_cairo(|c| {
                c.line_to(f64::from(x), f64::from(y));
                Ok(())
            })?;
        }

        self.call_cairo(|c| c.stroke())
    }

    fn fill_polygon<S: BackendStyle, I: IntoIterator<Item = BackendCoord>>(
        &mut self,
        path: I,
        style: &S,
    ) -> Result<(), DrawingErrorKind<Self::ErrorType>> {
        self.set_color(&style.color())?;
        self.set_stroke_width(style.stroke_width())?;

        let mut path = path.into_iter();

        if let Some((x, y)) = path.next() {
            self.call_cairo(|c| {
                c.move_to(f64::from(x), f64::from(y));
                Ok(())
            })?;

            for (x, y) in path {
                self.call_cairo(|c| {
                    c.line_to(f64::from(x), f64::from(y));
                    Ok(())
                })?;
            }

            self.call_cairo(|c| {
                c.close_path();
                c.fill()
            })
        } else {
            Ok(())
        }
    }

    fn draw_circle<S: BackendStyle>(
        &mut self,
        center: BackendCoord,
        radius: u32,
        style: &S,
        fill: bool,
    ) -> Result<(), DrawingErrorKind<Self::ErrorType>> {
        self.set_color(&style.color())?;
        self.set_stroke_width(style.stroke_width())?;

        self.call_cairo(|c| {
            c.new_sub_path();
            c.arc(
                f64::from(center.0),
                f64::from(center.1),
                f64::from(radius),
                0.0,
                std::f64::consts::PI * 2.0,
            );

            if fill {
                c.fill()
            } else {
                c.stroke()
            }
        })
    }

    fn estimate_text_size<S: BackendTextStyle>(
        &self,
        text: &str,
        font: &S,
    ) -> Result<(u32, u32), DrawingErrorKind<Self::ErrorType>> {
        self.set_font(font)?;
        self.call_cairo(|c| {
            c.text_extents(text).map(|extents| (extents.width as u32, extents.height as u32))
        })
    }

    fn draw_text<S: BackendTextStyle>(
        &mut self,
        text: &str,
        style: &S,
        pos: BackendCoord,
    ) -> Result<(), DrawingErrorKind<Self::ErrorType>> {
        let color = style.color();
        let (mut x, mut y) = (pos.0, pos.1);

        let degree = match style.transform() {
            FontTransform::None => 0.0,
            FontTransform::Rotate90 => 90.0,
            FontTransform::Rotate180 => 180.0,
            FontTransform::Rotate270 => 270.0,
            // FontTransform::RotateAngle(a) => a as f64,
        } / 180.0
            * std::f64::consts::PI;

        if degree != 0.0 {
            self.call_cairo(|c| {
                c.save()?;
                c.translate(f64::from(x), f64::from(y));
                c.rotate(degree);
                Ok(())
            })?;
            x = 0;
            y = 0;
        }

        self.set_font(style)?;
        self.set_color(&color)?;

        self.call_cairo(|c| {
            let extents = c.text_extents(text)?;
            let dx = match style.anchor().h_pos {
                HPos::Left => 0.0,
                HPos::Right => -extents.width,
                HPos::Center => -extents.width / 2.0,
            };
            let dy = match style.anchor().v_pos {
                VPos::Top => extents.height,
                VPos::Center => extents.height / 2.0,
                VPos::Bottom => 0.0,
            };
            c.move_to(
                f64::from(x) + dx - extents.x_bearing,
                f64::from(y) + dy - extents.y_bearing - extents.height,
            );
            c.show_text(text)?;
            if degree != 0.0 {
                c.restore()?;
            }
            Ok(())
        })
    }
}

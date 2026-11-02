//! Renders a [`Figure`] as an SVG line chart, in a light and a dark variant.
//!
//! Colors are the reference data-viz palette: categorical slots in fixed order (the first three
//! validate for every pair in both modes, so charts use at most three colored series), with
//! reference lines drawn dashed in neutral ink rather than taking a categorical slot.

use std::error::Error;
use std::path::Path;

use plotters::prelude::*;
use plotters::style::text_anchor::{HPos, Pos, VPos};

use crate::figure::{Figure, Format, Scale, Series, format_value};

struct Theme {
    surface: RGBColor,
    text_primary: RGBColor,
    text_secondary: RGBColor,
    grid: RGBColor,
    axis: RGBColor,
    reference: RGBColor,
    series: [RGBColor; 3],
}

const LIGHT: Theme = Theme {
    surface: RGBColor(0xfc, 0xfc, 0xfb),
    text_primary: RGBColor(0x0b, 0x0b, 0x0b),
    text_secondary: RGBColor(0x52, 0x51, 0x4e),
    grid: RGBColor(0xe7, 0xe6, 0xe2),
    axis: RGBColor(0xa3, 0xa2, 0x9c),
    reference: RGBColor(0x7a, 0x79, 0x74),
    series: [
        RGBColor(0x2a, 0x78, 0xd6),
        RGBColor(0xeb, 0x68, 0x34),
        RGBColor(0x1b, 0xaf, 0x7a),
    ],
};

const DARK: Theme = Theme {
    surface: RGBColor(0x1a, 0x1a, 0x19),
    text_primary: RGBColor(0xf0, 0xef, 0xec),
    text_secondary: RGBColor(0xc3, 0xc2, 0xb7),
    grid: RGBColor(0x2f, 0x2f, 0x2d),
    axis: RGBColor(0x5c, 0x5b, 0x57),
    reference: RGBColor(0x9a, 0x99, 0x92),
    series: [
        RGBColor(0x39, 0x87, 0xe5),
        RGBColor(0xd9, 0x59, 0x26),
        RGBColor(0x19, 0x9e, 0x70),
    ],
};

const WIDTH: u32 = 960;
const HEIGHT: u32 = 560;
const FONT: &str = "sans-serif";

/// Writes `<id>.svg` (light) and `<id>-dark.svg` into `dir`.
pub fn render(fig: &Figure, dir: &Path) -> Result<(), Box<dyn Error>> {
    let colored = fig.series.iter().filter(|s| s.is_primary()).count();
    assert!(
        colored <= 3,
        "{}: at most three colored series per chart",
        fig.id
    );
    draw(fig, &dir.join(format!("{}.svg", fig.id)), &LIGHT)?;
    draw(fig, &dir.join(format!("{}-dark.svg", fig.id)), &DARK)?;
    Ok(())
}

fn to_axis(v: f64, scale: Scale) -> f64 {
    match scale {
        Scale::Linear => v,
        Scale::Log2 => v.max(f64::MIN_POSITIVE).log2(),
        Scale::Log10 => v.max(f64::MIN_POSITIVE).log10(),
    }
}

/// Label for an axis position; non-integer positions on log axes get no label.
fn tick_label(pos: f64, scale: Scale, format: Format) -> String {
    let value = match scale {
        Scale::Linear => pos,
        Scale::Log2 | Scale::Log10 => {
            if (pos - pos.round()).abs() > 1e-6 {
                return String::new();
            }
            let base: f64 = if scale == Scale::Log2 { 2.0 } else { 10.0 };
            base.powf(pos.round())
        }
    };
    format_value(value, format)
}

/// Axis range covering `values`, padded. Log ranges always include at least two whole powers
/// so the axis has labels.
fn axis_range(values: &[f64], scale: Scale, from_zero: bool) -> (f64, f64) {
    let lo = values.iter().copied().fold(f64::INFINITY, f64::min);
    let hi = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    match scale {
        Scale::Linear => {
            let lo = if from_zero { 0.0 } else { lo };
            let span = (hi - lo).max(1e-9);
            (
                lo - if from_zero { 0.0 } else { span * 0.05 },
                hi + span * 0.08,
            )
        }
        Scale::Log2 | Scale::Log10 => {
            let (mut lo, mut hi) = (lo - 0.15, hi + 0.15);
            if hi.floor() - lo.ceil() < 1.0 {
                lo = lo.min(lo.floor());
                hi = hi.max(lo + 1.0);
            }
            (lo, hi)
        }
    }
}

/// Tick count that makes plotters place exactly one tick per whole power on a log axis.
fn tick_count(lo: f64, hi: f64, scale: Scale, linear: usize) -> usize {
    match scale {
        Scale::Linear => linear,
        Scale::Log2 | Scale::Log10 => (hi.floor() - lo.ceil()) as usize + 1,
    }
}

fn draw(fig: &Figure, path: &Path, theme: &Theme) -> Result<(), Box<dyn Error>> {
    let root = SVGBackend::new(path, (WIDTH, HEIGHT)).into_drawing_area();
    root.fill(&theme.surface)?;

    let left =
        TextStyle::from((FONT, 22).into_font().style(FontStyle::Bold)).color(&theme.text_primary);
    root.draw(&Text::new(
        fig.title.clone(),
        (28, 30),
        left.pos(Pos::new(HPos::Left, VPos::Center)),
    ))?;
    if !fig.subtitle.is_empty() {
        let sub = TextStyle::from((FONT, 15).into_font()).color(&theme.text_secondary);
        root.draw(&Text::new(
            fig.subtitle.clone(),
            (28, 56),
            sub.pos(Pos::new(HPos::Left, VPos::Center)),
        ))?;
    }

    // Legend row under the title: a swatch and name per series, in series order.
    let legend_font = TextStyle::from((FONT, 14).into_font())
        .color(&theme.text_secondary)
        .pos(Pos::new(HPos::Left, VPos::Center));
    let mut lx = 28;
    let ly = 86;
    let has_baselines = fig.series.iter().any(|s| s.baseline_of.is_some());
    let legend_entries = fig
        .series
        .iter()
        .filter(|s| s.baseline_of.is_none())
        .map(|s| (s.name.clone(), series_style(fig, s, theme)))
        .chain(
            has_baselines.then(|| ("previous run (dashed)".to_string(), (theme.reference, true))),
        );
    for (name, (color, dashed)) in legend_entries {
        if dashed {
            for i in 0..3 {
                root.draw(&PathElement::new(
                    vec![(lx + i * 9, ly), (lx + i * 9 + 5, ly)],
                    color.stroke_width(2),
                ))?;
            }
        } else {
            root.draw(&PathElement::new(
                vec![(lx, ly), (lx + 24, ly)],
                color.stroke_width(2),
            ))?;
            root.draw(&Circle::new((lx + 12, ly), 4, color.filled()))?;
        }
        root.draw(&Text::new(name.clone(), (lx + 32, ly), legend_font.clone()))?;
        lx += 32 + (name.chars().count() as i32 * 8) + 28;
    }

    let (_, plot_area) = root.split_vertically(104);

    let xs: Vec<f64> = fig
        .series
        .iter()
        .flat_map(|s| s.points.iter().map(|p| to_axis(p.0, fig.x_scale)))
        .collect();
    let ys: Vec<f64> = fig
        .series
        .iter()
        .flat_map(|s| s.points.iter().map(|p| to_axis(p.1, fig.y_scale)))
        .collect();
    let (x0, x1) = axis_range(&xs, fig.x_scale, false);
    let (y0, y1) = axis_range(&ys, fig.y_scale, true);

    let mut chart = ChartBuilder::on(&plot_area)
        .margin_left(16)
        .margin_right(170)
        .margin_top(8)
        .margin_bottom(12)
        .x_label_area_size(58)
        .y_label_area_size(84)
        .build_cartesian_2d(x0..x1, y0..y1)?;

    let label_style = TextStyle::from((FONT, 14).into_font()).color(&theme.text_secondary);
    let desc_style = TextStyle::from((FONT, 15).into_font()).color(&theme.text_secondary);
    let (xs_, xf, ys_, yf) = (fig.x_scale, fig.x_format, fig.y_scale, fig.y_format);
    let x_fmt = move |v: &f64| tick_label(*v, xs_, xf);
    let y_fmt = move |v: &f64| tick_label(*v, ys_, yf);
    let x_ticks = tick_count(x0, x1, fig.x_scale, 8);
    let y_ticks = tick_count(y0, y1, fig.y_scale, 6);
    chart
        .configure_mesh()
        .disable_x_mesh()
        .bold_line_style(theme.grid.stroke_width(1))
        .light_line_style(TRANSPARENT)
        .axis_style(theme.axis.stroke_width(1))
        .x_labels(x_ticks)
        .y_labels(y_ticks)
        .x_label_formatter(&x_fmt)
        .y_label_formatter(&y_fmt)
        .label_style(label_style)
        .axis_desc_style(desc_style)
        .x_desc(fig.x_label.clone())
        .y_desc(fig.y_label.clone())
        .draw()?;

    // Dashed lines first so data marks sit on top of them.
    let mut ends = Vec::new();
    for pass_dashed in [true, false] {
        for s in &fig.series {
            let (color, dashed) = series_style(fig, s, theme);
            if dashed != pass_dashed || s.points.is_empty() {
                continue;
            }
            let pts: Vec<(f64, f64)> = s
                .points
                .iter()
                .map(|&(x, y)| (to_axis(x, fig.x_scale), to_axis(y, fig.y_scale)))
                .filter(|&(_, y)| y >= y0 && y <= y1)
                .collect();
            if dashed {
                chart.draw_series(DashedLineSeries::new(
                    pts.clone(),
                    6,
                    5,
                    color.stroke_width(2),
                ))?;
            } else {
                chart.draw_series(LineSeries::new(pts.clone(), color.stroke_width(2)))?;
                // Markers with a 2px surface ring so overlapping points stay distinct.
                chart.draw_series(
                    pts.iter()
                        .map(|&p| Circle::new(p, 6, theme.surface.filled())),
                )?;
                chart.draw_series(pts.iter().map(|&p| Circle::new(p, 4, color.filled())))?;
            }
            if let Some(&last) = pts.last()
                && s.baseline_of.is_none()
            {
                ends.push((chart.backend_coord(&last), s.name.clone()));
            }
        }
    }

    // Direct labels at line ends in text ink, nudged apart vertically so they never overlap.
    ends.sort_by_key(|((_, y), _)| *y);
    let mut placed: Vec<i32> = Vec::new();
    for ((x, y), _) in &ends {
        let mut y = *y;
        if let Some(&prev) = placed.last() {
            y = y.max(prev + 17);
        }
        placed.push(y);
        let _ = x;
    }
    let end_font = TextStyle::from((FONT, 13).into_font())
        .color(&theme.text_secondary)
        .pos(Pos::new(HPos::Left, VPos::Center));
    for (((x, _), name), y) in ends.iter().zip(placed) {
        root.draw(&Text::new(name.clone(), (x + 12, y), end_font.clone()))?;
    }

    root.present()?;
    Ok(())
}

/// Color and dashing for a series. Primary series take categorical slots in order; a baseline
/// reuses its series' color, dashed; reference lines are dashed neutral ink.
fn series_style(fig: &Figure, s: &Series, theme: &Theme) -> (RGBColor, bool) {
    let slot = |name: &str| {
        fig.series
            .iter()
            .filter(|p| p.is_primary())
            .position(|p| p.name == name)
            .map(|i| theme.series[i % theme.series.len()])
    };
    if s.reference {
        (theme.reference, true)
    } else if let Some(of) = &s.baseline_of {
        (slot(of).unwrap_or(theme.reference), true)
    } else {
        (slot(&s.name).unwrap_or(theme.reference), false)
    }
}

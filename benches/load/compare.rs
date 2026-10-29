//! Compares a run against a baseline run to flag regressions.
//!
//! Points are paired by position within each series. A change counts as a regression or an
//! improvement only if it exceeds the figure's threshold: the larger of 10% and twice the
//! run-to-run noise. Noise is measured from an A/A pair: two runs of the baseline code.
//! For percentage figures (e.g. duplicate executions), changes are in percentage points.

use std::path::Path;

use crate::figure::{Figure, Format, Series, format_value};

const MIN_THRESHOLD_PCT: f64 = 10.0;
const MIN_THRESHOLD_POINTS: f64 = 5.0;

#[derive(Clone, Copy, PartialEq)]
enum Verdict {
    Regression,
    Improvement,
    Noise,
}

struct PointChange {
    series: String,
    x: f64,
    base: f64,
    current: f64,
    /// Signed change, oriented so that positive is better.
    goodness: f64,
    verdict: Verdict,
}

struct FigureChange {
    figure: Figure,
    noise: f64,
    threshold: f64,
    points: Vec<PointChange>,
}

/// Signed change from `base` to `current`: % for normal figures, points for percentages.
fn change(base: f64, current: f64, format: Format) -> f64 {
    match format {
        Format::Percent => current - base,
        _ if base.abs() < 1e-12 => 0.0,
        _ => (current - base) / base * 100.0,
    }
}

fn paired<'a>(current: &'a Figure, other: &'a Figure) -> Vec<(&'a Series, &'a Series)> {
    current
        .series
        .iter()
        .filter(|s| s.is_primary())
        .filter_map(|s| other.series.iter().find(|o| o.name == s.name).map(|o| (s, o)))
        .collect()
}

/// Median absolute change between two runs of the same code.
fn noise(baseline: &Figure, aa: &Figure) -> f64 {
    let mut changes: Vec<f64> = paired(baseline, aa)
        .into_iter()
        .flat_map(|(b, a)| {
            b.points
                .iter()
                .zip(&a.points)
                .map(|(p, q)| change(q.1, p.1, baseline.y_format).abs())
        })
        .collect();
    if changes.is_empty() {
        return 0.0;
    }
    changes.sort_by(|a, b| a.partial_cmp(b).unwrap());
    changes[changes.len() / 2]
}

fn compare_figure(current: &Figure, baseline: &Figure, aa: Option<&Figure>) -> FigureChange {
    let noise = aa.map(|a| noise(baseline, a)).unwrap_or(0.0);
    let floor = if current.y_format == Format::Percent { MIN_THRESHOLD_POINTS } else { MIN_THRESHOLD_PCT };
    let threshold = floor.max(2.0 * noise);
    let mut points = Vec::new();
    for (cur, base) in paired(current, baseline) {
        for (c, b) in cur.points.iter().zip(&base.points) {
            let delta = change(b.1, c.1, current.y_format);
            let goodness = if current.higher_is_better { delta } else { -delta };
            let verdict = if goodness < -threshold {
                Verdict::Regression
            } else if goodness > threshold {
                Verdict::Improvement
            } else {
                Verdict::Noise
            };
            points.push(PointChange {
                series: cur.name.clone(),
                x: c.0,
                base: b.1,
                current: c.1,
                goodness,
                verdict,
            });
        }
    }
    FigureChange { figure: current.clone(), noise, threshold, points }
}

/// A chart of the current run's series with the baseline drawn dashed in matching colors.
fn overlay(current: &Figure, baseline: &Figure) -> Figure {
    let mut fig = current.clone();
    fig.id = format!("compare_{}", current.id);
    fig.subtitle = "Solid: this change. Dashed: previous commit, rerun on the same machine.".into();
    fig.series.retain(|s| s.is_primary());
    for (cur, base) in paired(current, baseline) {
        fig.series.push(Series {
            name: format!("{} (before)", cur.name),
            reference: false,
            baseline_of: Some(cur.name.clone()),
            points: base.points.clone(),
        });
    }
    fig
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = values.len();
    if n % 2 == 1 { values[n / 2] } else { (values[n / 2 - 1] + values[n / 2]) / 2.0 }
}

/// Pointwise median of several runs of one figure, pairing points by position.
pub fn median_figure(runs: &[Figure]) -> Option<Figure> {
    let mut merged = runs.first()?.clone();
    for series in &mut merged.series {
        let others: Vec<&Series> = runs
            .iter()
            .filter_map(|f| f.series.iter().find(|s| s.name == series.name))
            .collect();
        for (i, point) in series.points.iter_mut().enumerate() {
            let at: Vec<(f64, f64)> = others.iter().filter_map(|s| s.points.get(i).copied()).collect();
            *point = (median(at.iter().map(|p| p.0).collect()), median(at.iter().map(|p| p.1).collect()));
        }
    }
    merged.subtitle = format!("{} (median of {} runs)", merged.subtitle, runs.len());
    Some(merged)
}

fn load(dir: &Path, id: &str) -> Option<Figure> {
    let json = std::fs::read_to_string(dir.join(format!("{id}.json"))).ok()?;
    serde_json::from_str(&json).ok()
}

fn unit(format: Format) -> &'static str {
    if format == Format::Percent { " pts" } else { "%" }
}

fn signed(v: f64, format: Format) -> String {
    format!("{v:+.1}{}", unit(format))
}

/// Writes `comparison.md` into `out_dir` and before/after charts into `graphs_dir`.
pub fn run(
    current: &[Figure],
    baseline_dir: &Path,
    aa_dir: Option<&Path>,
    out_dir: &Path,
    graphs_dir: &Path,
) -> std::io::Result<()> {
    let mut changes = Vec::new();
    let mut new_figures = Vec::new();
    for fig in current {
        match load(baseline_dir, &fig.id) {
            Some(base) => {
                let aa = aa_dir.and_then(|d| load(d, &fig.id));
                crate::plot::render(&overlay(fig, &base), graphs_dir).expect("render comparison");
                changes.push(compare_figure(fig, &base, aa.as_ref()));
            }
            None => new_figures.push(fig.id.clone()),
        }
    }

    let mut md = String::from("# Benchmark comparison\n\n");
    md += "Generated by `cargo bench --bench load -- --compare <baseline> --noise <aa>`.\n\n";
    md += "Positive changes are improvements, whichever direction the metric runs. A point is \
           flagged only when its change exceeds the figure's threshold: the larger of 10% \
           (5 percentage points for percentages) and twice the A/A noise between two runs of \
           the baseline code.\n\n";
    md += "| Figure | Median change | A/A noise | Threshold | Regressions | Improvements |\n";
    md += "|---|---:|---:|---:|---:|---:|\n";
    for c in &changes {
        let mut goods: Vec<f64> = c.points.iter().map(|p| p.goodness).collect();
        goods.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = goods.get(goods.len() / 2).copied().unwrap_or(0.0);
        let count = |v| c.points.iter().filter(|p| p.verdict == v).count();
        md += &format!(
            "| [{}](#{}) | {} | {:.1}{} | {:.1}{} | {} | {} |\n",
            c.figure.title,
            c.figure.id,
            signed(median, c.figure.y_format),
            c.noise,
            unit(c.figure.y_format),
            c.threshold,
            unit(c.figure.y_format),
            count(Verdict::Regression),
            count(Verdict::Improvement),
        );
    }
    if !new_figures.is_empty() {
        md += &format!("\nNew figures without a baseline: {}.\n", new_figures.join(", "));
    }

    for c in &changes {
        let f = &c.figure;
        md += &format!("\n## {}\n\n<a id=\"{}\"></a>\n\n", f.title, f.id);
        md += &format!(
            "![{} before and after](graphs/compare_{}.svg)\n\n",
            f.title, f.id
        );
        md += &format!("| Series | {} | Before | After | Change (+ is better) | Verdict |\n", f.x_label);
        md += "|---|---:|---:|---:|---:|---|\n";
        for p in &c.points {
            let verdict = match p.verdict {
                Verdict::Regression => "**regression**",
                Verdict::Improvement => "improvement",
                Verdict::Noise => "within noise",
            };
            md += &format!(
                "| {} | {} | {} | {} | {} | {} |\n",
                p.series,
                format_value(p.x, f.x_format),
                format_value(p.base, f.y_format),
                format_value(p.current, f.y_format),
                signed(p.goodness, f.y_format),
                verdict,
            );
        }
    }
    std::fs::write(out_dir.join("comparison.md"), md)
}

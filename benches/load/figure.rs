//! Serializable description of one benchmark chart and its data.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Scale {
    Linear,
    /// Values are plotted at log2(v); ticks fall on powers of two.
    Log2,
    /// Values are plotted at log10(v); ticks fall on powers of ten.
    Log10,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Format {
    Number,
    Bytes,
    Percent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Series {
    pub name: String,
    /// Reference lines (e.g. theoretical limits) are drawn dashed in neutral ink.
    pub reference: bool,
    pub points: Vec<(f64, f64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Figure {
    pub id: String,
    pub title: String,
    pub subtitle: String,
    pub x_label: String,
    pub y_label: String,
    pub x_scale: Scale,
    pub y_scale: Scale,
    pub x_format: Format,
    pub y_format: Format,
    pub series: Vec<Series>,
    /// Free-form measurements that don't fit the chart, keyed by name.
    #[serde(default)]
    pub notes: Vec<(String, String)>,
}

impl Figure {
    pub fn new(id: &str, title: &str, x_label: &str, y_label: &str) -> Self {
        Self {
            id: id.into(),
            title: title.into(),
            subtitle: String::new(),
            x_label: x_label.into(),
            y_label: y_label.into(),
            x_scale: Scale::Linear,
            y_scale: Scale::Linear,
            x_format: Format::Number,
            y_format: Format::Number,
            series: Vec::new(),
            notes: Vec::new(),
        }
    }

    pub fn series(&mut self, name: &str) -> &mut Series {
        if let Some(i) = self.series.iter().position(|s| s.name == name) {
            return &mut self.series[i];
        }
        self.series.push(Series { name: name.into(), reference: false, points: Vec::new() });
        self.series.last_mut().unwrap()
    }

    pub fn push(&mut self, series: &str, x: f64, y: f64) {
        self.series(series).points.push((x, y));
    }

    pub fn note(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.notes.push((key.into(), value.into()));
    }
}

pub fn format_value(v: f64, format: Format) -> String {
    match format {
        Format::Percent => {
            if v == 0.0 { "0%".into() } else if v < 10.0 { format!("{v:.1}%") } else { format!("{v:.0}%") }
        }
        Format::Bytes => {
            const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
            let mut v = v;
            let mut unit = 0;
            while v >= 1024.0 && unit < UNITS.len() - 1 {
                v /= 1024.0;
                unit += 1;
            }
            if v.fract() == 0.0 { format!("{v:.0} {}", UNITS[unit]) } else { format!("{v:.1} {}", UNITS[unit]) }
        }
        Format::Number => {
            let a = v.abs();
            if a >= 1e6 {
                trim(format!("{:.1}", v / 1e6)) + "M"
            } else if a >= 1e4 {
                format!("{:.0}k", v / 1e3)
            } else if a >= 1e3 {
                trim(format!("{:.1}", v / 1e3)) + "k"
            } else if a >= 100.0 || v.fract() == 0.0 {
                format!("{v:.0}")
            } else if a >= 10.0 {
                format!("{v:.1}")
            } else {
                format!("{v:.2}")
            }
        }
    }
}

fn trim(s: String) -> String {
    s.strip_suffix(".0").map(str::to_owned).unwrap_or(s)
}


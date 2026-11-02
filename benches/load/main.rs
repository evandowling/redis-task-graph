//! Load tests and benchmarks against a live Redis at `redis://127.0.0.1/`.
//!
//! ```text
//! cargo bench --bench load                         # run every scenario, then plot
//! cargo bench --bench load -- chain_latency io_bound
//! cargo bench --bench load -- --quick              # smaller runs for a smoke test
//! cargo bench --bench load -- --plot               # re-render graphs from saved results
//! cargo bench --bench load -- --compare <dir> [--noise <dir>]
//! ```
//!
//! Results are written to `perf/results/<scenario>.json`, graphs to `perf/graphs/`, and a
//! table of every result to `perf/results/summary.md`. `--compare` compares the saved results
//! against a baseline results directory, using a second baseline run (`--noise`) to estimate
//! run-to-run variation, and writes `perf/comparison.md` with before/after graphs.

mod compare;
mod figure;
mod harness;
mod plot;
mod scenarios;

use std::path::{Path, PathBuf};
use std::time::Instant;

use figure::{Figure, format_value};

fn perf_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("perf")
}

/// Value following `flag` on the command line, if present.
fn flag_value(args: &[String], flag: &str) -> Option<PathBuf> {
    args.iter().position(|a| a == flag).map(|i| {
        PathBuf::from(
            args.get(i + 1)
                .unwrap_or_else(|| panic!("{flag} needs a directory")),
        )
    })
}

fn main() {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| a != "--bench")
        .collect();
    let quick = args.iter().any(|a| a == "--quick");
    // With `--features otel`, `--otel` streams the library's metrics to the OTLP endpoint in
    // OTEL_EXPORTER_OTLP_ENDPOINT / _HEADERS while the scenarios run.
    let otel = args.iter().any(|a| a == "--otel").then(install_otel);
    if args.iter().any(|a| a == "--no-pushback") {
        harness::PUSHBACK.store(false, std::sync::atomic::Ordering::Relaxed);
    }
    if let Some(i) = args.iter().position(|a| a == "--merge-median") {
        let out = PathBuf::from(&args[i + 1]);
        let inputs: Vec<PathBuf> = args[i + 2..].iter().map(PathBuf::from).collect();
        merge_median(&out, &inputs);
        return;
    }
    let compare_dir = flag_value(&args, "--compare");
    let noise_dir = flag_value(&args, "--noise");
    let current_dir = flag_value(&args, "--current");
    let plot_only =
        args.iter().any(|a| a == "--plot") || compare_dir.is_some() || current_dir.is_some();
    let values: Vec<&PathBuf> = compare_dir
        .iter()
        .chain(noise_dir.iter())
        .chain(current_dir.iter())
        .collect();
    let selected: Vec<&str> = args
        .iter()
        .filter(|a| !a.starts_with("--") && !values.iter().any(|v| v.as_os_str() == a.as_str()))
        .map(String::as_str)
        .collect();
    for name in &selected {
        assert!(
            scenarios::ALL.iter().any(|(n, _)| n == name),
            "unknown scenario `{name}`"
        );
    }

    let results = current_dir.unwrap_or_else(|| perf_dir().join("results"));
    let graphs = perf_dir().join("graphs");
    std::fs::create_dir_all(&results).unwrap();
    std::fs::create_dir_all(&graphs).unwrap();

    if !plot_only {
        write_environment(&results);
        let opts = scenarios::Opts {
            scale: if quick { 0.1 } else { 1.0 },
        };
        for (name, run) in scenarios::ALL {
            if !selected.is_empty() && !selected.contains(name) {
                continue;
            }
            eprintln!("== {name}");
            let start = Instant::now();
            let fig = run(&opts);
            eprintln!("   done in {:.1}s", start.elapsed().as_secs_f64());
            let json = serde_json::to_string_pretty(&fig).unwrap();
            std::fs::write(results.join(format!("{name}.json")), json).unwrap();
        }
    }

    let mut figures = Vec::new();
    for (name, _) in scenarios::ALL {
        let path = results.join(format!("{name}.json"));
        if let Ok(json) = std::fs::read_to_string(&path) {
            let fig: Figure = serde_json::from_str(&json).unwrap();
            plot::render(&fig, &graphs).unwrap();
            figures.push(fig);
        }
    }
    std::fs::write(results.join("summary.md"), summary(&figures)).unwrap();
    if let Some(baseline) = &compare_dir {
        compare::run(
            &figures,
            baseline,
            noise_dir.as_deref(),
            &perf_dir(),
            &graphs,
        )
        .unwrap();
        eprintln!("wrote {}", perf_dir().join("comparison.md").display());
    }
    eprintln!("wrote {} graphs to {}", figures.len(), graphs.display());
    if let Some(provider) = otel {
        provider.shutdown().unwrap();
    }
}

fn install_otel() -> opentelemetry_sdk::metrics::SdkMeterProvider {
    use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .build()
        .expect("OTLP exporter");
    let provider = SdkMeterProvider::builder()
        .with_reader(
            PeriodicReader::builder(exporter)
                .with_interval(std::time::Duration::from_secs(5))
                .build(),
        )
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name("redis-task-graph-bench")
                .build(),
        )
        .build();
    opentelemetry::global::set_meter_provider(provider.clone());
    provider
}

/// Writes into `out` the pointwise median of each scenario's figures across `inputs`.
fn merge_median(out: &Path, inputs: &[PathBuf]) {
    std::fs::create_dir_all(out).unwrap();
    for (name, _) in scenarios::ALL {
        let figures: Vec<Figure> = inputs
            .iter()
            .filter_map(|dir| std::fs::read_to_string(dir.join(format!("{name}.json"))).ok())
            .map(|json| serde_json::from_str(&json).unwrap())
            .collect();
        if let Some(merged) = compare::median_figure(&figures) {
            let json = serde_json::to_string_pretty(&merged).unwrap();
            std::fs::write(out.join(format!("{name}.json")), json).unwrap();
        }
    }
    eprintln!("merged {} runs into {}", inputs.len(), out.display());
}

fn write_environment(dir: &Path) {
    let rt = harness::driver();
    let info: String = rt.block_on(async {
        let client = redis::Client::open(harness::REDIS_URL).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        redis::cmd("INFO")
            .arg("server")
            .query_async(&mut conn)
            .await
            .unwrap()
    });
    let field = |k: &str| {
        info.lines()
            .find_map(|l| l.strip_prefix(&format!("{k}:")))
            .unwrap_or("unknown")
            .trim()
            .to_string()
    };
    let env = serde_json::json!({
        "redis_version": field("redis_version"),
        "server": if info.contains("memurai") { format!("Memurai {}", field("memurai_version")) } else { "Redis".into() },
        "os": std::env::consts::OS,
        "hardware_threads": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
    });
    std::fs::write(
        dir.join("environment.json"),
        serde_json::to_string_pretty(&env).unwrap(),
    )
    .unwrap();
}

/// One markdown table per figure: a row per x value, a column per series.
fn summary(figures: &[Figure]) -> String {
    let mut out = String::from("# Benchmark results\n\nGenerated by `cargo bench --bench load`.\n");
    for fig in figures {
        out += &format!("\n## {} (`{}`)\n\n{}\n\n", fig.title, fig.id, fig.subtitle);
        let mut xs: Vec<f64> = fig
            .series
            .iter()
            .flat_map(|s| s.points.iter().map(|p| p.0))
            .collect();
        xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        xs.dedup();
        out += &format!("| {} |", fig.x_label);
        for s in &fig.series {
            out += &format!(" {} |", s.name);
        }
        out += "\n|---|";
        out += &"---:|".repeat(fig.series.len());
        out += "\n";
        for x in xs {
            out += &format!("| {} |", format_value(x, fig.x_format));
            for s in &fig.series {
                let cell = s
                    .points
                    .iter()
                    .find(|p| p.0 == x)
                    .map(|p| format_value(p.1, fig.y_format))
                    .unwrap_or_default();
                out += &format!(" {cell} |");
            }
            out += "\n";
        }
        if !fig.notes.is_empty() {
            out += "\n";
            for (k, v) in &fig.notes {
                out += &format!("- {k}: {v}\n");
            }
        }
    }
    out
}

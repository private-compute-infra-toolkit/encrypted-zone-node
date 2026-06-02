// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::{Context, Result};
use clap::Parser;
use manifest_proto::enforcer::v1::ez_manifest::ManifestType;
use manifest_proto::enforcer::v1::EzManifest;
use metrics::receiver::IsolateMetricsReceiver;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Offline simulator for testing metrics policies on OTel metrics streams"
)]
struct Args {
    /// Path to the EZ manifest JSON file.
    #[arg(short, long)]
    manifest: PathBuf,

    /// Path to input file containing OTLP ExportMetricsServiceRequest JSON payloads.
    #[arg(short, long)]
    input: PathBuf,

    /// Path to write the filtered baseline metrics (JSON).
    #[arg(long)]
    output_baseline: Option<PathBuf>,

    /// Path to write the DP noised metrics (JSON).
    #[arg(long)]
    output_dp: Option<PathBuf>,

    /// Number of runs to aggregate for statistical utility comparison.
    #[arg(short, long, default_value_t = 1)]
    runs: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct DataPointKey {
    metric_name: String,
    attributes: BTreeMap<String, String>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // 1. Parse manifest using manifest_parser library
    println!("Loading manifest from: {:?}", args.manifest);
    let manifest_path_str =
        args.manifest.to_str().context("Invalid manifest path unicode")?.to_string();
    let manifest = manifest_parser::parse_manifest(manifest_path_str)
        .context("Failed to parse manifest JSON")?;

    let (binary_manifest, manifest_to_use) = match manifest.manifest_type {
        Some(ManifestType::BinaryManifest(ref bm)) => (bm.clone(), manifest.clone()),
        Some(ManifestType::BundleManifest(ref bm)) => {
            // For now, find the first binary manifest in the bundle that has a metrics policy.
            // In the future, we could allow selecting via command line.
            let mut found: Option<(manifest_proto::enforcer::v1::BinaryManifest, EzManifest)> = None;
            for sub_manifest in &bm.manifests {
                if let Some(ManifestType::BinaryManifest(ref sub_bm)) = sub_manifest.manifest_type {
                    if sub_bm.metrics_policy.is_some() {
                        if found.is_some() {
                            println!("Warning: Multiple binary manifests with metrics policies found in bundle. Using the first one.");
                            break;
                        }
                        found = Some((sub_bm.clone(), sub_manifest.clone()));
                    }
                }
            }
            if let Some(f) = found {
                f
            } else {
                anyhow::bail!("Bundle manifest does not contain any binary manifest with a metrics policy.");
            }
        }
        _ => anyhow::bail!(
            "Manifest does not contain a BinaryManifest or BundleManifest. Simulator only supports binary workloads."
        ),
    };

    let metrics_policy = binary_manifest.metrics_policy.clone().unwrap_or_default();

    println!(
        "Loaded metrics policy: {} allowed metric entries",
        metrics_policy.allowed_metrics.len()
    );

    // 2. Load OTLP requests from input file
    println!("Loading OTLP payloads from: {:?}", args.input);
    let payloads = load_otlp_payloads(&args.input)?;
    println!("Loaded {} OTLP export requests", payloads.len());

    // 3. Instantiate the IsolateMetricsReceiver locally inside tokio block
    let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;

    // Create baseline receiver (does filtering only)
    let baseline_receiver = rt.block_on(async {
        IsolateMetricsReceiver::new(
            metrics_policy.clone(),
            manifest_to_use.isolate_name.clone(),
            manifest_to_use.publisher_id.clone(),
            false,           // is_ratified
            None,            // No real collector endpoint
            4 * 1024 * 1024, // default max message size
            false,           // Run metrics filtering
        )
        .await
    })?;

    // Create active simulation receiver (does filtering only at this stage)
    let receiver = rt.block_on(async {
        IsolateMetricsReceiver::new(
            metrics_policy,
            manifest_to_use.isolate_name,
            manifest_to_use.publisher_id,
            false,           // is_ratified
            None,            // No real collector endpoint during simulation
            4 * 1024 * 1024, // default max message size
            false,           // Run metrics filtering
        )
        .await
    })?;

    // 4. Run baseline metrics filtering (un-noised)
    println!("Processing baseline metrics stream (un-noised)...");
    let mut baseline_payloads = payloads.clone();
    for req in &mut baseline_payloads {
        baseline_receiver.filter_metrics(req);
        baseline_receiver.enrich_metrics(req);
    }

    if let Some(ref out_path) = args.output_baseline {
        println!("Saving baseline metrics to: {:?}", out_path);
        save_otlp_payloads(out_path, &baseline_payloads)?;
        println!("Saved baseline metrics successfully.");
    }

    // 5. Run simulation runs
    println!("Running {} simulation runs...", args.runs);
    let mut all_run_metrics = Vec::new();
    for run_idx in 0..args.runs {
        let mut run_payloads = payloads.clone();
        for req in &mut run_payloads {
            receiver.filter_metrics(req);
            receiver.enrich_metrics(req);
        }

        if run_idx == 0 {
            if let Some(ref out_path) = args.output_dp {
                println!("Saving sample DP noised metrics to: {:?}", out_path);
                save_otlp_payloads(out_path, &run_payloads)?;
                println!("Saved DP metrics successfully.");
            }
        }

        // Flatten values for statistical analysis
        let run_values = extract_values(&run_payloads);
        all_run_metrics.push(run_values);
    }

    // 6. Aggregate and print statistical utility table if runs > 1
    if args.runs > 1 {
        let baseline_values = extract_values(&baseline_payloads);
        print_utility_report(args.runs, &baseline_values, &all_run_metrics);
    } else {
        println!(
            "Simulation complete. Run with --runs > 1 to perform statistical DP utility analysis."
        );
    }

    Ok(())
}

/// Helper to load OTLP ExportMetricsServiceRequest JSON payloads.
/// Supports both a single JSON array or newline-delimited JSON.
fn load_otlp_payloads(
    path: impl AsRef<std::path::Path>,
) -> Result<Vec<ExportMetricsServiceRequest>> {
    let content = std::fs::read_to_string(path.as_ref()).context("failed to read input file")?;
    let trimmed = content.trim();

    if trimmed.starts_with('[') {
        let requests: Vec<ExportMetricsServiceRequest> =
            serde_json::from_str(trimmed).context("failed to parse OTLP JSON array")?;
        return Ok(requests);
    } else if trimmed.starts_with('{') {
        if let Ok(req) = serde_json::from_str(trimmed) {
            return Ok(vec![req]);
        }
    }

    let mut requests = Vec::new();
    for (line_idx, line) in trimmed.lines().enumerate() {
        let line_str = line.trim();
        if line_str.is_empty() {
            continue;
        }
        let req: ExportMetricsServiceRequest = serde_json::from_str(line_str)
            .with_context(|| format!("failed to parse OTLP JSON on line {}", line_idx + 1))?;
        requests.push(req);
    }

    if requests.is_empty() {
        anyhow::bail!("No valid OTLP requests found in input");
    }

    Ok(requests)
}

/// Helper to save OTLP ExportMetricsServiceRequest payloads as a pretty-printed JSON array.
fn save_otlp_payloads(
    path: impl AsRef<std::path::Path>,
    payloads: &[ExportMetricsServiceRequest],
) -> Result<()> {
    let file = File::create(path.as_ref()).context("failed to create output file")?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, payloads).context("failed to serialize OTLP JSON")?;
    use std::io::Write;
    writeln!(writer).context("failed to append trailing newline")?;
    Ok(())
}

/// Helper to parse proto attributes to a sorted BTreeMap.
fn parse_attributes(
    proto_attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for kv in proto_attrs {
        if let Some(ref av) = kv.value {
            if let Some(ref val) = av.value {
                let val_str = match val {
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s) => {
                        s.clone()
                    }
                    opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b) => {
                        b.to_string()
                    }
                    opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i) => {
                        i.to_string()
                    }
                    opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d) => {
                        d.to_string()
                    }
                    _ => "complex_value".to_string(),
                };
                map.insert(kv.key.clone(), val_str);
            }
        }
    }
    map
}

/// Helper to extract numerical value from NumberDataPoint.
fn extract_number_value(dp: &opentelemetry_proto::tonic::metrics::v1::NumberDataPoint) -> f64 {
    if let Some(ref val) = dp.value {
        match val {
            opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(d) => *d,
            opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(i) => {
                *i as f64
            }
        }
    } else {
        0.0
    }
}

/// Traverse an OTLP requests list and flatten all scalar/histogram metrics into a flat map.
fn extract_values(payloads: &[ExportMetricsServiceRequest]) -> BTreeMap<DataPointKey, f64> {
    let mut map = BTreeMap::new();
    for req in payloads {
        for rm in &req.resource_metrics {
            for sm in &rm.scope_metrics {
                for metric in &sm.metrics {
                    let name = &metric.name;
                    if let Some(ref data) = metric.data {
                        match data {
                            opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(gauge) => {
                                for dp in &gauge.data_points {
                                    let key = DataPointKey {
                                        metric_name: name.clone(),
                                        attributes: parse_attributes(&dp.attributes),
                                    };
                                    let val = extract_number_value(dp);
                                    map.insert(key, val);
                                }
                            }
                            opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum) => {
                                for dp in &sum.data_points {
                                    let key = DataPointKey {
                                        metric_name: name.clone(),
                                        attributes: parse_attributes(&dp.attributes),
                                    };
                                    let val = extract_number_value(dp);
                                    map.insert(key, val);
                                }
                            }
                            opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(
                                hist,
                            ) => {
                                for dp in &hist.data_points {
                                    let attrs = parse_attributes(&dp.attributes);

                                    // 1. Export sum
                                    let sum_key = DataPointKey {
                                        metric_name: format!("{}.sum", name),
                                        attributes: attrs.clone(),
                                    };
                                    map.insert(sum_key, dp.sum.unwrap_or(0.0));

                                    // 2. Export count
                                    let count_key = DataPointKey {
                                        metric_name: format!("{}.count", name),
                                        attributes: attrs.clone(),
                                    };
                                    map.insert(count_key, dp.count as f64);

                                    // 3. Export buckets
                                    for (bucket_idx, b_count) in dp.bucket_counts.iter().enumerate()
                                    {
                                        let bucket_key = DataPointKey {
                                            metric_name: format!("{}.bucket_{}", name, bucket_idx),
                                            attributes: attrs.clone(),
                                        };
                                        map.insert(bucket_key, *b_count as f64);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }
    map
}

/// Computes and prints the Markdown DP utility report comparing baseline vs simulated runs.
fn print_utility_report(
    runs: usize,
    baseline_values: &BTreeMap<DataPointKey, f64>,
    run_values_list: &[BTreeMap<DataPointKey, f64>],
) {
    println!("\n## Differential Privacy Simulation Utility Report");
    println!("Number of Runs: {}\n", runs);
    println!(
        "| Metric Name | Attributes | Baseline | Noised Mean (\u{03BC}) | Noised StdDev (\u{03C3}) | Mean Absolute Error (MAE) | Mean Relative Error (MRE) |"
    );
    println!("| :--- | :--- | :---: | :---: | :---: | :---: | :---: |");

    for (key, baseline_val) in baseline_values {
        let mut vals = Vec::new();
        for run_values in run_values_list {
            if let Some(val) = run_values.get(key) {
                vals.push(*val);
            } else {
                vals.push(0.0);
            }
        }

        // Calculate statistics
        let sum: f64 = vals.iter().sum();
        let mean = sum / (runs as f64);

        let variance: f64 = vals.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (runs as f64);
        let stddev = variance.sqrt();

        let mae: f64 = vals.iter().map(|x| (x - baseline_val).abs()).sum::<f64>() / (runs as f64);
        let mre = if baseline_val.abs() > 1e-9 { mae / baseline_val.abs() } else { f64::NAN };

        // Format attributes nicely
        let attr_str = key
            .attributes
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(",");

        println!(
            "| `{}` | `{}` | `{:.4}` | `{:.4}` | `{:.4}` | `{:.4}` | `{:.2}%` |",
            key.metric_name,
            attr_str,
            baseline_val,
            mean,
            stddev,
            mae,
            mre * 100.0
        );
    }
    println!();
}

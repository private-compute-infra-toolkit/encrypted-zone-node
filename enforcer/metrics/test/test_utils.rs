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

use fake_opentelemetry_collector::ExportedMetric;
use std::collections::HashMap;

pub struct MetricsVerifier {
    batches: Vec<ExportedMetric>,
}

impl MetricsVerifier {
    pub fn new(batches: Vec<ExportedMetric>) -> Self {
        Self { batches }
    }

    /// Extracts histogram data points (count, attributes) for a given metric name.
    pub fn get_histogram_points(&self, name: &str) -> Vec<(u64, HashMap<String, String>)> {
        let mut points = Vec::new();
        // Iterate over all batches and all metrics
        for batch in &self.batches {
            for metric in &batch.metrics {
                if metric.name != name {
                    continue;
                }

                let json = serde_json::to_value(metric).unwrap();
                let Some(data_points) =
                    json.pointer("/data/Histogram/data_points").and_then(|v| v.as_array())
                else {
                    continue;
                };

                for point in data_points {
                    let count = point.pointer("/count").and_then(|v| v.as_u64());
                    let attributes = point.pointer("/attributes").and_then(|v| v.as_object());

                    let (Some(cnt), Some(attrs)) = (count, attributes) else {
                        continue;
                    };

                    let mut attr_map = HashMap::new();
                    for (k, v) in attrs {
                        if let Some(s) = v.as_str() {
                            attr_map.insert(k.clone(), s.to_string());
                        }
                    }
                    points.push((cnt, attr_map));
                }
            }
        }
        points
    }
}

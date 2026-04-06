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

    /// Finds the given metric name across all batches and returns a list of its extracted values and attributes.
    /// Input: name (metric name to search for)
    /// Output: List of extracted values and their associated attributes
    /// Example: get_counter_points("my_metric") -> [(42, {"key": "value"})]
    pub fn get_counter_points(&self, name: &str) -> Vec<(u64, HashMap<String, String>)> {
        self.batches
            .iter()
            .flat_map(|batch| &batch.metrics)
            .filter(|metric| metric.name == name)
            .flat_map(|metric| {
                serde_json::to_value(metric)
                    .ok()
                    .and_then(|json| {
                        json.pointer("/data/Sum/data_points")
                            .and_then(|v| v.as_array())
                            .map(|arr| arr.to_vec())
                    })
                    .map(|data_points| {
                        data_points
                            .iter()
                            .filter_map(Self::process_counter_data_point)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    /// Finds the given gauge metric name across all batches and returns a list of its extracted values and attributes.
    /// Input: name (metric name to search for), is_float (whether to extract float or int)
    /// Output: List of extracted values (as f64) and their associated attributes
    /// Example: get_gauge_points("my_metric", false) -> [(42.0, {"key": "value"})]
    pub fn get_gauge_points(
        &self,
        name: &str,
        is_float: bool,
    ) -> Vec<(f64, HashMap<String, String>)> {
        let mut points = Vec::new();
        for batch in &self.batches {
            for metric in &batch.metrics {
                if metric.name != name {
                    continue;
                }
                let json = serde_json::to_value(metric).unwrap();

                // Try to find data points in Gauge, gauge, Sum, sum
                let data_points = json
                    .pointer("/data/Gauge/data_points")
                    .or_else(|| json.pointer("/data/gauge/data_points"))
                    .or_else(|| json.pointer("/data/Sum/data_points"))
                    .or_else(|| json.pointer("/data/sum/data_points"))
                    .and_then(|v| v.as_array());

                if let Some(data_points) = data_points {
                    for point in data_points {
                        if let Some(p) = Self::process_gauge_data_point(point, is_float) {
                            points.push(p);
                        }
                    }
                }
            }
        }
        points
    }

    /// Helper to extract value and attributes from a single gauge data point.
    /// Input: point (JSON value representing a single gauge data point), is_float
    /// Output: The value (as f64) and a map of its attributes, if present
    /// Example: {"value": {"AsInt": 2}, "attributes": {"key": {"StringValue": "val"}}} -> Some((2.0, {"key": "val"}))
    fn process_gauge_data_point(
        point: &serde_json::Value,
        is_float: bool,
    ) -> Option<(f64, HashMap<String, String>)> {
        let val = if is_float {
            point
                .get("as_double")
                .and_then(|v| v.as_f64())
                .or_else(|| point.pointer("/value/as_double").and_then(|v| v.as_f64()))
                .or_else(|| point.pointer("/value/AsDouble").and_then(|v| v.as_f64()))
                .or_else(|| point.get("AsDouble").and_then(|v| v.as_f64()))
        } else {
            point
                .get("as_int")
                .and_then(|v| v.as_i64())
                .or_else(|| point.pointer("/value/as_int").and_then(|v| v.as_i64()))
                .or_else(|| point.pointer("/value/AsInt").and_then(|v| v.as_i64()))
                .or_else(|| point.get("AsInt").and_then(|v| v.as_i64()))
                .map(|v| v as f64)
        };

        let attributes = point.get("attributes").and_then(|v| v.as_object());

        match (val, attributes) {
            (Some(v), Some(attrs)) => Some((v, Self::parse_metric_attributes(attrs))),
            (Some(v), None) => Some((v, HashMap::new())),
            _ => None,
        }
    }

    /// Helper to parse metric attributes from a JSON object.
    /// Input: attrs (JSON object representing metric attributes)
    /// Output: Map of parsed string attribute key-value pairs
    /// Example: {"method": {"StringValue": "ez_call"}} -> {"method": "ez_call"}
    fn parse_metric_attributes(
        attrs: &serde_json::Map<String, serde_json::Value>,
    ) -> HashMap<String, String> {
        let mut attr_map = HashMap::new();
        for (k, v) in attrs {
            if let Some(s) = v.as_str() {
                // Check for "Some(AnyValue { value: Some(StringValue(\"test_method\")) })"
                if let Some(val) =
                    s.split("StringValue(\"").nth(1).and_then(|v| v.split("\")").next())
                {
                    attr_map.insert(k.clone(), val.to_string());
                } else if let Some(stripped) =
                    s.strip_prefix("StringValue(\"").and_then(|s| s.strip_suffix("\")"))
                {
                    attr_map.insert(k.clone(), stripped.to_string());
                } else if let Some(stripped) =
                    s.strip_prefix("StringValue(\"").and_then(|s| s.strip_suffix("\""))
                {
                    attr_map.insert(k.clone(), stripped.to_string());
                } else {
                    attr_map.insert(k.clone(), s.to_string());
                }
            } else if let Some(obj) = v.as_object() {
                // sometimes the structure is {"StringValue": "foo"}
                if let Some(s) = obj.get("StringValue").and_then(|sv| sv.as_str()) {
                    attr_map.insert(k.clone(), s.to_string());
                } else if let Some(s) = obj.get("stringValue").and_then(|sv| sv.as_str()) {
                    attr_map.insert(k.clone(), s.to_string());
                }
            }
        }
        attr_map
    }

    /// Helper to extract value and attributes from a single counter data point.
    /// Input: point (JSON value representing a single counter data point)
    /// Output: The integer value and a map of its attributes, if present
    /// Example: {"value": {"AsInt": 1}, "attributes": {"key": {"StringValue": "val"}}} -> Some((1, {"key": "val"}))
    fn process_counter_data_point(
        point: &serde_json::Value,
    ) -> Option<(u64, HashMap<String, String>)> {
        let value = point.pointer("/value/AsInt").and_then(|v| v.as_u64());
        let attributes = point.pointer("/attributes").and_then(|v| v.as_object());

        match (value, attributes) {
            (Some(val), Some(attrs)) => Some((val, Self::parse_metric_attributes(attrs))),
            _ => None,
        }
    }
}

use metrics::common::ServiceMetrics;
use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};

#[derive(Clone, Default)]
pub struct TestMetrics {
    errors: Option<Counter<u64>>,
    active_requests: Option<UpDownCounter<i64>>,
    duration_sec: Option<Histogram<f64>>,
    message_processing_duration: Option<Histogram<f64>>,
    message_size_bytes: Option<Histogram<u64>>,
}

impl TestMetrics {
    pub fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            errors: Some(meter.u64_counter("test.errors").build()),
            active_requests: Some(meter.i64_up_down_counter("test.active_requests").build()),
            duration_sec: Some(meter.f64_histogram("test.duration").build()),
            message_processing_duration: Some(
                meter.f64_histogram("test.message_processing_duration").build(),
            ),
            message_size_bytes: Some(meter.u64_histogram("test.message_size_bytes").build()),
        }
    }
}

impl ServiceMetrics for TestMetrics {
    fn errors(&self) -> Option<&Counter<u64>> {
        self.errors.as_ref()
    }

    fn active_requests(&self) -> Option<&UpDownCounter<i64>> {
        self.active_requests.as_ref()
    }

    fn duration_sec(&self) -> Option<&Histogram<f64>> {
        self.duration_sec.as_ref()
    }

    fn message_processing_duration(&self) -> Option<&Histogram<f64>> {
        self.message_processing_duration.as_ref()
    }

    fn message_size_bytes(&self) -> Option<&Histogram<u64>> {
        self.message_size_bytes.as_ref()
    }
}

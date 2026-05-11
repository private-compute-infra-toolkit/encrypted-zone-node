// Copyright 2025 Google LLC
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

use manifest_proto::enforcer::v1::{
    allowed_metric::MetricType, AllowedMetric, IsolateMetricsPolicy,
};
use metrics::receiver::IsolateMetricsReceiver;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value::Value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    metric::Data, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};

#[tokio::test]
async fn test_filter_metrics_and_inject_attributes() {
    let policy = IsolateMetricsPolicy {
        allowed_metrics: vec![AllowedMetric {
            name: "allowed_gauge".to_string(),
            r#type: MetricType::Gauge as i32,
            allowed_attributes: vec!["allowed_attr".to_string()],
        }],
    };

    let receiver = IsolateMetricsReceiver::new(
        policy,
        "test-isolate".to_string(),
        "test-publisher".to_string(),
        None,
        4 * 1024 * 1024,
    )
    .await
    .unwrap();

    let mut request = create_test_request(vec![
        create_test_metric("allowed_gauge", vec![("allowed_attr", "val1")]),
        create_test_metric("forbidden_gauge", vec![]),
    ]);

    receiver.filter_metrics(&mut request);

    assert_eq!(request.resource_metrics.len(), 1);
    let rm = &request.resource_metrics[0];

    // Verify resource attributes
    let resource_attrs = &rm.resource.as_ref().unwrap().attributes;
    assert!(resource_attrs.iter().any(|kv| kv.key == "source"
        && kv.value.as_ref().unwrap().value == Some(Value::StringValue("isolate".to_string()))));
    assert!(resource_attrs.iter().any(|kv| kv.key == "ez_isolate_name"
        && kv.value.as_ref().unwrap().value
            == Some(Value::StringValue("test-isolate".to_string()))));
    assert!(resource_attrs.iter().any(|kv| kv.key == "ez_publisher_id"
        && kv.value.as_ref().unwrap().value
            == Some(Value::StringValue("test-publisher".to_string()))));

    assert_eq!(rm.scope_metrics.len(), 1);
    let sm = &rm.scope_metrics[0];

    // Verify that forbidden_gauge was removed
    assert_eq!(sm.metrics.len(), 1);
    assert_eq!(sm.metrics[0].name, "allowed_gauge");

    // Verify that attributes are preserved
    if let Some(Data::Gauge(gauge)) = &sm.metrics[0].data {
        assert_eq!(gauge.data_points.len(), 1);
        let dp = &gauge.data_points[0];
        assert_eq!(dp.attributes.len(), 1);
        assert_eq!(dp.attributes[0].key, "allowed_attr");
    } else {
        panic!("Expected Gauge data");
    }
}

fn create_test_request(metrics: Vec<Metric>) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Default::default()),
            scope_metrics: vec![ScopeMetrics { scope: None, metrics, schema_url: "".to_string() }],
            schema_url: "".to_string(),
        }],
    }
}

fn create_test_metric(name: &str, attrs_vec: Vec<(&str, &str)>) -> Metric {
    let mut attributes = Vec::new();
    for (key, val) in attrs_vec {
        attributes.push(KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value: Some(Value::StringValue(val.to_string())) }),
        });
    }
    Metric {
        name: name.to_string(),
        data: Some(Data::Gauge(Gauge {
            data_points: vec![NumberDataPoint { attributes, ..Default::default() }],
        })),
        ..Default::default()
    }
}

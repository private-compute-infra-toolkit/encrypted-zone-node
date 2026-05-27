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
use opentelemetry_proto::tonic::common::v1::{
    any_value::Value, AnyValue, ArrayValue, KeyValue, KeyValueList,
};
use opentelemetry_proto::tonic::metrics::v1::{
    metric::Data, number_data_point::Value as NumValue, ExponentialHistogram,
    ExponentialHistogramDataPoint, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint,
    ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint,
};
use opentelemetry_proto::tonic::resource::v1::Resource;

#[tokio::test]
async fn test_filter_metrics() {
    let policy = IsolateMetricsPolicy {
        allowed_metrics: vec![AllowedMetric {
            name: "allowed_gauge".to_string(),
            r#type: MetricType::Gauge as i32,
            allowed_attributes: vec!["allowed_attr".to_string()],
        }],
    };

    let receiver = create_test_receiver(policy).await;

    let mut request = create_test_request(vec![
        create_test_metric("allowed_gauge", vec![("allowed_attr", "val1")]),
        create_test_metric("forbidden_gauge", vec![]),
    ]);

    receiver.filter_metrics(&mut request);

    assert_eq!(request.resource_metrics.len(), 1);
    let rm = &request.resource_metrics[0];
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
        assert!(dp.attributes.iter().any(|kv| kv.key == "allowed_attr"
            && kv.value.as_ref().unwrap().value == Some(Value::StringValue("val1".to_string()))));
    } else {
        panic!("Expected Gauge data");
    }
}

#[tokio::test]
async fn test_enrich_metrics() {
    let policy = IsolateMetricsPolicy::default();
    let receiver = create_test_receiver(policy).await;

    let mut request = create_test_request(vec![create_test_metric(
        "allowed_gauge",
        vec![("allowed_attr", "val1")],
    )]);

    receiver.enrich_metrics(&mut request);

    assert_eq!(request.resource_metrics.len(), 1);
    let rm = &request.resource_metrics[0];

    // Verify resource attributes
    let resource_attrs = &rm.resource.as_ref().unwrap().attributes;
    assert!(resource_attrs.iter().any(|kv| kv.key == "ez_component_name"
        && kv.value.as_ref().unwrap().value == Some(Value::StringValue("isolate".to_string()))));
    assert!(resource_attrs.iter().any(|kv| kv.key == "ez_isolate_name"
        && kv.value.as_ref().unwrap().value
            == Some(Value::StringValue("test-isolate".to_string()))));
    assert!(resource_attrs.iter().any(|kv| kv.key == "ez_publisher_id"
        && kv.value.as_ref().unwrap().value
            == Some(Value::StringValue("test-publisher".to_string()))));
    assert!(resource_attrs.iter().any(|kv| kv.key == "ez_isolate_type"
        && kv.value.as_ref().unwrap().value == Some(Value::StringValue("opaque".to_string()))));
    assert!(resource_attrs.iter().any(|kv| kv.key == "ez_safety_level"
        && kv.value.as_ref().unwrap().value == Some(Value::StringValue("safe".to_string()))));
    assert!(resource_attrs
        .iter()
        .any(|kv| kv.key == "ez_enforcer_version" && kv.value.as_ref().unwrap().value.is_some()));
}

#[tokio::test]
async fn test_filter_metrics_prefix_matching() {
    let policy = IsolateMetricsPolicy {
        allowed_metrics: vec![
            AllowedMetric {
                name: "/experiments/framework/*".to_string(),
                r#type: MetricType::Gauge as i32,
                allowed_attributes: vec![],
            },
            AllowedMetric {
                name: "exact_gauge".to_string(),
                r#type: MetricType::Gauge as i32,
                allowed_attributes: vec![],
            },
        ],
    };

    let receiver = create_test_receiver(policy).await;

    let mut request = create_test_request(vec![
        create_test_metric("/experiments/framework/count1", vec![]),
        create_test_metric("/experiments/framework/count2", vec![]),
        create_test_metric("exact_gauge", vec![]),
        create_test_metric("/experiments/count", vec![]),
        create_test_metric("forbidden_gauge", vec![]),
    ]);

    receiver.filter_metrics(&mut request);

    assert_eq!(request.resource_metrics.len(), 1);
    let rm = &request.resource_metrics[0];
    assert_eq!(rm.scope_metrics.len(), 1);
    let sm = &rm.scope_metrics[0];

    // verify that exactly 3 metrics were allowed
    assert_eq!(sm.metrics.len(), 3);

    let allowed_names: Vec<&str> = sm.metrics.iter().map(|m| m.name.as_str()).collect();
    assert!(allowed_names.contains(&"/experiments/framework/count1"));
    assert!(allowed_names.contains(&"/experiments/framework/count2"));
    assert!(allowed_names.contains(&"exact_gauge"));
}

#[tokio::test]
async fn test_filter_metrics_multiple_prefix_types() {
    let policy = IsolateMetricsPolicy {
        allowed_metrics: vec![
            AllowedMetric {
                name: "/rpc/server/*".to_string(),
                r#type: MetricType::Gauge as i32,
                allowed_attributes: vec![],
            },
            AllowedMetric {
                name: "/rpc/server/*".to_string(),
                r#type: MetricType::Sum as i32,
                allowed_attributes: vec![],
            },
        ],
    };

    let receiver = create_test_receiver(policy).await;

    let mut request = create_test_request(vec![
        create_test_metric("/rpc/server/active_conns", vec![]),
        create_test_sum_metric("/rpc/server/total_requests", vec![]),
        create_test_metric("some_other_metric", vec![]),
        create_test_sum_metric("/rpc/server/incoming/requests", vec![]),
        create_test_sum_metric("/rpc/totals", vec![]),
    ]);

    receiver.filter_metrics(&mut request);

    assert_eq!(request.resource_metrics.len(), 1);
    let rm = &request.resource_metrics[0];
    assert_eq!(rm.scope_metrics.len(), 1);
    let sm = &rm.scope_metrics[0];

    // verify that exactly 3 metrics were allowed
    assert_eq!(sm.metrics.len(), 3);

    let allowed_names: Vec<&str> = sm.metrics.iter().map(|m| m.name.as_str()).collect();
    assert!(allowed_names.contains(&"/rpc/server/active_conns"));
    assert!(allowed_names.contains(&"/rpc/server/total_requests"));
    assert!(allowed_names.contains(&"/rpc/server/incoming/requests"));
}

#[tokio::test]
async fn test_filter_metrics_coverage_various_types() {
    let policy = IsolateMetricsPolicy {
        allowed_metrics: vec![
            AllowedMetric {
                name: "test_hist".to_string(),
                r#type: MetricType::Histogram as i32,
                allowed_attributes: vec!["allowed".to_string()],
            },
            AllowedMetric {
                name: "test_exphist".to_string(),
                r#type: MetricType::ExponentialHistogram as i32,
                allowed_attributes: vec!["allowed".to_string()],
            },
            AllowedMetric {
                name: "test_summary".to_string(),
                r#type: MetricType::Summary as i32,
                allowed_attributes: vec!["allowed".to_string()],
            },
        ],
    };

    let receiver = create_test_receiver(policy).await;

    let mut request = create_test_request(vec![
        // 1. Histogram metric
        Metric {
            name: "test_hist".to_string(),
            data: Some(Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    attributes: vec![
                        KeyValue {
                            key: "allowed".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("val".to_string())),
                            }),
                        },
                        KeyValue {
                            key: "forbidden".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("val".to_string())),
                            }),
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            })),
            ..Default::default()
        },
        // 2. Exponential Histogram metric
        Metric {
            name: "test_exphist".to_string(),
            data: Some(Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    attributes: vec![
                        KeyValue {
                            key: "allowed".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("val".to_string())),
                            }),
                        },
                        KeyValue {
                            key: "forbidden".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("val".to_string())),
                            }),
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            })),
            ..Default::default()
        },
        // 3. Summary metric
        Metric {
            name: "test_summary".to_string(),
            data: Some(Data::Summary(Summary {
                data_points: vec![SummaryDataPoint {
                    attributes: vec![
                        KeyValue {
                            key: "allowed".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("val".to_string())),
                            }),
                        },
                        KeyValue {
                            key: "forbidden".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("val".to_string())),
                            }),
                        },
                    ],
                    ..Default::default()
                }],
            })),
            ..Default::default()
        },
    ]);

    receiver.filter_metrics(&mut request);

    let rm = &request.resource_metrics[0];
    let sm = &rm.scope_metrics[0];
    assert_eq!(sm.metrics.len(), 3);

    // Verify all three are correctly sanitized
    for m in &sm.metrics {
        let attrs = match m.data.as_ref().unwrap() {
            Data::Histogram(h) => &h.data_points[0].attributes,
            Data::ExponentialHistogram(eh) => &eh.data_points[0].attributes,
            Data::Summary(s) => &s.data_points[0].attributes,
            _ => panic!("Unexpected type"),
        };
        assert_eq!(attrs.len(), 2);
        let attr_allowed = attrs.iter().find(|kv| kv.key == "allowed").unwrap();
        assert_eq!(
            attr_allowed.value.as_ref().unwrap().value,
            Some(Value::StringValue("val".to_string()))
        );
        let attr_forbidden = attrs.iter().find(|kv| kv.key == "forbidden").unwrap();
        assert_eq!(
            attr_forbidden.value.as_ref().unwrap().value,
            Some(Value::StringValue("".to_string()))
        );
    }
}

#[tokio::test]
async fn test_filter_metrics_coverage_attribute_value_types() {
    let policy = IsolateMetricsPolicy {
        allowed_metrics: vec![AllowedMetric {
            name: "allowed_gauge".to_string(),
            r#type: MetricType::Gauge as i32,
            allowed_attributes: vec![],
        }],
    };

    let receiver = create_test_receiver(policy).await;

    let mut request = create_test_request(vec![Metric {
        name: "allowed_gauge".to_string(),
        data: Some(Data::Gauge(Gauge {
            data_points: vec![NumberDataPoint {
                attributes: vec![
                    KeyValue {
                        key: "bool_val".to_string(),
                        value: Some(AnyValue { value: Some(Value::BoolValue(true)) }),
                    },
                    KeyValue {
                        key: "int_val".to_string(),
                        value: Some(AnyValue { value: Some(Value::IntValue(42)) }),
                    },
                    KeyValue {
                        key: "double_val".to_string(),
                        value: Some(AnyValue { value: Some(Value::DoubleValue(1.23)) }),
                    },
                    KeyValue {
                        key: "array_val".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::ArrayValue(ArrayValue {
                                values: vec![AnyValue { value: Some(Value::BoolValue(true)) }],
                            })),
                        }),
                    },
                    KeyValue {
                        key: "kvlist_val".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::KvlistValue(KeyValueList {
                                values: vec![KeyValue {
                                    key: "k".to_string(),
                                    value: Some(AnyValue { value: Some(Value::BoolValue(true)) }),
                                }],
                            })),
                        }),
                    },
                    KeyValue {
                        key: "bytes_val".to_string(),
                        value: Some(AnyValue { value: Some(Value::BytesValue(vec![1, 2, 3])) }),
                    },
                ],
                ..Default::default()
            }],
        })),
        ..Default::default()
    }]);

    receiver.filter_metrics(&mut request);

    let rm = &request.resource_metrics[0];
    let sm = &rm.scope_metrics[0];
    let m = &sm.metrics[0];

    if let Some(Data::Gauge(g)) = &m.data {
        let dp = &g.data_points[0];
        assert_eq!(dp.attributes.len(), 6);

        let bool_attr = dp.attributes.iter().find(|kv| kv.key == "bool_val").unwrap();
        assert_eq!(bool_attr.value.as_ref().unwrap().value, Some(Value::BoolValue(false)));

        let int_attr = dp.attributes.iter().find(|kv| kv.key == "int_val").unwrap();
        assert_eq!(int_attr.value.as_ref().unwrap().value, Some(Value::IntValue(0)));

        let double_attr = dp.attributes.iter().find(|kv| kv.key == "double_val").unwrap();
        assert_eq!(double_attr.value.as_ref().unwrap().value, Some(Value::DoubleValue(0.0)));

        let array_attr = dp.attributes.iter().find(|kv| kv.key == "array_val").unwrap();
        if let Some(Value::ArrayValue(arr)) = &array_attr.value.as_ref().unwrap().value {
            assert_eq!(arr.values.len(), 0);
        } else {
            panic!("Expected ArrayValue");
        }

        let kvlist_attr = dp.attributes.iter().find(|kv| kv.key == "kvlist_val").unwrap();
        if let Some(Value::KvlistValue(list)) = &kvlist_attr.value.as_ref().unwrap().value {
            assert_eq!(list.values.len(), 0);
        } else {
            panic!("Expected KvlistValue");
        }

        let bytes_attr = dp.attributes.iter().find(|kv| kv.key == "bytes_val").unwrap();
        if let Some(Value::BytesValue(bytes)) = &bytes_attr.value.as_ref().unwrap().value {
            assert_eq!(bytes.len(), 0);
        } else {
            panic!("Expected BytesValue");
        }
    }
}

#[tokio::test]
async fn test_filter_metrics_coverage_disable_filtering_and_purging() {
    // 1. Test disable_filtering = true early return
    let policy = IsolateMetricsPolicy {
        allowed_metrics: vec![AllowedMetric {
            name: "test_metric".to_string(),
            r#type: MetricType::Gauge as i32,
            allowed_attributes: vec![],
        }],
    };
    let receiver_disabled = IsolateMetricsReceiver::new(
        policy.clone(),
        "test-isolate".to_string(),
        "test-publisher".to_string(),
        false, // is_ratified
        None,
        4 * 1024 * 1024,
        true, // disable_filtering = true
    )
    .await
    .unwrap();

    let mut request_disabled =
        create_test_request(vec![create_test_metric("forbidden_metric", vec![])]);
    receiver_disabled.filter_metrics(&mut request_disabled);
    // Verify that forbidden_metric was NOT dropped (early return worked!)
    let rm_disabled = &request_disabled.resource_metrics[0];
    let sm_disabled = &rm_disabled.scope_metrics[0];
    assert_eq!(sm_disabled.metrics.len(), 1);
    assert_eq!(sm_disabled.metrics[0].name, "forbidden_metric");

    // 2. Test pre-existing identity attributes purging in resource
    let receiver_enabled = create_test_receiver(policy).await;
    let mut request_purged = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![
                    KeyValue {
                        key: "ez_isolate_name".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("malicious-isolate".to_string())),
                        }),
                    },
                    KeyValue {
                        key: "custom_resource_attr".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("keep-me".to_string())),
                        }),
                    },
                ],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![create_test_metric("test_metric", vec![])],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        }],
    };

    receiver_enabled.enrich_metrics(&mut request_purged);

    let rm_purged = &request_purged.resource_metrics[0];
    let resource_attrs = &rm_purged.resource.as_ref().unwrap().attributes;
    // Should have 7 attributes: ez_component_name, ez_isolate_name, ez_publisher_id, ez_isolate_type, ez_safety_level, ez_enforcer_version, and custom_resource_attr
    assert_eq!(resource_attrs.len(), 7);

    // Verify custom_resource_attr was retained
    let custom_attr = resource_attrs.iter().find(|kv| kv.key == "custom_resource_attr").unwrap();
    assert_eq!(
        custom_attr.value.as_ref().unwrap().value,
        Some(Value::StringValue("keep-me".to_string()))
    );

    // Verify ez_isolate_name was successfully purged and overwritten with the trusted one
    let identity_attr = resource_attrs.iter().find(|kv| kv.key == "ez_isolate_name").unwrap();
    assert_eq!(
        identity_attr.value.as_ref().unwrap().value,
        Some(Value::StringValue("test-isolate".to_string()))
    );
}

#[tokio::test]
async fn test_filter_metrics_coverage_uds_channel_pool_initialization() {
    let unique_id =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let uds_path = std::env::temp_dir().join(format!("test-coverage-uds-{}.sock", unique_id));

    // Bind to the Unix socket in background to let the GrpcChannelPool connect successfully
    let listener = tokio::net::UnixListener::bind(&uds_path).unwrap();
    let _server_task = tokio::spawn(async move {
        let _ = listener.accept().await;
    });

    let policy = IsolateMetricsPolicy { allowed_metrics: vec![] };
    let otel_endpoint = format!("unix:{}", uds_path.display());

    let receiver = IsolateMetricsReceiver::new(
        policy,
        "test-isolate".to_string(),
        "test-publisher".to_string(),
        false,               // is_ratified
        Some(otel_endpoint), // safe_endpoint = Some
        4 * 1024 * 1024,
        false,
    )
    .await;

    // Verify that it initialized successfully without returning an error
    assert!(receiver.is_ok(), "Failed to initialize metrics receiver with UDS endpoint");
}

#[tokio::test]
async fn test_filter_metrics_scalar_value_retention() {
    let policy = IsolateMetricsPolicy {
        allowed_metrics: vec![AllowedMetric {
            name: "scalar_metric".to_string(),
            r#type: MetricType::Gauge as i32,
            allowed_attributes: vec![],
        }],
    };

    let receiver = create_test_receiver(policy).await;

    let mut request = create_test_request(vec![
        // Scalar metric: has value, but NO attributes
        Metric {
            name: "scalar_metric".to_string(),
            data: Some(Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![],
                    value: Some(NumValue::AsInt(100)),
                    ..Default::default()
                }],
            })),
            ..Default::default()
        },
    ]);

    receiver.filter_metrics(&mut request);

    assert_eq!(request.resource_metrics.len(), 1);
    let rm = &request.resource_metrics[0];
    assert_eq!(rm.scope_metrics.len(), 1);
    let sm = &rm.scope_metrics[0];

    assert_eq!(sm.metrics.len(), 1);

    // Verify scalar_metric value is retained and attributes remain empty
    let m1 = sm.metrics.iter().find(|m| m.name == "scalar_metric").unwrap();
    if let Some(Data::Gauge(gauge)) = &m1.data {
        let dp = &gauge.data_points[0];
        assert_eq!(dp.attributes.len(), 0);
        assert_eq!(dp.value, Some(NumValue::AsInt(100)));
    } else {
        panic!("Expected Gauge");
    }
}

#[tokio::test]
async fn test_filter_metrics_empty_allowlist_anonymization() {
    let policy = IsolateMetricsPolicy {
        allowed_metrics: vec![AllowedMetric {
            name: "metric_with_empty_allowlist".to_string(),
            r#type: MetricType::Gauge as i32,
            allowed_attributes: vec![],
        }],
    };

    let receiver = create_test_receiver(policy).await;

    let mut request = create_test_request(vec![
        // Metric with attributes, but empty allowlist in policy
        create_test_metric(
            "metric_with_empty_allowlist",
            vec![("sensitive_id", "12345"), ("user_ip", "1.2.3.4")],
        ),
    ]);

    receiver.filter_metrics(&mut request);

    assert_eq!(request.resource_metrics.len(), 1);
    let rm = &request.resource_metrics[0];
    assert_eq!(rm.scope_metrics.len(), 1);
    let sm = &rm.scope_metrics[0];

    assert_eq!(sm.metrics.len(), 1);

    // Verify metric_with_empty_allowlist has all attributes anonymized/redacted to defaults (not dropped)
    let m2 = sm.metrics.iter().find(|m| m.name == "metric_with_empty_allowlist").unwrap();
    if let Some(Data::Gauge(gauge)) = &m2.data {
        let dp = &gauge.data_points[0];
        assert_eq!(dp.attributes.len(), 2);

        let attr1 = dp.attributes.iter().find(|kv| kv.key == "sensitive_id").unwrap();
        assert_eq!(attr1.value.as_ref().unwrap().value, Some(Value::StringValue("".to_string())));

        let attr2 = dp.attributes.iter().find(|kv| kv.key == "user_ip").unwrap();
        assert_eq!(attr2.value.as_ref().unwrap().value, Some(Value::StringValue("".to_string())));
    } else {
        panic!("Expected Gauge");
    }
}

async fn create_test_receiver(policy: IsolateMetricsPolicy) -> IsolateMetricsReceiver {
    IsolateMetricsReceiver::new(
        policy,
        "test-isolate".to_string(),
        "test-publisher".to_string(),
        false, // is_ratified
        None,
        4 * 1024 * 1024,
        false,
    )
    .await
    .unwrap()
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

fn create_test_sum_metric(name: &str, attrs_vec: Vec<(&str, &str)>) -> Metric {
    let mut attributes = Vec::new();
    for (key, val) in attrs_vec {
        attributes.push(KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value: Some(Value::StringValue(val.to_string())) }),
        });
    }
    Metric {
        name: name.to_string(),
        data: Some(Data::Sum(Sum {
            data_points: vec![NumberDataPoint { attributes, ..Default::default() }],
            ..Default::default()
        })),
        ..Default::default()
    }
}

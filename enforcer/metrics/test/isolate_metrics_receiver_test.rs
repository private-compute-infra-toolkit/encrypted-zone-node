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

use ez_error_trait::ToEzError;
use isolate_info::InstanceIdGenerator;
use manifest_proto::enforcer::v1::{
    allowed_metric::MetricType, AllowedMetric, IsolateMetricsPolicy,
};
use metrics::isolate_metrics_receiver::{
    AttributeName, CustomAttribute, IsolateMetricsReceiver, IsolateMetricsReceiverConfig,
    MetricsReceiverError,
};
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
    let receiver = create_test_receiver_with_instance_id(policy, "1".to_string()).await;

    let mut request = create_test_request(vec![create_test_metric(
        "allowed_gauge",
        vec![("allowed_attr", "val1")],
    )]);

    receiver.enrich_metrics(&mut request);

    assert_eq!(request.resource_metrics.len(), 1);
    let rm = &request.resource_metrics[0];

    // Verify resource attributes are enriched with the receiver's configured attributes
    let resource_attrs = &rm.resource.as_ref().unwrap().attributes;
    assert_resource_attributes_match(resource_attrs, &receiver.resource_attributes(), &[]);

    // Verify scope attributes match receiver's configured scope attributes (empty)
    let sm = &rm.scope_metrics[0];
    let scope = sm.scope.as_ref().unwrap();
    assert_eq!(scope.attributes, receiver.scope_attributes());

    // Verify datapoint attributes are unchanged
    let metric = &sm.metrics[0];
    if let Some(Data::Gauge(gauge)) = &metric.data {
        let dp = &gauge.data_points[0];
        assert_eq!(dp.attributes.len(), 1);
        assert!(dp.attributes.iter().any(|kv| kv.key == "allowed_attr"
            && kv.value.as_ref().unwrap().value == Some(Value::StringValue("val1".to_string()))));
    } else {
        panic!("Expected Gauge data");
    }
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

    // Verify all three are correctly sanitized (unauthorized attributes removed)
    for m in &sm.metrics {
        let attrs = match m.data.as_ref().unwrap() {
            Data::Histogram(h) => &h.data_points[0].attributes,
            Data::ExponentialHistogram(eh) => &eh.data_points[0].attributes,
            Data::Summary(s) => &s.data_points[0].attributes,
            _ => panic!("Unexpected type"),
        };
        assert_eq!(attrs.len(), 1);
        let attr_allowed = attrs.iter().find(|kv| kv.key == "allowed").unwrap();
        assert_eq!(
            attr_allowed.value.as_ref().unwrap().value,
            Some(Value::StringValue("val".to_string()))
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
        assert_eq!(dp.attributes.len(), 0);
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
    let receiver_disabled = IsolateMetricsReceiver::new(IsolateMetricsReceiverConfig {
        policy: policy.clone(),
        isolate_name: "test-isolate".to_string(),
        publisher_id: "test-publisher".to_string(),
        is_ratified: false,
        isolate_instance_id: InstanceIdGenerator::generate().into(),
        otel_endpoint: None,
        max_decoding_message_size: 4 * 1024 * 1024,
        disable_filtering: true,
        ..Default::default()
    })
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
    assert_resource_attributes_match(
        resource_attrs,
        &receiver_enabled.resource_attributes(),
        &[("custom_resource_attr", "keep-me")],
    );
    let sm = &rm_purged.scope_metrics[0];
    let scope = sm.scope.as_ref().unwrap();
    assert_eq!(scope.attributes, receiver_enabled.scope_attributes());
}

#[tokio::test]
async fn test_enrich_metrics_anti_spoofing_instance_id() {
    let policy = IsolateMetricsPolicy::default();
    let receiver = create_test_receiver_with_instance_id(policy, "1".to_string()).await;

    let mut request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![
                    KeyValue {
                        key: "ez_isolate_instance_id".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("malicious-instance-999".to_string())),
                        }),
                    },
                    KeyValue {
                        key: "custom_resource_tag".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("valid_tag".to_string())),
                        }),
                    },
                ],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                    attributes: vec![KeyValue {
                        key: "ez_isolate_instance_id".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("malicious-scope-instance".to_string())),
                        }),
                    }],
                    ..Default::default()
                }),
                metrics: vec![create_test_metric("test_metric", vec![])],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        }],
    };

    receiver.enrich_metrics(&mut request);

    let rm = &request.resource_metrics[0];
    let resource_attrs = &rm.resource.as_ref().unwrap().attributes;
    // Malicious instance id must be purged and replaced with verified instance id in resource
    assert_resource_attributes_match(
        resource_attrs,
        &receiver.resource_attributes(),
        &[("custom_resource_tag", "valid_tag")],
    );

    let sm = &rm.scope_metrics[0];
    let scope = sm.scope.as_ref().unwrap();
    assert_eq!(scope.attributes, receiver.scope_attributes());
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

    let receiver = IsolateMetricsReceiver::new(IsolateMetricsReceiverConfig {
        policy,
        isolate_name: "test-isolate".to_string(),
        publisher_id: "test-publisher".to_string(),
        is_ratified: false,
        isolate_instance_id: InstanceIdGenerator::generate().into(),
        otel_endpoint: Some(otel_endpoint),
        max_decoding_message_size: 4 * 1024 * 1024,
        disable_filtering: false,
        ..Default::default()
    })
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
async fn test_filter_metrics_empty_allowlist_removal() {
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

    // Verify metric_with_empty_allowlist has all attributes removed
    let m2 = sm.metrics.iter().find(|m| m.name == "metric_with_empty_allowlist").unwrap();
    if let Some(Data::Gauge(gauge)) = &m2.data {
        let dp = &gauge.data_points[0];
        assert_eq!(dp.attributes.len(), 0);
    } else {
        panic!("Expected Gauge");
    }
}

async fn create_test_receiver(policy: IsolateMetricsPolicy) -> IsolateMetricsReceiver {
    create_test_receiver_with_instance_id(policy, InstanceIdGenerator::generate().into()).await
}

async fn create_test_receiver_with_instance_id(
    policy: IsolateMetricsPolicy,
    isolate_instance_id: String,
) -> IsolateMetricsReceiver {
    IsolateMetricsReceiver::new(IsolateMetricsReceiverConfig {
        policy,
        isolate_name: "test-isolate".to_string(),
        publisher_id: "test-publisher".to_string(),
        is_ratified: false,
        isolate_instance_id,
        otel_endpoint: None,
        max_decoding_message_size: 4 * 1024 * 1024,
        disable_filtering: false,
        ..Default::default()
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn test_enrich_metrics_dynamic_instance_id_restart() {
    let policy = IsolateMetricsPolicy { allowed_metrics: vec![] };

    let instance_id_1 = "1700000000000000_0000000000000001".to_string();
    let instance_id_2 = "1700000000000001_0000000000000002".to_string();

    // receiver_1 represents the receiver before restart and receiver_2 represents receiver after restart
    let receiver_1 =
        create_test_receiver_with_instance_id(policy.clone(), instance_id_1.clone()).await;
    let receiver_2 = create_test_receiver_with_instance_id(policy, instance_id_2.clone()).await;

    let mut request_1 = create_test_request(vec![create_test_metric("test_metric", vec![])]);
    receiver_1.enrich_metrics(&mut request_1);

    let mut request_2 = create_test_request(vec![create_test_metric("test_metric", vec![])]);
    receiver_2.enrich_metrics(&mut request_2);

    let res_1 = request_1.resource_metrics[0].resource.as_ref().unwrap();
    let res_2 = request_2.resource_metrics[0].resource.as_ref().unwrap();

    let id_attr_1 = res_1.attributes.iter().find(|kv| kv.key == "ez_isolate_instance_id").unwrap();
    let id_attr_2 = res_2.attributes.iter().find(|kv| kv.key == "ez_isolate_instance_id").unwrap();

    // Verify that receiver_1 and receiver_2 properly set the expected instance_id in resource attributes
    assert_eq!(id_attr_1.value.as_ref().unwrap().value, Some(Value::StringValue(instance_id_1)));
    assert_eq!(id_attr_2.value.as_ref().unwrap().value, Some(Value::StringValue(instance_id_2)));
    assert_ne!(id_attr_1.value, id_attr_2.value);
}

#[tokio::test]
async fn test_isolate_scope_attributes_empty() {
    let receiver = create_test_receiver(IsolateMetricsPolicy::default()).await;
    assert!(receiver.scope_attributes().is_empty());
    let mut req = create_test_request(vec![create_test_metric("test_metric", vec![])]);
    receiver.enrich_metrics(&mut req);
    let scope = &req.resource_metrics[0].scope_metrics[0].scope;
    if let Some(scope) = scope {
        assert!(scope.attributes.is_empty());
    }
}

#[tokio::test]
async fn test_enrich_metrics_preserves_custom_scope_attributes() {
    let receiver = create_test_receiver(IsolateMetricsPolicy::default()).await;
    let mut request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Default::default()),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                    name: "test-lib".to_string(),
                    version: "1.0.0".to_string(),
                    attributes: vec![KeyValue {
                        key: "custom_scope_tag".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("custom_scope_val".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                metrics: vec![create_test_metric("test_metric", vec![])],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        }],
    };

    receiver.enrich_metrics(&mut request);

    let scope = request.resource_metrics[0].scope_metrics[0].scope.as_ref().unwrap();
    assert_eq!(scope.attributes.len(), 1);
    assert_eq!(scope.attributes[0].key, "custom_scope_tag");
    assert_eq!(
        scope.attributes[0].value.as_ref().unwrap().value,
        Some(Value::StringValue("custom_scope_val".to_string()))
    );
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

fn assert_resource_attributes_match(
    actual: &[KeyValue],
    receiver_configured: &[KeyValue],
    additional_expected: &[(&str, &str)],
) {
    assert_eq!(
        actual.len(),
        receiver_configured.len() + additional_expected.len(),
        "Attribute count mismatch. Actual: {actual:?}"
    );

    for expected_kv in receiver_configured {
        let found = actual.iter().find(|kv| kv.key == expected_kv.key);
        assert!(
            found.is_some(),
            "Expected configured resource attribute '{}' not found in actual: {:?}",
            expected_kv.key,
            actual
        );
        assert_eq!(
            found.unwrap().value,
            expected_kv.value,
            "Value mismatch for attribute '{}'",
            expected_kv.key
        );
    }

    for &(key, val) in additional_expected {
        let found = actual.iter().find(|kv| kv.key == key);
        assert!(
            found.is_some(),
            "Expected additional attribute '{}' not found in actual: {:?}",
            key,
            actual
        );
        assert_eq!(
            found.unwrap().value.as_ref().and_then(|v| v.value.as_ref()),
            Some(&Value::StringValue(val.to_string())),
            "Value mismatch for additional attribute '{}'",
            key
        );
    }
}

#[tokio::test]
async fn test_enrich_metrics_with_custom_resource_and_scope_attributes() {
    let mut config = IsolateMetricsReceiverConfig {
        policy: IsolateMetricsPolicy::default(),
        isolate_name: "test-isolate".to_string(),
        publisher_id: "test-publisher".to_string(),
        is_ratified: false,
        isolate_instance_id: "inst-123".to_string(),
        ..Default::default()
    };
    config
        .try_add_custom_attribute(
            CustomAttribute::Resource("custom_res_k1".into()),
            Value::StringValue("custom_res_v1".to_string()),
        )
        .unwrap();
    config
        .try_add_custom_attribute(
            CustomAttribute::Scope("custom_scope_k1".into()),
            Value::StringValue("custom_scope_v1".to_string()),
        )
        .unwrap();

    let receiver = IsolateMetricsReceiver::new(config).await.unwrap();

    // Verify configured receiver attributes match
    let all_attrs = receiver.attributes();
    assert_eq!(all_attrs.len(), 8); // 6 standard + 2 custom
    assert!(all_attrs.contains_key(&CustomAttribute::Resource("custom_res_k1".into())));
    assert!(all_attrs.contains_key(&CustomAttribute::Scope("custom_scope_k1".into())));

    let receiver_res_attrs = receiver.resource_attributes();
    assert!(receiver_res_attrs.iter().any(|kv| kv.key == "custom_res_k1"
        && kv.value.as_ref().and_then(|v| v.value.as_ref())
            == Some(&Value::StringValue("custom_res_v1".to_string()))));

    let receiver_scope_attrs = receiver.scope_attributes();
    assert_eq!(receiver_scope_attrs.len(), 1);
    assert_eq!(receiver_scope_attrs[0].key, "custom_scope_k1");
    assert_eq!(
        receiver_scope_attrs[0].value.as_ref().and_then(|v| v.value.as_ref()),
        Some(&Value::StringValue("custom_scope_v1".to_string()))
    );

    let mut request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Default::default()),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(Default::default()),
                metrics: vec![create_test_metric("test_metric", vec![])],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        }],
    };

    receiver.enrich_metrics(&mut request);

    let rm = &request.resource_metrics[0];
    let resource_attrs = &rm.resource.as_ref().unwrap().attributes;
    assert_resource_attributes_match(resource_attrs, &receiver_res_attrs, &[]);

    let sm = &rm.scope_metrics[0];
    let scope_attrs = &sm.scope.as_ref().unwrap().attributes;
    assert_eq!(scope_attrs, &receiver_scope_attrs);
}

#[test]
fn test_config_rejects_duplicate_resource_attributes() {
    let mut config = IsolateMetricsReceiverConfig {
        policy: IsolateMetricsPolicy::default(),
        isolate_name: "test-isolate".to_string(),
        publisher_id: "test-publisher".to_string(),
        is_ratified: false,
        isolate_instance_id: "inst-123".to_string(),
        ..Default::default()
    };
    config
        .try_add_custom_attribute(
            CustomAttribute::Resource("duplicate_res_key".into()),
            Value::StringValue("v1".to_string()),
        )
        .unwrap();
    let duplicate_res = config.try_add_custom_attribute(
        CustomAttribute::Resource("duplicate_res_key".into()),
        Value::StringValue("v2".to_string()),
    );
    assert!(duplicate_res.is_err());
    let err_msg = duplicate_res.unwrap_err().to_string();
    assert!(err_msg.contains("Duplicate resource attribute key: 'duplicate_res_key'"));
}

#[tokio::test]
async fn test_init_rejects_shadowing_standard_resource_attributes() {
    let mut shadow_config = IsolateMetricsReceiverConfig {
        policy: IsolateMetricsPolicy::default(),
        isolate_name: "test-isolate".to_string(),
        publisher_id: "test-publisher".to_string(),
        is_ratified: false,
        isolate_instance_id: "inst-123".to_string(),
        ..Default::default()
    };
    shadow_config
        .try_add_custom_attribute(
            CustomAttribute::Resource("ez_isolate_name".into()),
            Value::StringValue("shadowed".to_string()),
        )
        .unwrap();
    let receiver_res = IsolateMetricsReceiver::new(shadow_config).await;
    assert!(receiver_res.is_err());
    let err = receiver_res.err().unwrap();
    let err_msg = err.to_string();
    assert!(err_msg.contains("Duplicate resource attribute key: 'ez_isolate_name'"));
    let ez_err =
        err.downcast_ref::<MetricsReceiverError>().expect("MetricsReceiverError").to_ez_error();
    match ez_err {
        ez_error::EzError::EnforcerError(e) => {
            assert_eq!(e.error_code, tonic::Code::InvalidArgument);
        }
        _ => panic!("Expected EnforcerError"),
    }
}

#[tokio::test]
async fn test_init_rejects_duplicate_scope_attributes() {
    let mut config = IsolateMetricsReceiverConfig {
        policy: IsolateMetricsPolicy::default(),
        isolate_name: "test-isolate".to_string(),
        publisher_id: "test-publisher".to_string(),
        is_ratified: false,
        isolate_instance_id: "inst-123".to_string(),
        ..Default::default()
    };
    config
        .try_add_custom_attribute(
            CustomAttribute::Scope("duplicate_scope_key".into()),
            Value::StringValue("v1".to_string()),
        )
        .unwrap();
    let duplicate_res = config.try_add_custom_attribute(
        CustomAttribute::Scope("duplicate_scope_key".into()),
        Value::StringValue("v2".to_string()),
    );
    assert!(duplicate_res.is_err());
    let err_msg = duplicate_res.unwrap_err().to_string();
    assert!(err_msg.contains("Duplicate scope attribute key: 'duplicate_scope_key'"));
}

#[tokio::test]
async fn test_init_rejects_scope_attribute_shadowing_standard_resource_attributes() {
    let mut config_shadow_standard = IsolateMetricsReceiverConfig {
        policy: IsolateMetricsPolicy::default(),
        isolate_name: "test-isolate".to_string(),
        publisher_id: "test-publisher".to_string(),
        is_ratified: false,
        isolate_instance_id: "inst-123".to_string(),
        ..Default::default()
    };
    // "ez_isolate_name" is an automatically added standard resource attribute;
    // configuring a scope attribute with the same name triggers a collision error.
    config_shadow_standard
        .try_add_custom_attribute(
            CustomAttribute::Scope("ez_isolate_name".into()),
            Value::StringValue("shadow_scope".to_string()),
        )
        .unwrap();
    let receiver_res = IsolateMetricsReceiver::new(config_shadow_standard).await;
    assert!(receiver_res.is_err());
    let err = receiver_res.err().unwrap();
    let err_msg = err.to_string();
    assert!(err_msg.contains(
        "Attribute key 'ez_isolate_name' is defined in both resource and scope attributes"
    ));
    let ez_err =
        err.downcast_ref::<MetricsReceiverError>().expect("MetricsReceiverError").to_ez_error();
    match ez_err {
        ez_error::EzError::EnforcerError(e) => {
            assert_eq!(e.error_code, tonic::Code::InvalidArgument);
        }
        _ => panic!("Expected EnforcerError"),
    }
}

#[tokio::test]
async fn test_init_rejects_overlapping_custom_resource_and_scope_attributes() {
    let mut config_overlap_custom = IsolateMetricsReceiverConfig {
        policy: IsolateMetricsPolicy::default(),
        isolate_name: "test-isolate".to_string(),
        publisher_id: "test-publisher".to_string(),
        is_ratified: false,
        isolate_instance_id: "inst-123".to_string(),
        ..Default::default()
    };
    config_overlap_custom
        .try_add_custom_attribute(
            CustomAttribute::Resource("overlap_key".into()),
            Value::StringValue("res_val".to_string()),
        )
        .unwrap();
    config_overlap_custom
        .try_add_custom_attribute(
            CustomAttribute::Scope("overlap_key".into()),
            Value::StringValue("scope_val".to_string()),
        )
        .unwrap();

    let receiver_overlap = IsolateMetricsReceiver::new(config_overlap_custom).await;
    assert!(receiver_overlap.is_err());
    let err_msg = receiver_overlap.err().unwrap().to_string();
    assert!(err_msg
        .contains("Attribute key 'overlap_key' is defined in both resource and scope attributes"));
}

#[test]
fn test_attribute_name_and_custom_attribute_api() {
    let name1 = AttributeName::new("custom.attr1");
    assert_eq!(name1.as_str(), "custom.attr1");
    assert_eq!(&*name1, "custom.attr1");
    assert_eq!(format!("{name1}"), "custom.attr1");

    let name2 = AttributeName::from(String::from("custom.attr2"));
    assert_eq!(name2.as_str(), "custom.attr2");

    let res_attr = CustomAttribute::Resource(name1);
    assert_eq!(res_attr.name(), &AttributeName::from("custom.attr1"));

    let scope_attr = CustomAttribute::Scope(name2);
    assert_eq!(scope_attr.name(), &AttributeName::from("custom.attr2"));
}

#[tokio::test]
async fn test_filter_metrics_unspecified_and_summary() {
    let policy = IsolateMetricsPolicy {
        allowed_metrics: vec![
            AllowedMetric {
                name: "multi_policy_metric".to_string(),
                r#type: MetricType::Gauge as i32,
                allowed_attributes: vec!["g_attr".to_string()],
            },
            AllowedMetric {
                name: "multi_policy_metric".to_string(),
                r#type: MetricType::Sum as i32,
                allowed_attributes: vec!["s_attr".to_string()],
            },
            AllowedMetric {
                name: "unspecified_metric".to_string(),
                r#type: MetricType::Unspecified as i32,
                allowed_attributes: vec!["u_attr".to_string()],
            },
            AllowedMetric {
                name: "summary_metric".to_string(),
                r#type: MetricType::Summary as i32,
                allowed_attributes: vec!["sum_attr".to_string()],
            },
        ],
    };
    let receiver = IsolateMetricsReceiver::new(IsolateMetricsReceiverConfig {
        policy,
        isolate_name: "test".to_string(),
        publisher_id: "pub".to_string(),
        is_ratified: false,
        isolate_instance_id: "inst".to_string(),
        ..Default::default()
    })
    .await
    .unwrap();

    let mut request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Default::default()),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(Default::default()),
                metrics: vec![
                    Metric {
                        name: "multi_policy_metric".to_string(),
                        description: "".to_string(),
                        unit: "".to_string(),
                        data: Some(Data::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![
                                    KeyValue {
                                        key: "s_attr".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(Value::StringValue("val".to_string())),
                                        }),
                                    },
                                    KeyValue {
                                        key: "bad_attr".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(Value::StringValue("drop".to_string())),
                                        }),
                                    },
                                ],
                                ..Default::default()
                            }],
                            ..Default::default()
                        })),
                        metadata: vec![],
                    },
                    Metric {
                        name: "unspecified_metric".to_string(),
                        description: "".to_string(),
                        unit: "".to_string(),
                        data: Some(Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![
                                    KeyValue {
                                        key: "u_attr".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(Value::StringValue("val".to_string())),
                                        }),
                                    },
                                    KeyValue {
                                        key: "bad_attr".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(Value::StringValue("drop".to_string())),
                                        }),
                                    },
                                ],
                                ..Default::default()
                            }],
                        })),
                        metadata: vec![],
                    },
                    Metric {
                        name: "summary_metric".to_string(),
                        description: "".to_string(),
                        unit: "".to_string(),
                        data: Some(Data::Summary(Summary {
                            data_points: vec![SummaryDataPoint {
                                attributes: vec![
                                    KeyValue {
                                        key: "sum_attr".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(Value::StringValue("val".to_string())),
                                        }),
                                    },
                                    KeyValue {
                                        key: "bad_attr".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(Value::StringValue("drop".to_string())),
                                        }),
                                    },
                                ],
                                ..Default::default()
                            }],
                        })),
                        metadata: vec![],
                    },
                ],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        }],
    };

    receiver.filter_metrics(&mut request);
    let metrics = &request.resource_metrics[0].scope_metrics[0].metrics;
    assert_eq!(metrics.len(), 3);
}

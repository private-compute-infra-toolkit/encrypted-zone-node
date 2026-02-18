// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use opentelemetry::propagation::Extractor;
use tonic::Request;
use trace_context::{get_trace_context, TonicHeaderExtractor};

#[test]
fn test_tonic_header_extractor() {
    let mut request = Request::new(());
    let metadata = request.metadata_mut();
    metadata.insert("key1", "value1".parse().unwrap());
    metadata.insert("key2", "value2".parse().unwrap());

    let extractor = TonicHeaderExtractor::new(&request);

    assert_eq!(extractor.get("key1"), Some("value1"));
    assert_eq!(extractor.get("key2"), Some("value2"));
    assert_eq!(extractor.get("key3"), None);

    let keys = extractor.keys();
    assert!(keys.contains(&"key1"));
    assert!(keys.contains(&"key2"));
}

#[test]
fn test_get_trace_context_with_headers() {
    let mut request = Request::new(());
    let metadata = request.metadata_mut();
    let trace_id = "0af7651916cd43dd8448eb211c80319c";
    let span_id = "b7ad6b7169203331";
    let traceparent = format!("00-{}-{}-01", trace_id, span_id);
    metadata.insert("traceparent", traceparent.parse().unwrap());
    metadata.insert("tracestate", "rojo=00f067aa0ba902b7".parse().unwrap());

    let context = get_trace_context(&request);

    assert_eq!(context.get("traceparent"), Some(&traceparent));
    assert_eq!(context.get("tracestate"), Some(&"rojo=00f067aa0ba902b7".to_string()));
}

#[test]
fn test_get_trace_context_without_headers() {
    let request = Request::new(());
    let context = get_trace_context(&request);
    assert!(context.is_empty());
}

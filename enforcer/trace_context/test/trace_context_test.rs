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

use opentelemetry::{
    propagation::Extractor,
    trace::{TraceContextExt, Tracer, TracerProvider},
    Context,
};
use opentelemetry_sdk::trace as sdktrace;
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
fn test_get_trace_context_with_active_span() {
    let provider = sdktrace::SdkTracerProvider::default();
    let tracer = provider.tracer("test");

    let span = tracer.start("test-span");
    let cx = Context::current_with_span(span);

    // The guard must be kept for the scope of the test.
    let _guard = cx.attach();

    let current_context = Context::current();
    let current_span = current_context.span();
    let span_context = current_span.span_context();
    assert!(span_context.is_valid());

    let request = Request::new(());
    let headers = get_trace_context(&request);

    // A traceparent and tracestate should be injected.
    assert_eq!(headers.len(), 2);
    let traceparent = headers.get("traceparent").unwrap();

    let parts: Vec<&str> = traceparent.split('-').collect();
    assert_eq!(parts.len(), 4);
    assert_eq!(parts[0], "00");
    assert_eq!(parts[1], &span_context.trace_id().to_string());
    assert_eq!(parts[2], &span_context.span_id().to_string());
    assert_eq!(parts[3], "01");
}

#[test]
fn test_get_trace_context_without_headers() {
    let request = Request::new(());
    let context = get_trace_context(&request);
    assert!(context.is_empty());
}

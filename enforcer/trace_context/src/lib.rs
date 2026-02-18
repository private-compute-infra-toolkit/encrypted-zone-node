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

use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use std::collections::HashMap;
use tonic::Request;

/// A struct that can be used to extract OpenTelemetry context from tonic request metadata.
pub struct TonicHeaderExtractor<'a>(&'a tonic::metadata::MetadataMap);

impl<'a> TonicHeaderExtractor<'a> {
    pub fn new<T>(request: &'a Request<T>) -> Self {
        Self(request.metadata())
    }
}

impl<'a> Extractor for TonicHeaderExtractor<'a> {
    /// Get a value for a key from the MetadataMap.
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|metadata| metadata.to_str().ok())
    }

    /// Collect all the keys from the MetadataMap.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|key| match key {
                tonic::metadata::KeyRef::Ascii(k) => k.as_str(),
                tonic::metadata::KeyRef::Binary(k) => k.as_str(),
            })
            .collect()
    }
}

struct HashMapInjector<'a>(&'a mut HashMap<String, String>);

impl<'a> Injector for HashMapInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), value);
    }
}

/// Extracts trace context headers (traceparent, tracestate) from a tonic request.
pub fn get_trace_context<T>(req: &Request<T>) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    let propagator = TraceContextPropagator::new();
    let context = propagator.extract(&TonicHeaderExtractor::new(req));
    propagator.inject_context(&context, &mut HashMapInjector(&mut headers));

    if !headers.is_empty() {
        log::info!("Extracted trace context: {:?}", headers);
    } else {
        log::debug!("No trace context headers found");
    }

    headers
}

// Copyright 2025 Google LLC
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

use enforcer_proto::enforcer::v1::ControlPlaneMetadata;
use ez_service_proto::enforcer::v1::CallRequest;
use opentelemetry::{
    metrics::{Counter, Histogram, UpDownCounter},
    KeyValue,
};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

/// A trait for common metrics functionality.
pub trait ServiceMetrics: Clone + Send + Sync + 'static {
    fn requests(&self) -> Option<&Counter<u64>> {
        None
    }
    fn errors(&self) -> Option<&Counter<u64>> {
        None
    }
    fn active_requests(&self) -> Option<&UpDownCounter<i64>> {
        None
    }
    fn duration_sec(&self) -> Option<&Histogram<f64>> {
        None
    }
    fn message_processing_duration(&self) -> Option<&Histogram<f64>> {
        None
    }
    fn message_size_bytes(&self) -> Option<&Histogram<u64>> {
        None
    }

    fn record_error(&self, attributes: &[KeyValue], error: &'static str) {
        if let Some(errors) = self.errors() {
            let mut error_attributes = Vec::with_capacity(attributes.len() + 1);
            error_attributes.extend_from_slice(attributes);
            error_attributes.push(KeyValue::new("error", error));
            errors.add(1, &error_attributes);
        }
    }

    // Caller must pass attributes that already include the direction tag.
    fn record_message_size_bytes(&self, attributes: &[KeyValue], size: u64) {
        if let Some(message_size_bytes) = self.message_size_bytes() {
            message_size_bytes.record(size, attributes);
        }
    }

    fn track_call(&self, attributes: Arc<Vec<KeyValue>>) -> CallTracker<Self> {
        CallTracker::new(self.clone(), attributes)
    }

    fn track_message_processing<'a>(
        &'a self,
        attributes: &'a [KeyValue],
    ) -> MessageTimerGuard<'a, Self> {
        MessageTimerGuard::new(self, attributes)
    }
}
/// RAII guard to track all calls/executions (unary and streaming). Increments a request counter and an active requests counter on creation. Records duration and decrements the active requests counter on drop.
/// More on RAII pattern: https://doc.rust-lang.org/rust-by-example/scope/raii.html
pub struct CallTracker<T: ServiceMetrics> {
    metrics: T,
    attributes: Arc<Vec<KeyValue>>,
    start_time: Instant,
}

impl<T: ServiceMetrics> CallTracker<T> {
    pub fn new(metrics: T, attributes: Arc<Vec<KeyValue>>) -> Self {
        if let Some(requests) = metrics.requests() {
            requests.add(1, &attributes);
        }
        if let Some(active_requests) = metrics.active_requests() {
            active_requests.add(1, &attributes);
        }
        Self { metrics, attributes, start_time: Instant::now() }
    }
}

impl<T: ServiceMetrics> Drop for CallTracker<T> {
    fn drop(&mut self) {
        if let Some(active_requests) = self.metrics.active_requests() {
            active_requests.add(-1, &self.attributes);
        }
        if let Some(duration_sec) = self.metrics.duration_sec() {
            duration_sec.record(self.start_time.elapsed().as_secs_f64(), &self.attributes);
        }
    }
}

/// RAII guard to track the processing duration of a single message. Records the duration when dropped.
pub struct MessageTimerGuard<'a, T: ServiceMetrics> {
    metrics: &'a T,
    attributes: &'a [KeyValue],
    start_time: Instant,
}

impl<'a, T: ServiceMetrics> MessageTimerGuard<'a, T> {
    pub fn new(metrics: &'a T, attributes: &'a [KeyValue]) -> Self {
        Self { metrics, attributes, start_time: Instant::now() }
    }
}

impl<'a, T: ServiceMetrics> Drop for MessageTimerGuard<'a, T> {
    fn drop(&mut self) {
        if let Some(message_processing_duration) = self.metrics.message_processing_duration() {
            let duration = self.start_time.elapsed().as_secs_f64();
            message_processing_duration.record(duration, self.attributes);
        }
    }
}

#[derive(Debug)]
pub struct MetricAttributes {
    base_attributes: Arc<Vec<KeyValue>>,
    request_attributes: OnceLock<Vec<KeyValue>>,
    response_attributes: OnceLock<Vec<KeyValue>>,
}

impl MetricAttributes {
    pub fn new(domain: &str, service: &str, method: &str) -> Self {
        let base_attributes = Arc::new(vec![
            KeyValue::new("operator_domain", domain.to_string()),
            KeyValue::new("service_name", service.to_string()),
            KeyValue::new("method_name", method.to_string()),
        ]);
        Self {
            base_attributes,
            request_attributes: OnceLock::new(),
            response_attributes: OnceLock::new(),
        }
    }

    /// Returns a cheaply cloneable Arc<Vec<KeyValue>> for base attributes.
    pub fn base(&self) -> Arc<Vec<KeyValue>> {
        self.base_attributes.clone()
    }

    /// Returns a slice of KeyValues for request attributes, initialized once.
    pub fn request(&self) -> &[KeyValue] {
        self.request_attributes.get_or_init(|| {
            let base = &*self.base_attributes;
            let mut attributes = Vec::with_capacity(base.len() + 1);

            attributes.extend_from_slice(base);
            attributes.push(KeyValue::new("direction", "request"));
            attributes
        })
    }

    /// Returns a slice of KeyValues for response attributes, initialized once.
    pub fn response(&self) -> &[KeyValue] {
        self.response_attributes.get_or_init(|| {
            let base = &*self.base_attributes;
            let mut attributes = Vec::with_capacity(base.len() + 1);

            attributes.extend_from_slice(base);
            attributes.push(KeyValue::new("direction", "response"));
            attributes
        })
    }

    pub fn with_attribute<V: Into<opentelemetry::Value>>(
        mut self,
        key: &'static str,
        value: V,
    ) -> Self {
        // This will only clone the Vec if the Arc is shared (which it isn't during construction).
        Arc::make_mut(&mut self.base_attributes).push(KeyValue::new(key, value));

        // Invalidate lazy caches (safe to do since this is typically called before use)
        self.request_attributes = OnceLock::new();
        self.response_attributes = OnceLock::new();
        self
    }
}

/// Cloning this struct is cheap.
/// Note: Cloning resets the derived attribute caches
/// request() and response() will re-allocate vector on new instances.
impl Clone for MetricAttributes {
    fn clone(&self) -> Self {
        Self {
            base_attributes: self.base_attributes.clone(),
            // We must start with fresh, uninitialized locks for the derived attributes.
            request_attributes: OnceLock::new(),
            response_attributes: OnceLock::new(),
        }
    }
}

impl From<&CallRequest> for MetricAttributes {
    fn from(req: &CallRequest) -> Self {
        MetricAttributes::new(&req.operator_domain, &req.service_name, &req.method_name)
    }
}

impl From<Option<&ControlPlaneMetadata>> for MetricAttributes {
    fn from(metadata: Option<&ControlPlaneMetadata>) -> Self {
        if let Some(md) = metadata {
            MetricAttributes::new(
                &md.destination_operator_domain,
                &md.destination_service_name,
                &md.destination_method_name,
            )
        } else {
            MetricAttributes::new("", "", "")
        }
    }
}

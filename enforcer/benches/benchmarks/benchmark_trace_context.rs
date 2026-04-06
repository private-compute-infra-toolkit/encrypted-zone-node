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

use criterion::{criterion_group, Criterion};
use opentelemetry::{
    trace::{TraceContextExt, Tracer, TracerProvider},
    Context,
};
use opentelemetry_sdk::trace as sdktrace;
use tonic::Request;
use trace_context::get_trace_context;

fn benchmark_get_trace_context(c: &mut Criterion) {
    let mut group = c.benchmark_group("benchmark_get_trace_context");

    let provider = sdktrace::SdkTracerProvider::default();
    let tracer = provider.tracer("benchmark");

    let span = tracer.start("test-span");
    let cx = Context::current_with_span(span);

    // Keep the guard active so that Context::current() works inside get_trace_context
    let _guard = cx.attach();

    group.bench_function("get_trace_context_with_active_span", |b| {
        b.iter(|| {
            let request = Request::new(());
            get_trace_context(&request)
        })
    });

    // Also benchmark when there is no active span context
    drop(_guard);
    group.bench_function("get_trace_context_without_active_span", |b| {
        b.iter(|| {
            let request = Request::new(());
            get_trace_context(&request)
        })
    });
}

criterion_group!(benchmark_trace_context, benchmark_get_trace_context);

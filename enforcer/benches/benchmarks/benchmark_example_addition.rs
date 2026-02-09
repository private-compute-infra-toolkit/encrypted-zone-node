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

use criterion::{criterion_group, Criterion};
use std::hint::black_box;

fn add_two(a: i64) -> i64 {
    a + 2
}

async fn async_add_two(a: i64) -> i64 {
    add_two(a)
}

fn benchmark_add_two(c: &mut Criterion) {
    let mut group = c.benchmark_group("benchmark_example_addition");

    group.bench_function("add_two", |b| b.iter(|| add_two(2)));
    group.bench_function("add_two_add_two", |b| b.iter(|| add_two(add_two(2))));

    group.bench_function("add_two_black_box", |b| b.iter(|| add_two(black_box(2))));
    group
        .bench_function("add_two_add_two_black_box", |b| b.iter(|| add_two(add_two(black_box(2)))));

    group.bench_function("add_two_black_box_add_two_black_box", |b| {
        b.iter(|| add_two(black_box(add_two(black_box(2)))))
    });
}

fn benchmark_async_add_two(c: &mut Criterion) {
    let mut group = c.benchmark_group("benchmark_example_addition");

    let tokio_runtime = tokio::runtime::Runtime::new().unwrap();

    group.bench_function("async_add_two", |b| {
        b.to_async(&tokio_runtime).iter(|| async_add_two(black_box(2)))
    });
}

criterion_group!(benchmark_example_addition, benchmark_add_two, benchmark_async_add_two);

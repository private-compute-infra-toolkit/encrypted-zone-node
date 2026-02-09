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
use rand::Rng;
use std::time::Duration;

fn random_sleep_many_times(rng: &mut rand::rngs::ThreadRng, max_duration: Duration, times: u64) {
    let one_second = Duration::from_secs(1).as_nanos();

    for _ in 0..times {
        let sleep_ns = rng.random_range(0..max_duration.as_nanos());
        std::thread::sleep(Duration::new(
            (sleep_ns / one_second).try_into().unwrap(),
            (sleep_ns % one_second).try_into().unwrap(),
        ));
    }
}

fn benchmark_random_sleep_many_times(c: &mut Criterion) {
    let mut group = c.benchmark_group("benchmark_example_sleep");

    let mut rng = rand::rng();

    // Both benchmarks have same estimate, but stddev will differ
    group.bench_function("benchmark_sleep_100_times_upto_1ms", |b| {
        b.iter(|| random_sleep_many_times(&mut rng, Duration::from_millis(1), 100))
    });
    group.bench_function("benchmark_sleep_10_times_upto_10ms", |b| {
        b.iter(|| random_sleep_many_times(&mut rng, Duration::from_millis(10), 10))
    });
}

criterion_group!(benchmark_example_sleep, benchmark_random_sleep_many_times);

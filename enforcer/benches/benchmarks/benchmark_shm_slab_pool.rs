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
use rand::Rng;
use shm_slab_pool::{ShmSlabPool, ShmSlabPoolOptions};
use std::sync::Arc;

/// Parameters for configuring a single run of the shared memory slab pool benchmark.
///
/// Note: `num_slots` and `slot_size` map directly to the corresponding fields
/// in [`ShmSlabPoolOptions`](file:///usr/local/google/home/alexorozco/ez/node/enforcer/isolate/src/shm_slab_pool.rs#L101-L115) used to initialize the pool.
struct BenchmarkParams {
    /// The number of concurrent Tokio tasks (simulated threads) to spawn.
    num_threads: usize,
    /// The number of write/read round trips each task will perform.
    requests_per_thread: usize,
    /// The size (in bytes) of the payload to write and read in each request.
    request_size: usize,
    /// The total number of fixed-size block slots allocated in the shared memory pool.
    num_slots: u64,
    /// The fixed size (in bytes) of each individual slot in the pool.
    slot_size: u64,
}

fn generate_test_path() -> String {
    let mut rng = rand::rng();
    format!("/dev/shm/shm_slab_pool_benchmark_rust_{}", rng.random::<u64>())
}

fn cleanup_test_files(path: &str) {
    let _ = std::fs::remove_file(format!("{}-atomic-hdr", path));
    let _ = std::fs::remove_file(path);
}

fn run_shm_slab_pool_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("benchmark_shm_slab_pool");

    let configs = vec![
        BenchmarkParams {
            num_threads: 1,
            requests_per_thread: 1,
            request_size: 1024,
            num_slots: 1024,
            slot_size: 4096,
        },
        BenchmarkParams {
            num_threads: 10,
            requests_per_thread: 10,
            request_size: 1024,
            num_slots: 1024,
            slot_size: 4096,
        },
        BenchmarkParams {
            num_threads: 20,
            requests_per_thread: 20,
            request_size: 1024,
            num_slots: 1024,
            slot_size: 4096,
        },
        BenchmarkParams {
            num_threads: 50,
            requests_per_thread: 50,
            request_size: 4096,
            num_slots: 4096,
            slot_size: 4096,
        },
        BenchmarkParams {
            num_threads: 100,
            requests_per_thread: 100,
            request_size: 4096,
            num_slots: 4096,
            slot_size: 4096,
        },
    ];

    for params in configs {
        // Create a new Tokio runtime for each configuration, sizing the OS thread pool
        // to match the requested number of simulated threads for true hardware concurrency.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(params.num_threads)
            .enable_all()
            .build()
            .unwrap();

        let name = format!(
            "t_{}_r_{}_s_{}_slots_{}_ssize_{}",
            params.num_threads,
            params.requests_per_thread,
            params.request_size,
            params.num_slots,
            params.slot_size
        );

        group.throughput(criterion::Throughput::Elements(
            (params.num_threads * params.requests_per_thread) as u64,
        ));

        let mut total_failures = 0;
        let mut total_operations = 0;

        group.bench_function(&name, |b| {
            let path = generate_test_path();
            let pool = Arc::new(
                ShmSlabPool::new(ShmSlabPoolOptions {
                    file_name: path.clone(),
                    number_of_slots: params.num_slots,
                    slot_size: params.slot_size,
                    writer: true,
                })
                .unwrap(),
            );

            let mut iter_count = 0;
            b.iter(|| {
                iter_count += 1;
                rt.block_on(async {
                    let mut handles = Vec::with_capacity(params.num_threads);
                    for t in 0..params.num_threads {
                        let p_clone = pool.clone();
                        let reqs = params.requests_per_thread;
                        let size = params.request_size;
                        handles.push(tokio::spawn(async move {
                            let mut local_payload = vec![b'a'; size];
                            for r in 0..reqs {
                                // Embed thread ID and request ID at the start of the payload.
                                // We check the payload size to prevent a buffer overflow in case
                                // `request_size` is configured to be smaller than the 16 bytes
                                // needed to hold both size_t identifiers.
                                if local_payload.len() >= 16 {
                                    local_payload[0..8].copy_from_slice(&t.to_ne_bytes());
                                    local_payload[8..16].copy_from_slice(&r.to_ne_bytes());
                                }

                                let write_res =
                                    p_clone.write_to_pool(&local_payload).await.unwrap();
                                let read_res = p_clone.read_from_pool(&write_res).unwrap();
                                assert_eq!(read_res, local_payload);
                            }
                        }));
                    }
                    for h in handles {
                        h.await.unwrap();
                    }
                })
            });

            total_failures += pool.get_cas_failures();
            total_operations += iter_count * params.num_threads * params.requests_per_thread;

            cleanup_test_files(&path);
        });

        let failures_per_req = if total_operations > 0 {
            total_failures as f64 / total_operations as f64
        } else {
            0.0
        };

        println!(
            "\n[ {} ] Requests Processed: {} | Total CAS Failures: {} | Failures/Request: {:.2}\n",
            name, total_operations, total_failures, failures_per_req
        );
    }
}

criterion_group!(benchmark_shm_slab_pool, run_shm_slab_pool_benchmark);

# Enforcer Microbenchmarks

The enforcer/benches/ directory contains microbenchmarks for the Encrypted Zone Enforcer components.
The Rust crate [`Criterion.rs`](https://bheisler.github.io/criterion.rs/book/index.html) is used to
write and run these benchmarks. This setup utilizes `criterion::main` to handle the benchmark
execution.

## Overview

Microbenchmarks help measure the performance of specific functions and code paths within the
Enforcer. This is useful for identifying performance bottlenecks and tracking improvements.

## Tools

-   **Criterion.rs:** A statistics-driven microbenchmarking library for Rust. Key features include:
    -   Statistical analysis to detect performance changes.
    -   Support for A/B testing between different implementations.
    -   Detailed HTML reports.

## Running Benchmarks

To run all benchmarks defined in this directory, the `--bench` flag must be passed to the benchmark
binary. This is done by adding `-- --bench` after the Bazel command.

The output directory for reports can be controlled by setting the `CRITERION_HOME` environment
variable. By default, Criterion.rs generates HTML reports in `target/criterion` within the Bazel
execution root.

Here is an example of running the benchmarks with a custom report directory:

```bash
CRITERION_HOME=/tmp/ez_bench_results bazel run //enforcer/benches:enforcer_bench -- --bench
```

Criterion.rs will execute the benchmarks, print a summary to the console, and generate the HTML
report in `${CRITERION_HOME}/report/index.html` (e.g., `/tmp/ez_bench_results/report/index.html`).

> **TIP:** To see all available options and help for the benchmark executable, pass `-- --help`:
>
> ```bash
> bazel run //enforcer/benches:enforcer_bench -- --help
> ```
>
> **TIP:** To list all available benchmark functions, pass `-- --list`:
>
> ```bash
> bazel run //enforcer/benches:enforcer_bench -- --list
> ```

## Adding New Benchmarks

1.  Create a new Rust file (e.g., `my_benchmark.rs`) inside the `benchmarks/` subdirectory.
2.  Define benchmark functions using the Criterion.rs library API. For a simple example, see how
    benchmarks are structured in `benchmarks/benchmark_example_addition.rs`.
3.  Declare the new module in `benchmarks/mod.rs` (e.g., `pub mod my_benchmark;`).
4.  Register the new benchmark module and functions in `main.rs` within the `criterion_main!` macro.
5.  Add any new dependencies to the `deps` section of the `//enforcer/benches:enforcer_bench` target
    in the `BUILD.bazel` file.

## WARNING: Benchmark Naming

Benchmark group and function names, as defined within the Criterion.rs benchmark functions (e.g.,
`c.bench_function("my_function_name", ...)`), are used as identifiers by external tools for data
collection and historical performance tracking.

Renaming existing benchmark groups or functions can disrupt this tracking. If renaming is necessary,
please be mindful of potential impacts on performance data continuity.

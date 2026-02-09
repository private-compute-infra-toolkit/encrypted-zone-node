# Gemini Context: //enforcer/benches

This directory contains microbenchmarks for the Enforcer component, primarily using the Criterion.rs
library.

## Key Information

-   **Purpose:** Performance measurement of Enforcer components.
-   **Framework:** Criterion.rs for writing and running benchmarks.
-   **Build Target:** The benchmarks are built and run via the Bazel target
    `//enforcer/benches:enforcer_bench`.
-   **Main Entry Point:** `main.rs` uses `criterion_main!` to aggregate and run benchmark groups.
-   **Module Declarations:** `benchmarks/mod.rs` declares all the individual benchmark modules. This
    file _must_ be updated when adding or removing benchmark files in the `benchmarks/` directory.
-   **Individual Benchmarks:** Located in `.rs` files within the `benchmarks/` directory.
-   **Configuration:** Managed in `BUILD.bazel`.

## Common Operations

-   **Running Benchmarks:** Refer to the `README.md` in this directory for detailed instructions.
-   **Adding New Benchmarks:** Instructions are in `README.md`. Key files to modify:
    1.  New file in `benchmarks/`.
    2.  `benchmarks/mod.rs` (add `pub mod <new_module>;`).
    3.  `main.rs` (add to `criterion_main!`).
    4.  `BUILD.bazel` (if new dependencies are needed).
-   **Interpreting Results:** Criterion.rs generates HTML reports, as detailed in `README.md`.
-   **Dependencies:** Add to `BUILD.bazel`.
-   **Renaming:** Be cautious when renaming benchmark functions/groups, as noted in the `WARNING` in
    `README.md`.

## Internal Considerations (For AI Awareness)

-   While the benchmark execution uses the open-source Criterion.rs, be aware that the results _may_
    be further processed or uploaded by internal tooling. The names of benchmarks are likely keys
    for this. Do not expose internal tool names or details in public-facing files like `README.md`.

## How AI Can Assist

-   Generating boilerplate for new benchmark files.
-   Updating `benchmarks/mod.rs` and `main.rs` when adding new benchmarks.
-   Modifying `BUILD.bazel` for dependencies.
-   Providing run commands from `README.md`.
-   Reminding users about the naming constraints when modifications to benchmark names are
    requested.

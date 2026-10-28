# Gemini Code Understanding

This document provides a high-level overview of the Encrypted Zone (EZ) codebase and coding rules to
be used as instructional context for development and automated code generation.

## Project Overview

Encrypted Zone (EZ) is designed to enforce data privacy rules using Trusted Execution Environments
(TEEs). The system is built primarily in Rust and uses Bazel as its build system. The core component
is the `enforcer`, which manages the execution of services within isolated environments. The project
also includes an SDK for client applications to interact with the EZ. Communication with the EZ is
handled through gRPC services defined with Protocol Buffers.

## Project Configuration

The following configuration attributes pertain to this repo.

## Building and Running

The project is built using Bazel. The [`docs/Development.md`](docs/Development.md) file provides
instructions for setting up the development environment, including configuring `rust-analyzer` for
IDE support.

To build the project, you can use the `bazel build` command. For example, to build the `enforcer`
binary:

```bash
bazel build //enforcer/...
```

To run the tests, you can use the `bazel test` command:

```bash
bazel test //...
```

The [`docs/Microbenchmarks.md`](docs/Microbenchmarks.md) file provides instructions for running
benchmarks.

## Development Conventions

-   The project uses `rustfmt` for code formatting, as indicated by the `.rustfmt.toml` file.
-   The project uses `clang-format` for C/C++/Protobuf formatting.
-   The project uses pre-commit hooks for linting and formatting, as indicated by the
    `.pre-commit-config.yaml` file.
-   The `BUILD.bazel` files define the build rules and dependencies for each component.
-   The `MODULE.bazel` file defines the external dependencies for the project.

### Testing & Coverage

-   Write verified red-green unit tests covering real public behavioral contracts and observable
    outputs; never add artificial coverage-padding tests, trivial `Display` tests, or generated
    protobuf field assertions, and avoid asserting on internal magic counts or duplicated internal
    constants so internal refactors require zero test churn. Use real generated `prost` structs over
    hand-built byte stubs, and ratchet up or remove stale per-file exceptions in
    `coverage/thresholds.json` via small per-module CLs once coverage exceeds 90%.
-   Assert explicit post-conditions (`assert!` / `assert_eq!`) in every test -- never rely solely on
    `.unwrap()` or permissive `a || b` checks. Wrap async stream reads in `tokio::time::timeout`,
    match `Some(Err(status))` explicitly instead of masking gRPC errors behind EOF panics, and
    verify concurrent multi-request isolation by recording mock calls in `Vec<...>` and pairing each
    request's metadata and payload to its exact output.
-   Place unit tests in `test/` directories with all `#[test]` / `#[tokio::test]` functions at the
    top of the file and helper/mock utilities at the bottom. Delete unused helpers, configure only
    the non-default options under test, never branch production code for test environments, and run
    timing-sensitive async tests repeatedly to confirm zero flakes.

### Rust Idioms, API Design & Performance

-   Keep fields, caches, and helpers private (or `pub(crate)`) -- never widen visibility to `pub`
    for tests -- and avoid `new_with_*` constructor proliferation by extending a single `new(...)`
    with `Option<T>`, a config struct, or `..Default::default()` struct update syntax. Name
    entry-consuming methods `pop` or `take` rather than `get`.
-   Eliminate unnecessary `.clone()` allocations on hot paths and streams: move owned buffers with
    `std::mem::take` / `Option::take()`, match `Option` by reference (`if let Some(v) = &opt`), wrap
    shared per-stream metadata and configs in `Arc` outside streaming closures, prefer `HashSet` and
    single-pass `HashMap::insert` over `Vec::contains` or `contains_key` + `insert`, and keep gRPC
    clients cheaply `Clone` via internal `Arc` fields.
-   Group `use` imports at the top of each `.rs` file, extract magic timeouts and strings into
    shared `const` definitions used by both production code and tests, refactor long inline loops
    into helper functions, and prefer vetted crates (such as cloneable `mpsc::Sender` channels
    without `Mutex`) over custom primitives.

### Error Handling, Security & TEE Boundaries

-   Never panic in Enforcer runtime code (`unwrap`, `expect`, `assert!`, `debug_assert!`, or
    unchecked integer math). Validate wire and manifest inputs fail-fast before mutating state --
    enforcing bounded counts (`1..=MAX`), size caps, ID/slot uniqueness, path-traversal checks
    (rejecting empty, `/`, `.`, `..`), rollback on error, and canonicalization of stripped slices so
    suffix data cannot leak upstream.
-   Keep open-source code, flags, protos, and comments strictly vendor-neutral -- never reference
    proprietary or internal deployment systems, service names, or infrastructure acronyms, and route
    external and cross-isolate communication through standard Encrypted Zone interfaces. Enforce
    defense-in-depth on container mounts (`apply_restrictive_flags: true` preserving
    `nosuid`/`nodev`/`noexec`, private `0700` directories instead of world-writable `/dev/shm`
    `1777`, and explicit `metrics_policy` allowlists), keep `main.rs` thin via `NodeBootstrap` with
    Manifest v1/v2 parity, and scope proto fields to the narrowest message (`OpaqueControls`,
    `EzServiceSpec`, `EzBackendDependency`).

### Comments, Documentation & BUILD.bazel

-   Keep inline comments and docstrings brief (1-2 lines) and strictly limited to non-obvious
    invariants; keep `README.md` docs high-level without internal Rust types (`Arc<RwLock<...>>`,
    crate names); and attach a concrete `TODO(b/...)` bug ID to every `TODO` or temporary flag.
-   Keep `BUILD.bazel` and `MODULE.bazel` minimal and explicit: set `testonly = True` on test helper
    targets (`genrule`, `pkg_files`), avoid parenthetical suffixes in target names, prune unused
    crate dependencies, list active `.pem` certs explicitly instead of `glob("**/*.pem")`, and
    verify feature-gated dependency exclusion with `bazel query 'somepath(...)'`.

## Additional Context

-   **Enforcer Benchmarks:** See [`enforcer/benches/GEMINI.md`](enforcer/benches/GEMINI.md)

## Code Coverage

Code coverage thresholds are managed on a per-file basis using the `coverage/thresholds.json`
configuration file.

To run code coverage locally and verify compliance with the thresholds, run:

```bash
DEVKIT_DOCKER_RUN_ARGS="--privileged" \
  devkit/coverage --lines 90 --thresholds-file coverage/thresholds.json
```

### Managing Coverage Exclusions

Prefer writing verified unit tests over adding exclusions. Once a file's coverage exceeds 90%,
ratchet up or remove stale per-file entries in `coverage/thresholds.json` via small per-module CLs.

To exclude a file from code coverage validation (or to lower its minimum required coverage
threshold) when necessary, add an entry for the file path in `coverage/thresholds.json` with custom
limits:

-   **Exclude lines coverage**: Set `"lines": 0`.
-   **Exclude branch coverage**: Set `"branch": 0`.

Example configuration:

```json
{
    "enforcer/junction/trait.rs": {
        "lines": 0,
        "branch": 0
    }
}
```

## Commit Messages & CL Hygiene

Keep CLs small and single-purpose -- split independent features, refactors, and per-module coverage
additions into separate CLs.

This project follows the Conventional Commits specification. The commit message title must follow
this format:

```text
<type>(<scope>): <subject>
```

When providing a `<scope>`, it must be one of the scopes defined for the given `<type>` in the
`.versionrc.json` file at the root of the repository. For example, if you are adding a feature
related to the enforcer, your commit message might look like
`feat(enforcer): Add new validation logic`. If none of the `<scope>` values are relevant, the commit
message can also use the unscoped format:

```text
<type>: <subject>
```

Use concise Conventional Commit titles (`<= 72` characters), followed by one blank line and concise
`<= 80`-character body lines updated whenever patchset scope shifts. Keep all commit footers
(`Bug: b/<bug-identifier>` or `Bug: N/A`, `ORIGINAL_AUTHOR=<username>`, `Change-Id: ...`) strictly
contiguous at the bottom with zero blank lines between trailers, and explicitly attribute the human
requester. Ask the user to specify the Bug ID when none is known. The `Change-Id` footer is added by
the git `commit-msg` hook when creating a commit. Read the last 3 commit messages for examples.

## Open Source Codebase

### Git Repositories

-   **`Encrypted Zone` (Main Repository):** This is the primary repository for the project. It is a
    Rust project that uses the Bazel build system. Its main purpose is to enforce data privacy rules
    using Trusted Execution Environments (TEEs).
-   **`.devkit` (Submodule):** This is a separate Git repository included as a submodule. Its
    primary purpose is to provide consistent and reproducible compiler toolchains and developer
    tools. It is a general-purpose toolkit used across projects for bootstrapping and managing build
    images.

### Codebase Structure

-   **`enforcer/`:** Contains the core component of the Encrypted Zone, the **enforcer** -- a Rust
    binary that manages the execution of services within isolated, trusted environments.
    -   **`public_api/`**: Implements the public-facing `EzPublicApi` gRPC service, which is the
        primary external interface for the `enforcer`.
    -   **`isolate/`**: Manages the isolated environments where services run.
    -   **`container/`**: Manages the underlying container sandboxing and namespaces for Isolates.
    -   **`junction/`**: Manages communication channels between the `enforcer` and Isolates.
    -   **`data_scope/`**: Enforces data scope privacy rules across Isolate calls.
    -   **`state/`**: Manages Isolate lifecycle states and resets.
    -   **`v1/`**: Protocol Buffer definitions for the `enforcer` APIs and manifests.
-   **Other Folders:** Root-level directories and files hold project configuration (`.bazelrc`,
    `MODULE.bazel`), documentation (`README.md`, `docs/`), and development utilities (`tools/`).

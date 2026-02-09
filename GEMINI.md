# Gemini Code Understanding

This document provides a high-level overview of the Encrypted Zone (EZ) codebase to be used as
instructional context for future interactions.

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

The `DEVELOPMENT.md` file also provides instructions for running benchmarks.

## Development Conventions

-   The project uses `rustfmt` for code formatting, as indicated by the `.rustfmt.toml` file.
-   The project uses `clang-format` for C/C++/Protobuf formatting.
-   The project uses pre-commit hooks for linting and formatting, as indicated by the
    `.pre-commit-config.yaml` file.
-   The `BUILD.bazel` files define the build rules and dependencies for each component.
-   The `MODULE.bazel` file defines the external dependencies for the project.

## Additional Context

-   **Enforcer Benchmarks:** See `enforcer/benches/GEMINI.md`

## Commit Messages

This project follows the Conventional Commits specification. The commit message title must follow
this format:

```text
<type>(<scope>): <subject>
```

When providing a `<scope>`, it must be one of the scopes defined for the given `<type>` in the
`.versionrc.json` file at the root of the repository. For example, if you are adding a feature
related to the enforcer, your commit message might look like
`feat(enforcer): Add new validation logic`. If none of the `<scope>` are relevant, the commit
message can also be of the following format.

```text
<type>: <subject>
```

The title of the commit message have a maximum limit of 72 characters. The Bug Footer at the end
should be of the format `Bug: b/<bug-identifier>`. Ask the user to specify the Bug ID before
suggesting the commit message. For commits with no relevant bugs, the bug footer should be
`Bug: N/A`. The `Change-Id` footer will be added by a git hook after the commit is done. Do not
suggest commit messages with the `Change-Id` footer.

The commit message body should add relevant information about the commit. Each line in the commit
message body should maximum of 80 characters. Read last 3 commit messages to look for commit message
examples.

## Open source Codebase

### Git Repositories

-   **`Encrypted Zone` (Main Repository):** This is the primary repository for the project. It's a
    Rust project that uses the Bazel build system. Its main purpose is to enforce data privacy rules
    using Trusted Execution Environments (TEEs).
-   **`.devkit` (Submodule):** This is a separate Git repository included as a submodule. Its
    primary purpose is to provide consistent and reproducible compiler toolchains and developer
    tools ( source). It's a general-purpose toolkit that can be used across different projects for
    bootstrapping new projects, and managing build images.

### Codebase Structure

-   **`enforcer/`:** This directory contains the core component of the Encrypted Zone, the
    **enforcer**. It's a Rust binary that manages the execution of services within isolated, trusted
    environments.
    -   **`public_api/`**: Implements the public-facing `EzPublicApi` gRPC service, which is the
        primary external interface for the `enforcer`.
    -   **`isolate/`**: Manages the isolated environments where services run.
    -   **`container/`**: Contains the logic for managing the underlying container technology for
        the Isolates.
    -   **`junction/`**: Manages communication channels between the `enforcer` and the Isolates.
    -   **`data_scope/`**: Contains the logic for handling and enforcing data scopes for privacy.
    -   **`state/`**: Manages the state of the Isolates. This is primarily used for Isolate resets.
    -   **`v1/`**: Contains the Protocol Buffer definitions for the `enforcer`'s APIs.
-   **Other Folders:** The rest of the folders at the root of the repository are part of the main
    `Encrypted Zone` project. They contain project-level configuration (`.bazelrc`, `MODULE.bazel`),
    documentation (`README.md`), and tools for managing the project.

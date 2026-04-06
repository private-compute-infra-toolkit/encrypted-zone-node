# Tokio Console Support

The EZ Enforcer supports [`tokio-console`](https://github.com/tokio-rs/console), a diagnostics and
debugging tool for asynchronous Rust programs. This document is intended for local debugging. It
allows you to inspect the runtime state of tokio tasks, resources, and more in real-time.

## Prerequisites

1.  **Install the `tokio-console` CLI:**

    ```sh
    cargo install tokio-console
    ```

2.  **Build with `tokio_unstable`:** The enforcer must be compiled with the `tokio_unstable` cfg
    flag. This is enabled by default in the repository's `.bazelrc` for all builds.

    Build the enforcer debug binary and copy it to the `dist/enforcer/` directory:

    ```sh
    devkit/build bazel run //enforcer:copy_to_dist
    ```

## Usage

### 1. Launch the Enforcer with the Console Port

To enable the console subscriber, you must provide a port using the `--console-subscriber-port` flag
when starting the `enforcer_bin` executable:

```sh
./dist/enforcer/enforcer_bin --manifest-path=path/to/manifest.json --console-subscriber-port <CONSOLE-SUBSCRIBER-PORT>
```

### 2. Connect the Console

In a separate terminal, run the `tokio-console` command pointing to the enforcer's console port:

```sh
tokio-console http://localhost:<CONSOLE-SUBSCRIBER-PORT>
```

## Troubleshooting

-   **Connection Refused:** Ensure the enforcer is running and that you correctly specified the
    `--console-subscriber-port` flag.
-   **No Data:** Ensure the enforcer was built successfully after the console support was added.
    Check that `.bazelrc` contains the `--cfg=tokio_unstable` flag.

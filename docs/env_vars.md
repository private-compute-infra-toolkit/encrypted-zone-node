# Encrypted Zone (EZ) Environment Variables

The following environment variables can be set to configure the behavior of the `enforcer` binary
and its associated services:

## Connection Proxy Retries

These variables configure the behavior when connecting to external proxy or mTLS services.

### `PROXY_CONNECT_RETRY_COUNT`

-   **Description:** The number of times the Enforcer will attempt to retry an outbound connection
    (e.g. to the mTLS or EZ-to-External proxies) if the initial connection fails.
-   **Type:** Integer
-   **Default:** Defined by internal `grpc_connector` default (typically `3`).

### `PROXY_CONNECT_RETRY_DELAY_MS`

-   **Description:** The delay (in milliseconds) between successive proxy connection retry attempts.
-   **Type:** Integer
-   **Default:** Defined by internal `grpc_connector` default (typically `1000`).

## Server Configuration

### `EZ_PUBLIC_API_PORT`

-   **Description:** Specifies the port number the Enforcer should listen on for incoming API
    connections by default. Overrides the `--port` or `-p` command line argument when provided.
-   **Type:** Integer
-   **Default:** `53459` (unless overridden by flags).

## Logging

### `RUST_LOG`

-   **Description:** Controls the log verbosity of the Rust logging framework (`env_logger` or
    similar setup). This applies globally to all components.
-   **Type:** String (e.g., `info`, `debug`, `trace`, `warn`, `error`)
-   **Default:** Not set.

## Workload / Isolate Configuration

### `EZ_BACKEND_DEPENDENCIES`

-   **Description:** Serialized `textproto` representation of the `EzManifest` backend dependencies
    (`EzBackendDependencies` proto). Used by the SDK inside the Isolate to lookup `isolate_name` and
    `publisher_id` for `InvokeEZ` calls.
-   **Type:** String (protobuf text format)
-   **Default:** Not set (only populated if backend dependencies are defined in the manifest).

# Profiling the EZ Enforcer

This document provides instructions for collecting and analyzing performance profiles from a running
EZ Enforcer.

## Prerequisites

-   The Enforcer must be running locally (see [Running Locally](#running-locally)).
-   `grpcurl` and `jq` installed on your machine.
-   `pprof` installed for analyzing the output.

## Setup Environment

Clone the SDK repository and set the `SDK` environment variable:

```bash
git clone https://github.com/private-compute-infra-toolkit/encrypted-zone-sdk
SDK="/absolute/path/to/sdk"
```

Set up `$NODE` variable:

```bash
export NODE=$(pwd)
```

## Running Locally

To collect profiles, you first need a running Enforcer instance with the `DiagnosticService`
enabled.

1.  **Build the Enforcer and Manifest Descriptor:**

    ```bash
    devkit/build bazel run //enforcer:copy_to_dist
    ```

2.  **Prepare the Testing Directory:** The Enforcer expects a specific directory structure for its
    proto descriptors.

    ```bash
    # Create the directory structure
    mkdir -p manual_testing/enforcer/proto/v1

    # Copy the Enforcer binary
    cp dist/enforcer/enforcer_debug manual_testing/enforcer_bin

    # Copy the manifest descriptor set to the expected location
    cp dist/enforcer/v1/manifest_descriptor_set.pb \
      manual_testing/enforcer/proto/v1/
    ```

3.  **Build the Isolate Bundle:** Navigate to the SDK repository to build the example server bundle
    and copy it to the testing directory.

    ```bash
    cd "${SDK}"
    devkit/build bazel build //examples/summation_by_looping_monolithic_server/server:summation_by_looping_monolithic_server_bundle
    cp bazel-bin/examples/summation_by_looping_monolithic_server/server/summation_by_looping_monolithic_server_bundle.tar \
      $NODE/manual_testing/
    ```

4.  **Create and Configure the Manifest:** The Enforcer requires an EZ Manifest (JSON format) to
    start. Copy the example manifest and update the file paths for local execution.

    ```bash
    cd $NODE
    cp $SDK/examples/summation_by_looping_monolithic_server/server/summation_by_looping_monolithic_server_manifest.json \
      manual_testing/manifest.json

    # Update manifest to point to the local tarball
    sed -i "s/\/ez\///" manual_testing/manifest.json
    ```

5.  **Launch the Enforcer:**

    ```bash
    cd manual_testing
    ./enforcer_bin --manifest-path=manifest.json
    ```

    By default, the Enforcer will listen on `localhost:53459`.

## Generating Traffic

To get a meaningful CPU profile, you should generate some traffic while the profiler is running. You
can use one of the example clients:

```bash
# Example: Summation isolate client
cd $SDK
devkit/build bazel run //examples/summation_by_looping_monolithic_server/client:client
```

## Collecting a CPU Profile

The Enforcer exposes a `GetDiagnostic` RPC that can trigger on-demand CPU profiling.

1.  **Request a Profile:** Collect a 10-second CPU profile and save it to `cpu.pprof.gz`:

    ```bash
    # Note: duration_seconds is used to specify the collection time.
    grpcurl -plaintext -import-path . \
      -proto enforcer/diagnostics/v1/diagnostics_service.proto \
      -d '{"cpu": {"duration_seconds": 10}}' \
      localhost:53459 enforcer.diagnostics.v1.DiagnosticService/GetDiagnostic \
      | jq -r '.cpu.pprofData' | base64 -d > cpu.pprof.gz
    ```

    _Note: The response also includes a `captured_duration_seconds` field confirming the actual time
    spent profiling._

2.  **Analyze with pprof:** You can view a text-based top summary:

    ```bash
    pprof -top cpu.pprof.gz
    ```

    Or launch an interactive web interface:

    ```bash
    pprof -http=:8081 cpu.pprof.gz
    ```

## Other Diagnostic Options (NOT IMPLEMENTED)

The `GetDiagnostic` RPC supports other profile types:

-   **Heap Profile:** Capture a snapshot of memory allocations.

    ```bash
    grpcurl -plaintext -import-path . \
      -proto enforcer/diagnostics/v1/diagnostics_service.proto \
      -d '{"heap": {}}' \
      localhost:53459 enforcer.diagnostics.v1.DiagnosticService/GetDiagnostic \
      | jq -r '.heap.pprofData' | base64 -d > heap.pprof.gz
    ```

-   **Tokio Runtime Profile:** Capture diagnostics from the async runtime.

    ```bash
    grpcurl -plaintext -import-path . \
      -proto enforcer/diagnostics/v1/diagnostics_service.proto \
      -d '{"tokio": {"duration_seconds": 5}}' \
      localhost:53459 enforcer.diagnostics.v1.DiagnosticService/GetDiagnostic
    ```

## Implementation Notes

-   Profiling is a global resource. The Enforcer uses a global lock to ensure only one profiling
    session is active at a time.
-   The `duration_seconds` field is currently used instead of `google.protobuf.Duration` to avoid
    type resolution issues in certain environments.
    -   TODO: Find a better way of expressing Duration.

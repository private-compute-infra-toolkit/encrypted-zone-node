# Get Started with EZ Node

This guide provides instructions for setting up and testing Encrypted Zone (EZ) Node locally. In
this guide, you will:

1. Setup a server that performs summations from x to y integers to demonstrate how inter-Isolate RPC
   and memory-sharing is supported.
2. Accept a request with public data and return a response from an opaque isolate.

## Prerequisites

It's recommended that you read the
[EZ Node readme](https://github.com/private-compute-infra-toolkit/encrypted-zone-node/blob/main/README.md)
to familiarize yourself with EZ components and terminology before walking through this guide.

Bazel is a requirement for this guide. You can
[install](https://github.com/bazelbuild/bazelisk?tab=readme-ov-file#installation) and configure
Bazel versions via Bazelisk. We use bazel version 7.4.1.

## Clone the Repositories

To get started, you must clone both the
[Node](https://github.com/private-compute-infra-toolkit/encrypted-zone-node) and
[SDK](https://github.com/private-compute-infra-toolkit/encrypted-zone-sdk) repositories. To assist
in the guide, we recommend setting environment variables to simplify navigation:

```bash
NODE=<path to node repo>
SDK=<path to sdk repo>
```

## Creating Testing Artifacts

Navigate to your Node repository to create the necessary directories and build the enforcer binaries
and descriptors:

### 1. **Prepare the environment**

Create a directory for testing artifacts.

```bash
cd $NODE
mkdir -p manual_testing/enforcer/proto
```

### 2. **Build the [Enforcer](../README.public.md#4-ez-policy-enforcer) Binary and Generate Manifest Descriptors**

Use Bazel to build the Enforcer debug binary and the manifest proto descriptor, then move them to
the testing directory created in the previous step.

```bash
bazel build //enforcer:enforcer_debug && \
cp bazel-bin/enforcer/enforcer_debug manual_testing/enforcer_bin

bazel build enforcer:manifest_descriptor_set && \
cp bazel-bin/enforcer/manifest_descriptor_set.pb \
manual_testing/enforcer/proto
```

### 3. Build the Summation Isolate

Navigate to the SDK repository to build the example server bundle.

```bash
cd
$SDK bazel build
//examples/summation_by_looping_with_backend/server/cpp:summation_by_looping_with_backend_server_bundle
&& \
cp bazel-bin/examples/summation_by_looping_with_backend/server/cpp/summation_by_looping_with_backend_server_bundle.tar
$NODE/manual_testing
```

### 4. **Configure the Manifest**

Copy the summation isolate manifest and update the file paths for local execution.

```bash
cp
examples/summation_by_looping_with_backend/server/cpp/summation_by_looping_with_backend_manifest.json
$NODE/manual_testing/ sed -i "s/\/ez\/summation/summation/"
$NODE/manual_testing/summation_by_looping_with_backend_manifest.json
```

## Testing the Server

Once your artifacts are ready, you can launch the enforcer:

Navigate to the testing directory

```bash
cd $NODE/manual_testing
```

Run the enforcer binary with the manifest path:

```bash
./enforcer_bin --manifest-path=summation_by_looping_with_backend_manifest.json & BACKEND_PID=$!
```

By default, the enforcer runs on port 53459. If you anticipate a port conflict, you may override the
port via the `EZ_PUBLIC_API_PORT` environment variable.

## Validating the Implementation

To validate that the server is functioning correctly, run a client from a separate terminal tab:

Navigate to the SDK repo and execute the client using Bazel.

```bash
cd $SDK bazel run //examples/summation_by_lookup_table_with_backend/client:client
```

Once you've run the client, you should see a result similar to this:

```bash
INFO: Elapsed time: 204.347s, Critical Path: 186.59s INFO: 607 processes: 208 internal, 399
linux-sandbox. INFO: Build completed successfully, 607 total actions INFO: Running command line:
bazel-bin/examples/summation_by_lookup_table_with_backend/client/client Parsed response 0#0:
IntegerSequenceResponse { sequence_sum: 5050 }
```

Revisiting the server output, you should see the following output:

```bash
SimpleAdd.IntegerSequence inputs: start_at = 1 end_at = 100 expected_result = 5050
```

The server responded with the result = 5050. The client received this response before we were able
to see the server handle the request but the result was expected.

# Feedback

If you have any implementation questions or feedback on these new features, please
[file an issue](https://github.com/private-compute-infra-toolkit/encrypted-zone-node/issues) in our
Github repository.

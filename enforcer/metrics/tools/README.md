# EZ Metrics Observability Simulator Skeleton

`ez-metrics-simulator` is an offline command-line skeleton tool designed to load captured standard
OpenTelemetry metrics streams, validate them against allowed metrics policies in an EZ manifest, and
output a baseline metrics stream.

This skeleton target compiles cleanly and establishes the core file loading and serialization paths.

---

## How to Run the Simulator Skeleton

### 1. Build the Target

```bash
devkit/build bazel build //enforcer/metrics:ez_metrics_simulator
```

### 2. Execute the Skeleton

Pass absolute paths for the manifest and input payload files:

```bash
devkit/build bazel run //enforcer/metrics:ez_metrics_simulator -- \
  --manifest /absolute/path/to/test_manifest.json \
  --input /absolute/path/to/test_metrics_input.json \
  --output-baseline /absolute/path/to/baseline_out.json
```

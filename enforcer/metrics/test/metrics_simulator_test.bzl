# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

load("@bazel_skylib//rules:diff_test.bzl", "diff_test")

def metrics_simulator_test(*, name, manifest, input, expected):
    """Executes the ez_metrics_simulator tool and asserts output values against a checked-in telemetry baseline JSON.

    Args:
        name: Name of the E2E diff_test target (e.g., "simulator_integration_test").
        manifest: Label to the manifest JSON file (e.g., "test_data/baseline/manifest.json").
        input: Label to the raw telemetry input JSON file (e.g., "test_data/baseline/metrics_input.json").
        expected: Label to the expected output baseline JSON file (e.g., "test_data/baseline/expected_output.json").
    """
    actual_out_name = name + "_actual_output.json"
    genrule_name = name + "_run"

    # Hermetic execution phase
    native.genrule(
        name = genrule_name,
        srcs = [
            manifest,
            input,
        ],
        outs = [actual_out_name],
        cmd = "ENFORCER_VERSION_OVERRIDE=1.23.45 $(location //enforcer/metrics:ez_metrics_simulator) --manifest $(location {}) --input $(location {}) --output-baseline $@".format(manifest, input),
        tools = ["//enforcer/metrics:ez_metrics_simulator"],
    )

    # Standard diff assertion phase
    diff_test(
        name = name,
        file1 = expected,
        file2 = ":" + actual_out_name,
    )

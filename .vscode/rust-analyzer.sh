#!/bin/bash
# Copyright 2025 Google LLC
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

set -o errexit

declare -r PREFIX='{"reason":"compiler-message","package_id":"","target":{"kind":[""],"crate_types":[""],"name":"","src_path":"","edition":"2021","doc":true,"doctest":true,"test":true},"message":'
declare -r SUFFIX='}'
declare -r SAVED_FILE="$1"
declare -r PATH_PREFIX="$2"
declare -r OUTPUT_BASE="$3"
# see https://bazel.build/run/scripts
declare -r BAZEL_BUILD_FAILED_ERROR_CODE=1
declare -r BAZEL_SUCCESS_CODE=0

function run_analyzer() {
  declare -r FILE_PATH="${SAVED_FILE/#"${PATH_PREFIX}/"}"
  declare -r FILE_TARGET="$(bazel query "${FILE_PATH}")"
  declare -r BAZEL_TARGET="$(bazel query "attr('srcs', ${FILE_TARGET}, ${FILE_TARGET//:*/}:*)")"
  set +o errexit
  rustfmt "${FILE_PATH}"
  bazel --output_base="${OUTPUT_BASE}" build --@rules_rust//rust/settings:error_format=json "${BAZEL_TARGET}"
  declare -r -i RC=$?
  set -o errexit
  if [[ ${RC} -eq ${BAZEL_BUILD_FAILED_ERROR_CODE} ]]; then
    STD_ERR_DIR="${OUTPUT_BASE}/execroot/_main/bazel-out/_tmp/actions"
    while read -r line; do
      printf "%s%s%s\n" "${PREFIX}" "${line}" "${SUFFIX}"
    done <<< "$(cat "${STD_ERR_DIR}"/stderr-*)"
  elif [[ ${RC} -ne ${BAZEL_SUCCESS_CODE} ]]; then
    # TODO: update debug instructions when we can reproduce error and try bazel clean
    printf "XXXXXX rust-analyzer is out of sync, please try 'rm -rf \"%s\"' and save again XXXXXX\n" "${OUTPUT_BASE}"
    exit ${RC}
  fi
}

run_analyzer

#!/usr/bin/env bash
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

# Validates tracked DIR_METADATA files and agentic_review configurations.

set -o errexit
set -o nounset
set -o pipefail

# Use system dirmd if available; otherwise fetch and cache the CIPD package.
DIRMD_BIN="$(command -v dirmd || true)"
if [[ -z "${DIRMD_BIN}" ]]; then
  CACHE_DIR="${XDG_CACHE_HOME:-${HOME}/.cache}/pre-commit/dirmd"
  DIRMD_BIN="${CACHE_DIR}/dirmd"
  if [[ ! -x "${DIRMD_BIN}" ]]; then
    mkdir --parents "${CACHE_DIR}"
    curl --fail --silent --show-error --location \
      "https://chrome-infra-packages.appspot.com/dl/infra/tools/dirmd/linux-amd64/+/latest" \
      --output "${CACHE_DIR}/dirmd.zip"
    unzip -q -o "${CACHE_DIR}/dirmd.zip" -d "${CACHE_DIR}"
    rm --force "${CACHE_DIR}/dirmd.zip"
    chmod +x "${DIRMD_BIN}"
  fi
fi

# Check textproto syntax on each tracked DIR_METADATA file (no-op if none exist).
git ls-files "**/DIR_METADATA" "DIR_METADATA" \
  | xargs --no-run-if-empty "${DIRMD_BIN}" validate

# Check agentic_review semantics (tracked skill/rule paths and RE2 patterns).
"${DIRMD_BIN}" validate-agentic-review .

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

set -ex

LEAF_DER="./leaf.der"
ROOT_DER="./root.der"

LEAF_PEM=$(mktemp)
ROOT_PEM=$(mktemp)
openssl x509 -inform der -in "${LEAF_DER}" -out "${LEAF_PEM}"
openssl x509 -inform der -in "${ROOT_DER}" -out "${ROOT_PEM}"
openssl verify -CAfile "${ROOT_PEM}" "${LEAF_PEM}"

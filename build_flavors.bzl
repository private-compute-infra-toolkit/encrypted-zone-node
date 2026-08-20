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

prod = struct(
    suffix = "",
    features = [],
    rustc_flags = [
        "--codegen=opt-level=3",
        # Absolute smallest footprint to make reverse-engineering as hard as
        # possible.
        "--codegen=strip=symbols",
    ],
    ubuntu = "",
)
debug = struct(
    suffix = "_debug",
    features = ["debug", "test"],
    rustc_flags = [
        "--codegen=opt-level=0",
        "--codegen=debuginfo=full",
        "--codegen=strip=none",
    ],
    ubuntu = "_debug",
)

# perf binary is built with prod optimizations + debug logging.
perf = struct(
    suffix = "_perf",
    features = ["debug"],
    rustc_flags = [
        "--codegen=opt-level=3",
        "--codegen=strip=symbols",
    ],
    ubuntu = "",
)

flavors = (prod, debug, perf)

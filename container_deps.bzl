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

DISTROLESS_USERS = [
    struct(
        flavor = "nonroot",
        uid = 65532,
        user = "nonroot",
        gid = 65532,
        group = "nonroot",
    ),
    struct(
        flavor = "root",
        uid = 0,
        user = "root",
        gid = 0,
        group = "root",
    ),
]

def get_user(user = "nonroot"):
    """
    Extracts a struct with details from DISTROLESS_USERS based on the given user.

    Args:
      user: The user to search for (e.g., "root" or "nonroot").

    Returns:
      The struct with the matching user, or None if no match is found.
    """
    for entry in DISTROLESS_USERS:
        if entry.user == user:
            return entry
    return None

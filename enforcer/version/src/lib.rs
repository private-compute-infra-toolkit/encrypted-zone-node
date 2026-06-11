// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const fn get_first_line(s: &str) -> &str {
    let bytes = s.as_bytes();
    let mut len = 0;
    while len < bytes.len() {
        if bytes[len] == b'\n' || bytes[len] == b'\r' {
            break;
        }
        len += 1;
    }
    unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(bytes.as_ptr(), len)) }
}

/// The version string of the enforcer, read from `../../../version.txt`.
pub const VERSION: &str = get_first_line(include_str!("../../../version.txt"));

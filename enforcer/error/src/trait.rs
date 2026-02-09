// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use ez_error::EzError;

/// Trait to convert module internal errors to EzError.
pub trait ToEzError {
    /// Convert to EzError.
    fn to_ez_error(&self) -> EzError;

    /// Convert directly to tonic::Status. Can be used for brevity.
    fn to_tonic_status(&self) -> tonic::Status {
        self.to_ez_error().to_tonic_status()
    }
}

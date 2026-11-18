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

//! Overrides of an Opaque Isolate's command-line arguments and environment variables, set in its
//! IsolateStartupParameters.

use anyhow::{ensure, Result};
use common_proto::enforcer::v2::IsolateType;
use isolate_startup_parameters_proto::enforcer::v2::IsolateStartupParameters;
use std::collections::HashSet;

/// Checks that the key of every startup argument (`--key=value`) and variable (`KEY=value`)
/// of an Opaque Isolate is listed in allowed_overrides. Only the index of a rejected entry is
/// included in errors, since its value may be secret.
pub(crate) fn validate(
    isolate_type: &IsolateType,
    parameters: &IsolateStartupParameters,
    allowed_overrides: &[String],
) -> Result<()> {
    let allowed: HashSet<&str> = allowed_overrides.iter().map(String::as_str).collect();
    for (i, argument) in parameters.override_command_line_arguments.iter().enumerate() {
        let key = entry_key(argument);
        ensure!(
            argument.contains('=') && key.starts_with("--") && allowed.contains(key),
            "IsolateStartupParameters.override_command_line_arguments[{i}] for Opaque Isolate \
             {isolate_type:?} must be --key=value with --key listed in allowed_overrides"
        );
    }
    let mut keys = HashSet::new();
    for (i, variable) in parameters.override_environment_variables.iter().enumerate() {
        let key = entry_key(variable);
        ensure!(
            variable.contains('=') && !key.starts_with('-') && allowed.contains(key),
            "IsolateStartupParameters.override_environment_variables[{i}] for Opaque Isolate \
             {isolate_type:?} must be KEY=value with KEY listed in allowed_overrides"
        );
        ensure!(
            keys.insert(key),
            "Duplicate key in IsolateStartupParameters.override_environment_variables[{i}] for \
             Opaque Isolate {isolate_type:?}"
        );
    }
    Ok(())
}

/// Removes the entries that have the same key as an override, then appends the overrides.
pub(crate) fn apply(entries: &mut Vec<String>, overrides: &[String]) {
    let keys: HashSet<&str> = overrides.iter().map(|o| entry_key(o)).collect();
    entries.retain(|entry| !keys.contains(entry_key(entry)));
    entries.extend_from_slice(overrides);
}

/// Returns `key` for `key=value`, and the whole entry otherwise.
fn entry_key(entry: &str) -> &str {
    entry.split_once('=').map_or(entry, |(key, _)| key)
}

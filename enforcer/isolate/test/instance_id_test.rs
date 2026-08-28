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

use isolate_info::{InstanceId, InstanceIdGenerator};
use std::collections::HashSet;

#[test]
fn test_instance_id_generator() {
    let id1 = InstanceIdGenerator::generate();
    let id2 = InstanceIdGenerator::generate();

    assert_ne!(id1, id2);
    assert_ne!(&*id1, &*id2);

    let parsed1 = uuid::Uuid::parse_str(&id1);
    let parsed2 = uuid::Uuid::parse_str(&id2);
    assert!(parsed1.is_ok());
    assert!(parsed2.is_ok());
    assert_eq!(parsed1.unwrap().get_version_num(), 4);
    assert_eq!(parsed2.unwrap().get_version_num(), 4);
}

#[test]
fn test_instance_id_type() {
    let id: InstanceId = InstanceIdGenerator::generate();
    assert_eq!(&*id, id.to_string());
    assert!(!id.is_empty());
    // A canonical hyphenated UUID (8-4-4-4-12 format) is exactly 36 characters long.
    assert_eq!(
        id.len(),
        36,
        "InstanceId must have a length of 36 characters because it represents a UUID"
    );

    let string_val: String = id.clone().into();
    assert_eq!(string_val, &*id);

    let id_clone = id.clone();
    assert_eq!(id, id_clone);

    let mut set = HashSet::new();
    set.insert(id.clone());
    assert!(set.contains(&id_clone));

    assert!(format!("{id:?}").starts_with("InstanceId("));
}

#[test]
fn test_instance_id_generator_debug() {
    let generator = InstanceIdGenerator;
    assert_eq!(format!("{generator:?}"), "InstanceIdGenerator");
}

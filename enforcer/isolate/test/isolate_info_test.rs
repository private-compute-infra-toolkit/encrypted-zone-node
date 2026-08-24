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

use isolate_info::{
    get_isolate_name, get_isolate_type, register_isolate_type, BinaryServicesIndex, IsolateType,
};

#[test]
fn test_get_isolate_name_registered() {
    let index = BinaryServicesIndex::new(false);
    let isolate_type = IsolateType {
        publisher_id: "test_pub".to_string(),
        isolate_name: "test_isolate".to_string(),
    };
    register_isolate_type(index, isolate_type.clone());

    assert_eq!(get_isolate_type(&index), Some(isolate_type));
    assert_eq!(get_isolate_name(&index), "test_isolate");
}

#[test]
fn test_get_isolate_name_unregistered() {
    let index = BinaryServicesIndex::new(false);
    assert_eq!(get_isolate_name(&index), format!("{:?}", index));
}

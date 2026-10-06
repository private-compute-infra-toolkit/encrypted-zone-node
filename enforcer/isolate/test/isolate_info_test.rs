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

use common_proto::enforcer::v2::IsolateType;
use isolate_info::{
    get_binary_services_index, get_isolate_name, get_isolate_type, register_isolate_type,
    BinaryServicesIndex, IsolateId, IsolateServiceIndex, Route,
};

#[test]
fn test_get_isolate_name_registered() {
    let index = BinaryServicesIndex::new(false);
    let isolate_type = IsolateType {
        publisher_id: "test_pub".to_string(),
        isolate_name: "test_isolate".to_string(),
    };
    register_isolate_type(index, isolate_type.clone());

    assert_eq!(get_isolate_type(&index), Some(isolate_type.clone()));
    assert_eq!(get_binary_services_index(&isolate_type), Some(index));
    assert_eq!(get_isolate_name(&index), "test_isolate");
}

#[test]
fn test_get_isolate_name_unregistered() {
    let index = BinaryServicesIndex::new(false);
    assert_eq!(get_isolate_type(&index), None);
    assert_eq!(get_isolate_name(&index), format!("{:?}", index));
    let unregistered_type = IsolateType {
        publisher_id: "nonexistent_pub".to_string(),
        isolate_name: "nonexistent_isolate".to_string(),
    };
    assert_eq!(get_binary_services_index(&unregistered_type), None);
}

#[test]
fn test_binary_services_index() {
    let ratified_index = BinaryServicesIndex::new(true);
    assert!(ratified_index.is_ratified_binary());

    let opaque_index = BinaryServicesIndex::new(false);
    assert!(!opaque_index.is_ratified_binary());

    let ratified_isolate_id = IsolateId::new(ratified_index);
    assert!(ratified_isolate_id.is_ratified_isolate());
    assert_eq!(ratified_isolate_id.get_binary_services_index(), ratified_index);

    let service_index =
        IsolateServiceIndex::new(Some(ratified_index), "example.com", false).unwrap();
    assert_eq!(service_index.get_binary_services_index(), Some(ratified_index));
    assert_eq!(service_index.get_request_route(), Route::Internal);
}

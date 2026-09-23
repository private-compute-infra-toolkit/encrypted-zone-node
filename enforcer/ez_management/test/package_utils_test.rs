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

use ez_management::package_utils::calculate_sha256;

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const HELLO_WORLD_SHA256: &str = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";

#[test]
fn test_calculate_sha256_empty() {
    assert_eq!(calculate_sha256(b""), EMPTY_SHA256);
}

#[test]
fn test_calculate_sha256_known_string() {
    assert_eq!(calculate_sha256(b"hello world"), HELLO_WORLD_SHA256);
}

use common_proto::enforcer::v2::IsolateType;
use ez_management::package_utils::{
    AssembledPackage, PackageAccumulator, ENV_MAX_PACKAGE_SIZE_BYTES,
};
use ez_management_proto::enforcer::v2::IsolatePackageChunk;

fn isolate_type(isolate_name: &str) -> IsolateType {
    IsolateType { isolate_name: isolate_name.to_string(), publisher_id: "adtech.com".to_string() }
}

#[test]
fn test_accumulator_single_chunk() {
    let mut accumulator = PackageAccumulator::new();
    assert!(!accumulator.current_isolate_type.is_some());

    let chunk = IsolatePackageChunk {
        isolate_type: Some(isolate_type("test_isolate")),
        chunk_sequence: 0,
        package_tar_chunk: b"hello world".to_vec(),
        is_last_chunk: true,
        isolate_package_endorsements: b"test_endorsement".to_vec(),
    };

    let result = accumulator.push_chunk(chunk).expect("Should successfully push single chunk");
    assert_eq!(
        result,
        Some(AssembledPackage {
            isolate_type: isolate_type("test_isolate"),
            package_bytes: b"hello world".to_vec(),
            endorsements: b"test_endorsement".to_vec(),
        })
    );
    assert!(!accumulator.current_isolate_type.is_some());
    assert_eq!(accumulator.expected_sequence, 0);
}

#[test]
fn test_accumulator_multi_chunk() {
    let mut accumulator = PackageAccumulator::new();

    let chunk0 = IsolatePackageChunk {
        isolate_type: Some(isolate_type("test_isolate")),
        chunk_sequence: 0,
        package_tar_chunk: b"chunk_0_data,".to_vec(),
        is_last_chunk: false,
        isolate_package_endorsements: b"endorsements_data".to_vec(),
    };

    let res0 = accumulator.push_chunk(chunk0).expect("Chunk 0 should succeed");
    assert!(res0.is_none());
    assert!(accumulator.current_isolate_type.is_some());
    assert_eq!(accumulator.current_isolate_type, Some(isolate_type("test_isolate")));
    assert_eq!(accumulator.buffer, b"chunk_0_data,");
    assert_eq!(accumulator.expected_sequence, 1);
    assert_eq!(accumulator.endorsements, b"endorsements_data");

    let chunk1 = IsolatePackageChunk {
        isolate_type: None, // subsequent chunks can omit the isolate_type
        chunk_sequence: 1,
        package_tar_chunk: b"chunk_1_data,".to_vec(),
        is_last_chunk: false,
        isolate_package_endorsements: vec![],
    };

    let res1 = accumulator.push_chunk(chunk1).expect("Chunk 1 should succeed");
    assert!(res1.is_none());
    assert_eq!(accumulator.expected_sequence, 2);

    let chunk2 = IsolatePackageChunk {
        isolate_type: Some(isolate_type("test_isolate")),
        chunk_sequence: 2,
        package_tar_chunk: b"chunk_2_data".to_vec(),
        is_last_chunk: true,
        isolate_package_endorsements: vec![],
    };

    let res2 = accumulator.push_chunk(chunk2).expect("Chunk 2 should succeed");
    assert_eq!(
        res2,
        Some(AssembledPackage {
            isolate_type: isolate_type("test_isolate"),
            package_bytes: b"chunk_0_data,chunk_1_data,chunk_2_data".to_vec(),
            endorsements: b"endorsements_data".to_vec(),
        })
    );
    assert!(!accumulator.current_isolate_type.is_some());
    assert_eq!(accumulator.expected_sequence, 0);
}

#[test]
fn test_accumulator_wrong_isolate_type() {
    let mut accumulator = PackageAccumulator::new();

    let chunk0 = IsolatePackageChunk {
        isolate_type: Some(isolate_type("test_isolate")),
        chunk_sequence: 0,
        package_tar_chunk: b"hello".to_vec(),
        is_last_chunk: false,
        ..Default::default()
    };
    accumulator.push_chunk(chunk0).unwrap();

    let chunk1 = IsolatePackageChunk {
        isolate_type: Some(isolate_type("wrong_isolate")),
        chunk_sequence: 1,
        package_tar_chunk: b"world".to_vec(),
        is_last_chunk: false,
        ..Default::default()
    };
    let result = accumulator.push_chunk(chunk1);
    assert!(result.is_err());
    assert!(!accumulator.current_isolate_type.is_some());
}

#[test]
fn test_accumulator_same_isolate_name_different_publisher_rejected() {
    let mut accumulator = PackageAccumulator::new();

    let chunk0 = IsolatePackageChunk {
        isolate_type: Some(IsolateType {
            isolate_name: "test_isolate".to_string(),
            publisher_id: "adtech.com".to_string(),
        }),
        chunk_sequence: 0,
        package_tar_chunk: b"hello".to_vec(),
        is_last_chunk: false,
        ..Default::default()
    };
    accumulator.push_chunk(chunk0).unwrap();

    let chunk1 = IsolatePackageChunk {
        isolate_type: Some(IsolateType {
            isolate_name: "test_isolate".to_string(),
            publisher_id: "other-publisher.com".to_string(),
        }),
        chunk_sequence: 1,
        package_tar_chunk: b"world".to_vec(),
        is_last_chunk: false,
        ..Default::default()
    };
    let result = accumulator.push_chunk(chunk1);
    assert!(result.is_err());
    assert!(!accumulator.current_isolate_type.is_some());
}

#[test]
fn test_accumulator_wrong_sequence() {
    let mut accumulator = PackageAccumulator::new();

    let chunk0 = IsolatePackageChunk {
        isolate_type: Some(isolate_type("test_isolate")),
        chunk_sequence: 0,
        package_tar_chunk: b"hello".to_vec(),
        is_last_chunk: false,
        ..Default::default()
    };
    accumulator.push_chunk(chunk0).unwrap();

    let chunk_bad = IsolatePackageChunk {
        isolate_type: Some(isolate_type("test_isolate")),
        chunk_sequence: 2, // Expected 1
        package_tar_chunk: b"world".to_vec(),
        is_last_chunk: false,
        ..Default::default()
    };
    let result = accumulator.push_chunk(chunk_bad);
    assert!(result.is_err());
    assert!(!accumulator.current_isolate_type.is_some());
}

#[test]
fn test_accumulator_missing_isolate_type_on_first_chunk() {
    let mut accumulator = PackageAccumulator::new();

    let chunk = IsolatePackageChunk {
        isolate_type: None,
        chunk_sequence: 0,
        package_tar_chunk: b"hello".to_vec(),
        is_last_chunk: false,
        ..Default::default()
    };
    let result = accumulator.push_chunk(chunk);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("without isolate_type"));
    assert!(!accumulator.current_isolate_type.is_some());
}

#[test]
fn test_accumulator_max_size_limit_exceeded() {
    let mut accumulator = PackageAccumulator::new();
    accumulator.max_package_size_bytes = 10;

    let chunk0 = IsolatePackageChunk {
        isolate_type: Some(isolate_type("test_isolate")),
        chunk_sequence: 0,
        package_tar_chunk: b"12345678".to_vec(), // 8 bytes <= 10 bytes
        is_last_chunk: false,
        ..Default::default()
    };
    accumulator.push_chunk(chunk0).expect("Chunk under limit should succeed");

    let chunk1 = IsolatePackageChunk {
        isolate_type: Some(isolate_type("test_isolate")),
        chunk_sequence: 1,
        package_tar_chunk: b"12345".to_vec(), // 8 + 5 = 13 bytes > 10 bytes
        is_last_chunk: true,
        ..Default::default()
    };
    let result = accumulator.push_chunk(chunk1);
    assert!(result.is_err());
    let err_str = result.unwrap_err().to_string();
    assert!(err_str.contains("exceeds maximum allowed size"));
    assert!(!accumulator.current_isolate_type.is_some());
}

#[test]
fn test_accumulator_max_size_from_env_var() {
    std::env::set_var(ENV_MAX_PACKAGE_SIZE_BYTES, "15");
    let mut accumulator = PackageAccumulator::new();
    assert_eq!(accumulator.max_package_size_bytes, 15);

    let chunk = IsolatePackageChunk {
        isolate_type: Some(isolate_type("test_isolate")),
        chunk_sequence: 0,
        package_tar_chunk: vec![0u8; 20], // 20 bytes > 15 bytes
        is_last_chunk: true,
        ..Default::default()
    };
    let result = accumulator.push_chunk(chunk);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("exceeds maximum allowed size of 15 bytes"));
    std::env::remove_var(ENV_MAX_PACKAGE_SIZE_BYTES);
}

#[test]
fn test_accumulator_reset() {
    let mut accumulator = PackageAccumulator::new();

    let chunk0 = IsolatePackageChunk {
        isolate_type: Some(isolate_type("test_isolate")),
        chunk_sequence: 0,
        package_tar_chunk: b"hello".to_vec(),
        is_last_chunk: false,
        isolate_package_endorsements: b"endorsement".to_vec(),
    };
    accumulator.push_chunk(chunk0).unwrap();
    assert!(accumulator.current_isolate_type.is_some());

    accumulator.reset();
    assert!(!accumulator.current_isolate_type.is_some());
    assert_eq!(accumulator.current_isolate_type, None);
    assert_eq!(accumulator.buffer, b"");
    assert_eq!(accumulator.expected_sequence, 0);
    assert_eq!(accumulator.endorsements, b"");
}

#[test]
fn test_accumulator_invalid_env_var() {
    std::env::set_var(ENV_MAX_PACKAGE_SIZE_BYTES, "not_a_valid_number");
    let accumulator = PackageAccumulator::new();
    assert_eq!(
        accumulator.max_package_size_bytes,
        ez_management::package_utils::DEFAULT_MAX_PACKAGE_SIZE_BYTES
    );
    std::env::remove_var(ENV_MAX_PACKAGE_SIZE_BYTES);
}

#[test]
fn test_accumulator_endorsements_on_chunk_nonzero_rejected() {
    let mut accumulator = PackageAccumulator::new();
    let chunk0 = IsolatePackageChunk {
        isolate_type: Some(isolate_type("test_isolate")),
        chunk_sequence: 0,
        package_tar_chunk: b"chunk0".to_vec(),
        is_last_chunk: false,
        ..Default::default()
    };
    accumulator.push_chunk(chunk0).unwrap();

    let chunk1 = IsolatePackageChunk {
        isolate_type: Some(isolate_type("test_isolate")),
        chunk_sequence: 1,
        package_tar_chunk: b"chunk1".to_vec(),
        is_last_chunk: true,
        isolate_package_endorsements: b"late_endorsement".to_vec(),
    };
    let result = accumulator.push_chunk(chunk1);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("expected only on initial chunk"));
}

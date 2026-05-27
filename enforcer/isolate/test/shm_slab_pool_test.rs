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

use payload_proto::enforcer::v1::ShmSlotReference;
use shm_slab_pool::{ShmSlabPool, ShmSlabPoolError, ShmSlabPoolOptions};
use std::os::unix::fs::FileExt;

const TEST_PAYLOAD: &[u8] = b"this is a test payload";
const TEST_PAYLOAD_SIZE: usize = TEST_PAYLOAD.len();

// Verifies basic lifecycle.
#[tokio::test]
async fn test_shm_slab_pool_basic_and_layout() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shm_slab").to_string_lossy().to_string();
    let num_slots = 64;
    let block_size = 4;

    let pool = ShmSlabPool::new(ShmSlabPoolOptions {
        file_name: path.clone(),
        number_of_slots: num_slots,
        slot_size: block_size,
        writer: true,
    })
    .expect("Failed to initialize ShmSlabPool");

    // 1. Read raw file header to verify bitmasks are initialized to 0
    let header_path = format!("{}-atomic-hdr", path);
    let file = std::fs::File::open(&header_path).expect("Failed to open raw header backing file");
    let mut mask1_bytes = [0u8; 8];
    file.read_exact_at(&mut mask1_bytes, 0).expect("Failed to read initial bitmask");
    let expected_initial_mask = 0u64;
    assert_eq!(u64::from_ne_bytes(mask1_bytes), expected_initial_mask);

    // 2. Write standard data (22 bytes needs 6 blocks of size 4)
    let slot_refs =
        pool.write_to_pool(TEST_PAYLOAD).await.expect("Failed to write data to ShmSlabPool");
    let expected_first_reference = ShmSlotReference { slot_index: 0, length: 4 };
    let expected_last_reference = ShmSlotReference { slot_index: 5, length: 2 };
    assert_eq!(slot_refs.len(), 6);
    assert_eq!(slot_refs.first(), Some(&expected_first_reference));
    assert_eq!(slot_refs.last(), Some(&expected_last_reference));

    // 3. Re-read raw header to ensure bits 0 to 5 are flipped in the mask
    file.read_exact_at(&mut mask1_bytes, 0).expect("Failed to re-read active bitmask");
    assert_eq!(u64::from_ne_bytes(mask1_bytes), expected_initial_mask | 0b111111);

    // 4. Read the data back and verify integrity
    let read_data =
        pool.read_from_pool(&slot_refs).expect("Failed to read data back from ShmSlabPool");
    assert_eq!(TEST_PAYLOAD, &read_data[..]);
}

// Verifies isolation and concurrency stress.
#[tokio::test]
async fn test_shm_slab_pool_concurrency_isolation_and_stress() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shm_slab").to_string_lossy().to_string();
    let num_slots = 150;
    let block_size = 4;

    let pool = std::sync::Arc::new(
        ShmSlabPool::new(ShmSlabPoolOptions {
            file_name: path.clone(),
            number_of_slots: num_slots,
            slot_size: block_size,
            writer: true,
        })
        .expect("Failed to create locked memory isolation pool"),
    );

    // Thread 1 locks TEST_PAYLOAD and holds it
    let t1_refs =
        pool.write_to_pool(TEST_PAYLOAD).await.expect("Failed to claim initial locked blocks");
    let expected_refs = TEST_PAYLOAD_SIZE.div_ceil(block_size as usize);
    assert_eq!(t1_refs.len(), expected_refs);

    // Spawn 10 stress tasks hammer the pool concurrently around the locked memory
    let mut handles = Vec::new();
    for thread_id in 0..10 {
        let p_clone = pool.clone();
        let t1_refs_clone = t1_refs.clone();
        let h = tokio::spawn(async move {
            let payload = vec![thread_id as u8; TEST_PAYLOAD_SIZE];
            for _ in 0..15 {
                let t2_refs = p_clone.write_to_pool(&payload).await.expect("Stress write failed");
                assert_eq!(t2_refs.len(), 6);

                // Prove it allocated entirely distinct slots (not reusing t1's slots)
                for r2 in &t2_refs {
                    for r1 in &t1_refs_clone {
                        assert_ne!(r2.slot_index, r1.slot_index);
                    }
                }

                let read_back = p_clone.read_from_pool(&t2_refs).expect("Stress read failed");
                assert_eq!(read_back, payload);
            }
        });
        handles.push(h);
    }

    for handle in handles {
        handle.await.expect("Stress test task panicked");
    }

    // Finally verify T1's locked data is perfectly intact
    let t1_read = pool.read_from_pool(&t1_refs).expect("Failed to finalize locked block readout");
    assert_eq!(t1_read[..TEST_PAYLOAD_SIZE], *TEST_PAYLOAD);
}

// Verifies that the pool correctly blocks and waits until there is sufficient
// space to satisfy a large payload request, even if some slots are initially free.
#[tokio::test]
async fn test_shm_slab_pool_wait_for_sufficient_space() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shm_slab").to_string_lossy().to_string();
    let num_slots = 64;
    let block_size = 4;

    let pool = std::sync::Arc::new(
        ShmSlabPool::new(ShmSlabPoolOptions {
            file_name: path.clone(),
            number_of_slots: num_slots,
            slot_size: block_size,
            writer: true,
        })
        .expect("Failed to initialize pool"),
    );

    // Claim 59 slots, leaving 5 available.
    let small_payload = vec![1u8; 59 * 4];
    let small_refs = pool.write_to_pool(&small_payload).await.expect("Failed to lock fragment");
    assert_eq!(small_refs.len(), 59);

    // Request a payload needing 6 slots. It should block since only 5 are free.
    let p_clone = pool.clone();
    let frag_handle = tokio::spawn(async move {
        p_clone.write_to_pool(TEST_PAYLOAD).await.expect("Worker failed to allocate after waiting")
    });

    // A small sleep is required here to give the background worker task enough time
    // to spawn, attempt its allocation, and fall into its retry/backoff wait loop.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Reading the locked fragment provides the 59 slots, allowing the worker to proceed.
    pool.read_from_pool(&small_refs).expect("Failed to free fragment");

    let frag_refs = frag_handle.await.expect("Worker task panicked");
    assert_eq!(frag_refs.len(), 6);
}

// Tests boundary condition failures, ensuring inherently oversized payloads
// immediately fail while permanently blocked allocations return timeout errors.
#[tokio::test]
async fn test_shm_slab_pool_boundary_failures() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shm_slab").to_string_lossy().to_string();
    let num_slots = 64;
    let block_size = 4;

    let pool = ShmSlabPool::new(ShmSlabPoolOptions {
        file_name: path.clone(),
        number_of_slots: num_slots,
        slot_size: block_size,
        writer: true,
    })
    .expect("Failed to initialize boundary failures pool");

    // 1. Immediate failure test
    let huge_payload = vec![0u8; 300]; // 300 bytes > 256 bytes capacity
    let res1 = pool.write_to_pool(&huge_payload).await;
    assert!(res1.is_err());
    assert!(matches!(res1.unwrap_err(), ShmSlabPoolError::OversizedPayload { .. }));

    // 2. Timeout failure test
    let fill_payload = vec![0u8; 64 * 4];
    let refs = pool.write_to_pool(&fill_payload).await.expect("Failed to fill pool");
    assert_eq!(refs.len(), 64);

    let res2 = pool.write_to_pool(TEST_PAYLOAD).await;
    assert!(res2.is_err());
    assert!(matches!(res2.unwrap_err(), ShmSlabPoolError::AllocationTimeout));
}

// Verifies that calling write_to_pool on a pool configured as a reader (writer: false)
// returns an InvalidPermission error instead of panicking or segfaulting.
#[tokio::test]
async fn test_shm_slab_pool_reader_cannot_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shm_slab_reader").to_string_lossy().to_string();

    let pool = ShmSlabPool::new(ShmSlabPoolOptions {
        file_name: path,
        number_of_slots: 64,
        slot_size: 4,
        writer: false,
    })
    .expect("Failed to initialize reader pool");

    let res = pool.write_to_pool(TEST_PAYLOAD).await;
    assert!(res.is_err());
    assert!(matches!(res.unwrap_err(), ShmSlabPoolError::InvalidWritePermission));
}

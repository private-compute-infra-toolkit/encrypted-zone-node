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

// The tests below cover b/556034613: `ShmSlotReference` arrives from the peer over the wire, so
// `read_from_pool` must reject hostile indices and lengths with an error rather than performing
// out-of-bounds address arithmetic (historically a wild atomic read-modify-write in `free_slots`)
// or panicking. Every case must leave the process alive and the pool usable.

const MALICIOUS_POOL_SLOTS: u64 = 64;
const MALICIOUS_POOL_SLOT_SIZE: u64 = 4;

// Builds a writable pool with a known geometry for the hostile input tests.
fn new_test_pool(dir: &tempfile::TempDir, number_of_slots: u64, slot_size: u64) -> ShmSlabPool {
    let path = dir.path().join("shm_slab").to_string_lossy().to_string();
    ShmSlabPool::new(ShmSlabPoolOptions {
        file_name: path,
        number_of_slots,
        slot_size,
        writer: true,
    })
    .expect("Failed to initialize ShmSlabPool")
}

// Reads a single hostile slot reference out of a standard 64 x 4 pool.
fn read_one(pool: &ShmSlabPool, slot_index: i64, length: i64) -> ShmSlabPoolError {
    pool.read_from_pool(&[ShmSlotReference { slot_index, length }])
        .expect_err("read_from_pool must reject an out-of-bounds slot reference")
}

// The reproducer index from b/556034613: slot_index = 2^44, which nothing bounded before the fix.
// On this geometry the byte offset (2^46) fell outside the data mapping and panicked while
// slicing it; the wrapping and zero-geometry tests below cover the variants that instead reached
// free_slots. Every one of them killed the enforcer.
#[tokio::test]
async fn test_read_from_pool_rejects_wild_atomic_write_slot_index() {
    let dir = tempfile::tempdir().unwrap();
    let pool = new_test_pool(&dir, MALICIOUS_POOL_SLOTS, MALICIOUS_POOL_SLOT_SIZE);

    // length = 0 leaves the requested data range empty, so the index bound is the only thing that
    // can reject this: validation must never rely on the copy itself failing.
    let err = read_one(&pool, 1 << 44, 0);
    assert!(
        matches!(err, ShmSlabPoolError::InvalidSlotIndex { slot_index, .. } if slot_index == 1 << 44),
        "expected InvalidSlotIndex, got {err:?}"
    );

    // The pool must still work afterwards.
    let slot_refs = pool.write_to_pool(TEST_PAYLOAD).await.expect("Pool should still be usable");
    assert_eq!(pool.read_from_pool(&slot_refs).expect("Read should succeed"), TEST_PAYLOAD);
}

#[tokio::test]
async fn test_read_from_pool_rejects_out_of_range_slot_index() {
    let dir = tempfile::tempdir().unwrap();
    let pool = new_test_pool(&dir, MALICIOUS_POOL_SLOTS, MALICIOUS_POOL_SLOT_SIZE);

    // The first index past the end of the pool, and the largest index representable on the wire.
    for slot_index in [MALICIOUS_POOL_SLOTS as i64, i64::MAX] {
        let err = read_one(&pool, slot_index, 0);
        assert!(
            matches!(err, ShmSlabPoolError::InvalidSlotIndex { .. }),
            "slot_index {slot_index} should be rejected, got {err:?}"
        );
    }
}

// A slot_index chosen so that `slot_index * slot_size` wraps back into the mapping. This is the
// "release-mode wrapping" variant: the read looks in bounds, which previously let the caller fall
// through into the unbounded free_slots arithmetic.
#[tokio::test]
async fn test_read_from_pool_rejects_wrapping_slot_index() {
    let dir = tempfile::tempdir().unwrap();
    let pool = new_test_pool(&dir, 64, 4096);

    // 2^52 * 2^12 == 2^64, which wraps to a data offset of 0.
    let err = read_one(&pool, 1 << 52, 0);
    assert!(
        matches!(err, ShmSlabPoolError::InvalidSlotIndex { .. }),
        "a wrapping slot_index should be rejected, got {err:?}"
    );
}

// slot_index is an int64 on the wire, so a peer can send a negative value which becomes an
// enormous offset when cast to u64.
#[tokio::test]
async fn test_read_from_pool_rejects_negative_slot_index() {
    let dir = tempfile::tempdir().unwrap();
    let pool = new_test_pool(&dir, MALICIOUS_POOL_SLOTS, MALICIOUS_POOL_SLOT_SIZE);

    for slot_index in [-1, i64::MIN] {
        let err = read_one(&pool, slot_index, 0);
        assert!(
            matches!(err, ShmSlabPoolError::InvalidSlotIndex { .. }),
            "slot_index {slot_index} should be rejected, got {err:?}"
        );
    }
}

// length is also wire-supplied: negative values wrap when cast, and an oversized length would read
// past the end of the slot into neighbouring slots.
#[tokio::test]
async fn test_read_from_pool_rejects_invalid_length() {
    let dir = tempfile::tempdir().unwrap();
    let pool = new_test_pool(&dir, MALICIOUS_POOL_SLOTS, MALICIOUS_POOL_SLOT_SIZE);

    for length in [-1, i64::MIN, MALICIOUS_POOL_SLOT_SIZE as i64 + 1, i64::MAX] {
        let err = read_one(&pool, 0, length);
        assert!(
            matches!(err, ShmSlabPoolError::InvalidSlotLength { .. }),
            "length {length} should be rejected, got {err:?}"
        );
    }

    // A length exactly equal to the slot size is legitimate and must still be accepted.
    let slot_refs = pool.write_to_pool(TEST_PAYLOAD).await.expect("Failed to write to pool");
    assert_eq!(slot_refs[0].length, MALICIOUS_POOL_SLOT_SIZE as i64);
    assert_eq!(pool.read_from_pool(&slot_refs).expect("Read should succeed"), TEST_PAYLOAD);
}

// Without a cap on the batch size a peer can force an unbounded allocation in the enforcer by
// sending far more slot references than the pool could ever contain.
#[tokio::test]
async fn test_read_from_pool_rejects_too_many_slot_references() {
    let dir = tempfile::tempdir().unwrap();
    let pool = new_test_pool(&dir, MALICIOUS_POOL_SLOTS, MALICIOUS_POOL_SLOT_SIZE);

    let slot_refs = vec![
        ShmSlotReference { slot_index: 0, length: MALICIOUS_POOL_SLOT_SIZE as i64 };
        MALICIOUS_POOL_SLOTS as usize + 1
    ];
    let err = pool.read_from_pool(&slot_refs).expect_err("Oversized batch should be rejected");
    assert!(
        matches!(err, ShmSlabPoolError::TooManySlotReferences { requested, .. } if requested == 65),
        "expected TooManySlotReferences, got {err:?}"
    );
}

// Validation happens up front for the whole batch, so a request mixing valid and hostile
// references must be rejected without freeing anything.
#[tokio::test]
async fn test_read_from_pool_rejects_invalid_batch_without_freeing_slots() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shm_slab").to_string_lossy().to_string();
    let pool = ShmSlabPool::new(ShmSlabPoolOptions {
        file_name: path.clone(),
        number_of_slots: MALICIOUS_POOL_SLOTS,
        slot_size: MALICIOUS_POOL_SLOT_SIZE,
        writer: true,
    })
    .expect("Failed to initialize ShmSlabPool");

    // 22 bytes over 4-byte slots claims slots 0..=5, i.e. bits 0-5 of the first bitmask.
    let slot_refs = pool.write_to_pool(TEST_PAYLOAD).await.expect("Failed to write to pool");
    let header_file = std::fs::File::open(format!("{path}-atomic-hdr"))
        .expect("Failed to open raw header backing file");
    let mut mask_bytes = [0u8; 8];
    header_file.read_exact_at(&mut mask_bytes, 0).expect("Failed to read bitmask");
    assert_eq!(u64::from_ne_bytes(mask_bytes), 0b111111);

    // Append a hostile reference to an otherwise valid batch.
    let mut hostile_refs = slot_refs.clone();
    hostile_refs.push(ShmSlotReference { slot_index: 1 << 44, length: 0 });
    let err = pool.read_from_pool(&hostile_refs).expect_err("Hostile batch should be rejected");
    assert!(matches!(err, ShmSlabPoolError::InvalidSlotIndex { .. }), "got {err:?}");

    // The bitmask must be untouched: the valid slots in the rejected batch were not freed.
    header_file.read_exact_at(&mut mask_bytes, 0).expect("Failed to re-read bitmask");
    assert_eq!(
        u64::from_ne_bytes(mask_bytes),
        0b111111,
        "a rejected batch must not mutate the allocation bitmask"
    );

    // And the original, valid batch still reads back correctly and frees its slots.
    assert_eq!(pool.read_from_pool(&slot_refs).expect("Read should succeed"), TEST_PAYLOAD);
    header_file.read_exact_at(&mut mask_bytes, 0).expect("Failed to re-read bitmask");
    assert_eq!(u64::from_ne_bytes(mask_bytes), 0);
}

// Both --shm_num_slots and --shm_slot_size default to 0, which yields an empty mapping. With
// slot_size = 0 every index used to produce a data offset of 0, so the empty-slice read succeeded
// and handed any index straight to free_slots.
#[tokio::test]
async fn test_zero_geometry_pool_rejects_all_slot_references() {
    let dir = tempfile::tempdir().unwrap();
    let pool = new_test_pool(&dir, 0, 0);

    // The pool holds no slots at all, so even a single reference exceeds its capacity.
    let err = read_one(&pool, 1 << 44, 0);
    assert!(
        matches!(err, ShmSlabPoolError::TooManySlotReferences { .. }),
        "expected TooManySlotReferences, got {err:?}"
    );

    // An empty batch remains a well-defined no-op.
    assert!(pool.read_from_pool(&[]).expect("Empty batch should succeed").is_empty());
}

// The mirror image of the hostile-input tests: the largest legitimate reference sits exactly on
// the upper bound of the data mapping, and a batch of number_of_slots references sits exactly on
// the TooManySlotReferences cap, so an off-by-one in either check would reject real traffic.
#[tokio::test]
async fn test_read_from_pool_accepts_maximum_legitimate_slot_references() {
    let dir = tempfile::tempdir().unwrap();
    let pool = new_test_pool(&dir, MALICIOUS_POOL_SLOTS, MALICIOUS_POOL_SLOT_SIZE);

    // A payload that exactly fills the pool: 64 slots x 4 bytes = 256 bytes.
    let pool_capacity = (MALICIOUS_POOL_SLOTS * MALICIOUS_POOL_SLOT_SIZE) as usize;
    let payload: Vec<u8> = (0..pool_capacity).map(|byte| byte as u8).collect();

    let slot_refs = pool.write_to_pool(&payload).await.expect("Failed to fill the pool");

    // Exactly number_of_slots references, which is the largest batch the cap must still accept.
    assert_eq!(slot_refs.len() as u64, MALICIOUS_POOL_SLOTS);

    // The final slot spans the last byte of the mapping: start = 63 * 4 = 252, end = 256 = len.
    assert!(
        slot_refs.contains(&ShmSlotReference {
            slot_index: MALICIOUS_POOL_SLOTS as i64 - 1,
            length: MALICIOUS_POOL_SLOT_SIZE as i64,
        }),
        "expected a reference to the last slot, got {slot_refs:?}"
    );

    assert_eq!(pool.read_from_pool(&slot_refs).expect("Maximal batch must be accepted"), payload);
}

// A peer can reference the same written slot many times to inflate one slot into a pool-sized
// payload and to clear the same allocation bit repeatedly. See b/556034775.
#[tokio::test]
async fn test_read_from_pool_rejects_duplicate_slot_references() {
    let dir = tempfile::tempdir().unwrap();
    let pool = new_test_pool(&dir, MALICIOUS_POOL_SLOTS, MALICIOUS_POOL_SLOT_SIZE);

    let slot_refs = pool.write_to_pool(TEST_PAYLOAD).await.expect("Failed to write to pool");
    let hostile_refs = vec![slot_refs[0]; MALICIOUS_POOL_SLOTS as usize];

    let err = pool.read_from_pool(&hostile_refs).expect_err("Duplicates should be rejected");
    assert!(matches!(err, ShmSlabPoolError::DuplicateSlotReference { .. }), "got {err:?}");

    // The rejected batch had no side effects, so the original batch still reads back.
    assert_eq!(pool.read_from_pool(&slot_refs).expect("Read should succeed"), TEST_PAYLOAD);
}

#[tokio::test]
async fn test_write_to_pool_empty_payload_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let pool = new_test_pool(&dir, MALICIOUS_POOL_SLOTS, MALICIOUS_POOL_SLOT_SIZE);

    let slot_refs = pool.write_to_pool(&[]).await.expect("Empty payload write should succeed");
    assert!(slot_refs.is_empty());
    assert_eq!(pool.get_cas_failures(), 0);
}

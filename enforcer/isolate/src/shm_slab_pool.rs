// Copyright 2025 Google LLC
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
use std::fs::OpenOptions;
use std::os::unix::fs::FileExt; // Required by write_all_at
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;
use tokio_retry::Retry;

#[derive(Debug, Error)]
pub enum ShmSlabPoolError {
    #[error(
        "Slot allocation failed: requested slots ({requested}) exceeds pool capacity ({capacity})"
    )]
    OversizedAllocation { requested: u64, capacity: u64 },

    #[error(
        "Slot allocation failed: payload size ({payload_size} bytes) exceeds pool capacity ({capacity} bytes)"
    )]
    OversizedPayload { payload_size: u64, capacity: u64 },

    #[error(
        "Slot allocation pass failed: unable to make progress claiming requested slots ({requested}) due to contention or full pool"
    )]
    AllocationContention { requested: u64 },

    #[error("Timed out waiting for free slots in ShmSlabPool")]
    AllocationTimeout,

    #[error(
        "Operation not permitted: attempted to write to a pool opened with read-only permissions"
    )]
    InvalidWritePermission,

    #[error("Shared memory file operation failed: {0}")]
    FileOperationFailed(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, ShmSlabPoolError>;

const SHM_HEADER: &str = "atomic-hdr";

// This enum wraps both read-only (Mmap) and read-write (MmapMut) memory mappings.
// It is required because Rust struct fields must have a single concrete type at compile
// time, but ShmSlabPool supports opening backing files as read-only (where MmapMut is
// rejected by the OS due to missing write permissions) or read-write.
// Using an enum allows us to enforce strict, OS-level access control at runtime
// with zero heap allocation or dynamic dispatch overhead.
#[derive(Debug)]
enum DataMmap {
    Read(memmap2::Mmap),
    Write(memmap2::MmapMut),
}

// Implements Deref so we can index or read from the mapped memory slice identically
// regardless of whether it is mapped as read-only or read-write.
impl std::ops::Deref for DataMmap {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        match self {
            DataMmap::Read(mmap) => &mmap[..],
            DataMmap::Write(mmap) => &mmap[..],
        }
    }
}

impl DataMmap {
    // Returns a raw read-only pointer to the start of the memory-mapped region.
    // In write_to_pool, this pointer is cast to `*mut u8` to write to the region.
    // This is safe because write_to_pool explicitly verifies that the enum variant is Write.
    fn as_ptr(&self) -> *const u8 {
        match self {
            DataMmap::Read(mmap) => mmap.as_ptr(),
            DataMmap::Write(mmap) => mmap.as_ptr(),
        }
    }
}

// Configuration options for initializing a `ShmSlabPool`.
// Note: The same pool instance can be opened for both reading and writing if needed.
// A pool is always opened with Read permissions because memory-mapping a file
// inherently requires read permissions at the OS file descriptor level (e.g. O_RDONLY
// or O_RDWR).
#[derive(Clone, Debug)]
pub struct ShmSlabPoolOptions {
    // The absolute filesystem path to the backing data file.
    pub file_name: String,
    // Total number of fixed-size block slots in the pool.
    pub number_of_slots: u64,
    // The fixed size (in bytes) of each slot.
    pub slot_size: u64,
    // Whether the slab pool has write permission to allocate and write payloads to the data file.
    // If this is true, the file descriptor is opened as read-write (O_RDWR) and memory-mapped
    // with read-write protection (PROT_READ | PROT_WRITE).
    // If this is false, the file descriptor is opened as read-only (O_RDONLY) and memory-mapped
    // with read-only protection (PROT_READ) to enforce strict access control.
    pub writer: bool,
}

// This struct provides an interface for using a file in /dev/shm as a slab allocator.
// The file will be split up into fixed-size blocks and provide an API
// for reading and writing payloads to the blocks.
// To synchronize access to the slabs, we use a separate header file that contains
// a sequence of atomic u64 bitmasks. The header is laid out as follows:
// [ N*8 bytes: N bitmasks (u64) ]
// Where each bitmask represents the state (0 - free; 1 - used) of the next 64 slots.
// The data file has no specific format. It is simply a number_of_slots * slot_size
// buffer.
#[derive(Debug)]
pub struct ShmSlabPool {
    number_of_slots: u64,
    slot_size: u64,
    header_mmap: memmap2::MmapMut,
    data_mmap: DataMmap,
    // Used as signal to writer threads of where to begin searching for free
    // slots. We start at the last bitmask with successful allocations, skipping
    // previously full bitmasks
    search_index: AtomicU64,
}

impl ShmSlabPool {
    // Creates a new ShmSlabPool.
    // Optionally creates the backing file, otherwise simply opens
    // and mmaps the existing file.
    pub fn new(options: ShmSlabPoolOptions) -> Result<Self> {
        let header_file_name = format!("{}-{SHM_HEADER}", options.file_name);

        // Note: We must open data_file with write(true) at the OS file descriptor level
        // because both .truncate(true) and .set_len() (ftruncate) strictly require write
        // permissions to initialize the file size on disk.
        // Strict access control and security are enforced later during memory mapping, where
        // we only allow mutable mmap access if options.writer is true.
        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&options.file_name)?;
        let header_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&header_file_name)?;

        // Round up to nearest multiple of 64 to simplify atomic bit operations
        let number_of_slots = options.number_of_slots.div_ceil(64) * 64;
        let number_of_bitmasks = calculate_number_of_bitmasks(number_of_slots);
        let header_size = number_of_bitmasks * 8;
        let data_size = number_of_slots * options.slot_size;

        data_file.set_len(data_size)?;
        header_file.set_len(header_size)?;

        // Initialize all bitmasks to 0 (free) and write them directly to the header file at offset 0.
        let initial_header = vec![0u8; header_size as usize];
        header_file.write_all_at(&initial_header, 0)?;

        let header_mmap =
            unsafe { memmap2::MmapOptions::new().len(header_size as usize).map_mut(&header_file)? };

        // Enforce strict memory protection at the hardware/OS level:
        // If options.writer is true, map the file as mutable (PROT_READ | PROT_WRITE).
        // If options.writer is false, map the file as read-only (PROT_READ) to prevent any writes.
        let data_mmap = if options.writer {
            DataMmap::Write(unsafe {
                memmap2::MmapOptions::new().len(data_size as usize).map_mut(&data_file)?
            })
        } else {
            DataMmap::Read(unsafe {
                memmap2::MmapOptions::new().len(data_size as usize).map(&data_file)?
            })
        };

        Ok(Self {
            number_of_slots,
            slot_size: options.slot_size,
            header_mmap,
            data_mmap,
            search_index: AtomicU64::new(0),
        })
    }

    // Allocates `slots_needed` using Atomic CAS.
    // Returns a vector of slot indices.
    async fn allocate_slots(&self, slots_needed: u64) -> Result<Vec<ShmSlotReference>> {
        // If no slots are needed, return an empty vector.
        if slots_needed == 0 {
            return Ok(Vec::new());
        }
        // Fast-fail if num_slots needed is greater than the total pool capacity.
        if slots_needed > self.number_of_slots {
            return Err(ShmSlabPoolError::OversizedAllocation {
                requested: slots_needed,
                capacity: self.number_of_slots,
            });
        }

        let mut delay = std::time::Duration::from_micros(1);
        let max_delay = std::time::Duration::from_micros(10000);
        let retry_strategy = std::iter::from_fn(move || {
            let current = delay;
            delay = std::cmp::min(delay * 2, max_delay);
            Some(current)
        })
        .take(50);

        let action = || async { self.try_allocate_slots(slots_needed) };

        Retry::spawn(retry_strategy, action).await.map_err(|e| {
            log::warn!("Slot allocation failed: {:?}", e);
            ShmSlabPoolError::AllocationTimeout
        })
    }

    /// Attempts a single multi-pass allocation of the requested slots.
    ///
    /// # How this retry action behaves:
    /// 1. **Scan**: Performs a sequential scan over all the atomic bitmasks (starting at
    ///    `search_index`) using lock-free compare-and-swap (`compare_exchange`).
    /// 2. **Contention handling**: If a CAS race is lost, it retries that bitmask up to 5 times
    ///    before skipping to the next.
    /// 3. **Multi-pass progress**: If a pass claims *some* slots but not all, it loops immediately
    ///    without sleeping, continuing to hunt for the remaining slots.
    /// 4. **Contested/Full failure**: If a full pass over all bitmasks makes *zero progress*, the
    ///    allocation fails. All partially claimed slots are safely rolled back (freed), and an
    ///    allocation contention error is returned. This error triggers the external `Retry::spawn` backoff sleep
    ///    to wait for other threads to release slots.
    fn try_allocate_slots(&self, slots_needed: u64) -> Result<Vec<ShmSlotReference>> {
        let mut allocated_slots = Vec::new();
        let mut current_allocated = 0;
        let number_of_bitmasks = calculate_number_of_bitmasks(self.number_of_slots);

        while current_allocated < slots_needed {
            let mut made_progress = false;
            let start_idx = self.search_index.load(Ordering::Relaxed);

            for j in 0..number_of_bitmasks {
                let curr_mask_idx = (start_idx + j) % number_of_bitmasks;

                let atomic_mask = unsafe {
                    let mask_ptr = self.header_mmap.as_ptr() as *const AtomicU64;
                    &*mask_ptr.add(curr_mask_idx as usize)
                };
                let mut current_mask = atomic_mask.load(Ordering::Acquire);

                // Here we loop until there are no more bits to claim in this bitmask.
                // NOTE: we only enter this loop more than once if the CAS operation fails,
                // hence why we keep track of the failures to enforce a limit
                let mut cas_failures = 0;
                while current_mask != !0u64 {
                    let mut bits_to_acquire = 0u64;
                    let mut free_bits = !current_mask;
                    let needed = slots_needed - current_allocated;
                    let available = free_bits.count_ones() as u64;
                    // If slots needed is >= the slots available in the current
                    // bitmask, we can claim all the available slots.
                    let mut num_bits_to_claim = 0u64;
                    if needed >= available {
                        bits_to_acquire = free_bits;
                        num_bits_to_claim = available;
                    } else {
                        // Otherwise, we claim the first `needed` available slots.
                        while free_bits != 0 && num_bits_to_claim < needed {
                            let bit_idx = free_bits.trailing_zeros();
                            bits_to_acquire |= 1u64 << bit_idx;
                            num_bits_to_claim += 1;
                            free_bits &= !(1u64 << bit_idx);
                        }
                    }
                    // NOTE: Those this is named acquired mask here, it is only truly acquired
                    // if the CAS succeeds and we override the current mask
                    let acquired_mask = current_mask | bits_to_acquire;
                    match atomic_mask.compare_exchange(
                        current_mask,
                        acquired_mask,
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => {
                            // Success! We claimed `num_bits_to_claim` slots.
                            let mut bits_left_to_acquire = bits_to_acquire;
                            // Here we create the ShmSlotReference list of the claimed slots
                            // that will be transferred over UDS (replacing the payload)
                            while bits_left_to_acquire != 0 {
                                // bit_idx within current mask
                                let bit_idx = bits_left_to_acquire.trailing_zeros();
                                // 64 = slots per bitmask
                                let slot_index = curr_mask_idx * 64 + bit_idx as u64;
                                allocated_slots.push(ShmSlotReference {
                                    slot_index: slot_index as i64,
                                    length: self.slot_size as i64,
                                });
                                // Remove the bit we just added to our allocated_slots list
                                // from the bits_left_to_acquire mask
                                bits_left_to_acquire &= !(1u64 << bit_idx);
                            }
                            current_allocated += num_bits_to_claim;
                            // Update our local mask to the acquired mask, so we don't try to acquire the same bits again
                            current_mask = acquired_mask;
                            made_progress = true;
                            self.search_index.store(curr_mask_idx, Ordering::Relaxed);
                            if current_allocated == slots_needed {
                                return Ok(allocated_slots);
                            }
                        }
                        // CAS failure returns the current mask, we update our local copy so we try again.
                        Err(actual_mask) => {
                            cas_failures += 1;
                            if cas_failures >= 5 {
                                log::warn!("CAS failure limit reached for current bitmask, moving on to next bitmask");
                                break;
                            }
                            current_mask = actual_mask;
                        }
                    }
                }
            }

            if !made_progress {
                // Rollback any slots that were allocated in this pass and return.
                if !allocated_slots.is_empty() {
                    self.free_slots(&allocated_slots)?;
                }
                return Err(ShmSlabPoolError::AllocationContention { requested: slots_needed });
            }
        }

        Ok(allocated_slots)
    }

    // Allocates slots to fit the given payload and writes it to the pool using Atomic CAS.
    pub async fn write_to_pool(&self, payload: &[u8]) -> Result<Vec<ShmSlotReference>> {
        if matches!(self.data_mmap, DataMmap::Read(_)) {
            return Err(ShmSlabPoolError::InvalidWritePermission);
        }

        let payload_len = payload.len();
        let pool_capacity = self.number_of_slots * self.slot_size;

        // Fast-fail if payload size is larger than the entire pool.
        if payload_len as u64 > pool_capacity {
            return Err(ShmSlabPoolError::OversizedPayload {
                payload_size: payload_len as u64,
                capacity: pool_capacity,
            });
        } else if payload_len == 0 {
            // Return early if there is no payload to write
            return Ok(Vec::new());
        }

        let needed_slots = (payload_len as u64).div_ceil(self.slot_size);
        let mut allocated_slots = self.allocate_slots(needed_slots).await?;

        let mut payload_offset = 0;
        let mut bytes_left = payload_len;

        for slot_ref in &mut allocated_slots {
            let write_len = std::cmp::min(bytes_left, self.slot_size as usize);
            let slot_offset = slot_ref.slot_index as u64 * self.slot_size;
            let slot_ptr = self.data_mmap.as_ptr().wrapping_add(slot_offset as usize) as *mut u8;
            let slot_data = &payload[payload_offset..payload_offset + write_len];

            unsafe {
                std::ptr::copy_nonoverlapping(slot_data.as_ptr(), slot_ptr, write_len);
            }
            slot_ref.length = write_len as i64;
            payload_offset += write_len;
            bytes_left -= write_len;
        }

        Ok(allocated_slots)
    }

    // Reads the payload from the pool based on the given slot references and frees the slots.
    pub fn read_from_pool(&self, slot_references: &[ShmSlotReference]) -> Result<Vec<u8>> {
        let mut payload = Vec::new();

        for slot_ref in slot_references {
            let data_offset = slot_ref.slot_index as u64 * self.slot_size;
            let slot_data = &self.data_mmap
                [data_offset as usize..(data_offset + slot_ref.length as u64) as usize];
            // Here we are copying the payload out of the shared memory slots
            // into the payload vector returned to the caller so we can free the slot
            // immediately for reuse.
            payload.extend_from_slice(slot_data);
        }

        self.free_slots(slot_references)?;

        Ok(payload)
    }

    // Frees the allocated slots using Atomic CAS.
    fn free_slots(&self, slot_references: &[ShmSlotReference]) -> Result<()> {
        let mask_ptr = self.header_mmap.as_ptr() as *const AtomicU64;

        for slot_ref in slot_references {
            let mask_idx = slot_ref.slot_index as u64 / 64;
            let bit_idx = slot_ref.slot_index as u64 % 64;

            let atomic_mask = unsafe { &*mask_ptr.add(mask_idx as usize) };
            atomic_mask.fetch_and(!(1 << bit_idx), Ordering::SeqCst);
        }
        Ok(())
    }
}

/// Calculates the number of u64 bitmasks needed to cover the given number of slots.
#[inline]
fn calculate_number_of_bitmasks(number_of_slots: u64) -> u64 {
    number_of_slots.div_ceil(64)
}

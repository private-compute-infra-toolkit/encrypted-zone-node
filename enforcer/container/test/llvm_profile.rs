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

/// Flushes LLVM coverage counters before `execve` replaces the process image.
#[no_mangle]
unsafe extern "C" fn execve(
    path: *const libc::c_char,
    argv: *const *const libc::c_char,
    envp: *const *const libc::c_char,
) -> libc::c_int {
    let (write_file_addr, reset_counters_addr): (usize, usize);
    #[cfg(target_arch = "x86_64")]
    std::arch::asm!(
        ".weak __llvm_profile_write_file",
        ".weak __llvm_profile_reset_counters",
        "mov {write_fn}, QWORD PTR [rip + __llvm_profile_write_file@GOTPCREL]",
        "mov {reset_fn}, QWORD PTR [rip + __llvm_profile_reset_counters@GOTPCREL]",
        write_fn = out(reg) write_file_addr,
        reset_fn = out(reg) reset_counters_addr,
        options(readonly, nostack, preserves_flags),
    );
    #[cfg(target_arch = "aarch64")]
    std::arch::asm!(
        ".weak __llvm_profile_write_file",
        ".weak __llvm_profile_reset_counters",
        "adrp {write_fn}, :got:__llvm_profile_write_file",
        "ldr {write_fn}, [{write_fn}, :got_lo12:__llvm_profile_write_file]",
        "adrp {reset_fn}, :got:__llvm_profile_reset_counters",
        "ldr {reset_fn}, [{reset_fn}, :got_lo12:__llvm_profile_reset_counters]",
        write_fn = out(reg) write_file_addr,
        reset_fn = out(reg) reset_counters_addr,
        options(readonly, nostack, preserves_flags),
    );
    if write_file_addr != 0 {
        let write_file: unsafe extern "C" fn() -> libc::c_int =
            std::mem::transmute(write_file_addr);
        let _ = write_file();
        if reset_counters_addr != 0 {
            let reset_counters: unsafe extern "C" fn() = std::mem::transmute(reset_counters_addr);
            reset_counters();
        }
    }
    libc::syscall(libc::SYS_execve, path, argv, envp) as libc::c_int
}

/// Pre-opens `LLVM_PROFILE_FILE` before `fork()` so forked container processes merge coverage
/// counters over an inherited FD instead of opening paths inside isolated mount namespaces.
pub fn ensure_shared_profile_file() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        log::set_max_level(log::LevelFilter::Trace);
        type GetFilenameFn = unsafe extern "C" fn() -> *const libc::c_char;
        type SetFileObjectFn = unsafe extern "C" fn(*mut libc::FILE, libc::c_int) -> libc::c_int;

        let (get_filename_addr, set_file_addr): (usize, usize);
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::asm!(
                ".weak __llvm_profile_get_filename",
                ".weak __llvm_profile_set_file_object",
                "mov {get_fn}, QWORD PTR [rip + __llvm_profile_get_filename@GOTPCREL]",
                "mov {set_fn}, QWORD PTR [rip + __llvm_profile_set_file_object@GOTPCREL]",
                get_fn = out(reg) get_filename_addr,
                set_fn = out(reg) set_file_addr,
                options(readonly, nostack, preserves_flags),
            );
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            std::arch::asm!(
                ".weak __llvm_profile_get_filename",
                ".weak __llvm_profile_set_file_object",
                "adrp {get_fn}, :got:__llvm_profile_get_filename",
                "ldr {get_fn}, [{get_fn}, :got_lo12:__llvm_profile_get_filename]",
                "adrp {set_fn}, :got:__llvm_profile_set_file_object",
                "ldr {set_fn}, [{set_fn}, :got_lo12:__llvm_profile_set_file_object]",
                get_fn = out(reg) get_filename_addr,
                set_fn = out(reg) set_file_addr,
                options(readonly, nostack, preserves_flags),
            );
        }
        if get_filename_addr == 0 || set_file_addr == 0 {
            return;
        }
        unsafe {
            let get_filename: GetFilenameFn = std::mem::transmute(get_filename_addr);
            let set_file_object: SetFileObjectFn = std::mem::transmute(set_file_addr);
            let raw_path = get_filename();
            if raw_path.is_null() {
                return;
            }
            if !std::ffi::CStr::from_ptr(raw_path).to_bytes().is_empty() {
                let file = libc::fopen(raw_path, c"w+be".as_ptr());
                assert!(!file.is_null(), "fopen failed for LLVM profile file");
                assert_eq!(set_file_object(file, 1), 0);
                libc::free(raw_path.cast_mut().cast::<libc::c_void>());
            }
        }
    });
}

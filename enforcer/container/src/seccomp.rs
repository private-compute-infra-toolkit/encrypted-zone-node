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

//! Seccomp filter construction and application.
//!
//! This module implements Linux seccomp-bpf to sandbox processes.
//! It constructs Classical BPF (cBPF) programs to filter system calls dynamically.
//! Rather than depending on an external C library (like libseccomp), we manually
//! compile the BPF bytecode using the standard Linux `sock_filter` structures.
//!
//! TODO: Transition this manually-compiled Classical BPF to `seccompiler` library.
//! Using a library will resolve x32 ABI bypasses natively and optimize the rules to
//! compile into a Binary Search Tree (BST) using BPF_JGT/BPF_JGE (rather than a linear scan).

use anyhow::Context;
use container::{SeccompAction, SeccompProfile, SeccompRule};
use libc::{c_ushort, sock_filter, sock_fprog};
use std::sync::OnceLock;

// Architecture identifiers for validation.
// It is critical to validate the audit architecture in seccomp filters.
// Without this, a 32-bit binary running on a 64-bit kernel could bypass filters
// because the syscall numbers differ between architectures.
#[cfg(target_arch = "x86_64")]
const AUDIT_ARCH: u32 = 0xC000003E; // AUDIT_ARCH_X86_64
#[cfg(target_arch = "aarch64")]
const AUDIT_ARCH: u32 = libc::AUDIT_ARCH_AARCH64;

// Offsets in the `seccomp_data` C struct. When a system call is made,
// the kernel provides a `seccomp_data` struct to the BPF program containing
// the syscall number (nr), architecture (arch), instruction pointer, and arguments.
const SECCOMP_DATA_NR_OFFSET: u32 = 0;
const SECCOMP_DATA_ARCH_OFFSET: u32 = 4;

// -----------------------------------------------------------------------------
// BPF Instruction Macros
// -----------------------------------------------------------------------------
// These map to standard Linux <linux/bpf_common.h> constants.
const BPF_LD: u16 = 0x00; // Load instruction
const BPF_W: u16 = 0x00; // Word size (32-bit)
const BPF_ABS: u16 = 0x20; // Absolute offset (read from the provided data packet)
const BPF_JMP: u16 = 0x05; // Jump instruction
const BPF_JEQ: u16 = 0x10; // Jump if equal
const BPF_JGE: u16 = 0x30; // Jump if greater than or equal
const BPF_K: u16 = 0x00; // Use constant `k` in the instruction (literal value)
const BPF_RET: u16 = 0x06; // Return instruction

static DEFAULT_FILTER: OnceLock<Vec<sock_filter>> = OnceLock::new();

/// Helper to create a basic BPF statement (an instruction without conditional jumps).
/// `code` defines the operation (e.g., Load, Return).
/// `k` provides the immediate argument (e.g., an offset to read, or a value to return).
fn bpf_stmt(code: u16, k: u32) -> sock_filter {
    sock_filter { code, jt: 0, jf: 0, k }
}

/// Helper to create a conditional BPF jump instruction.
/// `code` defines the jump operation (e.g., Jump if Equal).
/// `k` is the value to compare against.
/// `jt` (jump true) is the relative offset to jump if the condition is true.
/// `jf` (jump false) is the relative offset to jump if the condition is false.
fn bpf_jump(code: u16, k: u32, jt: u8, jf: u8) -> sock_filter {
    sock_filter { code, jt, jf, k }
}

/// Converts our high-level `SeccompAction` enum into Linux kernel `SECCOMP_RET_*` constants.
fn action_to_ret(action: &SeccompAction) -> u32 {
    match action {
        SeccompAction::Allow => libc::SECCOMP_RET_ALLOW,
        SeccompAction::KillProcess => libc::SECCOMP_RET_KILL_PROCESS,
        SeccompAction::Errno(e) => libc::SECCOMP_RET_ERRNO | (*e as u32 & libc::SECCOMP_RET_DATA),
    }
}

pub fn set_profile(profile: &SeccompProfile) -> anyhow::Result<()> {
    match profile {
        SeccompProfile::Unconfined => Ok(()),
        SeccompProfile::Default => set_default(),
        SeccompProfile::Custom { rules, default_action } => set_custom(rules, default_action),
    }
}

/// Pre-compiles the default profile BPF instructions.
/// It is highly recommended to call this in the parent process before any forks
/// to avoid compiling overhead and heap allocations in the child process.
pub fn pre_initialize_default_profile() {
    let _ = get_default_filter();
}

fn get_default_filter() -> &'static [sock_filter] {
    DEFAULT_FILTER.get_or_init(|| {
        let blocked_syscalls = get_default_blocked_syscalls();
        let mut rules = Vec::with_capacity(blocked_syscalls.len());
        for syscall in blocked_syscalls {
            rules.push(SeccompRule {
                syscall_number: syscall,
                action: SeccompAction::Errno(libc::EPERM),
            });
        }
        compile_filter(&rules, &SeccompAction::Allow)
    })
}

#[allow(deprecated)]
fn get_default_blocked_syscalls() -> Vec<i64> {
    let mut blocked_syscalls = vec![
        libc::SYS_acct,
        libc::SYS_add_key,
        libc::SYS_bpf,
        libc::SYS_clock_adjtime,
        libc::SYS_clock_settime,
        libc::SYS_clone,
        libc::SYS_clone3,
        libc::SYS_delete_module,
        libc::SYS_finit_module,
        libc::SYS_fsconfig,
        libc::SYS_fsmount,
        libc::SYS_fsopen,
        libc::SYS_get_mempolicy,
        libc::SYS_init_module,
        libc::SYS_io_uring_enter,
        libc::SYS_io_uring_register,
        libc::SYS_io_uring_setup,
        libc::SYS_kcmp,
        libc::SYS_kexec_file_load,
        libc::SYS_kexec_load,
        libc::SYS_keyctl,
        libc::SYS_lookup_dcookie,
        libc::SYS_mbind,
        libc::SYS_mount,
        libc::SYS_move_mount,
        libc::SYS_name_to_handle_at,
        libc::SYS_open_by_handle_at,
        libc::SYS_open_tree,
        libc::SYS_perf_event_open,
        libc::SYS_personality,
        libc::SYS_pivot_root,
        libc::SYS_process_vm_readv,
        libc::SYS_process_vm_writev,
        libc::SYS_ptrace,
        libc::SYS_quotactl,
        libc::SYS_reboot,
        libc::SYS_request_key,
        libc::SYS_set_mempolicy,
        libc::SYS_setns,
        libc::SYS_settimeofday,
        libc::SYS_syslog,
        libc::SYS_umount2,
        libc::SYS_unshare,
        libc::SYS_userfaultfd,
    ];

    #[cfg(target_arch = "x86_64")]
    {
        blocked_syscalls.push(libc::SYS_create_module);
        blocked_syscalls.push(libc::SYS_get_kernel_syms);
        blocked_syscalls.push(libc::SYS_ioperm);
        blocked_syscalls.push(libc::SYS_iopl);
        blocked_syscalls.push(libc::SYS_nfsservctl);
        blocked_syscalls.push(libc::SYS_query_module);
        blocked_syscalls.push(libc::SYS_sysfs);
        blocked_syscalls.push(libc::SYS__sysctl);
        blocked_syscalls.push(libc::SYS_uselib);
        blocked_syscalls.push(libc::SYS_ustat);
    }

    blocked_syscalls
}

fn compile_filter(rules: &[SeccompRule], default_action: &SeccompAction) -> Vec<sock_filter> {
    let mut filter = vec![
        // 1. Validate the architecture to prevent bypass attacks.
        // BPF Instruction: Load a 32-bit Word from the Absolute offset of `arch` in `seccomp_data`
        bpf_stmt(BPF_LD | BPF_W | BPF_ABS, SECCOMP_DATA_ARCH_OFFSET),
        // BPF Instruction: If `arch` == `AUDIT_ARCH`, jump 1 instruction forward (to the nr load).
        // If false, jump 0 instructions forward (fallthrough to the KILL_PROCESS return).
        bpf_jump(BPF_JMP | BPF_JEQ | BPF_K, AUDIT_ARCH, 1, 0),
        // BPF Instruction: Return KILL_PROCESS (executes if architecture didn't match).
        bpf_stmt(BPF_RET | BPF_K, libc::SECCOMP_RET_KILL_PROCESS),
        // 2. Load the syscall number into the BPF accumulator register.
        bpf_stmt(BPF_LD | BPF_W | BPF_ABS, SECCOMP_DATA_NR_OFFSET),
    ];

    #[cfg(target_arch = "x86_64")]
    {
        // 2b. Reject x32 ABI bypasses: if syscall_nr >= 0x40000000, reject.
        filter.push(bpf_jump(BPF_JMP | BPF_JGE | BPF_K, 0x40000000, 0, 1));
        filter.push(bpf_stmt(BPF_RET | BPF_K, libc::SECCOMP_RET_KILL_PROCESS));
    }

    // 3. Chain jump statements for each rule we defined.
    for rule in rules {
        let action_ret = action_to_ret(&rule.action);

        // BPF Instruction: If syscall_nr == `rule.syscall_number`, jump 0 instructions forward (fallthrough).
        // If false, jump 1 instruction forward (skip the return statement, continuing the loop).
        filter.push(bpf_jump(BPF_JMP | BPF_JEQ | BPF_K, rule.syscall_number as u32, 0, 1));

        // BPF Instruction: Return the designated action for this syscall.
        filter.push(bpf_stmt(BPF_RET | BPF_K, action_ret));
    }

    // 4. If none of the rules matched, return the default action.
    filter.push(bpf_stmt(BPF_RET | BPF_K, action_to_ret(default_action)));

    filter
}

fn apply_filter(filter: &[sock_filter]) -> anyhow::Result<()> {
    let prog =
        sock_fprog { len: filter.len() as c_ushort, filter: filter.as_ptr() as *mut sock_filter };

    unsafe {
        if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
            return Err(std::io::Error::last_os_error())
                .context("Failed to set PR_SET_NO_NEW_PRIVS");
        }

        if libc::prctl(
            libc::PR_SET_SECCOMP,
            libc::SECCOMP_MODE_FILTER,
            &prog as *const _ as *mut libc::c_void,
        ) != 0
        {
            return Err(std::io::Error::last_os_error()).context("Failed to set PR_SET_SECCOMP");
        }
    }

    Ok(())
}

/// Applies a default Docker-like profile blocking dangerous syscalls.
// See: https://docs.docker.com/engine/security/seccomp/
fn set_default() -> anyhow::Result<()> {
    let filter = get_default_filter();
    apply_filter(filter)
}

/// Dynamically builds and applies a BPF bytecode program for the given rules.
fn set_custom(rules: &[SeccompRule], default_action: &SeccompAction) -> anyhow::Result<()> {
    let filter = compile_filter(rules, default_action);
    apply_filter(&filter)
}

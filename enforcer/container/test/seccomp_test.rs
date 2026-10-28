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

mod llvm_profile;

use container::{SeccompAction, SeccompProfile, SeccompRule};
use container_custom::seccomp::{pre_initialize_default_profile, set_profile};
use nix::sys::signal::Signal;
use nix::sys::wait::{waitpid, WaitStatus};
use nix::unistd::{self, ForkResult};

#[test]
fn unconfined_profile_leaves_seccomp_unchanged_and_allows_syscalls() {
    pre_initialize_default_profile();
    let status = run_in_forked_child(|| {
        let mode_before = unsafe { libc::prctl(libc::PR_GET_SECCOMP) };
        assert!(set_profile(&SeccompProfile::Unconfined).is_ok());
        assert_eq!(unsafe { libc::prctl(libc::PR_GET_SECCOMP) }, mode_before);
        assert_eq!(std::thread::Builder::new().spawn(|| 42).unwrap().join().unwrap(), 42);
    });
    assert_eq!(status, WaitStatus::Exited(status.pid().unwrap(), 0));
}

#[test]
fn default_profile_blocks_privileged_syscalls_with_eperm() {
    pre_initialize_default_profile();
    let status = run_in_forked_child(|| {
        assert!(set_profile(&SeccompProfile::Default).is_ok());
        assert_eq!(unsafe { libc::prctl(libc::PR_GET_SECCOMP) }, libc::SECCOMP_MODE_FILTER as i32);
        assert!(unsafe { libc::getpid() } > 0);
        let err = std::thread::Builder::new().spawn(|| 42).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EPERM));
    });
    assert_eq!(status, WaitStatus::Exited(status.pid().unwrap(), 0));
}

#[test]
fn custom_profile_enforces_errno_allow_and_kill_process_actions() {
    let status = run_in_forked_child(|| {
        let profile = SeccompProfile::Custom {
            rules: vec![
                SeccompRule {
                    syscall_number: libc::SYS_getppid,
                    action: SeccompAction::Errno(libc::EACCES),
                },
                SeccompRule { syscall_number: libc::SYS_getpid, action: SeccompAction::Allow },
                SeccompRule { syscall_number: libc::SYS_acct, action: SeccompAction::KillProcess },
            ],
            default_action: SeccompAction::Allow,
        };
        assert!(set_profile(&profile).is_ok());

        assert!(unsafe { libc::getpid() } > 0);
        let ppid_rc = unsafe { libc::syscall(libc::SYS_getppid) };
        assert_eq!(ppid_rc, -1);
        assert_eq!(std::io::Error::last_os_error().raw_os_error(), Some(libc::EACCES));

        let killed_status = run_in_forked_child(|| {
            let _ = unsafe { libc::syscall(libc::SYS_acct, std::ptr::null::<libc::c_char>()) };
        });
        assert!(matches!(killed_status, WaitStatus::Signaled(_, Signal::SIGSYS, _)));
    });
    assert_eq!(status, WaitStatus::Exited(status.pid().unwrap(), 0));
}

#[test]
fn oversized_custom_profile_returns_error() {
    let status = run_in_forked_child(|| {
        let rules: Vec<SeccompRule> = (0..4096)
            .map(|i| SeccompRule { syscall_number: 10000 + i, action: SeccompAction::Allow })
            .collect();
        let err =
            set_profile(&SeccompProfile::Custom { rules, default_action: SeccompAction::Allow })
                .unwrap_err();
        assert!(
            err.to_string().contains("Failed to set PR_SET_SECCOMP"),
            "unexpected error: {err:#}"
        );
    });
    assert_eq!(status, WaitStatus::Exited(status.pid().unwrap(), 0));
}

fn run_in_forked_child<F: FnOnce()>(body: F) -> WaitStatus {
    llvm_profile::ensure_shared_profile_file();
    match unsafe { unistd::fork() }.expect("fork failed") {
        ForkResult::Child => {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body));
            std::process::exit(if outcome.is_ok() { 0 } else { 1 });
        }
        ForkResult::Parent { child } => waitpid(child, None).expect("waitpid failed"),
    }
}

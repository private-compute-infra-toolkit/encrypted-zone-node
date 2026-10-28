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

use nix::mount::{self, MsFlags};
use nix::sched::{self, CloneFlags};
use nix::sys::wait::{waitpid, WaitStatus};
use nix::unistd::{self, ForkResult};
use std::fs;
use std::io::{pipe, Read, Write};
use std::path::Path;
use tempfile::NamedTempFile;

#[test]
fn unpack_file_system_extracts_valid_archive_and_rejects_invalid_inputs() {
    let tar_file = NamedTempFile::new().unwrap();
    {
        let mut builder = tar::Builder::new(fs::File::create(tar_file.path()).unwrap());
        let payload = b"hello isolate";
        let mut header = tar::Header::new_gnu();
        header.set_size(payload.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append_data(&mut header, "rootfs/greeting.txt", &payload[..]).unwrap();
        builder.finish().unwrap();
    }

    let unpacked = utils::unpack_file_system(tar_file.path().to_str().unwrap()).unwrap();
    assert_eq!(
        fs::read_to_string(unpacked.path().join("rootfs/greeting.txt")).unwrap(),
        "hello isolate"
    );

    assert!(utils::unpack_file_system("/nonexistent/archive_path.tar").is_err());

    let corrupt = NamedTempFile::new().unwrap();
    fs::write(corrupt.path(), vec![0xffu8; 1024]).unwrap();
    assert!(utils::unpack_file_system(corrupt.path().to_str().unwrap()).is_err());
}

#[test]
fn create_mount_destinaton_creates_targets_and_rejects_symlinks_and_invalid_paths() {
    let temp = tempfile::tempdir().unwrap();

    assert!(utils::create_mount_destinaton(temp.path().to_path_buf(), "a/b/mounted_file", false)
        .is_ok());
    assert!(temp.path().join("a/b/mounted_file").is_file());
    assert!(utils::create_mount_destinaton(temp.path().to_path_buf(), "a/b/mounted_file", false)
        .is_ok());

    assert!(
        utils::create_mount_destinaton(temp.path().to_path_buf(), "a/b/mounted_dir", true).is_ok()
    );
    assert!(temp.path().join("a/b/mounted_dir").is_dir());
    assert!(
        utils::create_mount_destinaton(temp.path().to_path_buf(), "a/b/mounted_dir", true).is_ok()
    );

    assert!(utils::create_mount_destinaton(temp.path().to_path_buf(), "/abs/path", false).is_err());
    assert!(
        utils::create_mount_destinaton(temp.path().join("no_such_parent"), "dest", false).is_err()
    );

    let host_secret = NamedTempFile::new().unwrap();
    fs::write(host_secret.path(), "secret").unwrap();
    std::os::unix::fs::symlink(host_secret.path(), temp.path().join("a/trailing_link")).unwrap();
    assert!(utils::create_mount_destinaton(temp.path().to_path_buf(), "a/trailing_link", false)
        .is_err());
    assert_eq!(fs::read_to_string(host_secret.path()).unwrap(), "secret");

    let outside_dir = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside_dir.path(), temp.path().join("dir_link")).unwrap();
    assert!(utils::create_mount_destinaton(temp.path().to_path_buf(), "dir_link/escaped", true)
        .is_err());
    assert!(!outside_dir.path().join("escaped").exists());
}

#[test]
fn move_mount_to_namespace_transfers_mounts_and_rejects_invalid_inputs() {
    let supports_mount_setattr = kernel_supports_mount_setattr();
    run_in_isolated_child(|stage| {
        let (src_rw, src_ro) = (stage.join("src_rw"), stage.join("src_ro"));
        let (dst_rw, dst_ro) = (stage.join("dst_rw"), stage.join("dst_ro"));
        for dir in [&src_rw, &src_ro, &dst_rw, &dst_ro] {
            fs::create_dir_all(dir).unwrap();
        }
        fs::write(src_rw.join("item.txt"), "rw_initial").unwrap();
        fs::write(src_ro.join("item.txt"), "ro_initial").unwrap();

        let (mut ready_rx, mut ready_tx) = pipe().unwrap();
        let (mut go_rx, mut go_tx) = pipe().unwrap();

        match unsafe { unistd::fork() }.expect("fork failed") {
            ForkResult::Child => {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    drop((ready_rx, go_tx));
                    enter_private_mount_ns();
                    ready_tx.write_all(b"1").unwrap();
                    drop(ready_tx);

                    let mut done = [0u8; 1];
                    go_rx.read_exact(&mut done).unwrap();
                    assert_eq!(fs::read_to_string(dst_rw.join("item.txt")).unwrap(), "rw_initial");
                    fs::write(dst_rw.join("written_in_ns.txt"), "ok").unwrap();
                    if supports_mount_setattr {
                        assert_eq!(
                            fs::read_to_string(dst_ro.join("item.txt")).unwrap(),
                            "ro_initial"
                        );
                        assert!(fs::write(dst_ro.join("blocked.txt"), "nope").is_err());
                    } else {
                        assert!(!dst_ro.join("item.txt").exists());
                    }
                }));
                std::process::exit(if outcome.is_ok() { 0 } else { 1 });
            }
            ForkResult::Parent { child } => {
                drop((ready_tx, go_rx));
                let mut r = [0u8; 1];
                ready_rx.read_exact(&mut r).unwrap();

                let user_ns = format!("/proc/{}/ns/user", child.as_raw());
                let mnt_ns = format!("/proc/{}/ns/mnt", child.as_raw());
                let dst_rw_rel = dst_rw.strip_prefix("/").unwrap().to_str().unwrap();
                let dst_ro_rel = dst_ro.strip_prefix("/").unwrap().to_str().unwrap();

                for (src, dst, u_ns, m_ns) in [
                    ("/nonexistent/src", dst_rw_rel, user_ns.as_str(), mnt_ns.as_str()),
                    (src_rw.to_str().unwrap(), dst_rw_rel, "/nonexistent/ns", mnt_ns.as_str()),
                    (src_rw.to_str().unwrap(), dst_rw_rel, "/dev/null", mnt_ns.as_str()),
                    (src_rw.to_str().unwrap(), dst_rw_rel, user_ns.as_str(), "/dev/null"),
                    (
                        src_rw.to_str().unwrap(),
                        "nonexistent/dst",
                        user_ns.as_str(),
                        mnt_ns.as_str(),
                    ),
                ] {
                    assert!(utils::move_mount_to_namespace(src, dst, u_ns, m_ns, false).is_err());
                }

                assert!(utils::move_mount_to_namespace(
                    src_rw.to_str().unwrap(),
                    dst_rw_rel,
                    &user_ns,
                    &mnt_ns,
                    false,
                )
                .is_ok());

                let ro_res = utils::move_mount_to_namespace(
                    src_ro.to_str().unwrap(),
                    dst_ro_rel,
                    &user_ns,
                    &mnt_ns,
                    true,
                );
                assert_eq!(ro_res.is_ok(), supports_mount_setattr);

                go_tx.write_all(b"1").unwrap();
                drop(go_tx);
                assert_eq!(waitpid(child, None).unwrap(), WaitStatus::Exited(child, 0));
                assert_eq!(fs::read_to_string(src_rw.join("written_in_ns.txt")).unwrap(), "ok");
            }
        }
    });
}

#[test]
fn bring_up_loopback_interface_activates_loopback_idempotently() {
    run_in_isolated_child(|_| {
        sched::unshare(CloneFlags::CLONE_NEWNET).unwrap();
        assert!(!interfaces::Interface::get_by_name("lo").unwrap().unwrap().is_up());

        assert!(utils::bring_up_loopback_interface().is_ok());
        assert!(interfaces::Interface::get_by_name("lo").unwrap().unwrap().is_up());
        assert!(utils::bring_up_loopback_interface().is_ok());
        assert!(interfaces::Interface::get_by_name("lo").unwrap().unwrap().is_up());
    });
}

fn kernel_supports_mount_setattr() -> bool {
    let rc = unsafe {
        libc::syscall(
            libc::SYS_mount_setattr,
            -1,
            std::ptr::null::<libc::c_char>(),
            0,
            std::ptr::null::<libc::mount_attr>(),
            0,
        )
    };
    assert_eq!(rc, -1);
    std::io::Error::last_os_error().raw_os_error() != Some(libc::ENOSYS)
}

fn enter_private_mount_ns() {
    let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
    sched::unshare(CloneFlags::CLONE_NEWUSER).unwrap();
    fs::write("/proc/self/setgroups", "deny").unwrap();
    fs::write("/proc/self/uid_map", format!("0 {uid} 1")).unwrap();
    fs::write("/proc/self/gid_map", format!("0 {gid} 1")).unwrap();
    sched::unshare(CloneFlags::CLONE_NEWNS).unwrap();
    mount::mount(
        None::<&str>,
        "/",
        None::<&str>,
        MsFlags::MS_REC | MsFlags::MS_PRIVATE,
        None::<&str>,
    )
    .unwrap();
}

fn run_in_isolated_child<F: FnOnce(&Path)>(body: F) {
    llvm_profile::ensure_shared_profile_file();
    let arena = tempfile::tempdir().unwrap();
    match unsafe { unistd::fork() }.expect("fork failed") {
        ForkResult::Parent { child } => {
            assert_eq!(waitpid(child, None).unwrap(), WaitStatus::Exited(child, 0));
        }
        ForkResult::Child => {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                enter_private_mount_ns();
                mount::mount(
                    Some("tmpfs"),
                    arena.path(),
                    Some("tmpfs"),
                    MsFlags::empty(),
                    Some("size=16M,mode=0755"),
                )
                .unwrap();
                body(arena.path());
            }));
            std::mem::forget(arena);
            std::process::exit(if outcome.is_ok() { 0 } else { 1 });
        }
    }
}

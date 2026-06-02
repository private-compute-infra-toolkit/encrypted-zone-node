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

use anyhow::Context;
use std::ffi::CStr;
use std::fs::File;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use tar::Archive;
use tempfile::TempDir;

/// Unpack the tar file containing the Isolate filesystem into a temporary directory.
pub fn unpack_file_system(tar_file_path: &str) -> anyhow::Result<TempDir> {
    // Unpack the tar file.
    let file = File::open(tar_file_path).context(format!("opening {tar_file_path}"))?;
    let mut archive = Archive::new(file);
    let tmp_dir = tempfile::Builder::new()
        .prefix("isolate")
        .tempdir()
        .context("creating temporary directory for isolate tar")?;
    archive.unpack(&tmp_dir).context(format!("unpacking file {tar_file_path}"))?;
    Ok(tmp_dir)
}

/// Helper to open a directory relative to a directory file descriptor.
fn openat_dir(
    dirfd: &std::os::fd::OwnedFd,
    path: &std::ffi::CStr,
) -> std::io::Result<std::os::fd::OwnedFd> {
    let fd = unsafe {
        libc::openat(
            dirfd.as_raw_fd(),
            path.as_ptr(),
            libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        // Note: std::io::Error::last_os_error() reads from thread-local `errno`
        // so it does not rely on global state across threads and is safe from race conditions.
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) })
    }
}

/// Helper to create a directory relative to a directory file descriptor.
fn mkdirat(
    dirfd: &std::os::fd::OwnedFd,
    path: &std::ffi::CStr,
    mode: libc::mode_t,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    if unsafe { libc::mkdirat(dirfd.as_raw_fd(), path.as_ptr(), mode) } < 0 {
        // Note: std::io::Error::last_os_error() reads from thread-local `errno`
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Helper to open or create a file relative to a directory file descriptor.
fn openat_file(
    dirfd: &std::os::fd::OwnedFd,
    path: &std::ffi::CStr,
    flags: libc::c_int,
    mode: libc::mode_t,
) -> std::io::Result<std::os::fd::OwnedFd> {
    use std::os::fd::{AsRawFd, FromRawFd};
    let fd = unsafe {
        libc::openat(
            dirfd.as_raw_fd(),
            path.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            mode,
        )
    };
    if fd < 0 {
        // Note: std::io::Error::last_os_error() reads from thread-local `errno`
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) })
    }
}

/// Create a mount destination for the container safely, without following symlinks.
pub fn create_mount_destinaton(
    parent: PathBuf,
    destination: &str,
    is_directory: bool,
) -> anyhow::Result<()> {
    if destination.starts_with('/') {
        anyhow::bail!("destination cannot be an absolute path");
    }

    let dest_path = PathBuf::from(destination);
    let components: Vec<_> = dest_path.components().collect();

    // Open the parent directory securely and maintain a file descriptor to it.
    // By walking the path using `openat` relative to this descriptor, we ensure that
    // path resolution is immune to TOCTOU (Time-of-Check to Time-of-Use) race conditions,
    // where an attacker might swap a directory for a symlink mid-resolution.
    let parent_c = std::ffi::CString::new(parent.as_os_str().as_bytes())?;
    let raw_fd = unsafe {
        libc::open(parent_c.as_ptr(), libc::O_PATH | libc::O_DIRECTORY | libc::O_CLOEXEC)
    };
    if raw_fd < 0 {
        anyhow::bail!("Failed to open parent directory: {}", std::io::Error::last_os_error());
    }
    let mut current_fd: OwnedFd = unsafe { OwnedFd::from_raw_fd(raw_fd) };

    for (i, comp) in components.iter().enumerate() {
        let is_last = i == components.len() - 1;
        let comp_c = std::ffi::CString::new(comp.as_os_str().as_bytes())?;

        if is_last && !is_directory {
            // Create file safely using O_NOFOLLOW to explicitly reject trailing symlinks.
            // 0o644: read and write access to the file owner, while restricting group members and others to read-only
            openat_file(&current_fd, &comp_c, libc::O_CREAT | libc::O_WRONLY, 0o644)
                .context("Failed to create destination file")?;
            break;
        } else {
            // Check if directory exists and is not a symlink. We use O_NOFOLLOW on every
            // intermediate directory to guarantee we never cross a symlink boundary, fully
            // neutralizing intermediate symlink traversal attacks.
            current_fd = match openat_dir(&current_fd, &comp_c) {
                Ok(fd) => fd,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    // 0o755: read, write, and execute access to the owner, while restricting group members and others to read and execute only
                    if let Err(mkdir_err) = mkdirat(&current_fd, &comp_c, 0o755) {
                        if mkdir_err.kind() != std::io::ErrorKind::AlreadyExists {
                            anyhow::bail!("Failed to create directory: {}", mkdir_err);
                        }
                    }
                    openat_dir(&current_fd, &comp_c)
                        .context("Failed to open directory after creating it")?
                }
                Err(err) => anyhow::bail!("Failed to open directory: {}", err),
            };
        }
    }
    Ok(())
}

/// Move a mount point to a container's filesystem namespace.
pub fn move_mount_to_namespace(
    mount_point: &str,
    destination: &str,
    user_namespace_path: &str,
    mount_namespace_path: &str,
    readonly: bool,
) -> anyhow::Result<()> {
    let user_ns = File::options()
        .read(true)
        .open(user_namespace_path)
        .context(format!("Failed to open user namespace {}", user_namespace_path))?;
    let mount_ns = File::options()
        .read(true)
        .open(mount_namespace_path)
        .context(format!("Failed to open mount namespace {}", mount_namespace_path))?;

    // Spawn a dedicated process to move a mount point to the container's
    // filesystem. We do so for the following reasons:
    // 1. Joining namespaces from a multi-threaded process is not supported as
    //    all the threads in a process should be in the same namespace.
    // 2. We cannot control the scheduling of threads in tokio as tasks gets
    //    moved across threads/processes transparently so we might join a
    //    namespace and the scheduler shortly moves us to another thread or
    //    schedules another task in the same process.
    unsafe {
        let pid = libc::fork();
        if pid == 0 {
            // Attach a mount point to a file descriptor.
            let mnt_fd = libc::syscall(
                libc::SYS_open_tree,
                /*dirfd=*/ -1,
                format!("{mount_point}\0").as_ptr(),
                libc::OPEN_TREE_CLONE | libc::OPEN_TREE_CLOEXEC,
            );
            if mnt_fd == -1 {
                log::error!(
                    "open_tree failed: {:?}",
                    CStr::from_ptr(libc::strerror(*libc::__errno_location()))
                );
                libc::exit(libc::EXIT_FAILURE);
            }

            // Join container's user namespace.
            if libc::setns(user_ns.as_raw_fd(), 0) == -1 {
                log::error!(
                    "setns failed: {:?}",
                    CStr::from_ptr(libc::strerror(*libc::__errno_location()))
                );
                libc::exit(libc::EXIT_FAILURE);
            }

            // Join container's mount namespace.
            if libc::setns(mount_ns.as_raw_fd(), 0) == -1 {
                log::error!(
                    "setns failed: {:?}",
                    CStr::from_ptr(libc::strerror(*libc::__errno_location()))
                );
                libc::exit(libc::EXIT_FAILURE);
            }

            if readonly {
                let mount_attr = libc::mount_attr {
                    attr_clr: 0,
                    attr_set: libc::MOUNT_ATTR_RDONLY,
                    propagation: 0,
                    userns_fd: 0,
                };
                if libc::syscall(
                    libc::SYS_mount_setattr,
                    mnt_fd,
                    /*pathname=*/ c"".as_ptr(),
                    libc::AT_EMPTY_PATH,
                    &mount_attr,
                    size_of::<libc::mount_attr>(),
                ) == -1
                {
                    log::error!(
                        "syscall {:?}",
                        CStr::from_ptr(libc::strerror(*libc::__errno_location()))
                    );
                    libc::exit(libc::EXIT_FAILURE);
                }
            }

            // Move the mount point to a file in the container's root fs.
            if libc::syscall(
                libc::SYS_move_mount,
                mnt_fd,
                /*from_pathname=*/ c"".as_ptr(),
                /*to_dirfd=*/ -1,
                format!("/{}\0", destination).as_ptr(),
                libc::MOVE_MOUNT_F_EMPTY_PATH,
            ) == -1
            {
                log::error!(
                    "move_mount failed: {:?}",
                    CStr::from_ptr(libc::strerror(*libc::__errno_location()))
                );
                libc::exit(libc::EXIT_FAILURE);
            }

            libc::exit(libc::EXIT_SUCCESS);
        } else {
            let mut status = 0 as libc::c_int;
            if libc::waitpid(pid, &mut status, 0) == -1 {
                anyhow::bail!("failed to wait for the process handling mount point moving across namespaces. Check the process logs for error information");
            }
            if libc::WEXITSTATUS(status) != 0 {
                anyhow::bail!("The process handling mount point moving across namespaces terminated abnormally. Check the process logs for error information");
            }
        }
    }

    Ok(())
}

pub fn bring_up_loopback_interface() -> anyhow::Result<()> {
    let mut iface = interfaces::Interface::get_by_name("lo")
        .map_err(|e| anyhow::anyhow!("Failed to get lo interface: {}", e))?;
    if let Some(ref mut lo) = iface {
        if !lo.is_up() {
            lo.set_up(true)
                .map_err(|e| anyhow::anyhow!("Failed to bring up lo interface: {}", e))?;
        }
    }
    Ok(())
}

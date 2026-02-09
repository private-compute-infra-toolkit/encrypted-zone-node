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

use anyhow::Context;
use container::{Container, ContainerOptions, ContainerRoot, ContainerRunStatus};
use nix::errno::Errno;
use nix::mount::{self, MntFlags, MsFlags};
use nix::sched::{self, CloneFlags};
use nix::sys::prctl;
use nix::sys::signal::{self, Signal};
use nix::sys::statvfs;
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::{self, ForkResult, Pid};
use std::ffi::NulError;
use std::fs::OpenOptions;
use std::io::{pipe, PipeWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::{Arc, Once};
use std::{ffi::CString, fs};
use tempfile::TempDir;

const OLD_ROOT: &str = "old_root";
const ROOTFS: &str = "rootfs";

static INIT_AS_SUBREAPER: Once = Once::new();

enum Status {
    New,
    Started,
    Stopped,
}

/// A custom container runtime that implements the `Container` trait.
pub struct ContainerCustom {
    status: Status,
    root: PathBuf,
    name: String,
    pid: Option<Pid>,
    // Holds the reference to the TempDir backing root.
    // Ensure the root directory is not deleted while the container is running.
    _root_dir: Arc<TempDir>,
}

/// Implements a custom container runtime which implements the `Container` trait.
impl ContainerCustom {
    /// Containerizes a new process with namespaces and filesystem mount and write the PID.
    fn containerize(&self, opts: &ContainerOptions, pid_writer: PipeWriter) -> anyhow::Result<()> {
        // Set the process name. Not strictly needed and can be removed for performance reasons.
        let name = CString::new(self.name.as_str())?;
        prctl::set_name(name.as_c_str())
            .map_err(|e| anyhow::anyhow!("Failed to set process name: {}", e))?;
        // Create a new user namespace.
        self.setup_user_ns().map_err(|e| anyhow::anyhow!("Failed to set up user ns: {}", e))?;
        // Create remaining namespaces nested under the user namespace created above.
        let mut ns_flags = CloneFlags::CLONE_NEWNS
            | CloneFlags::CLONE_NEWCGROUP
            | CloneFlags::CLONE_NEWPID
            | CloneFlags::CLONE_NEWIPC
            | CloneFlags::CLONE_NEWUTS;
        if !opts.network.disable_network_namespace {
            ns_flags |= CloneFlags::CLONE_NEWNET;
        }
        sched::unshare(ns_flags)?;
        if !opts.network.disable_network_namespace && opts.network.bring_up_loopback_interface {
            // Bring up the loopback interface in the new network namespace.
            utils::bring_up_loopback_interface()
                .map_err(|e| anyhow::anyhow!("Failed to bring up loopback interface: {}", e))?;
        }
        // Do the remaining steps in a new process which will be in the new PID namespace.
        self.continue_execution_in_new_process(pid_writer)?;
        // Mount the container root filesystem as the new root.
        self.mount_and_pivot_root()?;
        // Mount the special filesystems and devices in the new root.
        self.mount_devs()?;
        self.mount_sys()?;
        self.mount_proc_fs()?;
        // Mount the requested files and dirs in the new root.
        for mount in opts.mounts.iter() {
            self.mount_safe(
                &mount.source,
                &mount.destination,
                None::<&str>,
                MsFlags::MS_BIND | MsFlags::MS_REC,
            )
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to mount {} to {}: {}",
                    mount.source.display(),
                    mount.destination.display(),
                    e
                )
            })?;
        }
        // Unmount the old root.
        mount::umount2(
            PathBuf::from("/")
                .join(OLD_ROOT)
                .to_str()
                .ok_or(anyhow::anyhow!("Failed to convert old root path to string"))?,
            MntFlags::MNT_DETACH,
        )
        .map_err(|e| anyhow::anyhow!("Failed to unmount old root: {}", e))?;
        self.remount_root_readonly()?;
        Ok(())
    }

    fn run(&self, opts: &ContainerOptions) -> anyhow::Result<()> {
        let bin_path_c = CString::new(opts.binary_filename.clone())?;
        let cmd_args: Result<Vec<CString>, NulError> =
            opts.command_line_arguments.clone().into_iter().map(CString::new).collect();
        let mut cmd_args = cmd_args?;
        cmd_args.insert(0, bin_path_c.clone());
        let env: Result<Vec<CString>, NulError> =
            opts.env.iter().map(|s| CString::new(s.as_str())).collect();
        let env = env?;
        unistd::execve(bin_path_c.as_c_str(), &cmd_args, &env).map_err(|e| {
            anyhow::anyhow!("Failed to execute binary {}: {}", opts.binary_filename, e)
        })?;
        Ok(())
    }

    fn setup_user_ns(&self) -> anyhow::Result<()> {
        // Do this first since the pid/gid values will change to nobody after unshare.
        let uid_map = format!("0 {} 1", users::get_current_uid());
        let gid_map = format!("0 {} 1", users::get_current_gid());
        // Create new user namespace with unshare.
        sched::unshare(CloneFlags::CLONE_NEWUSER)
            .map_err(|e| anyhow::anyhow!("Failed to unshare new user namespace: {}", e))?;
        // We need to disable setgroups to prevent the process from modifying its group
        // membership and bypassing security (e.g. possibly removing denylisted groups).
        std::fs::write("/proc/self/setgroups", "deny")
            .map_err(|e| anyhow::anyhow!("Failed to write to /proc/self/setgroups: {}", e))?;
        // Maps user / groups from the container to the host.
        std::fs::write("/proc/self/uid_map", uid_map)
            .map_err(|e| anyhow::anyhow!("Failed to write to /proc/self/uid_map: {}", e))?;
        std::fs::write("/proc/self/gid_map", gid_map)
            .map_err(|e| anyhow::anyhow!("Failed to write to /proc/self/gid_map: {}", e))?;
        Ok(())
    }

    // Forks a new child process and continues execution in the child.
    fn continue_execution_in_new_process(&self, mut pid_writer: PipeWriter) -> anyhow::Result<()> {
        match unsafe { unistd::fork() } {
            Ok(ForkResult::Parent { child }) => {
                // Send the child PID to the original parent process for tracking.
                let pid = child.as_raw().to_ne_bytes();
                pid_writer
                    .write_all(&pid)
                    .map_err(|e| anyhow::anyhow!("Failed to write child PID to pipe: {}", e))?;
                drop(pid_writer);
                exit(0);
            }
            Ok(ForkResult::Child) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("Failed to fork child: {}", e)),
        }
    }

    fn mount_and_pivot_root(&self) -> anyhow::Result<()> {
        // In the scope of the new mount namespace, set the fs as private.
        // This prevents future propagations of mount events to / from the outer namespace.
        mount::mount(
            None::<&str>,
            "/",
            None::<&str>,
            MsFlags::MS_REC | MsFlags::MS_PRIVATE,
            None::<&str>,
        )?;
        // Create a path to place the old root after pivot_root.
        let old_root = self.root.join(OLD_ROOT);
        let root = self.root.to_str().ok_or(anyhow::anyhow!(
            "Failed to convert root path to string: {}",
            self.root.display()
        ))?;
        let old_root = old_root.to_str().ok_or(anyhow::anyhow!(
            "Failed to convert old root path to string: {}",
            old_root.display()
        ))?;
        fs::create_dir_all(old_root)
            .map_err(|e| anyhow::anyhow!("Failed to create old root directory: {}", e))?;
        // Changes the root directory of the container into a mount point -- needed for pivot_root.
        mount::mount(
            Some(root),
            root,
            None::<&str>,
            MsFlags::MS_BIND | MsFlags::MS_REC,
            None::<&str>,
        )
        .map_err(|e| anyhow::anyhow!("Failed to remount root filesystem: {}", e))?;
        // Change the root filesystem to the container rootfs. The old root will be moved to /old_root.
        unistd::pivot_root(root, old_root)
            .map_err(|e| anyhow::anyhow!("Failed to pivot root filesystem: {}", e))?;
        // Change the current working directory to the new root.
        unistd::chdir("/")
            .map_err(|e| anyhow::anyhow!("Failed to change directory to new root: {}", e))?;
        Ok(())
    }

    /// Mounts device files in the new root filesystem from the old root.
    fn mount_devs(&self) -> anyhow::Result<()> {
        let devs =
            ["/dev/random", "/dev/urandom", "/dev/null", "/dev/zero", "/dev/full", "/dev/tty"];

        for dev in devs {
            self.mount_safe(
                &PathBuf::from(dev),
                &PathBuf::from(dev),
                None::<&str>,
                MsFlags::MS_BIND,
            )
            .map_err(|e| anyhow::anyhow!("Failed to mount device {}: {}", dev, e))?;
        }
        Ok(())
    }

    /// Mounts the /sys filesystem from the old root into the new root.
    /// This is intended to be called after pivot_root.
    /// TODO: only mount the minimum required info
    fn mount_sys(&self) -> anyhow::Result<()> {
        const SYS_PATH: &str = "/sys";
        self.mount_safe(
            &PathBuf::from(SYS_PATH),
            &PathBuf::from(SYS_PATH),
            None::<&str>,
            MsFlags::MS_BIND | MsFlags::MS_REC,
        )
        .map_err(|e| anyhow::anyhow!("Failed to bind mount /sys: {}", e))?;
        Ok(())
    }

    // Mounts the proc filesystem in the container.
    fn mount_proc_fs(&self) -> anyhow::Result<()> {
        const PROC: &str = "/proc";
        std::fs::create_dir_all(PROC)
            .map_err(|e| anyhow::anyhow!("Failed to create {} directory: {}", PROC, e))?;
        mount::mount(None::<&str>, PROC, Some("proc"), MsFlags::empty(), None::<&str>)
            .map_err(|e| anyhow::anyhow!("Failed to mount proc filesystem at {}: {}", PROC, e))?;
        Ok(())
    }

    fn remount_root_readonly(&self) -> anyhow::Result<()> {
        // Remount the root filesystem as readonly. Preserve existing flags
        // (nosuid, nodev, noexec) to avoid EPERM.
        let stats =
            statvfs::statvfs("/").map_err(|e| anyhow::anyhow!("Failed to statvfs /: {}", e))?;
        let current_flags = stats.flags();
        let mut remount_flags = MsFlags::MS_REMOUNT | MsFlags::MS_BIND | MsFlags::MS_RDONLY;
        let flags_map = [
            (statvfs::FsFlags::ST_NOSUID, MsFlags::MS_NOSUID),
            (statvfs::FsFlags::ST_NODEV, MsFlags::MS_NODEV),
            (statvfs::FsFlags::ST_NOEXEC, MsFlags::MS_NOEXEC),
        ];
        for (fs_flag, ms_flag) in flags_map {
            if current_flags.contains(fs_flag) {
                remount_flags |= ms_flag;
            }
        }
        mount::mount(None::<&str>, "/", None::<&str>, remount_flags, None::<&str>)
            .map_err(|e| anyhow::anyhow!("Failed to remount root filesystem as readonly: {}", e))?;
        Ok(())
    }

    fn mount_safe(
        &self,
        src: &Path,
        dest: &Path,
        fstype: Option<&str>,
        flags: MsFlags,
    ) -> anyhow::Result<()> {
        let src = self.path_from_old_root(src)?;
        if src.is_dir() {
            fs::create_dir_all(dest).map_err(|e| {
                anyhow::anyhow!("Failed to create dest directory {}: {}", dest.display(), e)
            })?;
        } else {
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent).map_err(|e| {
                    anyhow::anyhow!("Failed to create parent directory {}: {}", parent.display(), e)
                })?;
            }
            OpenOptions::new().write(true).create(true).truncate(true).open(dest).map_err(|e| {
                anyhow::anyhow!("Failed to create dest file {}: {}", dest.display(), e)
            })?;
        }
        let src = src.to_str().ok_or(anyhow::anyhow!("Failed to convert src path to string"))?;
        let dest = dest.to_str().ok_or(anyhow::anyhow!("Failed to convert dest path to string"))?;
        mount::mount(Some(src), dest, fstype, flags, None::<&str>)?;
        Ok(())
    }

    fn path_from_old_root(&self, path: &Path) -> anyhow::Result<PathBuf> {
        // Remove the leading separator if it exists.
        let path = path.to_str().ok_or(anyhow::anyhow!("Failed to convert path to string"))?;
        let separator = std::path::MAIN_SEPARATOR.to_string();
        let path = match path.strip_prefix(&separator) {
            Some(stripped) => PathBuf::from(stripped),
            None => PathBuf::from(path),
        };
        Ok(PathBuf::from("/").join(OLD_ROOT).join(path))
    }

    fn get_user_namespace_path(&self) -> anyhow::Result<String> {
        Ok(format!("/proc/{}/ns/user", self.pid.ok_or(anyhow::anyhow!("PID is not set"))?.as_raw()))
    }

    fn get_mount_namespace_path(&self) -> anyhow::Result<String> {
        Ok(format!("/proc/{}/ns/mnt", self.pid.ok_or(anyhow::anyhow!("PID is not set"))?.as_raw()))
    }
}

#[tonic::async_trait]
impl Container for ContainerCustom {
    fn new(root: ContainerRoot) -> anyhow::Result<Self> {
        // Set the current process as a subreaper once.
        // This will allow all descendent processes to be reparented to this
        // process if their parent process dies. This is necessary because
        // waitpid can only be called on child processes and the containerized
        // process (which must be reaped) is a grandchild of this process.
        INIT_AS_SUBREAPER.call_once(|| {
            if let Err(e) = prctl::set_child_subreaper(true) {
                panic!(
                    "Failed to set as child subreaper: {}.
                    The Enforcer must be able to set this to reap Isolates it creates.
                    Is the Enforcer being run with sufficient permissions?",
                    e
                );
            }
        });

        let dir = match root {
            ContainerRoot::TarImagePath(path) => {
                Arc::new(utils::unpack_file_system(path.as_str())?)
            }
            ContainerRoot::ReadOnlyRoot(dir) => dir,
        };
        Ok(Self {
            status: Status::New,
            root: dir.path().join(ROOTFS),
            _root_dir: dir,
            name: String::new(),
            pid: None,
        })
    }

    async fn start(&mut self, opts: &ContainerOptions) -> anyhow::Result<()> {
        self.name = opts.name.clone();
        match self.status {
            Status::New => {
                let (mut pid_reader, pid_writer) = pipe()?;
                let (mut status_reader, status_writer) = pipe()?;
                return match unsafe { unistd::fork() } {
                    Ok(ForkResult::Child) => {
                        drop(pid_reader);
                        drop(status_reader);
                        let mut status_writer = status_writer;
                        if let Err(e) = self.containerize(opts, pid_writer) {
                            eprintln!("Error containerizing: {}", e);
                            let _ = status_writer.write_all(e.to_string().as_bytes());
                            std::process::exit(1);
                        }
                        if let Err(e) = self.run(opts) {
                            eprintln!("Error running binary in container: {}", e);
                            let _ = status_writer.write_all(e.to_string().as_bytes());
                            std::process::exit(1);
                        }
                        unreachable!();
                    }
                    Ok(ForkResult::Parent { child: _ }) => {
                        drop(pid_writer);
                        drop(status_writer);
                        // TODO.
                        // Read the grandchild PID from the pipe. This is necessary since the child process
                        // from the fork here will also fork after it creates the new namespaces.
                        let mut buffer = [0; 4];
                        pid_reader
                            .read_exact(&mut buffer)
                            .map_err(|e| anyhow::anyhow!("Failed to read from pipe: {}", e))?;
                        self.pid = Some(Pid::from_raw(i32::from_ne_bytes(buffer)));
                        let mut status_buf = String::new();
                        status_reader.read_to_string(&mut status_buf)?;
                        if !status_buf.is_empty() {
                            return Err(anyhow::anyhow!("Child process failed: {}", status_buf));
                        }
                        self.status = Status::Started;
                        Ok(())
                    }
                    Err(e) => Err(anyhow::anyhow!("Containerize fork failed: {}", e)),
                };
            }
            _ => Err(anyhow::anyhow!("Can only start a new container")),
        }
    }

    async fn stop(&mut self) -> anyhow::Result<()> {
        match self.status {
            Status::Started => {
                signal::kill(self.pid.ok_or(anyhow::anyhow!("PID missing"))?, Signal::SIGKILL)?;
                self.status = Status::Stopped;
                Ok(())
            }
            _ => Err(anyhow::anyhow!("Can only stop a started container")),
        }
    }

    async fn delete(&mut self) -> anyhow::Result<()> {
        match self.status {
            // No-op. The tmp directory will be cleaned up automatically on drop.
            Status::Stopped => Ok(()),
            _ => return Err(anyhow::anyhow!("Can only delete a stopped container")),
        }
    }

    fn mount(&mut self, source: &str, destination: &str) -> anyhow::Result<()> {
        match self.status {
            Status::Started => {
                utils::create_mount_destinaton(self.root.clone(), destination)?;
                utils::move_mount_to_namespace(
                    source,
                    destination,
                    self.get_user_namespace_path()?.as_str(),
                    self.get_mount_namespace_path()?.as_str(),
                    false,
                )
                .context("Error moving mount to container namespace")?;
                Ok(())
            }
            _ => Err(anyhow::anyhow!("Can only mount file in a started container")),
        }
    }

    fn mount_readonly(&mut self, source: &str, destination: &str) -> anyhow::Result<()> {
        match self.status {
            Status::Started => {
                utils::create_mount_destinaton(self.root.clone(), destination)?;
                utils::move_mount_to_namespace(
                    source,
                    destination,
                    self.get_user_namespace_path()?.as_str(),
                    self.get_mount_namespace_path()?.as_str(),
                    true,
                )
                .context("Error moving mount to container namespace")?;
                Ok(())
            }
            _ => Err(anyhow::anyhow!("Can only mount file in a started container")),
        }
    }

    fn get_run_status(&self) -> anyhow::Result<ContainerRunStatus> {
        let pid = self.pid.ok_or(anyhow::anyhow!("PID missing"))?;
        // Signal 0 (None) can be used to check if a process exists.
        // This is more robust than checking if waitpid returns ECHILD
        // because multiple errors conditions map to ECHILD.
        if let Err(Errno::ESRCH) = signal::kill(pid, None) {
            // Process not found (even in a zombie state).
            return Ok(ContainerRunStatus::NotFound);
        }
        // We know the process exists at this point unless if a race killed
        // it after the last check. Confirm it's still alive (non-zombie case)
        // or determine how it was killed (zombie case).
        match waitpid(pid, Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::StillAlive) => Ok(ContainerRunStatus::Running),
            Ok(WaitStatus::Exited(_, c)) => Ok(ContainerRunStatus::Exited(c)),
            Ok(WaitStatus::Signaled(_, s, _)) => Ok(ContainerRunStatus::Signaled(s as i32)),
            Ok(ws) => Err(anyhow::anyhow!("Unhandled WaitStatus: {:?}", ws)),
            Err(Errno::ECHILD) => Err(anyhow::anyhow!("Exited or not child of calling process")),
            Err(e) => Err(anyhow::anyhow!("Waitpid failed with error: {}", e)),
        }
    }
}

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

mod llvm_profile;

use container::{
    Container, ContainerOptions, ContainerRoot, ContainerRunStatus, MountOptions, NetworkOptions,
    SeccompAction, SeccompProfile, SeccompRule, StateResetEngine, StateResetSession,
};
use container_custom::ContainerCustom;
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, Interest};
use tokio::net::UnixListener;
use tokio::time::{self, sleep, Duration};

const BUNDLE_PATH: &str = "enforcer/container/test_data/test_bundle.tar";
// Run the binary in enforcer/container/test_data/main.rs
// `/usr/local/bin/main` comes from `test_binary` pkg_tar rule in
// enforcer/container/test_data/BUILD.
const BINARY_FILENAME: &str = "/usr/local/bin/main";
const INSTANCE_NAME: &str = "test_ez1";

#[tokio::test]
async fn container_run_successful() {
    let (uds_dir, uds_listener) = setup();
    let server_handle = handle(uds_listener, 100);
    let opts = default_container_opts("send-uds", Some(uds_dir.path().to_path_buf()));
    let mut container = default_container();
    container.start(&opts).await.unwrap();
    let data = server_handle.await.unwrap();
    assert_eq!(data, b"EZ is cool");
    assert_eq!(
        container.mount("source", "destination").unwrap_err().to_string(),
        "Error moving mount to container namespace"
    );
    assert_eq!(
        container.mount_readonly("source", "destination").unwrap_err().to_string(),
        "Error moving mount to container namespace"
    );
}

#[tokio::test]
async fn container_lifecycle_successful() {
    let opts = default_container_opts("sleep", None);
    let mut container = default_container();
    assert!(container.start(&opts).await.is_ok());
    assert_eq!(container.get_run_status().unwrap(), ContainerRunStatus::Running);
    assert!(container.stop().await.is_ok());
    assert!(container.delete().await.is_ok());
}

#[tokio::test]
async fn container_new_unexpected_calls_fails() {
    let mut container = default_container();
    assert!(container.stop().await.is_err());
    assert!(container.delete().await.is_err());
    assert_eq!(
        container.mount("source", "destination").unwrap_err().to_string(),
        "Can only mount file in a started container"
    );
    assert_eq!(
        container.mount_readonly("source", "destination").unwrap_err().to_string(),
        "Can only mount file in a started container"
    );
    assert_eq!(
        container.checkpoint().await.unwrap_err().to_string(),
        "Can only checkpoint a started container"
    );
    assert_eq!(
        container.reset().await.unwrap_err().to_string(),
        "Can only reset a started container"
    );
}

#[tokio::test]
async fn container_started_unexpected_calls_fails() {
    let opts = default_container_opts("sleep", None);
    let mut container = default_container();
    container.start(&opts).await.unwrap();
    assert!(container.start(&opts).await.is_err());
    assert!(container.delete().await.is_err());
    assert_eq!(
        container.reset().await.unwrap_err().to_string(),
        "Container has not been checkpointed"
    );
}

#[tokio::test]
async fn container_stopped_unexpected_calls_fails() {
    let opts = default_container_opts("sleep", None);
    let mut container = default_container();
    container.start(&opts).await.unwrap();
    container.stop().await.unwrap();
    assert!(container.start(&opts).await.is_err());
    assert!(container.stop().await.is_err());
    assert_eq!(
        container.mount("source", "destination").unwrap_err().to_string(),
        "Can only mount file in a started container"
    );
    assert_eq!(
        container.mount_readonly("source", "destination").unwrap_err().to_string(),
        "Can only mount file in a started container"
    );
    assert_eq!(
        container.checkpoint().await.unwrap_err().to_string(),
        "Can only checkpoint a started container"
    );
    assert_eq!(
        container.reset().await.unwrap_err().to_string(),
        "Can only reset a started container"
    );
}

#[tokio::test]
async fn container_create_thread() {
    let (uds_dir, uds_listener) = setup();
    let server_handle = handle(uds_listener, 100);
    let opts = default_container_opts("create-thread", Some(uds_dir.path().to_path_buf()));
    let mut container = default_container();
    container.start(&opts).await.unwrap();
    let data = server_handle.await.unwrap();
    assert_eq!(data, b"thread created");
}

#[tokio::test]
async fn container_reusable_fs() {
    // Use a single root filesystem for both containers.
    let root = Arc::new(utils::unpack_file_system(BUNDLE_PATH).unwrap());
    let mut file = File::create(root.path().join("rootfs/message")).unwrap();
    write!(file, "Hello from shared fs!").unwrap();
    let uds_dir_1 = tempfile::Builder::new().prefix("test_uds_1").tempdir().unwrap();
    let server_handle_1 =
        handle(UnixListener::bind(uds_dir_1.path().join("isolate_ipc")).unwrap(), 100);
    let mut container_1 =
        ContainerCustom::new(ContainerRoot::ReadOnlyRoot(Arc::clone(&root)), None).unwrap();
    let uds_dir_2 = tempfile::Builder::new().prefix("test_uds_2").tempdir().unwrap();
    let server_handle_2 =
        handle(UnixListener::bind(uds_dir_2.path().join("isolate_ipc")).unwrap(), 100);
    let mut container_2 =
        ContainerCustom::new(ContainerRoot::ReadOnlyRoot(Arc::clone(&root)), None).unwrap();
    // Start both containers.
    container_1
        .start(&default_container_opts("read-file", Some(uds_dir_1.path().to_path_buf())))
        .await
        .unwrap();
    container_2
        .start(&default_container_opts("read-file", Some(uds_dir_2.path().to_path_buf())))
        .await
        .unwrap();
    let data = server_handle_1.await.unwrap();
    assert_eq!(data, b"Hello from shared fs!");
    let data = server_handle_2.await.unwrap();
    assert_eq!(data, b"Hello from shared fs!");
}

#[tokio::test]
async fn container_fs_mounts() {
    let (uds_dir, uds_listener) = setup();
    let server_handle = handle(uds_listener, 10000);
    let opts = ContainerOptions {
        mounts: vec![
            MountOptions {
                source: uds_dir.path().to_path_buf(),
                destination: PathBuf::from("/ez"),
                apply_restrictive_flags: false,
                optional: false,
            },
            // Mount a file inside a directory that does not exist.
            MountOptions {
                source: PathBuf::from("/dev/random"),
                destination: PathBuf::from("/a/b"),
                apply_restrictive_flags: false,
                optional: false,
            },
        ],
        ..default_container_opts("inspect-fs-root", None)
    };
    let mut container = default_container();
    container.start(&opts).await.unwrap();
    let data = String::from_utf8(server_handle.await.unwrap()).unwrap();
    let files: HashSet<String> = data.split(',').map(|s| s.to_string()).collect();
    // Check that at least one file from each of the expected directories is present.
    assert!(files.contains("/a/b"));
    assert!(files.contains("/ez/isolate_ipc"));
    assert!(files.contains("/dev/random"));
    assert!(files.contains("/proc/fs"));
    assert!(files.contains("/sys/devices"));
}

#[tokio::test]
async fn container_loopback_interface_up() {
    let (uds_dir, uds_listener) = setup();
    let server_handle = handle(uds_listener, 100);
    let opts = ContainerOptions {
        network: NetworkOptions {
            disable_network_namespace: false,
            bring_up_loopback_interface: true,
        },
        ..default_container_opts("ping-loopback", Some(uds_dir.path().to_path_buf()))
    };
    let mut container = default_container();
    container.start(&opts).await.unwrap();
    let data = String::from_utf8(server_handle.await.unwrap()).unwrap();
    assert_eq!(data, "loopback addresses reachable");
}

#[tokio::test]
async fn container_loopback_interface_down() {
    let (uds_dir, uds_listener) = setup();
    let server_handle = handle(uds_listener, 100);
    let opts = ContainerOptions {
        network: NetworkOptions {
            disable_network_namespace: false,
            bring_up_loopback_interface: false,
        },
        ..default_container_opts("ping-loopback", Some(uds_dir.path().to_path_buf()))
    };
    let mut container = default_container();
    container.start(&opts).await.unwrap();
    let data = String::from_utf8(server_handle.await.unwrap()).unwrap();
    assert_eq!(data, "loopback ping failed");
}

#[tokio::test]
async fn container_with_no_env_vars() {
    let (uds_dir, uds_listener) = setup();
    let server_handle = handle(uds_listener, 100);
    let opts = default_container_opts("echo-all-envs", Some(uds_dir.path().to_path_buf()));
    let mut container = default_container();
    container.start(&opts).await.unwrap();
    let data = server_handle.await.unwrap();
    assert_eq!(data, b"");
}

#[tokio::test]
async fn container_with_multiple_env_vars() {
    let (uds_dir, uds_listener) = setup();
    let server_handle = handle(uds_listener, 100);
    let opts = ContainerOptions {
        env: vec!["VAR2=value2".to_string(), "VAR1=value1".to_string()],
        ..default_container_opts("echo-all-envs", Some(uds_dir.path().to_path_buf()))
    };
    let mut container = default_container();
    container.start(&opts).await.unwrap();
    let data = server_handle.await.unwrap();
    assert_eq!(data, b"VAR1=value1;VAR2=value2");
}

#[tokio::test]
async fn container_run_status_exited_success() {
    let opts = default_container_opts("exit-success", None);
    let mut container = default_container();
    container.start(&opts).await.unwrap();
    sleep(Duration::from_millis(500)).await;
    assert_eq!(container.get_run_status().unwrap(), ContainerRunStatus::Exited(0));
    assert_eq!(container.get_run_status().unwrap(), ContainerRunStatus::NotFound);
}

#[tokio::test]
async fn container_run_status_exited_fail() {
    let opts = default_container_opts("exit-fail", None);
    let mut container = default_container();
    container.start(&opts).await.unwrap();
    sleep(Duration::from_millis(500)).await;
    assert_eq!(container.get_run_status().unwrap(), ContainerRunStatus::Exited(1));
    assert_eq!(container.get_run_status().unwrap(), ContainerRunStatus::NotFound);
}

#[tokio::test]
async fn container_run_status_signaled() {
    let opts = default_container_opts("sleep", None);
    let mut container = default_container();
    container.start(&opts).await.unwrap();
    // Give some time for the process to be reparented to the subreaper.
    sleep(Duration::from_millis(100)).await;
    assert_eq!(container.get_run_status().unwrap(), ContainerRunStatus::Running);
    container.stop().await.unwrap();
    // Give some time for the signal to be delivered to the process and clean up.
    sleep(Duration::from_millis(500)).await;
    assert_eq!(container.get_run_status().unwrap(), ContainerRunStatus::Signaled(9));
    assert_eq!(container.get_run_status().unwrap(), ContainerRunStatus::NotFound);
}

#[tokio::test]
async fn container_root_is_readonly() {
    let (uds_dir, uds_listener) = setup();
    let server_handle = handle(uds_listener, 100);
    let opts = default_container_opts("try-write-root", Some(uds_dir.path().to_path_buf()));
    let mut container = default_container();
    container.start(&opts).await.unwrap();
    let data = String::from_utf8(server_handle.await.unwrap()).unwrap();
    assert!(data.contains("Read-only file system"), "Expected read-only error, got: {}", data);
}

#[tokio::test]
async fn container_tmpfs_mount() {
    let (uds_dir, uds_listener) = setup();
    let server_handle = handle(uds_listener, 100);
    let opts = default_container_opts("verify-tmpfs-mount", Some(uds_dir.path().to_path_buf()));
    let mut container = default_container();
    container.start(&opts).await.unwrap();
    let data = String::from_utf8(server_handle.await.unwrap()).unwrap();
    assert_ne!(data, "not found", "/tmp mount not found");
    let options: Vec<&str> = data.split(',').collect();
    assert!(options.contains(&"nosuid"), "missing nosuid: {}", data);
    assert!(options.contains(&"nodev"), "missing nodev: {}", data);
    assert!(options.contains(&"noexec"), "missing noexec: {}", data);
}

#[tokio::test]
async fn container_run_unprivileged() {
    let (uds_dir, uds_listener) = setup();
    let server_handle = handle(uds_listener, 100);
    let mut opts = default_container_opts("get-uid", Some(uds_dir.path().to_path_buf()));
    opts.run_isolate_as_unprivileged = true;

    let mut container = default_container();
    container.start(&opts).await.expect("Failed to start unprivileged container");

    let data = String::from_utf8(server_handle.await.unwrap()).unwrap();
    assert_eq!(data, "1000", "Expected UID 1000 inside unprivileged container, got: {}", data);
}

#[tokio::test]
async fn container_restrictive_flags_mount() {
    let (uds_dir, uds_listener) = setup();
    let server_handle = handle(uds_listener, 100);
    let mut opts = default_container_opts("verify-ez-mount", Some(uds_dir.path().to_path_buf()));
    opts.mounts[0].apply_restrictive_flags = true;

    let mut container = default_container();
    container.start(&opts).await.unwrap();
    let data = String::from_utf8(server_handle.await.unwrap()).unwrap();
    assert_ne!(data, "not found", "/ez mount not found");
    let options: Vec<&str> = data.split(',').collect();
    assert!(options.contains(&"nosuid"), "missing nosuid: {}", data);
    assert!(options.contains(&"nodev"), "missing nodev: {}", data);
    assert!(options.contains(&"noexec"), "missing noexec: {}", data);
}

#[tokio::test]
async fn container_prevents_symlink_file_truncation() {
    // 1. Unpack the default test bundle to use as a base rootfs
    let root = Arc::new(utils::unpack_file_system(BUNDLE_PATH).unwrap());

    // 2. Create a "host" sensitive file and write some data to it
    let host_sensitive_file = tempfile::NamedTempFile::new().unwrap();
    let host_path = host_sensitive_file.path().to_path_buf();
    std::fs::write(&host_path, "SENSITIVE_DATA_12345").unwrap();

    // 3. Create a malicious symlink in the container's rootfs
    // The symlink points to the host's sensitive file through /old_root
    let symlink_dest = root.path().join("rootfs/malicious_link");
    let old_root_path =
        PathBuf::from("/old_root").join(host_path.strip_prefix("/").unwrap_or(&host_path));
    std::os::unix::fs::symlink(old_root_path, &symlink_dest).unwrap();

    // 4. Configure container options to mount something (e.g. /dev/null) to our malicious link
    let opts = ContainerOptions {
        mounts: vec![MountOptions {
            source: PathBuf::from("/dev/null"),
            destination: PathBuf::from("/malicious_link"),
            apply_restrictive_flags: false,
            optional: false,
        }],
        ..default_container_opts("sleep", None)
    };

    // 5. Start the container, which will fail because O_NOFOLLOW prevents opening the symlink
    let mut container = ContainerCustom::new(ContainerRoot::ReadOnlyRoot(root), None).unwrap();
    let start_result = container.start(&opts).await;
    assert!(start_result.is_err(), "Expected container start to fail due to symlink rejection");

    // 6. Verify that the host file was NOT truncated
    let content = std::fs::read_to_string(&host_path).unwrap();
    assert_eq!(content, "SENSITIVE_DATA_12345", "The host file should remain unharmed.");
}

#[tokio::test]
async fn container_prevents_intermediate_symlink_traversal() {
    // 1. Unpack the default test bundle to use as a base rootfs
    let root = Arc::new(utils::unpack_file_system(BUNDLE_PATH).unwrap());

    // 2. Create a "host" sensitive directory
    let host_sensitive_dir = tempfile::Builder::new().prefix("host_sensitive").tempdir().unwrap();
    let host_path = host_sensitive_dir.path().to_path_buf();

    // 3. Create a malicious symlink in the container's rootfs
    // The symlink is a directory that points to the host's sensitive dir through /old_root
    let symlink_dest = root.path().join("rootfs/malicious_dir");
    let old_root_path =
        PathBuf::from("/old_root").join(host_path.strip_prefix("/").unwrap_or(&host_path));
    std::os::unix::fs::symlink(old_root_path, &symlink_dest).unwrap();

    // 4. Configure container options to mount something to a file *inside* the malicious link
    // e.g. /malicious_dir/created_by_container
    let opts = ContainerOptions {
        mounts: vec![MountOptions {
            source: PathBuf::from("/dev/null"),
            destination: PathBuf::from("/malicious_dir/created_by_container"),
            apply_restrictive_flags: false,
            optional: false,
        }],
        ..default_container_opts("sleep", None)
    };

    // 5. Start the container
    let mut container = ContainerCustom::new(ContainerRoot::ReadOnlyRoot(root), None).unwrap();
    let start_result = container.start(&opts).await;

    // The container should fail to start because the intermediate symlink is detected and rejected
    assert!(
        start_result.is_err(),
        "Container start should fail due to intermediate symlink rejection"
    );

    // 6. Verify that the file was NOT created on the host
    let created_file_path = host_path.join("created_by_container");
    assert!(!created_file_path.exists(), "The host file should not be created!");
}

#[tokio::test]
async fn container_seccomp_kill() {
    let (status, _) = run_seccomp_test(SeccompAction::KillProcess).await;
    assert_eq!(status, ContainerRunStatus::Signaled(libc::SIGSYS));
}

#[tokio::test]
async fn container_seccomp_errno() {
    let (status, response) = run_seccomp_test(SeccompAction::Errno(libc::EPERM)).await;
    assert_eq!(
        response,
        libc::EPERM.to_string(),
        "Container should have reported EPERM from seccomp"
    );
    assert_eq!(status, ContainerRunStatus::Exited(0));
}

#[tokio::test]
async fn container_checkpoint_and_reset_workflow() {
    let checkpoint_calls = Arc::new(AtomicUsize::new(0));
    let reset_calls = Arc::new(AtomicUsize::new(0));
    let create_session_calls = Arc::new(AtomicUsize::new(0));
    let recorded_pid = Arc::new(std::sync::Mutex::new(None));

    let engine = Arc::new(MockResetEngine {
        create_session_calls: Arc::clone(&create_session_calls),
        checkpoint_calls: Arc::clone(&checkpoint_calls),
        reset_calls: Arc::clone(&reset_calls),
        recorded_pid: Arc::clone(&recorded_pid),
        fail: Arc::new(AtomicBool::new(false)),
    });

    let root = ContainerRoot::TarImagePath(BUNDLE_PATH.to_string());
    let mut container = ContainerCustom::new(root, Some(engine)).unwrap();
    let opts = default_container_opts("sleep", None);
    assert!(container.start(&opts).await.is_ok());

    // Reset before checkpoint should fail.
    assert_eq!(
        container.reset().await.unwrap_err().to_string(),
        "Container has not been checkpointed"
    );

    // Initial checkpoint should succeed.
    assert!(container.checkpoint().await.is_ok());
    assert_eq!(create_session_calls.load(Ordering::SeqCst), 1);
    assert_eq!(checkpoint_calls.load(Ordering::SeqCst), 1);
    assert!(recorded_pid.lock().unwrap().is_some());

    // Checkpointing a second time should fail.
    assert_eq!(
        container.checkpoint().await.unwrap_err().to_string(),
        "Container has already been checkpointed"
    );
    assert_eq!(checkpoint_calls.load(Ordering::SeqCst), 1);

    // Reset should succeed.
    assert!(container.reset().await.is_ok());
    assert_eq!(reset_calls.load(Ordering::SeqCst), 1);

    assert!(container.stop().await.is_ok());
    assert!(container.delete().await.is_ok());
}

#[tokio::test]
async fn container_checkpoint_engine_error_propagates() {
    let fail = Arc::new(AtomicBool::new(true));
    let engine = Arc::new(MockResetEngine { fail: Arc::clone(&fail), ..Default::default() });
    let root = ContainerRoot::TarImagePath(BUNDLE_PATH.to_string());
    let mut container = ContainerCustom::new(root, Some(engine)).unwrap();
    let opts = default_container_opts("sleep", None);
    assert!(container.start(&opts).await.is_ok());

    assert_eq!(container.checkpoint().await.unwrap_err().to_string(), "FSR checkpoint failed");
    assert_eq!(
        container.reset().await.unwrap_err().to_string(),
        "Container has not been checkpointed"
    );

    fail.store(false, Ordering::SeqCst);
    assert!(container.checkpoint().await.is_ok());
    fail.store(true, Ordering::SeqCst);
    assert_eq!(container.reset().await.unwrap_err().to_string(), "FSR reset failed");

    assert!(container.stop().await.is_ok());
    assert!(container.delete().await.is_ok());
}

#[tokio::test]
async fn container_checkpoint_without_engine_fails() {
    let mut container = default_container();
    let opts = default_container_opts("sleep", None);
    assert!(container.start(&opts).await.is_ok());

    assert_eq!(
        container.checkpoint().await.unwrap_err().to_string(),
        "Fast State Reset is not configured or supported in this environment"
    );

    assert!(container.stop().await.is_ok());
    assert!(container.delete().await.is_ok());
}

#[tokio::test]
async fn container_memory_stats_and_unstarted_run_status() {
    let mut container = default_container();
    assert!(container.get_run_status().is_err());
    assert_eq!(container.get_memory_stats().unwrap(), None);

    let opts = default_container_opts("sleep", None);
    assert!(container.start(&opts).await.is_ok());

    let stats = container.get_memory_stats().unwrap().expect("expected stats for running process");
    assert!(stats.rss_bytes > 0);
    assert!(stats.virt_bytes >= stats.rss_bytes);

    assert!(container.stop().await.is_ok());
    sleep(Duration::from_millis(500)).await;
    assert_eq!(container.get_run_status().unwrap(), ContainerRunStatus::Signaled(libc::SIGKILL));
    assert_eq!(container.get_memory_stats().unwrap(), None);
    assert!(container.delete().await.is_ok());
}

#[tokio::test]
async fn container_optional_mount_failure_is_skipped() {
    let (uds_dir, uds_listener) = setup();
    let server_handle = handle(uds_listener, 100);
    let mut opts = default_container_opts("send-uds", Some(uds_dir.path().to_path_buf()));
    opts.mounts.push(MountOptions {
        source: PathBuf::from("/nonexistent_optional_host_source"),
        destination: PathBuf::from("/optional_skipped"),
        apply_restrictive_flags: false,
        optional: true,
    });

    let mut container = default_container();
    assert!(container.start(&opts).await.is_ok());
    assert_eq!(server_handle.await.unwrap(), b"EZ is cool");

    assert!(container.stop().await.is_ok());
    assert!(container.delete().await.is_ok());
}

#[tokio::test]
async fn container_host_network_namespace() {
    let recorded_pid = Arc::new(std::sync::Mutex::new(None));
    let engine =
        Arc::new(MockResetEngine { recorded_pid: Arc::clone(&recorded_pid), ..Default::default() });
    let opts = ContainerOptions {
        network: NetworkOptions {
            disable_network_namespace: true,
            bring_up_loopback_interface: false,
        },
        ..default_container_opts("sleep", None)
    };
    let mut container =
        ContainerCustom::new(ContainerRoot::TarImagePath(BUNDLE_PATH.to_string()), Some(engine))
            .unwrap();
    assert!(container.start(&opts).await.is_ok());
    assert!(container.checkpoint().await.is_ok());

    let pid = recorded_pid.lock().unwrap().unwrap();
    assert_eq!(
        std::fs::read_link(format!("/proc/{pid}/ns/net")).unwrap(),
        std::fs::read_link("/proc/self/ns/net").unwrap()
    );

    assert!(container.stop().await.is_ok());
    assert!(container.delete().await.is_ok());
}

#[tokio::test]
async fn container_missing_binary_reports_exec_failure() {
    for (opts, expected) in [
        (
            ContainerOptions { name: "bad\0name".into(), ..default_container_opts("sleep", None) },
            "Failed to read from pipe",
        ),
        (
            ContainerOptions {
                binary_filename: "/missing_entrypoint".into(),
                ..default_container_opts("sleep", None)
            },
            "Failed to execute binary /missing_entrypoint",
        ),
        (
            ContainerOptions {
                binary_filename: "/bad\0bin".into(),
                ..default_container_opts("sleep", None)
            },
            "nul byte found in provided data",
        ),
        (
            ContainerOptions {
                command_line_arguments: vec!["bad\0arg".into()],
                ..default_container_opts("sleep", None)
            },
            "nul byte found in provided data",
        ),
        (
            ContainerOptions {
                env: vec!["BAD\0ENV".into()],
                ..default_container_opts("sleep", None)
            },
            "nul byte found in provided data",
        ),
    ] {
        let mut container = default_container();
        let err = container.start(&opts).await.unwrap_err();
        assert!(format!("{err:#}").contains(expected), "unexpected error: {err:#}");
    }
}

#[tokio::test]
async fn container_mount_safe_and_special_fs_errors_propagate() {
    let bad_root = Arc::new(tempfile::tempdir().unwrap());
    std::fs::write(bad_root.path().join("rootfs"), "not_a_dir").unwrap();
    let mut bad_container =
        ContainerCustom::new(ContainerRoot::ReadOnlyRoot(bad_root), None).unwrap();
    let err = bad_container.start(&default_container_opts("sleep", None)).await.unwrap_err();
    assert!(err.to_string().contains("Failed to create old root directory"), "{err:#}");

    let pivot_conflict = Arc::new(tempfile::tempdir().unwrap());
    std::fs::create_dir_all(pivot_conflict.path().join("rootfs")).unwrap();
    std::os::unix::fs::symlink("/proc", pivot_conflict.path().join("rootfs/old_root")).unwrap();
    let mut pivot_container =
        ContainerCustom::new(ContainerRoot::ReadOnlyRoot(pivot_conflict), None).unwrap();
    let err = pivot_container.start(&default_container_opts("sleep", None)).await.unwrap_err();
    assert!(err.to_string().contains("Failed to pivot root filesystem"), "{err:#}");

    for (conflict_path, is_dir, expected_msg) in [
        ("dev/null", true, "Failed to mount device /dev/null"),
        ("sys", false, "Failed to bind mount /sys"),
        ("proc", false, "Failed to create /proc directory"),
        ("tmp", false, "Failed to create /tmp directory"),
    ] {
        let root = Arc::new(tempfile::tempdir().unwrap());
        let target = root.path().join("rootfs").join(conflict_path);
        if is_dir {
            std::fs::create_dir_all(&target).unwrap();
        } else {
            std::fs::create_dir_all(target.parent().unwrap()).unwrap();
            std::fs::write(&target, "conflict").unwrap();
        }
        let mut container = ContainerCustom::new(ContainerRoot::ReadOnlyRoot(root), None).unwrap();
        let err = container.start(&default_container_opts("sleep", None)).await.unwrap_err();
        assert!(err.to_string().contains(expected_msg), "got: {err:#}");
    }

    for (src, dst) in [
        ("nonexistent_rel_src", "/rel"),
        ("/dev/null", "/proc/1/ro_file"),
        ("/dev/null", "/proc/1/ro_dir/file"),
        ("/dev/null", "/dev/null/child"),
    ] {
        let opts = ContainerOptions {
            mounts: vec![MountOptions {
                source: PathBuf::from(src),
                destination: PathBuf::from(dst),
                apply_restrictive_flags: false,
                optional: false,
            }],
            ..default_container_opts("sleep", None)
        };
        let mut container = default_container();
        let err = container.start(&opts).await.unwrap_err();
        assert!(
            err.to_string().contains(&format!("Failed to mount {src} to {dst}")),
            "got: {err:#}"
        );
    }
}

fn setup() -> (TempDir, UnixListener) {
    let uds_dir = tempfile::Builder::new().prefix("test_uds").tempdir().unwrap();
    let uds_socket = uds_dir.path().join("isolate_ipc");
    let uds_listener = UnixListener::bind(uds_socket).unwrap();
    (uds_dir, uds_listener)
}

fn handle(uds_listener: UnixListener, buffer_size: usize) -> tokio::task::JoinHandle<Vec<u8>> {
    tokio::spawn(async move {
        let timeout = Duration::from_secs(2);
        match time::timeout(timeout, uds_listener.accept()).await {
            Ok(Ok((stream, _addr))) => {
                let ready = stream.ready(Interest::READABLE).await.unwrap();
                let mut n = buffer_size;
                let mut data = vec![0; n];
                if ready.is_readable() {
                    n = stream.try_read(&mut data).unwrap();
                }
                data[..n].to_vec()
            }
            Ok(Err(e)) => panic!("Failed to accept connection: {}", e),
            Err(_) => panic!("Timeout while waiting for connection"),
        }
    })
}

fn default_container_opts(operation: &str, mount_src: Option<PathBuf>) -> ContainerOptions {
    llvm_profile::ensure_shared_profile_file();
    ContainerOptions {
        name: INSTANCE_NAME.to_string(),
        binary_filename: BINARY_FILENAME.to_string(),
        command_line_arguments: vec!["--operation".to_string(), operation.to_string()],
        mounts: match mount_src {
            Some(m) => {
                vec![MountOptions {
                    source: m,
                    destination: PathBuf::from("/ez"),
                    apply_restrictive_flags: false,
                    optional: false,
                }]
            }
            None => vec![],
        },
        network: NetworkOptions::default(),
        env: vec![],
        run_isolate_as_unprivileged: false,
        seccomp_profile: Default::default(),
    }
}

fn default_container() -> ContainerCustom {
    llvm_profile::ensure_shared_profile_file();
    ContainerCustom::new(ContainerRoot::TarImagePath(BUNDLE_PATH.to_string()), None).unwrap()
}

async fn run_seccomp_test(action: SeccompAction) -> (ContainerRunStatus, String) {
    let (uds_dir, listener) = setup();

    let opts = ContainerOptions {
        seccomp_profile: SeccompProfile::Custom {
            rules: vec![SeccompRule { syscall_number: libc::SYS_mkdir, action }],
            default_action: SeccompAction::Allow,
        },
        ..default_container_opts("try-mkdir", Some(uds_dir.path().to_path_buf()))
    };

    let mut container = default_container();
    container.start(&opts).await.unwrap();

    let mut response = String::new();
    if let Ok(Ok((mut socket, _))) = time::timeout(Duration::from_secs(2), listener.accept()).await
    {
        let _ = socket.read_to_string(&mut response).await;
    }

    sleep(Duration::from_millis(500)).await;
    (container.get_run_status().unwrap(), response)
}

#[derive(Debug, Default)]
struct MockResetSession {
    checkpoint_calls: Arc<AtomicUsize>,
    reset_calls: Arc<AtomicUsize>,
    fail: Arc<AtomicBool>,
}

impl StateResetSession for MockResetSession {
    fn checkpoint(&mut self) -> anyhow::Result<()> {
        if self.fail.load(Ordering::SeqCst) {
            anyhow::bail!("FSR checkpoint failed");
        }
        self.checkpoint_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<usize> {
        if self.fail.load(Ordering::SeqCst) {
            anyhow::bail!("FSR reset failed");
        }
        self.reset_calls.fetch_add(1, Ordering::SeqCst);
        Ok(1)
    }
}

#[derive(Debug, Default)]
struct MockResetEngine {
    create_session_calls: Arc<AtomicUsize>,
    checkpoint_calls: Arc<AtomicUsize>,
    reset_calls: Arc<AtomicUsize>,
    recorded_pid: Arc<std::sync::Mutex<Option<i32>>>,
    fail: Arc<AtomicBool>,
}

impl StateResetEngine for MockResetEngine {
    fn create_session(
        &self,
        pid: i32,
        _checkpoint_dir: PathBuf,
        _sandbox_root: Option<PathBuf>,
    ) -> anyhow::Result<Box<dyn StateResetSession>> {
        self.create_session_calls.fetch_add(1, Ordering::SeqCst);
        *self.recorded_pid.lock().unwrap() = Some(pid);
        Ok(Box::new(MockResetSession {
            checkpoint_calls: Arc::clone(&self.checkpoint_calls),
            reset_calls: Arc::clone(&self.reset_calls),
            fail: Arc::clone(&self.fail),
        }))
    }
}

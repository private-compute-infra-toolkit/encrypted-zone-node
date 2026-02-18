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

use container::{
    Container, ContainerOptions, ContainerRoot, ContainerRunStatus, MountOptions, NetworkOptions,
};
use container_custom::ContainerCustom;
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::io::Interest;
use tokio::net::UnixListener;
use tokio::time::{self, sleep, Duration};

const BUNDLE_PATH: &str = "enforcer/container/test_data/test_bundle.tar";
// Run the binary in enforcer/container/test_data/main.rs
// `/usr/local/bin/main` comes from `test_binary` pkg_tar rule in
// enforcer/container/test_data/BUILD.
const BINARY_FILENAME: &str = "/usr/local/bin/main";
const INSTANCE_NAME: &str = "test_ez1";

fn setup() -> (TempDir, UnixListener) {
    let uds_dir = tempfile::Builder::new().prefix("test_uds").tempdir().unwrap();
    let uds_socket = uds_dir.path().join("isolate_ipc");
    let uds_listener = UnixListener::bind(uds_socket.clone()).unwrap();
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
    ContainerOptions {
        name: INSTANCE_NAME.to_string(),
        binary_filename: BINARY_FILENAME.to_string(),
        command_line_arguments: vec!["--operation".to_string(), operation.to_string()],
        mounts: match mount_src {
            Some(m) => vec![MountOptions { source: m, destination: PathBuf::from("/ez") }],
            None => vec![],
        },
        network: NetworkOptions::default(),
        env: vec![],
    }
}

fn default_container() -> ContainerCustom {
    ContainerCustom::new(ContainerRoot::TarImagePath(BUNDLE_PATH.to_string())).unwrap()
}

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
    container.start(&opts).await.unwrap();
    container.stop().await.unwrap();
    container.delete().await.unwrap();
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
}

#[tokio::test]
async fn container_started_unexpected_calls_fails() {
    let opts = default_container_opts("sleep", None);
    let mut container = default_container();
    container.start(&opts).await.unwrap();
    assert!(container.start(&opts).await.is_err());
    assert!(container.delete().await.is_err());
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
        ContainerCustom::new(ContainerRoot::ReadOnlyRoot(Arc::clone(&root))).unwrap();
    let uds_dir_2 = tempfile::Builder::new().prefix("test_uds_2").tempdir().unwrap();
    let server_handle_2 =
        handle(UnixListener::bind(uds_dir_2.path().join("isolate_ipc")).unwrap(), 100);
    let mut container_2 =
        ContainerCustom::new(ContainerRoot::ReadOnlyRoot(Arc::clone(&root))).unwrap();
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
            },
            // Mount a file inside a directory that does not exist.
            MountOptions {
                source: PathBuf::from("/dev/random"),
                destination: PathBuf::from("/a/b"),
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

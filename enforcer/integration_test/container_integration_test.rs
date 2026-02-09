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

use container::{Container, ContainerOptions, ContainerRoot, MountOptions, NetworkOptions};
use container_custom::ContainerCustom;
use std::ffi::CStr;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use tempfile::NamedTempFile;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

const EZ_IPC_DIR: &str = "/ez";
const EZ_IPC_FILE: &str = "isolate_ipc";
const SHARED_MEMORY_FILE_RW: &str = "ez_container_rw";
const SHARED_MEMORY_FILE_RO: &str = "ez_container_ro";

fn create_container(bundle_path: &str) -> anyhow::Result<(Box<dyn Container>, TempDir)> {
    let root_dir = tempfile::Builder::new().prefix("test_runc_state").tempdir()?;
    let container =
        Box::new(ContainerCustom::new(ContainerRoot::TarImagePath(bundle_path.to_string()))?);
    Ok((container, root_dir))
}

fn create_uds_socket_and_listener() -> anyhow::Result<(TempDir, PathBuf, UnixListener)> {
    let uds_dir = tempfile::Builder::new().prefix("test_uds").tempdir()?;
    let uds_socket = uds_dir.path().join(EZ_IPC_FILE);
    let uds_listener = UnixListener::bind(uds_socket.clone())?;
    Ok((uds_dir, uds_socket, uds_listener))
}

async fn send_filename_and_read_response(
    filename: &str,
    uds_listener: UnixListener,
) -> anyhow::Result<Vec<u8>> {
    let (mut stream, _addr) = uds_listener.accept().await?;
    stream.write_all(filename.as_bytes()).await?;
    let mut data = vec![0; 500];
    let n = stream.read(&mut data).await?;
    Ok(data[..n].to_vec())
}

enum Operation {
    Write,
    Read,
    VerifyReadOnly,
}

fn command_line_arguments_for_operation(operation: Operation) -> Vec<String> {
    let mut args = vec!["--operation".to_string()];
    let operation_name = match operation {
        Operation::Write => "write-shm",
        Operation::Read => "read-shm",
        Operation::VerifyReadOnly => "verify-read-only-shm",
    };
    args.push(operation_name.to_string());
    args
}

// `/usr/local/bin/main` comes from `test_binary` pkg_tar rule in
// enforcer/test_data/BUILD.
// The source code of the binary is in enforcer/test_data/main.rs
const BINARY_FILENAME: &str = "/usr/local/bin/main";

// Verify that code running in a container can write to a shared memory file
// mounted dynamically.
async fn verify_write(bundle_path: &str) -> anyhow::Result<(NamedTempFile, Vec<u8>)> {
    // Create a temporary file, launcher a container and ask the workload to it
    // with random data and send back the digest.
    // Create a file in tmpfs.
    let file: NamedTempFile =
        tempfile::Builder::new().disable_cleanup(true).tempfile_in("/dev/shm")?;

    let (uds_dir, _uds_socket, uds_listener) = create_uds_socket_and_listener()?;
    let (mut container, _root_dir) = create_container(bundle_path)?;

    // Run the binary in enforcer/test_data/main.rs
    let opts = ContainerOptions {
        name: "test_ez_write".to_string(),
        binary_filename: BINARY_FILENAME.to_string(),
        command_line_arguments: command_line_arguments_for_operation(Operation::Write),
        mounts: vec![MountOptions {
            source: uds_dir.path().to_path_buf(),
            destination: PathBuf::from(EZ_IPC_DIR),
        }],
        network: NetworkOptions::default(),
        env: vec![],
    };
    container.start(&opts).await?;

    let Some(file_path) = file.path().to_str() else {
        anyhow::bail!("cannot get file path");
    };

    // Dynamically mount a writable file
    container.mount(file_path, SHARED_MEMORY_FILE_RW)?;

    // Send `shared_memory_file` over the UDS to tell the main service i.e.
    // `/usr/local/bin/main` to write random data to the specified shared memory
    // file, and then collect the response. The response is expected to be a
    // Sha256 digest of the random-data the test service wrote into the shared
    // memory file.
    let digest = send_filename_and_read_response(SHARED_MEMORY_FILE_RW, uds_listener).await?;
    Ok((file, digest))
}

// Verify that code running in a container can read from a shared memory file
// mounted dynamically.
async fn verify_read(bundle_path: &str, file: &NamedTempFile) -> anyhow::Result<Vec<u8>> {
    // Launch a container, give it a shared file containing random data and
    // ask it to read it and calculate the digest.

    let (uds_dir, _uds_socket, uds_listener) = create_uds_socket_and_listener()?;
    let (mut container, _root_dir) = create_container(bundle_path)?;

    // Run the binary in enforcer/test_data/main.rs
    let opts = ContainerOptions {
        name: "test_ez_read".to_string(),
        binary_filename: BINARY_FILENAME.to_string(),
        command_line_arguments: command_line_arguments_for_operation(Operation::Read),
        mounts: vec![MountOptions {
            source: uds_dir.path().to_path_buf(),
            destination: PathBuf::from(EZ_IPC_DIR),
        }],
        network: NetworkOptions::default(),
        env: vec![],
    };
    container.start(&opts).await?;

    let Some(file_path) = file.path().to_str() else {
        anyhow::bail!("cannot get file path");
    };

    // Dynamically mount a read-only file.
    container.mount_readonly(file_path, SHARED_MEMORY_FILE_RO)?;

    // Send `shared_memory_file` over the UDS to tell the main service i.e.
    // `/usr/local/bin/main` to read all the data in the provided shared memory
    // file, and then collect the response. The response is expected to be a
    // Sha256 digest of the content of the shared memory file.
    let digest = send_filename_and_read_response(SHARED_MEMORY_FILE_RO, uds_listener).await?;
    Ok(digest)
}

// Verify that code running in a container cannot write to a supposedly
// read-only shared memory file.
async fn verify_read_only(bundle_path: &str, file: &NamedTempFile) -> anyhow::Result<Vec<u8>> {
    // Launch a container, give it a read-only shared file and ask it to try
    // to write to it.

    let (uds_dir, _uds_socket, uds_listener) = create_uds_socket_and_listener()?;
    let (mut container, _root_dir) = create_container(bundle_path)?;

    // Run the binary in enforcer/test_data/main.rs
    let opts = ContainerOptions {
        name: "test_ez_read".to_string(),
        binary_filename: BINARY_FILENAME.to_string(),
        command_line_arguments: command_line_arguments_for_operation(Operation::VerifyReadOnly),
        mounts: vec![MountOptions {
            source: uds_dir.path().to_path_buf(),
            destination: PathBuf::from(EZ_IPC_DIR),
        }],
        network: NetworkOptions::default(),
        env: vec![],
    };
    container.start(&opts).await?;

    let Some(file_path) = file.path().to_str() else {
        anyhow::bail!("cannot get file path");
    };

    // Dynamically mount a read-only file.
    container.mount_readonly(file_path, SHARED_MEMORY_FILE_RO)?;

    // Send `shared_memory_file` over the UDS to tell the main service i.e.
    // `/usr/local/bin/main` to try to open the shared memory file for writing,
    // and then collect the response. The response is expected to report if
    // the service under test can open the file for writing or not.
    let response = send_filename_and_read_response(SHARED_MEMORY_FILE_RO, uds_listener).await?;
    Ok(response)
}

fn setup_namespace() -> anyhow::Result<()> {
    // Get the `uid` and `gid` of the process before creating a new user
    // namespace.
    let uid = users::get_current_uid();
    let gid = users::get_current_gid();

    // Create a new user, mount and cgroup namespaces.
    unsafe {
        let r = libc::unshare(libc::CLONE_NEWUSER | libc::CLONE_NEWNS | libc::CLONE_NEWCGROUP);
        if r != 0 {
            anyhow::bail!(
                "unshare failed: {:#?}",
                CStr::from_ptr(libc::strerror(*libc::__errno_location()))
            );
        }
    };

    // Disable `setgroups` is required to set {uid|gid} mappings.
    let mut file = OpenOptions::new().write(true).open("/proc/self/setgroups")?;
    file.write_all(b"deny")?;

    // Map uid 0 - root - to the uid running the program.
    let mut file = OpenOptions::new().write(true).open("/proc/self/uid_map")?;
    file.write_all(format!("0 {uid} 1").as_bytes())?;

    // Map gid 0 to the gid running the program.
    let mut file = OpenOptions::new().write(true).open("/proc/self/gid_map")?;
    file.write_all(format!("0 {gid} 1").as_bytes())?;

    Ok(())
}

fn main() -> anyhow::Result<()> {
    // `unshare()` syscall does not go along with multi-threaded processes.
    // We use `unshare()` to create new namespaces where we have the necessary
    // capabilities to mount files and other operations, instead of giving these
    // permission to the program in the main namespace.
    // We cannot use async main provided by tokio as it might create threads
    // for us before executing our code.
    // Here we use the vanilla `main()` where there is a single process, we
    // create our namespaces, then we start tokio runtime to run async functions.

    setup_namespace()?;

    let runfiles = runfiles::Runfiles::create()?;
    // `enforcer/container/test_data/test_bundle.tar` comes from the `data` attribute of
    // `container_integration_test` rust_binary rule in enforcer/BUILD.
    let bundle_path = runfiles.rlocation("_main/enforcer/container/test_data/test_bundle.tar");
    let bundle_path = bundle_path
        .expect("Failed to resolve test_bundle.tar")
        .into_os_string()
        .into_string()
        .map_err(|_| anyhow::anyhow!("failed to convert bundle_path to string."))?;

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let (file, digest_written) = verify_write(&bundle_path).await?;
        let digest_read = verify_read(&bundle_path, &file).await?;
        assert_eq!(digest_written, digest_read);
        assert_eq!(
            verify_read_only(&bundle_path, &file).await?,
            b"Read-only file system (os error 30)"
        );
        Ok::<_, anyhow::Error>(())
    })?;
    Ok(())
}

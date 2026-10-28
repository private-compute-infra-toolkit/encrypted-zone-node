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

use container::{
    Container, ContainerOptions, ContainerRoot, ContainerRunStatus, MountOptions, NetworkOptions,
};
use container_test_utils::{FakeContainer, Status};
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::test]
async fn fake_container_lifecycle_and_state_tracking() {
    FakeContainer::clear_tracker();
    let tracker = FakeContainer::get_tracker();
    assert!(tracker.is_empty());

    let tar_container =
        FakeContainer::new(ContainerRoot::TarImagePath("/tmp/bundle.tar".to_string()), None)
            .unwrap();
    assert_eq!(
        tracker.get(&tar_container.fake_container_id).unwrap().root_dir,
        PathBuf::from("/tmp/bundle.tar")
    );

    let ro_dir = Arc::new(tempfile::tempdir().unwrap());
    let expected_root = ro_dir.path().to_path_buf();
    let mut container = FakeContainer::new(ContainerRoot::ReadOnlyRoot(ro_dir), None).unwrap();
    assert_eq!(tracker.get(&container.fake_container_id).unwrap().status, Status::New);
    assert_eq!(tracker.get(&container.fake_container_id).unwrap().root_dir, expected_root);

    let opts = ContainerOptions {
        name: "test-isolate".to_string(),
        binary_filename: "/bin/isolate".to_string(),
        command_line_arguments: vec!["--flag".to_string()],
        mounts: vec![MountOptions {
            source: PathBuf::from("/tmp/share"),
            destination: PathBuf::from("/ez"),
            apply_restrictive_flags: false,
            optional: false,
        }],
        network: NetworkOptions::default(),
        env: vec!["KEY=VAL".to_string()],
        run_isolate_as_unprivileged: true,
        seccomp_profile: Default::default(),
    };

    assert!(container.start(&opts).await.is_ok());
    {
        let data = tracker.get(&container.fake_container_id).unwrap();
        assert_eq!(data.status, Status::Started);
        assert_eq!(data.binary_filename.as_deref(), Some("/bin/isolate"));
        assert_eq!(data.command_line_arguments, vec!["--flag".to_string()]);
        assert_eq!(data.env, vec!["KEY=VAL".to_string()]);
        assert_eq!(data.boot_mounts.len(), 1);
        assert_eq!(data.boot_mounts[0].source, PathBuf::from("/tmp/share"));
        assert_eq!(data.boot_mounts[0].destination, PathBuf::from("/ez"));
        assert!(!data.boot_mounts[0].apply_restrictive_flags);
        assert!(!data.boot_mounts[0].optional);
        assert_eq!(
            data.isolate_ez_bridge_enforcer_side_uds_path.as_deref(),
            Some("/tmp/share/isolate-ez-bridge-uds")
        );
        assert!(!data.checkpointed);
    }
    assert_eq!(container.get_run_status().unwrap(), ContainerRunStatus::Running);
    let stats = container.get_memory_stats().unwrap().unwrap();
    assert!(stats.rss_bytes > 0);
    assert!(stats.virt_bytes >= stats.rss_bytes);

    assert!(container.mount("/host/rw", "guest/rw").is_ok());
    assert!(container.mount_readonly("/host/ro", "guest/ro").is_ok());
    {
        let data = tracker.get(&container.fake_container_id).unwrap();
        assert_eq!(data.mounts.get("/host/rw").map(String::as_str), Some("guest/rw"));
        assert_eq!(data.readonly_mounts.get("/host/ro").map(String::as_str), Some("guest/ro"));
    }

    // Reset before checkpoint fails.
    assert!(container.reset().await.is_err());

    // Checkpoint then reset succeeds.
    assert!(container.checkpoint().await.is_ok());
    assert!(tracker.get(&container.fake_container_id).unwrap().checkpointed);
    assert!(container.reset().await.is_ok());

    assert!(container.stop().await.is_ok());
    assert_eq!(tracker.get(&container.fake_container_id).unwrap().status, Status::Stopped);

    assert!(container.delete().await.is_ok());
    assert_eq!(tracker.get(&container.fake_container_id).unwrap().status, Status::Deleted);

    FakeContainer::clear_tracker();
    assert!(tracker.is_empty());
}

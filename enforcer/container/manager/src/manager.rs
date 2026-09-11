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

pub mod dependency_graph;

use anyhow::{ensure, Context, Result};
use container::{Container, ContainerOptions, ContainerRoot, MountOptions, NetworkOptions};
use container_manager_request::{
    ContainerManagerRequest, GetRunStatusRequest, GetRunStatusResponse,
    GetSetupIsolateClientResponse, LoadWorkloadIsolatesResponse, LoadWorkloadManifestsResponse,
    MountDirectoryResponse, MountFileResponse, MountReadOnlyDirectory, MountReadOnlyFile,
    MountWritableDirectory, MountWritableFile, ResetIsolateRequest, ResetIsolateResponse,
};
use dashmap::DashMap;
use data_scope::manifest_validator::ManifestValidator;
use data_scope::request::{
    AddBackendDependenciesRequest, AddIsolateRequest, AddManifestScopeRequest, RemoveIsolateRequest,
};
use data_scope_proto::enforcer::v1::DataScopeType;
use derivative::Derivative;
use fileshare_manager::FileshareManager;
use interceptor::Interceptor;
use isolate_ez_service_manager::IsolateEzServiceManager;
use isolate_info::{
    get_isolate_name, BinaryServicesIndex, InstanceIdGenerator, IsolateId, IsolateServiceInfo,
};
use isolate_service_mapper::IsolateServiceMapper;
use junction_trait::Junction;
use manifest_parser::v1::{flatten_manifest, parse_manifest, serialize_backend_dependencies};
use manifest_parser::v2::{SetupManifest, WorkloadManifests};
use manifest_parser::{get_strictest_scope, ParsedIsolate};
use manifest_proto::enforcer::v1::ez_backend_dependency::RouteType;
use manifest_proto::enforcer::v1::{
    EzBackendDependency, EzServiceSpec, IsolateMetricsPolicy, IsolateRuntimeConfigs,
};
use nix::sys::stat::Mode;
use nix::unistd::mkfifo;
use setup_isolate_client::SetupIsolateClient;
use shared_memory_manager::SharedMemManager;
use state_manager::IsolateStateManager;
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::env;

use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::Receiver;

// The dir name that will appear in Container where all shared files and UDSs will exist.
// This dir will be mounted for a [TempDir] in Enforcer for each Isolate.
const SHARING_DIR_NAME: &str = "/enforcer-isolate-shared";
// UDS name for EZ -> Isolate Bridge. This will exist under [SHARING_DIR_NAME] for Container.
const EZ_ISOLATE_BRIDGE_UDS: &str = "ez-isolate-bridge-uds";
// UDS name for Isolate -> EZ Bridge. This will exist under [SHARING_DIR_NAME] for Container.
const ISOLATE_EZ_BRIDGE_ENFORCER_UDS: &str = "isolate-ez-bridge-uds";
// Name of FIFO used to signal from Enforcer to Isolate that the Isolate -> EZ Bridge is ready.
const ISOLATE_EZ_BRIDGE_ENFORCER_UDS_READY: &str = "isolate-ez-bridge-uds.ready";
// UDS name for exporting OTel metrics
const OTLP_METRICS_UDS: &str = "otlp-metrics.sock";
// UDS name for OTLP traces. This will exist under [SHARING_DIR_NAME] for Container.
const OTEL_TRACES_UDS: &str = "traces-otlp.sock";
const DEV_SHM_PATH: &str = "/dev/shm";
const RATIFIED_ISOLATE_DOMAIN: &str = "EZ_Trusted";

#[derive(Debug, Derivative)]
#[derivative(Clone(bound = ""))]
pub struct ContainerManager<ContainerT: Container> {
    // IsolateId to Container index (used to kill containers)
    isolate_container_map: Arc<DashMap<IsolateId, IsolateContainer<ContainerT>>>,
    // IsolateServiceIndex to ContainerStartupArgs map (used to start new containers via IsolateMngrRequest)
    container_startup_args_map: Arc<DashMap<BinaryServicesIndex, ContainerStartupArgs>>,
    etc_hosts_written: Arc<DashMap<(String, String), ()>>,
    isolate_junction: Box<dyn Junction>,
    state_manager: IsolateStateManager,
    manifest_validator: ManifestValidator,
    isolate_service_mapper: IsolateServiceMapper,
    isolate_ez_service_mngr: IsolateEzServiceManager,
    shared_mem_manager: SharedMemManager,
    fileshare_manager: FileshareManager,
    interceptor: Interceptor,
    otel_traces_endpoint: Option<String>,
    run_isolate_as_unprivileged: bool,
    enable_syscall_filtering: bool,
    operator_role: String,
    isolate_arg_config: IsolateArgConfig,
    setup_isolate_client: Option<Arc<SetupIsolateClient>>,
}

#[derive(Debug)]
pub struct ContainerManagerArgs {
    pub isolate_junction: Box<dyn Junction>,
    pub container_manager_request_rx: Receiver<ContainerManagerRequest>,
    pub isolate_state_manager: IsolateStateManager,
    pub isolate_service_mapper: IsolateServiceMapper,
    pub isolate_ez_service_mngr: IsolateEzServiceManager,
    pub shared_mem_manager: SharedMemManager,
    pub fileshare_manager: FileshareManager,
    pub manifest_validator: ManifestValidator,
    pub interceptor: Interceptor,
    pub manifest_source: ManifestSource,
    pub common_bind_mounts: Vec<String>,
    pub max_decoding_message_size: usize,
    pub isolate_runtime_configs: IsolateRuntimeConfigs,
    pub otel_traces_endpoint: Option<String>,
    pub run_isolate_as_unprivileged: bool,
    pub enable_syscall_filtering: bool,
    pub shm_num_slots: u64,
    pub shm_slot_size: u64,
    pub shm_payload_threshold: u64,
    pub operator_role: String,
}

/// Arguments that describe the shape of shared memory to be used between
/// the Enforcer and Isolate.
#[derive(Clone, Debug)]
struct ShmIPCArgs {
    shm_num_slots: u64,
    shm_slot_size: u64,
    shm_payload_threshold: u64,
}

/// Configuration parameters for isolate manifest processing and container initialization.
#[derive(Clone, Debug)]
struct IsolateArgConfig {
    common_bind_mounts: Vec<String>,
    max_decoding_message_size: usize,
    isolate_runtime_configs: IsolateRuntimeConfigs,
    shm_ipc_args: ShmIPCArgs,
}

/// Data required to initiate a new container.
#[derive(Clone, Debug)]
pub(crate) struct ContainerStartupArgs {
    binary_filename: String,
    command_line_args: Vec<String>,
    strictest_scope: DataScopeType,
    package_filename: String,
    shared_root: Option<Arc<TempDir>>,
    bind_mounts: Vec<String>,
    env_vars: Vec<String>,
    publisher_id: String,
    isolate_name: String,
    metrics_policy: IsolateMetricsPolicy,
    run_isolate_as_unprivileged: bool,
    number_of_isolates: i32,
    pub(crate) backend_dependencies: Vec<EzBackendDependency>,
}

// Internal struct to store Container and its related properties.
#[derive(Debug)]
struct IsolateContainer<ContainerT: Container> {
    pub container: ContainerT,
    // dir where all mounted data related to Container will be stored
    pub _sharing_dir: TempDir,
    pub restart_count: u32,
}

/// Source of isolate manifest configuration for ContainerManager initialization.
#[derive(Debug)]
pub enum ManifestSource {
    /// Legacy v1 monolithic or bundle manifest path.
    V1 { manifest_path: String },
    /// v2 setup isolate manifest path for bootstrap.
    V2 { setup_isolate_manifest_path: String },
}

impl<ContainerT: Container + 'static> ContainerManager<ContainerT> {
    pub async fn start(args: ContainerManagerArgs) -> Result<Self> {
        let isolate_arg_config = IsolateArgConfig {
            common_bind_mounts: args.common_bind_mounts,
            max_decoding_message_size: args.max_decoding_message_size,
            isolate_runtime_configs: args.isolate_runtime_configs,
            shm_ipc_args: ShmIPCArgs {
                shm_num_slots: args.shm_num_slots,
                shm_slot_size: args.shm_slot_size,
                shm_payload_threshold: args.shm_payload_threshold,
            },
        };

        let mut setup_isolate_client = None;
        let is_manifest_v1 = matches!(args.manifest_source, ManifestSource::V1 { .. });
        let initial_isolates = match args.manifest_source {
            ManifestSource::V1 { manifest_path } => {
                let ez_manifest =
                    parse_manifest(manifest_path).context("couldn't parse EzManifest")?;
                flatten_manifest(ez_manifest).context("Failed to flatten EzManifest")?
            }
            ManifestSource::V2 { setup_isolate_manifest_path } => {
                let setup_manifest = SetupManifest::load_from_path(setup_isolate_manifest_path)
                    .context("Failed to load v2 setup isolate manifest")?;
                let sni = setup_manifest.extract_sni_params();
                setup_isolate_client = Some(Arc::new(SetupIsolateClient::new(
                    args.isolate_junction.clone(),
                    sni.publisher_id,
                    sni.isolate_name,
                )));
                vec![setup_manifest
                    .into_parsed_isolate()
                    .context("Failed to parse setup isolate descriptor")?]
            }
        };

        let isolate_mngr = Self {
            isolate_container_map: Arc::new(DashMap::new()),
            container_startup_args_map: Arc::new(DashMap::new()),
            etc_hosts_written: Arc::new(DashMap::new()),
            isolate_junction: args.isolate_junction,
            state_manager: args.isolate_state_manager,
            manifest_validator: args.manifest_validator,
            isolate_service_mapper: args.isolate_service_mapper,
            isolate_ez_service_mngr: args.isolate_ez_service_mngr,
            shared_mem_manager: args.shared_mem_manager,
            fileshare_manager: args.fileshare_manager,
            interceptor: args.interceptor,
            otel_traces_endpoint: args.otel_traces_endpoint,
            run_isolate_as_unprivileged: args.run_isolate_as_unprivileged,
            enable_syscall_filtering: args.enable_syscall_filtering,
            operator_role: args.operator_role.clone(),
            isolate_arg_config,
            setup_isolate_client,
        };

        isolate_mngr.process_and_boot_isolates(initial_isolates).await?;
        if is_manifest_v1 {
            isolate_mngr.state_manager.set_isolates_registered();
        }

        // Spawn to avoid blocking the constructor
        let mut isolate_mngr_clone = isolate_mngr.clone();
        tokio::spawn(async move {
            isolate_mngr_clone.process_requests(args.container_manager_request_rx).await;
        });

        Ok(isolate_mngr)
    }

    /// Parses and processes workload manifests (Ratified and Opaque) without launching isolates or unpacking packages.
    /// Isolate package file paths are not processed here.
    async fn load_workload_manifests(
        &self,
        workload_manifests: WorkloadManifests,
    ) -> Result<Vec<BinaryServicesIndex>> {
        let workload_isolates = workload_manifests
            .into_parsed_isolates()
            .context("Failed to parse workload isolates")?;
        ensure!(!workload_isolates.is_empty(), "workload isolates cannot be empty");
        let indices = self.process_and_register_isolates(workload_isolates).await?;
        self.build_and_validate_dependency_graph().await?;
        self.state_manager.set_isolates_registered();
        Ok(indices)
    }

    /// Starts workload isolates given a mapping of BinaryServicesIndex to updated package paths.
    async fn load_workload_isolates(
        &self,
        isolate_packages: HashMap<BinaryServicesIndex, String>,
    ) -> Result<Vec<BinaryServicesIndex>> {
        ensure!(!isolate_packages.is_empty(), "isolate packages cannot be empty");
        let target_indices = self.update_isolate_package_filenames(isolate_packages)?;
        self.boot_isolates(&target_indices).await?;
        Ok(target_indices)
    }

    pub async fn stop(&mut self) {
        for mut isolate_id_container_ref in self.isolate_container_map.iter_mut() {
            let stop_result = isolate_id_container_ref.value_mut().container.stop().await;
            if stop_result.is_err() {
                log::error!("Error while stopping container {:?}", stop_result.err());
            }
        }
        // This will drop all values which will delete all tempDirs
        self.isolate_container_map.clear();
    }

    fn update_isolate_package_filenames(
        &self,
        isolate_packages: HashMap<BinaryServicesIndex, String>,
    ) -> Result<Vec<BinaryServicesIndex>> {
        let mut target_indices = Vec::with_capacity(isolate_packages.len());
        for (binary_index, package_path) in isolate_packages {
            let mut startup_args = self
                .container_startup_args_map
                .get_mut(&binary_index)
                .context("Registered isolate not found in startup args map")?;
            startup_args.package_filename = package_path;
            target_indices.push(binary_index);
        }
        Ok(target_indices)
    }

    // Used by v2 manifest to load manifests without booting the isolates.
    async fn process_and_register_isolates(
        &self,
        isolates: Vec<ParsedIsolate>,
    ) -> Result<Vec<BinaryServicesIndex>> {
        let indices = self.process_isolates(isolates, &self.isolate_arg_config).await?;
        for binary_services_index in &indices {
            if let Some(entry) = self.container_startup_args_map.get(binary_services_index) {
                self.state_manager
                    .register_isolate_scope(*binary_services_index, entry.strictest_scope)
                    .await;
            }
        }
        Ok(indices)
    }

    async fn build_and_validate_dependency_graph(
        &self,
    ) -> Result<HashMap<BinaryServicesIndex, HashSet<BinaryServicesIndex>>> {
        let isolate_deps_map: HashMap<BinaryServicesIndex, Vec<EzBackendDependency>> = self
            .container_startup_args_map
            .iter()
            .map(|entry| (*entry.key(), entry.value().backend_dependencies.clone()))
            .collect();

        let graph = dependency_graph::build_isolate_dependency_graph(
            &isolate_deps_map,
            &self.isolate_service_mapper,
        )
        .await
        .context("Failed to build isolate dependency graph")?;
        dependency_graph::validate_isolate_dependency_graph(&graph)?;
        Ok(graph)
    }

    async fn boot_isolates(&self, indices: &[BinaryServicesIndex]) -> Result<()> {
        let graph = self.build_and_validate_dependency_graph().await?;
        self.start_isolates(indices, &graph).await
    }

    // Used by v1 manifest to load the manifests and boot isolates.
    async fn process_and_boot_isolates(
        &self,
        isolates: Vec<ParsedIsolate>,
    ) -> Result<Vec<BinaryServicesIndex>> {
        let indices = self.process_and_register_isolates(isolates).await?;
        self.boot_isolates(&indices).await?;
        Ok(indices)
    }

    async fn process_isolates(
        &self,
        isolates: Vec<ParsedIsolate>,
        config: &IsolateArgConfig,
    ) -> Result<Vec<BinaryServicesIndex>> {
        let mut indices = Vec::new();
        for isolate in &isolates {
            let index = self.process_binary_manifest(isolate, config).await?;
            indices.push(index);
        }

        for isolate in &isolates {
            self.process_binary_backend_dependencies(
                isolate.publisher_id.clone(),
                isolate.isolate_name.clone(),
                isolate.service_specs.clone(),
                isolate.ez_backend_dependencies.clone(),
            )
            .await
            .context("Failed to add backend dependencies for binary services index")?;
        }

        for isolate in &isolates {
            for intercepting_service in isolate.services_to_intercept() {
                self.interceptor
                    .add_interceptor(intercepting_service.clone())
                    .await
                    .context("Failed to add interceptor")?;
            }
        }

        Ok(indices)
    }

    async fn process_binary_manifest(
        &self,
        isolate: &ParsedIsolate,
        config: &IsolateArgConfig,
    ) -> Result<BinaryServicesIndex> {
        let (strictest_scope, binary_services_index) = self
            .process_binary_scope(
                isolate.publisher_id.clone(),
                isolate.isolate_name.clone(),
                isolate.service_specs.clone(),
                isolate.is_ratified(),
            )
            .await
            .context("Failed to process scope of binary")?;

        let runtime_config = config.isolate_runtime_configs.configs.iter().find(|c| {
            let isolate_name_matches =
                c.isolate_name.is_empty() || c.isolate_name == isolate.isolate_name;
            isolate_name_matches
                && c.publisher_id == isolate.publisher_id
                && c.binary_filename == isolate.binary_filename()
        });
        let command_line_args = if let Some(c) = runtime_config {
            c.command_line_arguments.as_slice()
        } else {
            isolate.command_line_arguments()
        };
        let base_env_vars = if let Some(c) = runtime_config {
            c.environment_variables.as_slice()
        } else {
            isolate.environment_variables()
        };

        let etc_hosts = if let Some(c) = runtime_config { &c.etc_hosts } else { "" };
        let mut bind_mounts = config.common_bind_mounts.clone();
        if !etc_hosts.is_empty() {
            let etc_hosts_path =
                get_etc_hosts_path(&isolate.publisher_id, isolate.binary_filename());
            let etc_hosts_parent_dir =
                etc_hosts_path.parent().context("etc_hosts_path has no parent")?;
            tokio::fs::create_dir_all(etc_hosts_parent_dir)
                .await
                .context(format!("Failed to create directory {etc_hosts_parent_dir:?}"))?;

            let key = (isolate.publisher_id.clone(), isolate.binary_filename().to_string());
            if self.etc_hosts_written.insert(key, ()).is_none() {
                match tokio::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&etc_hosts_path)
                    .await
                {
                    Ok(mut file) => {
                        file.write_all(etc_hosts.as_bytes())
                            .await
                            .context(format!("Failed to write to file {:?}", etc_hosts_path))?;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        println!("File already exists: {:?}. Skipping write.", etc_hosts_path);
                    }
                    Err(e) => {
                        return Err(e).context(format!(
                            "Failed to exclusively create file {:?}",
                            etc_hosts_path
                        ));
                    }
                };
            }

            bind_mounts.push(format!("{}:{}", etc_hosts_path.display(), "/etc/hosts"));
        }

        // Copy is required because of the mutation statement.
        let mut env_vars = base_env_vars.to_vec();
        env_vars
            .push(format!("EZ_MAX_DECODING_MESSAGE_SIZE={:#?}", config.max_decoding_message_size));
        env_vars.push(format!("EZ_SHM_NUM_SLOTS={:#?}", config.shm_ipc_args.shm_num_slots));
        env_vars.push(format!("EZ_SHM_SLOT_SIZE={:#?}", config.shm_ipc_args.shm_slot_size));
        env_vars.push(format!(
            "EZ_SHM_PAYLOAD_THRESHOLD={:#?}",
            config.shm_ipc_args.shm_payload_threshold
        ));
        if !isolate.ez_backend_dependencies.is_empty() {
            let serialized_deps =
                serialize_backend_dependencies(isolate.ez_backend_dependencies.clone())?;
            env_vars.push(format!("EZ_BACKEND_DEPENDENCIES={}", serialized_deps));
        }
        env_vars.push(format!("EZ_OPERATOR_ROLE={}", self.operator_role));

        let container_startup_args = ContainerStartupArgs {
            binary_filename: isolate.binary_filename().to_string(),
            // Copy is required because command_line_args is now owned by container_startup_args.
            command_line_args: command_line_args.to_vec(),
            strictest_scope,
            package_filename: isolate.package_filename.clone(),
            shared_root: None,
            bind_mounts,
            env_vars,
            publisher_id: isolate.publisher_id.clone(),
            isolate_name: isolate.isolate_name.clone(),
            // Use an empty metrics policy if nothing is specified
            metrics_policy: isolate.metrics_policy.clone().unwrap_or_default(),
            run_isolate_as_unprivileged: self.run_isolate_as_unprivileged,
            number_of_isolates: isolate.number_of_isolates,
            backend_dependencies: isolate.ez_backend_dependencies.clone(),
        };

        self.container_startup_args_map.insert(binary_services_index, container_startup_args);

        let number_of_isolates = isolate.number_of_isolates;
        if number_of_isolates == 0 {
            log::warn!(
                "number_of_isolates is 0 for package {:#?}, no isolates will be launched.",
                isolate.publisher_id
            );
        }
        Ok(binary_services_index)
    }

    async fn process_binary_scope(
        &self,
        publisher_id: String,
        isolate_name: String,
        service_specs: Vec<EzServiceSpec>,
        is_ratified_binary: bool,
    ) -> Result<(DataScopeType, BinaryServicesIndex)> {
        let mut strictest_input_scope = DataScopeType::Unspecified;
        let mut strictest_output_scope = DataScopeType::Unspecified;
        let mut isolate_service_infos = vec![];
        if service_specs.is_empty() {
            // Isolate that never hosts a service and is just a client
            strictest_input_scope = DataScopeType::Public;
            strictest_output_scope = DataScopeType::Public;
            isolate_service_infos.push(IsolateServiceInfo {
                operator_domain: publisher_id.clone(),
                service_name: "".to_string(),
                isolate_name: isolate_name.clone(),
                publisher_id: publisher_id.clone(),
            });
        } else {
            for service_spec in service_specs.iter() {
                let (input_scope, output_scope) =
                    get_strictest_scope(service_spec.method_specs.clone());
                strictest_input_scope = max(strictest_input_scope, input_scope);
                strictest_output_scope = max(strictest_output_scope, output_scope);
                isolate_service_infos.push(IsolateServiceInfo {
                    operator_domain: publisher_id.clone(),
                    service_name: service_spec.service_name.clone(),
                    isolate_name: isolate_name.clone(),
                    publisher_id: publisher_id.clone(),
                });
            }
        }

        let binary_services_index = self
            .isolate_service_mapper
            .new_binary_index(
                isolate_service_infos.clone(),
                is_ratified_binary,
                publisher_id.clone(),
                isolate_name.clone(),
            )
            .await
            .context("Failed to get binary services index")?;

        // TODO: Perform attestation for Ratified Isolates
        ensure!(
            binary_services_index.is_ratified_binary() == publisher_id.eq(RATIFIED_ISOLATE_DOMAIN),
            "Provided Package domain is incorrect {}",
            publisher_id
        );

        self.manifest_validator
            .add_scope_info(AddManifestScopeRequest {
                binary_services_index,
                max_input_scope: strictest_input_scope,
                max_output_scope: strictest_output_scope,
            })
            .await
            .context("Failed to add service specs in manifest scope validator")?;

        let strictest_scope = max(strictest_input_scope, strictest_output_scope);
        Ok((strictest_scope, binary_services_index))
    }

    async fn process_binary_backend_dependencies(
        &self,
        publisher_id: String,
        isolate_name: String,
        service_specs: Vec<EzServiceSpec>,
        backend_dependencies: Vec<EzBackendDependency>,
    ) -> Result<()> {
        let binary_services_index = self
            .get_binary_services_index_from_first_isolate_info(
                publisher_id.to_string(),
                isolate_name,
                service_specs,
            )
            .await
            .context("Failed to fetch binary services index during post manifest processing")?;

        for backend_dependency in backend_dependencies.iter() {
            let route_type = RouteType::try_from(backend_dependency.route_type)
                .unwrap_or(RouteType::Unspecified);
            // TODO: Service indexing will eventually be based on
            // (publisher_id, operator_domain, workload_id).
            let backend_isolate_services_index = self
                .isolate_service_mapper
                .add_backend_dependency_service(
                    &IsolateServiceInfo {
                        operator_domain: backend_dependency.operator_domain.clone(),
                        service_name: backend_dependency.service_name.clone(),
                        isolate_name: backend_dependency.isolate_name.clone(),
                        publisher_id: backend_dependency.publisher_id.clone(),
                    },
                    route_type,
                )
                .await
                .context("Failed to add backend dependency in Isolate Service Mapper")?;
            self.manifest_validator
                .add_backend_dependencies(AddBackendDependenciesRequest {
                    binary_services_index,
                    dependency_index: backend_isolate_services_index,
                })
                .await
                .context("Failed to add backend dependency in Manifest Scope Validator")?;
        }
        Ok(())
    }

    async fn get_binary_services_index_from_first_isolate_info(
        &self,
        publisher_id: String,
        isolate_name: String,
        service_specs: Vec<EzServiceSpec>,
    ) -> Result<BinaryServicesIndex> {
        let first_isolate_info = if service_specs.is_empty() {
            IsolateServiceInfo {
                operator_domain: publisher_id.clone(),
                service_name: "".to_string(),
                isolate_name: isolate_name.clone(),
                publisher_id: publisher_id.clone(),
            }
        } else {
            IsolateServiceInfo {
                operator_domain: publisher_id.clone(),
                service_name: service_specs[0].service_name.clone(),
                isolate_name: isolate_name.clone(),
                publisher_id: publisher_id.clone(),
            }
        };
        self.isolate_service_mapper
            .get_binary_index(&first_isolate_info)
            .await
            .context("Failed to fetch binary services index from first isolate info")
    }

    /// Processes requests from IsolateJunction
    async fn process_requests(
        &mut self,
        mut container_manager_request_rx: Receiver<ContainerManagerRequest>,
    ) {
        while let Some(container_manager_request) = container_manager_request_rx.recv().await {
            match container_manager_request {
                ContainerManagerRequest::ResetIsolateRequest { req, resp } => {
                    let _ = resp.send(self.process_reset_isolate_request(req).await);
                }
                ContainerManagerRequest::MountWritableFile { req, resp } => {
                    let _ = resp.send(self.process_mount_writable_file_request(req).await);
                }
                ContainerManagerRequest::MountReadOnlyFile { req, resp } => {
                    let _ = resp.send(self.process_mount_read_only_file_request(req).await);
                }
                ContainerManagerRequest::MountWritableDirectory { req, resp } => {
                    let _ = resp.send(self.process_mount_writable_directory_request(req).await);
                }
                ContainerManagerRequest::MountReadOnlyDirectory { req, resp } => {
                    let _ = resp.send(self.process_mount_read_only_directory_request(req).await);
                }
                ContainerManagerRequest::GetRunStatus { req, resp } => {
                    let _ = resp.send(self.process_get_run_status_request(req).await);
                }
                ContainerManagerRequest::LoadWorkloadManifests { req, resp } => {
                    let result = self.load_workload_manifests(req.workload_manifests).await.map(
                        |registered_indices| LoadWorkloadManifestsResponse { registered_indices },
                    );
                    let _ = resp.send(result);
                }
                ContainerManagerRequest::LoadWorkloadIsolates { req, resp } => {
                    let result = self
                        .load_workload_isolates(req.isolate_packages)
                        .await
                        .map(|loaded_indices| LoadWorkloadIsolatesResponse { loaded_indices });
                    let _ = resp.send(result);
                }
                ContainerManagerRequest::GetSetupIsolateClient { resp } => {
                    let _ = resp.send(Ok(GetSetupIsolateClientResponse {
                        client: self.setup_isolate_client.clone(),
                    }));
                }
            }
        }
    }

    async fn add_new_isolate(&self, request: AddIsolateRequest, restart_count: u32) -> Result<()> {
        let binary_services_index = request.isolate_id.get_binary_services_index();
        let container_startup_args_ref = self
            .container_startup_args_map
            .get(&binary_services_index)
            .context("IsolateManager received unrecognized IsolateServiceIndex")?;
        let container_startup_args = container_startup_args_ref.value().clone();
        drop(container_startup_args_ref); // drop ref to minimize contention for DashMap

        let isolate_id = request.isolate_id;
        log::info!("adding isolate: {isolate_id:?}");
        let root = Arc::clone(
            container_startup_args
                .shared_root
                .as_ref()
                .context("shared_root is not initialized for isolate")?,
        );
        // Check that DEV_SHM_PATH exists and is a directory.
        if !std::path::Path::new(DEV_SHM_PATH).is_dir() {
            anyhow::bail!("directory not found: {}", DEV_SHM_PATH);
        }
        let sharing_dir = tempfile::Builder::new()
            .prefix(&container_startup_args.publisher_id)
            .tempdir_in(DEV_SHM_PATH)?;
        log::info!("sharing_dir: {sharing_dir:?}");
        let mut container = ContainerT::new(ContainerRoot::ReadOnlyRoot(root), None)?;

        let ez_isolate_bridge_enforcer_side_uds_path =
            get_ez_isolate_bridge_enforcer_side_uds_path(&sharing_dir);
        let isolate_ez_bridge_enforcer_side_uds_path =
            get_isolate_ez_bridge_enforcer_side_uds_path(&sharing_dir);
        let isolate_ez_bridge_ready_enforcer_side_fifo_path =
            get_isolate_ez_bridge_ready_enforcer_side_fifo_path(&sharing_dir);
        let otlp_metrics_enforcer_side_uds_path =
            get_otlp_metrics_enforcer_side_uds_path(&sharing_dir);
        let mut mounts = vec![MountOptions {
            source: sharing_dir.path().to_path_buf(),
            destination: PathBuf::from(SHARING_DIR_NAME),
            apply_restrictive_flags: true,
            optional: false,
        }];

        for mount in container_startup_args.bind_mounts {
            if let Some((source, destination)) = mount.split_once(':') {
                let source_path = PathBuf::from(source);
                mounts.push(MountOptions {
                    source: source_path,
                    destination: PathBuf::from(destination),
                    apply_restrictive_flags: false,
                    optional: false,
                });
            } else {
                log::warn!("Malformed bind mount ignored: {mount}");
            }
        }

        if let Some(ref endpoint) = self.otel_traces_endpoint {
            if let Some(source_path) = extract_unix_path(endpoint) {
                mounts.push(MountOptions {
                    source: source_path,
                    destination: PathBuf::from(SHARING_DIR_NAME).join(OTEL_TRACES_UDS),
                    apply_restrictive_flags: false,
                    optional: true,
                });
            }
        }

        // Create a fifo that the Isolate will block on until the EZ server is ready.
        mkfifo(isolate_ez_bridge_ready_enforcer_side_fifo_path.as_str(), Mode::S_IRWXU)?;
        let sharing_dir_name =
            sharing_dir.path().to_str().context("sharing_dir path is not valid UTF-8")?.to_string();
        self.shared_mem_manager
            .setup_bridge_communication_buffers(isolate_id, &sharing_dir_name)
            .await?;

        let opts = ContainerOptions {
            name: isolate_id.to_string(),
            binary_filename: container_startup_args.binary_filename.clone(),
            command_line_arguments: container_startup_args.command_line_args,
            mounts,
            env: container_startup_args.env_vars,
            #[cfg(feature = "disable_netns")]
            network: NetworkOptions {
                disable_network_namespace: true,
                bring_up_loopback_interface: false,
            },
            #[cfg(not(feature = "disable_netns"))]
            network: NetworkOptions::default(),
            run_isolate_as_unprivileged: container_startup_args.run_isolate_as_unprivileged,
            seccomp_profile: if self.enable_syscall_filtering {
                container::SeccompProfile::Default
            } else {
                container::SeccompProfile::Unconfined
            },
        };
        self.isolate_ez_service_mngr
            .start_isolate_ez_server(isolate_ez_service_manager::StartIsolateEzServerArgs {
                isolate_id,
                isolate_address: isolate_ez_bridge_enforcer_side_uds_path.clone(),
                isolate_fifo_path: isolate_ez_bridge_ready_enforcer_side_fifo_path,
                otel_metrics_address: otlp_metrics_enforcer_side_uds_path,
                metrics_policy: container_startup_args.metrics_policy,
                isolate_type: isolate_info::IsolateType {
                    isolate_name: container_startup_args.isolate_name.clone(),
                    publisher_id: container_startup_args.publisher_id,
                },
                isolate_instance_id: InstanceIdGenerator::generate().into(),
            })
            .await;

        container.start(&opts).await?;

        self.isolate_container_map.insert(
            isolate_id,
            IsolateContainer { container, _sharing_dir: sharing_dir, restart_count },
        );
        self.add_isolate_to_state_mngr(isolate_id, request.allowed_data_scope_type).await?;
        self.add_isolate_to_junction(isolate_id, ez_isolate_bridge_enforcer_side_uds_path)
            .await
            .context("IsolateJunction addIsolate failed")?;
        self.state_manager.mark_channel_connected(isolate_id).await?;
        Ok(())
    }

    async fn add_isolate_to_junction(
        &self,
        isolate_id: IsolateId,
        uds_string: String,
    ) -> Result<()> {
        self.isolate_junction.connect_isolate(isolate_id, uds_string).await?;
        Ok(())
    }

    async fn add_isolate_to_state_mngr(
        &self,
        isolate_id: IsolateId,
        allowed_data_scope_type: DataScopeType,
    ) -> Result<()> {
        let add_isolate_request = AddIsolateRequest {
            isolate_id,
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type,
        };
        self.state_manager.add_isolate(add_isolate_request).await;
        Ok(())
    }

    async fn process_reset_isolate_request(
        &mut self,
        reset_isolate_req: ResetIsolateRequest,
    ) -> Result<ResetIsolateResponse> {
        let (_isolate_id, mut isolate_container) = self
            .isolate_container_map
            .remove(&reset_isolate_req.isolate_id)
            .context("IsolateManager received unrecognized IsolateId")?;
        let restart_count = isolate_container.restart_count;
        if let Err(e) = isolate_container.container.stop().await {
            log::warn!(
                "Failed to stop container during reset for isolate {:?}: {:?}. Proceeding with cleanup and relaunch.",
                reset_isolate_req.isolate_id,
                e
            );
        }
        let binary_services_index = reset_isolate_req.isolate_id.get_binary_services_index();
        self.state_manager
            .remove_isolate(RemoveIsolateRequest { isolate_id: reset_isolate_req.isolate_id })
            .await?;
        self.isolate_ez_service_mngr.stop_isolate_servers(reset_isolate_req.isolate_id).await;
        let _ = self.shared_mem_manager.remove_isolate(reset_isolate_req.isolate_id).await;
        let _ = self.fileshare_manager.remove_isolate(reset_isolate_req.isolate_id).await;

        let container_startup_args_ref = self
            .container_startup_args_map
            .get(&binary_services_index)
            .context("IsolateManager received unrecognized IsolateServiceIndex")?;
        let container_startup_args = container_startup_args_ref.value().clone();
        drop(container_startup_args_ref); // drop ref to minimize contention for DashMap

        // Since the container has stopped, the Isolate's client is dead and won't receive this
        // response in a real scenario. However, Container Manager tests use this response to
        // synchronize and wait for the new container and its Isolate EZ Server to start.
        self.add_new_isolate(
            AddIsolateRequest {
                isolate_id: IsolateId::new(binary_services_index),
                current_data_scope_type: DataScopeType::Public,
                allowed_data_scope_type: container_startup_args.strictest_scope,
            },
            restart_count + 1,
        )
        .await?;
        Ok(ResetIsolateResponse {})
    }

    async fn process_mount_writable_file_request(
        &self,
        mount_writable_file_req: MountWritableFile,
    ) -> Result<MountFileResponse> {
        let enforcer_file_path =
            get_file_mount_enforcer_path(&mount_writable_file_req.enforcer_file_name);
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(enforcer_file_path.clone())
            .await?;
        file.set_len(
            mount_writable_file_req
                .region_size
                .try_into()
                .context("Invalid file size while mounting writable file")?,
        )
        .await
        .context("Could not set file size while mounting writable file")?;

        // Need write lock because container.mount is a mutable operation
        let mut isolate_container_ref = self
            .isolate_container_map
            .get_mut(&mount_writable_file_req.isolate_id)
            .context("Isolate not recognized while mounting writable file")?;
        let isolate_container = isolate_container_ref.value_mut();

        // Dynamically mount a writable file
        isolate_container
            .container
            .mount(&enforcer_file_path, &mount_writable_file_req.container_file_name)
            .context("Could not mount writable file")?;
        Ok(MountFileResponse {})
    }

    async fn process_mount_read_only_file_request(
        &self,
        mount_read_only_file_req: MountReadOnlyFile,
    ) -> Result<MountFileResponse> {
        // Need write lock because container.mount is a mutable operation
        let mut isolate_container_ref = self
            .isolate_container_map
            .get_mut(&mount_read_only_file_req.isolate_id)
            .context("Isolate not recognized while mounting writable file")?;
        let isolate_container = isolate_container_ref.value_mut();

        let enforcer_file_path =
            get_file_mount_enforcer_path(&mount_read_only_file_req.enforcer_file_name);

        // Dynamically mount a read-only file.
        isolate_container
            .container
            .mount_readonly(&enforcer_file_path, &mount_read_only_file_req.container_file_name)
            .context("Could not mount read-only file")?;
        Ok(MountFileResponse {})
    }

    async fn process_mount_writable_directory_request(
        &self,
        req: MountWritableDirectory,
    ) -> Result<MountDirectoryResponse> {
        let enforcer_dir_path = get_file_mount_enforcer_path(&req.enforcer_dir_name);
        tokio::fs::create_dir_all(&enforcer_dir_path)
            .await
            .context("Could not create enforcer directory")?;

        let mut isolate_container_ref = self
            .isolate_container_map
            .get_mut(&req.isolate_id)
            .context("Isolate not recognized while mounting writable directory")?;
        let isolate_container = isolate_container_ref.value_mut();

        isolate_container
            .container
            .mount(&enforcer_dir_path, &req.container_dir_name)
            .context("Could not mount writable directory")?;

        Ok(MountDirectoryResponse {})
    }

    async fn process_mount_read_only_directory_request(
        &self,
        req: MountReadOnlyDirectory,
    ) -> Result<MountDirectoryResponse> {
        let mut isolate_container_ref = self
            .isolate_container_map
            .get_mut(&req.isolate_id)
            .context("Isolate not recognized while mounting read-only directory")?;
        let isolate_container = isolate_container_ref.value_mut();

        let enforcer_dir_path = get_file_mount_enforcer_path(&req.enforcer_dir_name);

        isolate_container
            .container
            .mount_readonly(&enforcer_dir_path, &req.container_dir_name)
            .context("Could not mount read-only directory")?;

        Ok(MountDirectoryResponse {})
    }

    async fn process_get_run_status_request(
        &self,
        req: GetRunStatusRequest,
    ) -> Result<GetRunStatusResponse> {
        let isolate_container_ref = self
            .isolate_container_map
            .get(&req.isolate_id)
            .context("Isolate not recognized while getting run status")?;
        let restart_count = isolate_container_ref.value().restart_count;
        let status = isolate_container_ref.value().container.get_run_status()?;
        let memory_stats = isolate_container_ref.value().container.get_memory_stats()?;
        let (rss_bytes, peak_rss_bytes, virt_bytes, shared_bytes, data_bytes) = memory_stats
            .map(|s| {
                (
                    Some(s.rss_bytes),
                    s.peak_rss_bytes,
                    Some(s.virt_bytes),
                    Some(s.shared_bytes),
                    Some(s.data_bytes),
                )
            })
            .unwrap_or_default();
        Ok(GetRunStatusResponse {
            status,
            rss_bytes,
            peak_rss_bytes,
            virt_bytes,
            shared_bytes,
            data_bytes,
            restart_count,
        })
    }

    /// Boots all isolate replicas defined in the startup args map asynchronously in dependency order.
    async fn start_isolates(
        &self,
        indices: &[BinaryServicesIndex],
        dep_graph: &HashMap<BinaryServicesIndex, HashSet<BinaryServicesIndex>>,
    ) -> Result<()> {
        for &binary_services_index in indices {
            let mut startup_args_entry = self
                .container_startup_args_map
                .get_mut(&binary_services_index)
                .context("Unrecognized BinaryServicesIndex while starting isolates")?;
            if startup_args_entry.shared_root.is_none() {
                let package_filename = startup_args_entry.package_filename.clone();
                let shared_root = tokio::task::spawn_blocking(move || {
                    utils::unpack_file_system(&package_filename)
                })
                .await
                .context("Failed to run spawn_blocking for file system unpack")?
                .context(format!(
                    "Failed to unpack file system from {:?}",
                    startup_args_entry.package_filename
                ))?;
                startup_args_entry.shared_root = Some(Arc::new(shared_root));
            }
        }

        for &binary_services_index in indices {
            if let Some(entry) = self.container_startup_args_map.get(&binary_services_index) {
                let num_isolates = entry.number_of_isolates;
                let strictest_scope = entry.strictest_scope;
                let dependencies =
                    dep_graph.get(&binary_services_index).cloned().unwrap_or_default();
                let isolate_mngr = self.clone();

                tokio::spawn(async move {
                    let boot_res: Result<()> = async {
                        let source_isolate_name = get_isolate_name(&binary_services_index);
                        if !dependencies.is_empty() {
                            for dep_index in &dependencies {
                                isolate_mngr
                                    .state_manager
                                    .wait_for_isolate_ready(*dep_index)
                                    .await?;
                            }
                            log::info!(
                                "Dependencies are Ready for Isolate {}",
                                source_isolate_name
                            );
                        }

                        let mut join_set = tokio::task::JoinSet::new();
                        for _ in 0..num_isolates {
                            let add_req = AddIsolateRequest {
                                isolate_id: IsolateId::new(binary_services_index),
                                current_data_scope_type: DataScopeType::Public,
                                allowed_data_scope_type: strictest_scope,
                            };
                            let isolate_mngr = isolate_mngr.clone();
                            join_set.spawn(async move {
                                isolate_mngr.add_new_isolate(add_req, /*restart_count=*/ 0).await
                            });
                        }
                        while let Some(res) = join_set.join_next().await {
                            res.context("Add Isolate failed")??;
                        }
                        Ok(())
                    }
                    .await;

                    if let Err(e) = boot_res {
                        log::error!(
                            "Failed to add isolate {:?} ({}): {:?}",
                            binary_services_index,
                            get_isolate_name(&binary_services_index),
                            e
                        );
                    }
                });
            }
        }
        Ok(())
    }
}

fn get_ez_isolate_bridge_enforcer_side_uds_path(sharing_dir: &TempDir) -> String {
    sharing_dir.path().join(EZ_ISOLATE_BRIDGE_UDS).display().to_string()
}

fn get_isolate_ez_bridge_enforcer_side_uds_path(sharing_dir: &TempDir) -> String {
    sharing_dir.path().join(ISOLATE_EZ_BRIDGE_ENFORCER_UDS).display().to_string()
}

fn get_isolate_ez_bridge_ready_enforcer_side_fifo_path(sharing_dir: &TempDir) -> String {
    sharing_dir.path().join(ISOLATE_EZ_BRIDGE_ENFORCER_UDS_READY).display().to_string()
}

fn get_otlp_metrics_enforcer_side_uds_path(sharing_dir: &TempDir) -> String {
    sharing_dir.path().join(OTLP_METRICS_UDS).display().to_string()
}

fn get_file_mount_enforcer_path(enforcer_file_name: &str) -> String {
    std::path::Path::new(DEV_SHM_PATH).join(enforcer_file_name).display().to_string()
}

fn get_etc_hosts_path(publisher_id: &str, binary_filename: &str) -> PathBuf {
    let mut base_path = match env::var("TEST_TMPDIR") {
        Ok(tmpdir) => {
            println!("Under Bazel test, use TEST_TMPDIR: {}", tmpdir);
            PathBuf::from(tmpdir)
        }
        Err(_) => PathBuf::from("/tmp"),
    };

    let sanitized_publisher_id = sanitize_path_component(publisher_id);
    let sanitized_binary_filename = sanitize_path_component(binary_filename);
    base_path.push(format!(
        "isolate_etc_hosts/{}_{}/etc/hosts",
        sanitized_publisher_id, sanitized_binary_filename
    ));
    base_path
}

fn extract_unix_path(endpoint: &str) -> Option<PathBuf> {
    endpoint.strip_prefix("unix:").map(|mut path| {
        while path.starts_with("//") {
            path = &path[1..];
        }
        PathBuf::from(path)
    })
}

pub fn sanitize_path_component(component: &str) -> String {
    component
        .chars()
        .map(|c| {
            // Allow alphanumeric and a few safe separators
            if c.is_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

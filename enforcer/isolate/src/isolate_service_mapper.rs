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

use anyhow::{Context, Ok, Result};
use isolate_info::{BinaryServicesIndex, IsolateServiceIndex, IsolateServiceInfo};
use manifest_proto::enforcer::ez_backend_dependency::RouteType;
use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Mapper which maps [IsolateServiceInfo] to [IsolateServiceIndex]. Clients are
/// encouraged to clone [IsolateServiceMapper].
/// TODO: move all to ConcurrentMap
#[derive(Debug, Default, Clone)]
pub struct IsolateServiceMapper {
    isolate_service_info_index: Arc<RwLock<HashMap<IsolateServiceInfo, IsolateServiceIndex>>>,
    binary_services_index_info: Arc<RwLock<HashMap<BinaryServicesIndex, Vec<IsolateServiceInfo>>>>,
}

impl IsolateServiceMapper {
    /// Create a new [BinaryServicesIndex] based on the provided [IsolateServiceIndex] list.
    /// If the [IsolateServiceIndex] already exists, returns the already associated [BinaryServicesIndex].
    /// Also adds new mapping of [BinaryServicesIndex] to Vec<[IsolateServiceIndex]>
    pub async fn new_binary_index(
        &self,
        isolate_service_infos: Vec<IsolateServiceInfo>,
        is_ratified_binary: bool,
    ) -> Result<BinaryServicesIndex> {
        // Write lock acquired, all reads and writes are blocked now
        let mut isolate_service_info_index = self.isolate_service_info_index.write().await;
        let mut binary_services_index_info = self.binary_services_index_info.write().await;
        let new_binary_services_index = BinaryServicesIndex::new(is_ratified_binary);

        for isolate_service_info in isolate_service_infos.iter() {
            if let Entry::Vacant(entry) =
                isolate_service_info_index.entry(isolate_service_info.clone())
            {
                let new_index = IsolateServiceIndex::new(
                    Some(new_binary_services_index),
                    &isolate_service_info.operator_domain,
                    false,
                )
                .context(format!(
                    "Failed to create Isolate Service Index for {}",
                    isolate_service_info
                ))?;
                entry.insert(new_index);
            } else {
                // The service info already exists in the map.
                anyhow::bail!("Service {} already present in the map", isolate_service_info);
            }
        }
        binary_services_index_info.insert(new_binary_services_index, isolate_service_infos);
        Ok(new_binary_services_index)
    }

    /// Retrieves the [IsolateServiceIndex] for a given [IsolateServiceInfo].
    pub async fn get_service_index(
        &self,
        isolate_service_info: &IsolateServiceInfo,
    ) -> Option<IsolateServiceIndex> {
        self.isolate_service_info_index.read().await.get(isolate_service_info).cloned()
    }

    /// Retrieves the [BinaryServicesIndex] for a given [IsolateServiceInfo].
    pub async fn get_binary_index(
        &self,
        isolate_service_info: &IsolateServiceInfo,
    ) -> Option<BinaryServicesIndex> {
        match self.get_service_index(isolate_service_info).await {
            Some(isolate_service_index) => isolate_service_index.get_binary_services_index(),
            None => None,
        }
    }

    /// Retrieves the [IsolateServiceInfo] list for a given [BinaryServicesIndex].
    pub async fn get_isolate_service_infos(
        &self,
        binary_services_index: &BinaryServicesIndex,
    ) -> Option<Vec<IsolateServiceInfo>> {
        self.binary_services_index_info.read().await.get(binary_services_index).cloned()
    }

    /// Adds a service that is a backend dependency (external, remote, or internal).
    ///
    /// This is used for services that are not part of a locally managed binary but are
    /// destinations for requests. It creates an appropriate index for routing.
    /// Returns an error if the service is already registered.
    pub async fn add_backend_dependency_service(
        &self,
        isolate_service_info: &IsolateServiceInfo,
        route_type: RouteType,
    ) -> Result<IsolateServiceIndex> {
        // Acquire a write lock to safely modify the index.
        let mut isolate_service_info_index = self.isolate_service_info_index.write().await;

        match isolate_service_info_index.entry(isolate_service_info.clone()) {
            Entry::Occupied(entry) => Ok(*entry.get()),
            Entry::Vacant(entry) => {
                let is_force_external = route_type == RouteType::External;

                let new_index = IsolateServiceIndex::new(
                    None,
                    &isolate_service_info.operator_domain,
                    is_force_external,
                )
                .context(format!(
                    "Failed to create Isolate Service Index for {}",
                    isolate_service_info
                ))?;
                entry.insert(new_index);
                Ok(new_index)
            }
        }
    }
}

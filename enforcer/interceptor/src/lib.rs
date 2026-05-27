// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::{Ok, Result};
use enforcer_proto::enforcer::v1::InvokeEzRequest;
use ez_service_proto::enforcer::v1::CallRequest;
use isolate_info::IsolateServiceInfo;
use isolate_service_mapper::IsolateServiceMapper;
use manifest_proto::enforcer::v1::InterceptingServices;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Intercepts requests that implement the `TargetableRequest` trait and sends them to a Ratified
/// Isolate Service instead.
/// Interceptors can be configured in EzManifest.
#[derive(Clone, Debug)]
pub struct Interceptor {
    // Map from Opaque Service that should be intercepted to the Intercepting Ratified Service
    // TODO Consider removing the RwLock on this map by doing a mutable borrow.
    interceptor_info: Arc<RwLock<HashMap<IsolateServiceInfo, RatifiedInterceptorServiceInfo>>>,
    isolate_service_mapper: IsolateServiceMapper,
}

/// Trait for requests that can be intercepted by the Interceptor.
pub trait TargetableRequest {
    fn operator_domain(&self) -> String;
    fn set_operator_domain(&mut self, domain: String);
    fn service_name(&self) -> String;
    fn set_service_name(&mut self, name: String);
    fn set_method_name(&mut self, name: String);
    fn isolate_name(&self) -> String;
    fn set_isolate_name(&mut self, name: String);
    fn publisher_id(&self) -> String;
    fn set_publisher_id(&mut self, id: String);
}

impl TargetableRequest for CallRequest {
    fn operator_domain(&self) -> String {
        self.operator_domain.clone()
    }

    fn set_operator_domain(&mut self, domain: String) {
        self.operator_domain = domain;
    }

    fn service_name(&self) -> String {
        self.service_name.clone()
    }

    fn set_service_name(&mut self, name: String) {
        self.service_name = name;
    }

    fn set_method_name(&mut self, name: String) {
        self.method_name = name;
    }

    fn isolate_name(&self) -> String {
        self.isolate_name.clone()
    }

    fn set_isolate_name(&mut self, name: String) {
        self.isolate_name = name;
    }

    fn publisher_id(&self) -> String {
        self.publisher_id.clone()
    }

    fn set_publisher_id(&mut self, id: String) {
        self.publisher_id = id;
    }
}

impl TargetableRequest for InvokeEzRequest {
    fn operator_domain(&self) -> String {
        self.control_plane_metadata
            .as_ref()
            .map(|m| m.destination_operator_domain.clone())
            .unwrap_or_default()
    }

    fn set_operator_domain(&mut self, domain: String) {
        if let Some(m) = self.control_plane_metadata.as_mut() {
            m.destination_operator_domain = domain;
        } else {
            self.control_plane_metadata =
                Some(enforcer_proto::enforcer::v1::ControlPlaneMetadata {
                    destination_operator_domain: domain,
                    ..Default::default()
                });
        }
    }

    fn service_name(&self) -> String {
        self.control_plane_metadata
            .as_ref()
            .map(|m| m.destination_service_name.clone())
            .unwrap_or_default()
    }

    fn set_service_name(&mut self, name: String) {
        if let Some(m) = self.control_plane_metadata.as_mut() {
            m.destination_service_name = name;
        } else {
            self.control_plane_metadata =
                Some(enforcer_proto::enforcer::v1::ControlPlaneMetadata {
                    destination_service_name: name,
                    ..Default::default()
                });
        }
    }

    fn set_method_name(&mut self, name: String) {
        if let Some(m) = self.control_plane_metadata.as_mut() {
            m.destination_method_name = name;
        } else {
            self.control_plane_metadata =
                Some(enforcer_proto::enforcer::v1::ControlPlaneMetadata {
                    destination_method_name: name,
                    ..Default::default()
                });
        }
    }

    fn isolate_name(&self) -> String {
        self.control_plane_metadata
            .as_ref()
            .map(|m| m.destination_isolate_name.clone())
            .unwrap_or_default()
    }

    fn set_isolate_name(&mut self, name: String) {
        if let Some(m) = self.control_plane_metadata.as_mut() {
            m.destination_isolate_name = name;
        } else {
            self.control_plane_metadata =
                Some(enforcer_proto::enforcer::v1::ControlPlaneMetadata {
                    destination_isolate_name: name,
                    ..Default::default()
                });
        }
    }

    fn publisher_id(&self) -> String {
        self.control_plane_metadata
            .as_ref()
            .map(|m| m.destination_publisher_id.clone())
            .unwrap_or_default()
    }

    fn set_publisher_id(&mut self, id: String) {
        if let Some(m) = self.control_plane_metadata.as_mut() {
            m.destination_publisher_id = id;
        } else {
            self.control_plane_metadata =
                Some(enforcer_proto::enforcer::v1::ControlPlaneMetadata {
                    destination_publisher_id: id,
                    ..Default::default()
                });
        }
    }
}

/// Public API grpc request type
pub enum RequestType {
    Unary,
    Streaming,
}

#[derive(Clone, Debug)]
struct RatifiedInterceptorServiceInfo {
    interceptor_operator_domain: String,
    interceptor_service_name: String,
    interceptor_isolate_name: String,
    interceptor_publisher_id: String,
    interceptor_method_for_unary: String,
    interceptor_method_for_streaming: String,
}

impl Interceptor {
    pub fn new(isolate_service_mapper: IsolateServiceMapper) -> Self {
        Self { interceptor_info: Arc::new(RwLock::new(HashMap::new())), isolate_service_mapper }
    }
}

impl Interceptor {
    /// Add interceptor. Only Opaque Isolate services can be intercepted. Only Ratified Isolate
    /// services can act as interceptors.
    pub async fn add_interceptor(&self, interceptor_info: InterceptingServices) -> Result<()> {
        self.validate_intercepting_info(interceptor_info.clone()).await?;

        let opaque_service_info = IsolateServiceInfo {
            operator_domain: interceptor_info.intercepting_operator_domain.clone(),
            service_name: interceptor_info.intercepting_service_name.clone(),
            isolate_name: interceptor_info.intercepting_isolate_name.clone(),
            publisher_id: interceptor_info.intercepting_publisher_id.clone(),
        };
        let ratified_interceptor_service_info = RatifiedInterceptorServiceInfo {
            interceptor_operator_domain: interceptor_info.interceptor_operator_domain,
            interceptor_service_name: interceptor_info.interceptor_service_name,
            interceptor_isolate_name: interceptor_info.interceptor_isolate_name,
            interceptor_publisher_id: interceptor_info.interceptor_publisher_id,
            interceptor_method_for_unary: interceptor_info.interceptor_method_for_unary,
            interceptor_method_for_streaming: interceptor_info.interceptor_method_for_streaming,
        };

        self.interceptor_info
            .write()
            .await
            .insert(opaque_service_info, ratified_interceptor_service_info);
        Ok(())
    }

    /// Replace the service targeting fields with the interceptor if an interceptor is configured
    /// for the given service.
    pub async fn replace_with_interceptor<T: TargetableRequest>(
        &self,
        request: &mut T,
        request_type: RequestType,
    ) {
        let target_service_info = IsolateServiceInfo {
            operator_domain: request.operator_domain(),
            service_name: request.service_name(),
            isolate_name: request.isolate_name(),
            publisher_id: request.publisher_id(),
        };

        let interceptor_info_reader = self.interceptor_info.read().await;
        let interceptor_info = if let Some(info) = interceptor_info_reader.get(&target_service_info)
        {
            Some(info.clone())
        } else if target_service_info.isolate_name.is_empty()
            && target_service_info.publisher_id.is_empty()
        {
            // TODO: Remove this fallback logic once all services are updated to include
            // isolate_name and publisher_id. For now we match on service_name and
            // operator_domain.
            interceptor_info_reader.iter().find_map(|(key, value)| {
                if key.service_name == target_service_info.service_name
                    && key.operator_domain == target_service_info.operator_domain
                {
                    Some(value.clone())
                } else {
                    None
                }
            })
        } else {
            None
        };

        let Some(interceptor_info) = interceptor_info else {
            // No interceptor configured, nothing to do
            return;
        };

        request.set_operator_domain(interceptor_info.interceptor_operator_domain.clone());
        request.set_service_name(interceptor_info.interceptor_service_name.clone());
        request.set_isolate_name(interceptor_info.interceptor_isolate_name.clone());
        request.set_publisher_id(interceptor_info.interceptor_publisher_id.clone());

        request.set_method_name(match request_type {
            RequestType::Unary => interceptor_info.interceptor_method_for_unary.clone(),
            RequestType::Streaming => interceptor_info.interceptor_method_for_streaming.clone(),
        });
    }

    async fn validate_intercepting_info(
        &self,
        interceptor_info: InterceptingServices,
    ) -> Result<()> {
        let opaque_service_info = IsolateServiceInfo {
            operator_domain: interceptor_info.intercepting_operator_domain,
            service_name: interceptor_info.intercepting_service_name,
            isolate_name: interceptor_info.intercepting_isolate_name,
            publisher_id: interceptor_info.intercepting_publisher_id,
        };
        if let Some(opaque_binary_service_index) =
            self.isolate_service_mapper.get_binary_index(&opaque_service_info).await
        {
            if opaque_binary_service_index.is_ratified_binary() {
                anyhow::bail!("Intercepting Ratified Isolate services is not allowed")
            }
        }

        let ratified_service_info = IsolateServiceInfo {
            operator_domain: interceptor_info.interceptor_operator_domain,
            service_name: interceptor_info.interceptor_service_name,
            isolate_name: interceptor_info.interceptor_isolate_name,
            publisher_id: interceptor_info.interceptor_publisher_id,
        };
        let Some(ratified_binary_service_index) =
            self.isolate_service_mapper.get_binary_index(&ratified_service_info).await
        else {
            anyhow::bail!("Unrecognized Ratified Service provided in InterceptingServices")
        };
        if !ratified_binary_service_index.is_ratified_binary() {
            anyhow::bail!("Only Ratified Isolates Services can be set as interceptors")
        }

        if self.interceptor_info.read().await.contains_key(&opaque_service_info) {
            anyhow::bail!("Interceptor already configured for this Opaque Service")
        }

        Ok(())
    }
}

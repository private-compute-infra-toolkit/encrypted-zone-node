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

use anyhow::{ensure, Ok, Result};
use derivative::Derivative;
use external_proxy_connector_constants::EXTERNAL_PREFIX;
use once_cell::sync::Lazy;
use std::{fmt, hash::Hash};

static EXTERNAL_BINARY_SERVICES_INDEX: Lazy<BinaryServicesIndex> =
    Lazy::new(|| BinaryServicesIndex::new(false));
static REMOTE_BINARY_SERVICES_INDEX: Lazy<BinaryServicesIndex> =
    Lazy::new(|| BinaryServicesIndex::new(false));

/// Represents the destination route for a service request.
///
/// This enum is used to determine whether a request is intended for an
/// internal service, an external one, or if the destination is unknown.
#[derive(Debug, PartialEq, Eq)]
pub enum Route {
    /// The request is for an internal service.
    Internal,
    /// The request is for an external service.
    External,
    /// The request is for a service on a remote EZ instance.
    Remote,
    /// The service could not be found, returns error.
    Unknown,
}

impl Route {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Route::Internal => "internal",
            Route::External => "external",
            Route::Remote => "remote",
            Route::Unknown => "unknown",
        }
    }
}

/// `IsolateId` is a unique identifier for a single Isolate.
#[derive(Derivative, Clone, Copy, Debug)]
#[derivative(PartialEq, Eq, Hash)]
pub struct IsolateId {
    /// Selects a random 64 bit IsolateId (very low probability of collision)
    id: u64,
    /// Binary Services Index it is serving
    /// Don't include this field in hash because the ID uniquely identifies the Isolate
    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    binary_services_index: BinaryServicesIndex,
}

/// `IsolateServiceInfo` contains information about a service, without the isolate address.
#[derive(Debug, Default, PartialEq, Eq, Hash, Clone)]
pub struct IsolateServiceInfo {
    /// The domain of the service.
    pub operator_domain: String,
    /// The name of the service.
    pub service_name: String,
}

/// `IsolateServiceIndex` provides a compact, indexed representation of an `IsolateServiceInfo`.
#[derive(Debug, Derivative, Clone, Copy)]
#[derivative(PartialEq, Eq, Hash)]
pub struct IsolateServiceIndex {
    /// Selects a random 64 bit IsolateId (very low probability of collision)
    /// A randomly generated index for the service domain.
    pub operator_domain_idx: u64,
    /// Selects a random 64 bit IsolateId (very low probability of collision)
    /// A randomly generated index for the service name.
    pub service_name_idx: u64,
    /// The Binary Services Index that serves this Isolate Service
    /// For external and remote services, we will set this field to some sentinel values that identifies them
    /// Don't include this field in hash because the IDs uniquely identifies the Isolate Service
    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    binary_services_index: BinaryServicesIndex,
}

/// [BinaryServicesIndex] is a unique identifier for a set of services exposed by a binary.
/// This is used to efficiently look up the services associated with a particular binary.
#[derive(Derivative, Debug, Clone, Copy)]
#[derivative(PartialEq, Eq, Hash)]
pub struct BinaryServicesIndex {
    /// Selects a random 64 bit IsolateId (very low probability of collision)
    binary_services_idx: u64,
    /// If the Binary Services is served by a Ratified Isolate
    /// Don't include this field in hash because the ID uniquely identifies the Binary Services
    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    is_ratified_binary: bool,
}

impl IsolateServiceIndex {
    /// Returns error if there is a binary services index when is_external_service is True
    pub fn new(
        binary_services_index_option: Option<BinaryServicesIndex>,
        operator_domain: &str,
        is_external_service: bool,
    ) -> Result<Self> {
        let binary_services_index = match binary_services_index_option {
            Some(binary_service_index) => {
                ensure!(
                    !operator_domain.starts_with(EXTERNAL_PREFIX),
                    "Binary Services Index should be none for external service domain"
                );
                ensure!(
                    !is_external_service,
                    "A service with a provided BinaryServicesIndex cannot be forced external"
                );
                binary_service_index
            }
            None => {
                // TODO - Remove duplicated logic for external routing in the enforcer.
                if is_external_service || operator_domain.starts_with(EXTERNAL_PREFIX) {
                    *EXTERNAL_BINARY_SERVICES_INDEX
                } else {
                    *REMOTE_BINARY_SERVICES_INDEX
                }
            }
        };
        Ok(Self {
            operator_domain_idx: rand::random(),
            service_name_idx: rand::random(),
            binary_services_index,
        })
    }

    pub fn get_binary_services_index(&self) -> Option<BinaryServicesIndex> {
        let special_indices = [*EXTERNAL_BINARY_SERVICES_INDEX, *REMOTE_BINARY_SERVICES_INDEX];
        if special_indices.contains(&self.binary_services_index) {
            return None;
        }
        Some(self.binary_services_index)
    }

    pub fn get_request_route(&self) -> Route {
        match self.binary_services_index {
            ind if ind == *EXTERNAL_BINARY_SERVICES_INDEX => Route::External,
            ind if ind == *REMOTE_BINARY_SERVICES_INDEX => Route::Remote,
            _ => Route::Internal,
        }
    }
}

impl IsolateId {
    /// Creates a new IsolateId
    pub fn new(binary_services_index: BinaryServicesIndex) -> Self {
        Self { id: rand::random(), binary_services_index }
    }

    /// Returns true if this `IsolateId` belongs to a Ratified Isolate.
    pub fn is_ratified_isolate(&self) -> bool {
        self.binary_services_index.is_ratified_binary()
    }

    pub fn get_binary_services_index(&self) -> BinaryServicesIndex {
        self.binary_services_index
    }
}

impl BinaryServicesIndex {
    /// Creates a new BinaryServicesIndex
    pub fn new(is_ratified_binary: bool) -> Self {
        Self { binary_services_idx: rand::random(), is_ratified_binary }
    }

    /// Returns true if this `BinaryServicesIndex` is for a ratified binary.
    pub fn is_ratified_binary(&self) -> bool {
        self.is_ratified_binary
    }
}

impl fmt::Display for IsolateServiceInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IsolateServiceInfo[{}/{}]", self.operator_domain, self.service_name)
    }
}

impl fmt::Display for IsolateServiceIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let route_string = match self.binary_services_index {
            ind if ind == *EXTERNAL_BINARY_SERVICES_INDEX => "external",
            ind if ind == *REMOTE_BINARY_SERVICES_INDEX => "remote",
            _ => "internal",
        };
        write!(
            f,
            "IsolateServiceIndex[{}/{}/{}]",
            self.operator_domain_idx, self.service_name_idx, route_string,
        )
    }
}

impl fmt::Display for IsolateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:IsolateId-{}-BinaryServicesIndex-{}",
            if self.is_ratified_isolate() { "Ratified" } else { "Opaque" },
            self.id,
            self.binary_services_index.binary_services_idx
        )
    }
}

impl fmt::Display for BinaryServicesIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:BinaryServicesIndex-{}",
            if self.is_ratified_binary { "Ratified" } else { "Opaque" },
            self.binary_services_idx
        )
    }
}

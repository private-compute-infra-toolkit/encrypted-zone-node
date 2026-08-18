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

use anyhow::{Context, Result};
use ez_backend_dependencies_proto::enforcer::v2::EzBackendDependency as EzBackendDependencyV2;
use ez_service_spec_proto::enforcer::v2::EzServiceSpec as EzServiceSpecV2;
use intercepting_services_proto::enforcer::v2::InterceptingServices as InterceptingServicesV2;
use isolate_metrics_policy_proto::enforcer::v2::IsolateMetricsPolicy as IsolateMetricsPolicyV2;
use manifest_proto::enforcer::v1::{
    AllowedMetric, BinaryManifest, EzBackendDependency, EzMethodSpec, EzServiceSpec,
    InterceptingServices, IsolateMetricsPolicy,
};
use opaque_isolate_manifest_proto::enforcer::v2::OpaqueIsolateDescriptor;
use prost_reflect::prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use ratified_isolate_manifest_proto::enforcer::v2::RatifiedIsolateDescriptor;
use serde_json::de::Deserializer;
use setup_isolate_manifest_proto::enforcer::v2::SetupIsolateDescriptor;

use super::ParsedIsolate;

/// Generic helper to deserialize a JSON string into a protobuf message.
pub(crate) fn parse_proto_message<T: Message + Default>(
    pool: &DescriptorPool,
    message_name: &str,
    json_string: &str,
) -> Result<T> {
    let message_descriptor = pool
        .get_message_by_name(message_name)
        .with_context(|| format!("Couldn't find message descriptor for {message_name}"))?;
    let mut deserializer = Deserializer::from_str(json_string);
    let dynamic_message = DynamicMessage::deserialize(message_descriptor, &mut deserializer)
        .with_context(|| format!("couldn't parse {message_name}"))?;
    dynamic_message.transcode_to().with_context(|| format!("couldn't transcode {message_name}"))
}

pub(crate) struct V2ToBinaryManifestArgs {
    pub binary_filename: String,
    pub command_line_arguments: Vec<String>,
    pub unpacked_archive_size: i64,
    pub disk_reservation_size: i64,
    pub service_specs: Vec<EzServiceSpecV2>,
    pub ez_backend_dependencies: Vec<EzBackendDependencyV2>,
    pub environment_variables: Vec<String>,
    pub number_of_isolates: i32,
    pub is_ratified_isolate: bool,
    pub services_to_intercept: Vec<InterceptingServicesV2>,
    pub metrics_policy: Option<IsolateMetricsPolicyV2>,
}

pub(crate) fn build_binary_manifest_from_v2(args: V2ToBinaryManifestArgs) -> BinaryManifest {
    BinaryManifest {
        binary_filename: args.binary_filename,
        command_line_arguments: args.command_line_arguments,
        unpacked_archive_size: args.unpacked_archive_size,
        disk_reservation_size: args.disk_reservation_size,
        service_specs: convert_service_specs_v2_to_v1(args.service_specs),
        ez_backend_dependencies: convert_backend_dependencies_v2_to_v1(
            args.ez_backend_dependencies,
        ),
        environment_variables: args.environment_variables,
        number_of_isolates: args.number_of_isolates,
        is_ratified_isolate: args.is_ratified_isolate,
        services_to_intercept: convert_intercepting_services_v2_to_v1(args.services_to_intercept),
        metrics_policy: args.metrics_policy.map(convert_metrics_policy_v2_to_v1),
    }
}

/// Converts a Setup isolate descriptor into a normalized [`ParsedIsolate`].
pub(crate) fn convert_setup_descriptor_to_parsed_isolate(
    desc: SetupIsolateDescriptor,
) -> ParsedIsolate {
    ParsedIsolate {
        isolate_name: desc.isolate_name,
        publisher_id: desc.publisher_id,
        package_filename: desc.package_filename,
        binary_manifest: build_binary_manifest_from_v2(V2ToBinaryManifestArgs {
            binary_filename: desc.binary_filename,
            command_line_arguments: desc.command_line_arguments,
            unpacked_archive_size: desc.unpacked_archive_size,
            disk_reservation_size: desc.disk_reservation_size,
            service_specs: desc.service_specs,
            ez_backend_dependencies: desc.ez_backend_dependencies,
            environment_variables: desc.environment_variables,
            number_of_isolates: desc.number_of_isolates,
            is_ratified_isolate: true,
            services_to_intercept: desc.services_to_intercept,
            metrics_policy: desc.metrics_policy,
        }),
    }
}

/// Converts a Ratified isolate descriptor into a normalized [`ParsedIsolate`].
pub(crate) fn convert_ratified_descriptor_to_parsed_isolate(
    desc: RatifiedIsolateDescriptor,
) -> ParsedIsolate {
    ParsedIsolate {
        isolate_name: desc.isolate_name,
        publisher_id: desc.publisher_id,
        package_filename: desc.package_filename,
        binary_manifest: build_binary_manifest_from_v2(V2ToBinaryManifestArgs {
            binary_filename: desc.binary_filename,
            command_line_arguments: desc.command_line_arguments,
            unpacked_archive_size: desc.unpacked_archive_size,
            disk_reservation_size: desc.disk_reservation_size,
            service_specs: desc.service_specs,
            ez_backend_dependencies: desc.ez_backend_dependencies,
            environment_variables: desc.environment_variables,
            number_of_isolates: desc.number_of_isolates,
            is_ratified_isolate: true,
            services_to_intercept: desc.services_to_intercept,
            metrics_policy: desc.metrics_policy,
        }),
    }
}

/// Converts an Opaque isolate descriptor into a normalized [`ParsedIsolate`].
pub(crate) fn convert_opaque_descriptor_to_parsed_isolate(
    desc: OpaqueIsolateDescriptor,
) -> ParsedIsolate {
    ParsedIsolate {
        isolate_name: desc.isolate_name,
        publisher_id: desc.publisher_id,
        package_filename: desc.package_filename,
        binary_manifest: build_binary_manifest_from_v2(V2ToBinaryManifestArgs {
            binary_filename: desc.binary_filename,
            command_line_arguments: desc.command_line_arguments,
            unpacked_archive_size: desc.unpacked_archive_size,
            disk_reservation_size: desc.disk_reservation_size,
            service_specs: desc.service_specs,
            ez_backend_dependencies: desc.ez_backend_dependencies,
            environment_variables: desc.environment_variables,
            number_of_isolates: desc.number_of_isolates,
            is_ratified_isolate: false,
            services_to_intercept: desc.services_to_intercept,
            metrics_policy: desc.metrics_policy,
        }),
    }
}

fn convert_service_specs_v2_to_v1(specs: Vec<EzServiceSpecV2>) -> Vec<EzServiceSpec> {
    specs
        .into_iter()
        .map(|s| EzServiceSpec {
            service_name: s.service_name,
            method_specs: s
                .method_specs
                .into_iter()
                .map(|m| EzMethodSpec {
                    method_name: m.method_name,
                    input_scope_types: m.input_scope_types,
                    output_scope_types: m.output_scope_types,
                })
                .collect(),
        })
        .collect()
}

fn convert_backend_dependencies_v2_to_v1(
    deps: Vec<EzBackendDependencyV2>,
) -> Vec<EzBackendDependency> {
    deps.into_iter()
        .map(|d| EzBackendDependency {
            operator_domain: d.operator_domain,
            publisher_id: d.publisher_id,
            isolate_name: d.isolate_name,
            service_name: d.service_name,
            method_name: d.method_name,
            route_type: d.route_type,
        })
        .collect()
}

fn convert_intercepting_services_v2_to_v1(
    intercepts: Vec<InterceptingServicesV2>,
) -> Vec<InterceptingServices> {
    intercepts
        .into_iter()
        .map(|i| InterceptingServices {
            intercepting_operator_domain: i.intercepting_operator_domain,
            intercepting_publisher_id: i.intercepting_publisher_id,
            intercepting_isolate_name: i.intercepting_isolate_name,
            intercepting_service_name: i.intercepting_service_name,
            interceptor_operator_domain: i.interceptor_operator_domain,
            interceptor_publisher_id: i.interceptor_publisher_id,
            interceptor_isolate_name: i.interceptor_isolate_name,
            interceptor_service_name: i.interceptor_service_name,
            interceptor_method_for_unary: i.interceptor_method_for_unary,
            interceptor_method_for_streaming: i.interceptor_method_for_streaming,
        })
        .collect()
}

fn convert_metrics_policy_v2_to_v1(policy: IsolateMetricsPolicyV2) -> IsolateMetricsPolicy {
    IsolateMetricsPolicy {
        allowed_metrics: policy
            .allowed_metrics
            .into_iter()
            .map(|m| AllowedMetric {
                name: m.name,
                r#type: m.r#type,
                allowed_attributes: m.allowed_attributes,
            })
            .collect(),
    }
}

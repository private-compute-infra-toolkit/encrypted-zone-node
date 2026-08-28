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

use anyhow::{bail, ensure, Context, Result};
use common_proto::enforcer::v2::IsolateType;
use ez_backend_dependencies_proto::enforcer::v2::EzBackendDependency as EzBackendDependencyV2;
use ez_service_spec_proto::enforcer::v2::EzServiceSpec as EzServiceSpecV2;
use intercepting_services_proto::enforcer::v2::InterceptingServices as InterceptingServicesV2;
use isolate_metrics_policy_proto::enforcer::v2::IsolateMetricsPolicy as IsolateMetricsPolicyV2;
use isolate_startup_parameters_proto::enforcer::v2::IsolateStartupParameters;
use manifest_proto::enforcer::v1::{
    AllowedMetric, EzBackendDependency, EzMethodSpec, EzServiceSpec, InterceptingServices,
    IsolateMetricsPolicy,
};
use opaque_isolate_manifest_proto::enforcer::v2::OpaqueIsolateDescriptor;
use prost_reflect::prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use ratified_isolate_manifest_proto::enforcer::v2::RatifiedIsolateDescriptor;
use serde_json::de::Deserializer;
use setup_isolate_manifest_proto::enforcer::v2::SetupIsolateDescriptor;

use super::{IsolateKind, ParsedIsolate};

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

/// Converts a Setup isolate descriptor into a normalized [`ParsedIsolate`].
pub(crate) fn convert_setup_descriptor_to_parsed_isolate(
    desc: SetupIsolateDescriptor,
) -> Result<ParsedIsolate> {
    let (isolate_name, publisher_id) = extract_isolate_identity(desc.isolate_type)?;
    let number_of_isolates = extract_number_of_isolates(desc.startup_parameters.as_ref())?;

    Ok(ParsedIsolate {
        isolate_name,
        publisher_id,
        package_filename: desc.package_filename,
        number_of_isolates,
        service_specs: convert_service_specs_v2_to_v1(desc.service_specs)?,
        ez_backend_dependencies: convert_backend_dependencies_v2_to_v1(
            desc.ez_backend_dependencies,
        )?,
        metrics_policy: None,
        kind: IsolateKind::Ratified {
            binary_filename: desc.binary_filename,
            command_line_arguments: desc.command_line_arguments,
            environment_variables: desc.environment_variables,
            services_to_intercept: vec![],
        },
    })
}

/// Converts a Ratified isolate descriptor into a normalized [`ParsedIsolate`].
pub(crate) fn convert_ratified_descriptor_to_parsed_isolate(
    desc: RatifiedIsolateDescriptor,
) -> Result<ParsedIsolate> {
    let (isolate_name, publisher_id) = extract_isolate_identity(desc.isolate_type)?;
    let number_of_isolates = extract_number_of_isolates(desc.startup_parameters.as_ref())?;

    Ok(ParsedIsolate {
        isolate_name,
        publisher_id,
        package_filename: desc.package_filename,
        number_of_isolates,
        service_specs: convert_service_specs_v2_to_v1(desc.service_specs)?,
        ez_backend_dependencies: convert_backend_dependencies_v2_to_v1(
            desc.ez_backend_dependencies,
        )?,
        metrics_policy: desc.metrics_policy.map(convert_metrics_policy_v2_to_v1),
        kind: IsolateKind::Ratified {
            binary_filename: desc.binary_filename,
            command_line_arguments: desc.command_line_arguments,
            environment_variables: desc.environment_variables,
            services_to_intercept: convert_intercepting_services_v2_to_v1(
                desc.services_to_intercept,
            )?,
        },
    })
}

/// Converts an Opaque isolate descriptor into a normalized [`ParsedIsolate`].
pub(crate) fn convert_opaque_descriptor_to_parsed_isolate(
    desc: OpaqueIsolateDescriptor,
) -> Result<ParsedIsolate> {
    let (isolate_name, publisher_id) = extract_isolate_identity(desc.isolate_type)?;
    let number_of_isolates = extract_number_of_isolates(desc.startup_parameters.as_ref())?;

    Ok(ParsedIsolate {
        isolate_name,
        publisher_id,
        package_filename: desc.package_filename,
        number_of_isolates,
        service_specs: convert_service_specs_v2_to_v1(desc.service_specs)?,
        ez_backend_dependencies: convert_backend_dependencies_v2_to_v1(
            desc.ez_backend_dependencies,
        )?,
        metrics_policy: desc.metrics_policy.map(convert_metrics_policy_v2_to_v1),
        kind: IsolateKind::Opaque {
            binary_filename: desc.binary_filename,
            command_line_arguments: vec![],
            environment_variables: vec![],
        },
    })
}

fn convert_service_specs_v2_to_v1(specs: Vec<EzServiceSpecV2>) -> Result<Vec<EzServiceSpec>> {
    let mut out = Vec::with_capacity(specs.len());
    for s in specs {
        ensure!(!s.service_name.is_empty(), "service_name cannot be empty in EzServiceSpec");
        let mut method_specs = Vec::with_capacity(s.method_specs.len());
        for m in s.method_specs {
            ensure!(!m.method_name.is_empty(), "method_name cannot be empty in EzMethodSpec");
            method_specs.push(EzMethodSpec {
                method_name: m.method_name,
                input_scope_types: m.input_scope_types,
                output_scope_types: m.output_scope_types,
            });
        }
        out.push(EzServiceSpec { service_name: s.service_name, method_specs });
    }
    Ok(out)
}

fn convert_backend_dependencies_v2_to_v1(
    deps: Vec<EzBackendDependencyV2>,
) -> Result<Vec<EzBackendDependency>> {
    let mut out = Vec::with_capacity(deps.len());
    for d in deps {
        ensure!(!d.service_name.is_empty(), "service_name cannot be empty in EzBackendDependency");
        ensure!(!d.method_name.is_empty(), "method_name cannot be empty in EzBackendDependency");
        out.push(EzBackendDependency {
            operator_domain: d.operator_domain,
            publisher_id: d.publisher_id,
            isolate_name: d.isolate_name,
            service_name: d.service_name,
            method_name: d.method_name,
            route_type: d.route_type,
        });
    }
    Ok(out)
}

fn convert_intercepting_services_v2_to_v1(
    intercepts: Vec<InterceptingServicesV2>,
) -> Result<Vec<InterceptingServices>> {
    let mut out = Vec::with_capacity(intercepts.len());
    for i in intercepts {
        ensure!(
            !i.intercepting_service_name.is_empty(),
            "intercepting_service_name cannot be empty in InterceptingServices"
        );
        ensure!(
            !i.interceptor_service_name.is_empty(),
            "interceptor_service_name cannot be empty in InterceptingServices"
        );
        out.push(InterceptingServices {
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
        });
    }
    Ok(out)
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

fn extract_isolate_identity(isolate_type: Option<IsolateType>) -> Result<(String, String)> {
    let it = isolate_type.context("isolate_type must be specified")?;
    ensure!(!it.isolate_name.is_empty(), "isolate_name cannot be empty in isolate_type");
    ensure!(!it.publisher_id.is_empty(), "publisher_id cannot be empty in isolate_type");
    Ok((it.isolate_name, it.publisher_id))
}

fn extract_number_of_isolates(
    startup_parameters: Option<&IsolateStartupParameters>,
) -> Result<i32> {
    let num = startup_parameters.map_or(0, |sp| sp.number_of_isolates);
    if num < 0 {
        bail!("number_of_isolates cannot be negative, got {}", num);
    }
    Ok(num)
}

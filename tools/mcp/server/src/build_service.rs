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

use anyhow::Result;
use log::info;
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, Content, ServerCapabilities, ServerInfo},
    schemars,
    serde_json::json,
    tool, tool_handler, tool_router, ErrorData as McpError, ServerHandler,
};
use tokio::process::Command;

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct Targets {
    pub targets: Vec<String>,
}

#[derive(Clone, Default, Debug)]
pub struct BuildService {
    tool_router: ToolRouter<BuildService>,
}

#[tool_router]
impl BuildService {
    pub fn new() -> Self {
        Self { tool_router: Self::tool_router() }
    }

    #[tool(
        name = "ez_build_fix",
        description = "Runs rustfmt and bazel build commands and applies any changes.
        The tool executes 'bazel run @rules_rust//:rustfmt //path/to:target' and if
        that is successful executes 'bazel build //build:target' in that orders.

        Args:
        targets: A vector of string containing the list of build targets.
        "
    )]
    pub async fn ez_build_fix(
        &self,
        Parameters(args): Parameters<Targets>,
    ) -> Result<CallToolResult, McpError> {
        info!("args {:?}", args);
        let targets = args.targets;
        let rustfmt_result = self
            .run_command(
                vec![
                    String::from("bazel"),
                    String::from("run"),
                    String::from("@rules_rust//:rustfmt"),
                ],
                targets.clone(),
            )
            .await
            .map_err(|e: McpError| {
                McpError::invalid_request(
                    format!("Failed to run rustfmt: {}", e).to_string(),
                    Some(json!({ "error": e.to_string() })),
                )
            });

        match rustfmt_result {
            Ok(rustfmt_res) => {
                info!("rustfmt result {:?}", &rustfmt_res.status.success());
                if !rustfmt_res.status.success() {
                    info!("rusfmt result {:?}", &rustfmt_res.stderr);
                    let stderr = String::from_utf8_lossy(&rustfmt_res.stderr).to_string();
                    return Ok(CallToolResult::error(vec![Content::text(stderr)]));
                }
            }
            Err(e) => return Ok(CallToolResult::error(vec![Content::text(e.to_string())])),
        }

        let bazel_result = self
            .run_command(vec![String::from("bazel"), String::from("build")], targets.clone())
            .await
            .map_err(|e: McpError| {
                McpError::invalid_request(
                    format!("Failed to run bazel build: {}", e).to_string(),
                    Some(json!({ "error": e.to_string() })),
                )
            });
        match bazel_result {
            Ok(bazel_res) => {
                info!("bazel result {:?}", &bazel_res.status.success());
                if bazel_res.status.success() {
                    Ok(CallToolResult::success(vec![Content::text(
                        String::from_utf8_lossy(&bazel_res.stdout).to_string(),
                    )]))
                } else {
                    Ok(CallToolResult::error(vec![Content::text(
                        String::from_utf8_lossy(&bazel_res.stderr).to_string(),
                    )]))
                }
            }
            Err(e) => Ok(CallToolResult::error(vec![Content::text(e.to_string())])),
        }
    }

    async fn run_command(
        &self,
        command: Vec<String>,
        args: Vec<String>,
    ) -> Result<std::process::Output, McpError> {
        info!("Running {:?} with args: {:?}", command, args);
        let output =
            Command::new(&command[0]).args(&command[1..]).args(&args).output().await.map_err(
                |e| {
                    McpError::invalid_request(
                        format!("Failed to execute command: {:?} {:?} \n {}", command, args, e),
                        Some(json!({ "error": e.to_string() })),
                    )
                },
            )?;
        Ok(output)
    }
}

#[tool_handler]
impl ServerHandler for BuildService {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            instructions: Some("A server that provides tools compiling rust.".into()),
            ..Default::default()
        }
    }
}

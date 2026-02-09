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

use tonic::{Status, Streaming};

/// Simplifies Tonic Streaming by converting the Result<Option<T>, Status>
/// messages to Option<T>. Individual Status Errors are converted to Option::None.
pub struct SimpleStreamingWrapper<T> {
    // Keeping this public so that this Wrapper doesn't hide other streaming methods.
    pub inner_stream: Streaming<T>,
}

impl<T> SimpleStreamingWrapper<T> {
    pub async fn message(&mut self) -> Option<T> {
        let msg: Result<Option<T>, Status> = self.inner_stream.message().await;

        match msg {
            Ok(msg_option) => msg_option,
            Err(e) => {
                // Eventually, gRPC will be replaced by simpler & faster IPC where we
                // won't have the complex Result<Option<T>, Status>
                log::error!("Error received by gRPC stream {:?}", e);
                None
            }
        }
    }
}

impl<T> From<Streaming<T>> for SimpleStreamingWrapper<T> {
    fn from(inner_stream: Streaming<T>) -> Self {
        SimpleStreamingWrapper { inner_stream }
    }
}

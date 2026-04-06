// Copyright 2026 Google LLC
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

use crate::common::{CallTracker, MetricAttributes, ServiceMetrics};
use std::sync::Arc;
use tokio::sync::mpsc::{error::SendError, Sender};
use tokio::sync::watch;

// Shared state initialized upon the first message.
// Only holds the attributes since CallTracker is now managed by ChannelLifecycleGuard
#[derive(Clone)]
pub struct SharedMetricState {
    pub attributes: MetricAttributes,
}

/// Guard to manage the metrics lifecycle of a Proxy Channel.
///
/// Because MPSC channels have multiple senders, the CallTracker cannot be
/// embedded directly into ObservedSender, otherwise dropping a single sender
/// clone would prematurely terminate the request metric.
///
/// This guard is held in an Arc by all copies of the request sender.
/// These request senders are responsible for initializing the shared metric
/// attributes. The CallTracker is dropped and the duration metric is recorded
/// only when the last request sender is dropped.
pub struct ChannelLifecycleGuard<M: ServiceMetrics> {
    pub metrics: M,
    pub tracker: std::sync::OnceLock<CallTracker<M>>,
}

impl<M: ServiceMetrics> Drop for ChannelLifecycleGuard<M> {
    fn drop(&mut self) {
        // If it was ALREADY initialized, the CallTracker will get dropped
        // naturally when ChannelLifecycleGuard's memory is freed, thanks to RAII.
        // We only need to intervene if it's uninitialized, which means the channel was
        // opened but never used (early-abort).
        if self.tracker.get().is_none() {
            let unknown_attrs = MetricAttributes::new("unknown", "unknown", "unknown");
            let mut fallback_tracker = CallTracker::new(self.metrics.clone(), unknown_attrs.base());
            fallback_tracker.activate();

            // fallback_tracker naturally drops here, recording the "unknown" metric
            // representing the aborted stream.
        }
    }
}

// Enum to differentiate sender roles in attribute initialization
#[derive(Clone)]
enum MetricStateRole<M: ServiceMetrics> {
    Provider {
        attr_tx: Arc<watch::Sender<Option<SharedMetricState>>>,
        guard: Arc<ChannelLifecycleGuard<M>>,
    },
    Consumer(watch::Receiver<Option<SharedMetricState>>),
}

pub struct ProxyChannels<TReq, TRes, M: ServiceMetrics> {
    pub req_tx: ObservedSender<TReq, M>,
    pub req_rx: tokio::sync::mpsc::Receiver<TReq>,
    pub res_tx: ObservedSender<TRes, M>,
    pub res_rx: tokio::sync::mpsc::Receiver<TRes>,
}

/// Creates a pair of MPSC channels for bidirectional proxying.
/// The Sender sides are automatically instrumented with linked metrics.
/// Returns all 4 ends of the channels within a ProxyChannels struct.
pub fn create_proxy_channels<TReq, TRes, M>(
    buffer_size: usize,
    metrics: M,
) -> ProxyChannels<TReq, TRes, M>
where
    M: ServiceMetrics + Clone,
{
    let (req_tx, req_rx) = tokio::sync::mpsc::channel(buffer_size);
    let (res_tx, res_rx) = tokio::sync::mpsc::channel(buffer_size);

    let (wrapped_req_tx, wrapped_res_tx) = create_linked_senders(req_tx, res_tx, metrics);

    ProxyChannels { req_tx: wrapped_req_tx, req_rx, res_tx: wrapped_res_tx, res_rx }
}

/// Links two channel senders for request-response flows.
/// The request sender is responsible for initializing the shared
/// MetricAttributes and the CallTracker from the first message sent.
/// Because MPSC channels can distribute sending across multiple tasks,
/// the initialization components are wrapped in Arcs.
pub fn create_linked_senders<TReq, TRes, M>(
    req_tx: Sender<TReq>,
    res_tx: Sender<TRes>,
    metrics: M,
) -> (ObservedSender<TReq, M>, ObservedSender<TRes, M>)
where
    M: ServiceMetrics + Clone,
{
    let (attr_tx, attr_rx) = watch::channel(None);

    let req_sender = ObservedSender {
        sender: req_tx,
        metrics: metrics.clone(),
        role: MetricStateRole::Provider {
            attr_tx: Arc::new(attr_tx),
            guard: Arc::new(ChannelLifecycleGuard {
                metrics: metrics.clone(),
                tracker: std::sync::OnceLock::new(),
            }),
        },
        cached_attributes: Arc::new(std::sync::OnceLock::new()),
    };

    let res_sender = ObservedSender {
        sender: res_tx,
        metrics,
        role: MetricStateRole::Consumer(attr_rx),
        cached_attributes: Arc::new(std::sync::OnceLock::new()),
    };

    (req_sender, res_sender)
}

/// A wrapper around tokio::sync::mpsc::Sender that records metrics.
pub struct ObservedSender<T, M: ServiceMetrics> {
    sender: Sender<T>,
    metrics: M,
    role: MetricStateRole<M>,
    cached_attributes: Arc<std::sync::OnceLock<MetricAttributes>>,
}

impl<T, M: ServiceMetrics + Clone> Clone for ObservedSender<T, M> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            metrics: self.metrics.clone(),
            role: self.role.clone(),
            cached_attributes: self.cached_attributes.clone(),
        }
    }
}

impl<T, M> ObservedSender<T, M>
where
    M: ServiceMetrics + Clone,
{
    fn ensure_attributes_initialized(&self, value: &T)
    where
        MetricAttributes: for<'a> From<&'a T>,
    {
        if let MetricStateRole::Provider { attr_tx, guard } = &self.role {
            attr_tx.send_if_modified(|shared| {
                if shared.is_none() {
                    let extracted = MetricAttributes::from(value);

                    let mut tracker = CallTracker::new(self.metrics.clone(), extracted.base());
                    tracker.activate();

                    let _ = guard.tracker.set(tracker);

                    let state = SharedMetricState { attributes: extracted.clone() };

                    *shared = Some(state);
                    self.cached_attributes.get_or_init(|| extracted);
                    true
                } else {
                    false
                }
            });
        }
    }

    async fn get_attributes(&self) -> &MetricAttributes {
        if let Some(attrs) = self.cached_attributes.get() {
            return attrs;
        }

        let resolved_state = match &self.role {
            MetricStateRole::Provider { attr_tx, .. } => attr_tx.borrow().clone(),
            MetricStateRole::Consumer(rx) => {
                let mut attr_rx = rx.clone();

                let cloned_state = attr_rx.borrow_and_update().clone();
                if let Some(state) = cloned_state {
                    Some(state)
                } else if attr_rx.changed().await.is_ok() {
                    attr_rx.borrow().clone()
                } else {
                    None
                }
            }
        };

        self.cached_attributes.get_or_init(|| {
            resolved_state
                .map(|state| state.attributes)
                .unwrap_or_else(|| MetricAttributes::new("unknown", "unknown", "unknown"))
        })
    }

    /// Sends a value directly to the underlying channel without recording metrics or blocking
    /// on attribute initialization. Use this for early error responses.
    pub async fn unobserved_send(&self, value: T) -> Result<(), SendError<T>> {
        self.sender.send(value).await
    }

    /// Sends a response and records size metrics.
    ///
    /// This method is blocking until metric attributes are initialized
    /// by the first request sent via the paired send_request method.
    /// If all clones of the request Sender drop before initialization, it defaults
    /// to unknown attributes and unblocks.
    ///
    /// WARNING: Do not call this to return early errors before forwarding the
    /// first request to avoid a deadlock. Use unobserved_send instead.
    pub async fn observed_send(&self, value: T, size: u64) -> Result<(), SendError<T>> {
        let attrs = self.get_attributes().await;
        self.metrics.record_message_size_bytes(attrs.response(), size);
        self.sender.send(value).await
    }
}

// Extension for processing Request items implementing prost::Message
impl<T, M: ServiceMetrics + Clone> ObservedSender<T, M>
where
    T: prost::Message,
{
    /// Sends a request value, extracting and initializing MetricAttributes.
    pub async fn send_request(&self, value: T) -> Result<(), SendError<T>>
    where
        MetricAttributes: for<'a> From<&'a T>,
    {
        self.ensure_attributes_initialized(&value);
        let attrs = self.get_attributes().await;
        self.metrics.record_message_size_bytes(attrs.request(), value.encoded_len() as u64);
        self.sender.send(value).await
    }

    /// Sends a response directly.
    pub async fn send_response_and_block_for_attrs(&self, value: T) -> Result<(), SendError<T>> {
        let size = value.encoded_len() as u64;
        self.observed_send(value, size).await
    }
}

// Extension for processing Result wrapper items containing a prost::Message
impl<T, E, M: ServiceMetrics + Clone> ObservedSender<Result<T, E>, M>
where
    T: prost::Message,
{
    /// Sends a response Result.
    pub async fn send_response(&self, value: Result<T, E>) -> Result<(), SendError<Result<T, E>>> {
        let size = match &value {
            Ok(msg) => msg.encoded_len() as u64,
            Err(_) => 0,
        };
        self.observed_send(value, size).await
    }
}

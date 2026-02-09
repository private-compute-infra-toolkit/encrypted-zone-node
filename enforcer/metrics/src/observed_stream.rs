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
use futures::task::AtomicWaker;
use prost::Message;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio_stream::Stream;

struct SharedState<M: ServiceMetrics> {
    attr: Mutex<Option<MetricAttributes>>,
    call_tracker: Mutex<Option<CallTracker<M>>>,
    waker: AtomicWaker,
}

/// Creates a linked pair of wrappers for observing a bi-directional stream:
/// ObservedRequestStream: Wraps the incoming request stream.
/// ObservedResponseStream: Wraps the response stream.
///
/// Note on Blocking Behavior:
/// The response stream will not yield messages until the first request message
/// is received. This is necessary to extract metric attributes from the
/// initial request for stream-wide tagging.
///
/// This is safe in EZ as all streams require an initial client request
/// to identify the destination Isolate.
pub fn pair<'a, SReq, SRes, M, TReq, TRes, EReq, ERes>(
    request_stream: SReq,
    response_stream: SRes,
    metrics: M,
) -> (ObservedRequestStream<SReq, M>, ObservedResponseStream<SRes, M>)
where
    SReq: Stream<Item = Result<TReq, EReq>> + Unpin,
    SRes: Stream<Item = Result<TRes, ERes>> + Unpin,
    M: ServiceMetrics + Clone,
    TReq: Message + 'a,
    TRes: Message,
    MetricAttributes: From<&'a TReq>,
{
    let shared_state = Arc::new(SharedState {
        attr: Mutex::new(None),
        call_tracker: Mutex::new(None),
        waker: AtomicWaker::new(),
    });

    let req_wrapper = ObservedRequestStream {
        stream: request_stream,
        metrics: metrics.clone(),
        shared_state: shared_state.clone(),
        cached_attributes: MetricAttributes::new("unknown", "unknown", "unknown"),
        attributes_initialized: false,
    };

    let res_wrapper = ObservedResponseStream {
        stream: response_stream,
        metrics,
        shared_state,
        cached_attributes: MetricAttributes::new("unknown", "unknown", "unknown"),
        attributes_initialized: false,
    };

    (req_wrapper, res_wrapper)
}

/// Lazily inits metrics on the first message and passes context to Response Wrapper.
///
/// This wrapper also simplifies the error/option layer by swallowing errors and
/// returning 'None' on error, so the stream yields 'T' directly instead of 'Result<T, E>'.
pub struct ObservedRequestStream<S, M: ServiceMetrics> {
    stream: S,
    metrics: M,
    shared_state: Arc<SharedState<M>>,
    cached_attributes: MetricAttributes,
    attributes_initialized: bool,
}

impl<S, M, T, E> ObservedRequestStream<S, M>
where
    S: Stream<Item = Result<T, E>> + Unpin,
    M: ServiceMetrics + Clone + Unpin,
    T: Message,
    E: Unpin + std::fmt::Debug,
    MetricAttributes: for<'a> From<&'a T>,
{
    /// Returns the cached MetricAttributes.
    pub fn attributes(&self) -> &MetricAttributes {
        &self.cached_attributes
    }

    fn initialize_attributes(&mut self, msg: &T) {
        let mut attr_guard = self.shared_state.attr.lock().unwrap_or_else(|e| e.into_inner());

        if attr_guard.is_none() {
            let extracted_attr = MetricAttributes::from(msg);

            // Create and store the CallTracker in SharedState
            let tracker = CallTracker::new(self.metrics.clone(), extracted_attr.base());
            let mut tracker_guard =
                self.shared_state.call_tracker.lock().unwrap_or_else(|e| e.into_inner());
            *tracker_guard = Some(tracker);

            *attr_guard = Some(extracted_attr.clone());
            self.cached_attributes = extracted_attr;
            self.attributes_initialized = true;
            // Wake up the response stream task, if it's waiting.
            self.shared_state.waker.wake();
        } else if let Some(attr) = attr_guard.as_ref() {
            self.cached_attributes = attr.clone();
            self.attributes_initialized = true;
        }
    }
}

impl<S, M, T, E> Stream for ObservedRequestStream<S, M>
where
    S: Stream<Item = Result<T, E>> + Unpin,
    M: ServiceMetrics + Clone + Unpin,
    T: Message,
    E: Unpin + std::fmt::Debug,
    MetricAttributes: for<'a> From<&'a T>,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.stream).poll_next(cx);

        if let Poll::Ready(Some(Ok(ref msg))) = poll {
            if !this.attributes_initialized {
                this.initialize_attributes(msg);
            }

            this.metrics.record_message_size_bytes(
                this.cached_attributes.request(),
                msg.encoded_len() as u64,
            );
        }

        match poll {
            Poll::Ready(Some(Ok(msg))) => Poll::Ready(Some(msg)),
            Poll::Ready(Some(Err(e))) => {
                log::error!("Error received by gRPC stream {:?}", e);
                Poll::Ready(None)
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S, M: ServiceMetrics> Drop for ObservedRequestStream<S, M> {
    fn drop(&mut self) {
        let mut attr_guard = self.shared_state.attr.lock().unwrap_or_else(|e| e.into_inner());

        if attr_guard.is_none() {
            // Stream ended (or failed) before the first message was received.
            let unknown_attrs = MetricAttributes::new("unknown", "unknown", "unknown");

            let tracker = CallTracker::new(self.metrics.clone(), unknown_attrs.base());
            let mut tracker_guard =
                self.shared_state.call_tracker.lock().unwrap_or_else(|e| e.into_inner());
            *tracker_guard = Some(tracker);

            *attr_guard = Some(unknown_attrs);
            self.shared_state.waker.wake();
        }
    }
}

pub struct ObservedResponseStream<RS, M: ServiceMetrics> {
    stream: RS,
    metrics: M,
    shared_state: Arc<SharedState<M>>,
    cached_attributes: MetricAttributes,
    attributes_initialized: bool,
}

impl<RS, M: ServiceMetrics> ObservedResponseStream<RS, M> {
    pub fn attributes(&self) -> &MetricAttributes {
        &self.cached_attributes
    }
}

impl<RS, M, T, E> Stream for ObservedResponseStream<RS, M>
where
    RS: Stream<Item = Result<T, E>> + Unpin,
    M: ServiceMetrics + Clone + Unpin,
    T: Message + Unpin,
    E: Unpin,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if !this.attributes_initialized {
            this.shared_state.waker.register(cx.waker());
            let attr_guard = this.shared_state.attr.lock().unwrap_or_else(|e| e.into_inner());

            if let Some(attr) = &*attr_guard {
                this.cached_attributes = attr.clone();
                this.attributes_initialized = true;
            } else {
                return Poll::Pending;
            }
        }

        let poll = Pin::new(&mut this.stream).poll_next(cx);

        if let Poll::Ready(Some(Ok(ref msg))) = poll {
            let attr = &this.cached_attributes;
            this.metrics.record_message_size_bytes(attr.response(), msg.encoded_len() as u64);
        }
        poll
    }
}

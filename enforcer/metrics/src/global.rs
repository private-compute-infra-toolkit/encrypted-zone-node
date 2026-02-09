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

use opentelemetry::metrics::{Meter, MeterProvider};
use opentelemetry::InstrumentationScope;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use std::sync::{Arc, OnceLock, RwLock};

type DynMeterProvider = Arc<dyn MeterProvider + Send + Sync>;

struct GlobalProviders {
    safe_provider: DynMeterProvider,
    unsafe_provider: DynMeterProvider,
}

impl GlobalProviders {
    fn new() -> Self {
        // This is effectively a "No-Op" provider that ignores the global registry.
        let noop_fallback = || Arc::new(SdkMeterProvider::builder().build());

        Self { safe_provider: noop_fallback(), unsafe_provider: noop_fallback() }
    }
}

/// The global singleton holding both providers.
static GLOBAL_PROVIDERS: OnceLock<RwLock<GlobalProviders>> = OnceLock::new();

#[inline]
fn global_providers() -> &'static RwLock<GlobalProviders> {
    GLOBAL_PROVIDERS.get_or_init(|| RwLock::new(GlobalProviders::new()))
}

// =========================================================================
// SAFE METERS
// =========================================================================

/// Sets the "Safe" MeterProvider.
pub fn set_safe_meter_provider<P>(new_provider: P)
where
    P: MeterProvider + Send + Sync + 'static,
{
    let mut global = global_providers().write();
    if let Ok(ref mut state) = global {
        state.safe_provider = Arc::new(new_provider);
        log::info!(target: "MeterProvider.SafeSet", "Safe global meter provider set.");
    } else {
        log::error!(target: "MeterProvider.SafeSetFailed", "Failed to set safe global provider.");
    }
}

/// Returns the currently configured "Safe" global MeterProvider.
pub fn safe_meter_provider() -> DynMeterProvider {
    let global = global_providers().read();
    if let Ok(state) = global {
        state.safe_provider.clone()
    } else {
        log::error!(target: "MeterProvider.SafeGetFailed", "Failed to get safe global provider.");
        // Fallback to a fresh isolated NoOp
        Arc::new(SdkMeterProvider::builder().build())
    }
}

/// Creates a named Meter via the "Safe" global provider.
pub fn safe_meter(name: &'static str) -> Meter {
    safe_meter_provider().meter(name)
}

/// Creates a Meter with scope via the "Safe" global provider.
pub fn safe_meter_with_scope(scope: InstrumentationScope) -> Meter {
    safe_meter_provider().meter_with_scope(scope)
}

// =========================================================================
// UNSAFE METERS
// =========================================================================

/// Sets the "Unsafe" MeterProvider.
pub fn set_unsafe_meter_provider<P>(new_provider: P)
where
    P: MeterProvider + Send + Sync + 'static,
{
    let mut global = global_providers().write();
    if let Ok(ref mut state) = global {
        state.unsafe_provider = Arc::new(new_provider);
        log::info!(target: "MeterProvider.UnsafeSet", "Unsafe global meter provider set.");
    } else {
        log::error!(target: "MeterProvider.UnsafeSetFailed", "Failed to set unsafe global provider.");
    }
}

/// Returns the currently configured "Unsafe" global MeterProvider.
pub fn unsafe_meter_provider() -> DynMeterProvider {
    let global = global_providers().read();
    if let Ok(state) = global {
        state.unsafe_provider.clone()
    } else {
        log::error!(target: "MeterProvider.UnsafeGetFailed", "Failed to get unsafe global provider.");
        // Fallback to a fresh isolated NoOp
        Arc::new(SdkMeterProvider::builder().build())
    }
}

/// Creates a named Meter via the "Unsafe" global provider.
pub fn unsafe_meter(name: &'static str) -> Meter {
    unsafe_meter_provider().meter(name)
}

/// Creates a Meter with scope via the "Unsafe" global provider.
pub fn unsafe_meter_with_scope(scope: InstrumentationScope) -> Meter {
    unsafe_meter_provider().meter_with_scope(scope)
}

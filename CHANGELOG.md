# Changelog

All notable changes to this project will be documented in this file. See [commit-and-tag-version](https://github.com/absolute-version/commit-and-tag-version) for commit guidelines.

## 0.15.0 (2026-06-26)


### EZ Policy Enforcer

* **enforcer:** add container_restart_count to EzIsolateHealth
* **enforcer:** Disable egress of Domain scoped data from PublicApi
* **enforcer:** Disable tracing when UDS connection fail
* **enforcer:** Handle context propagation in streams
* **enforcer:** Inject traceparent from current span in public API
* **enforcer:** report restart count in HealthManager
* **enforcer:** Sanitize etc/hosts path components
* **enforcer:** Shutdown enforcer when metrics UDS connection fail


### Features

* add isolate_name to IsolateRuntimeConfig
* **container:** track and report restart count in ContainerManager
* match IsolateRuntimeConfig by isolate_name
* **setup_isolate:** Add operator role and hint
* **setup_isolate:** Add operator_role as argument
* **traces:** Enable traces in enforcer

## 0.14.0 (2026-06-25)


### Dependencies

* **deps:** Update DevKit to release-3.9.0


### EZ Policy Enforcer

* **enforcer:** record pre-ready exit metric on container crash


### Bug Fixes

* missing ELF due to extractor absolute symlink changes


### Documentation

* **gemini:** Add rebasing and conflict resolution to Gerrit skill

## 0.13.0 (2026-06-11)


### EZ Policy Enforcer

* **enforcer:** Add detailed enforcer memory metrics
* **enforcer:** Add enforcer integration test case for histograms
* **enforcer:** Add more test cases for histogram integration test
* **enforcer:** Add per-isolate container memory to health metrics
* **enforcer:** bake version from version.txt into binary and metrics
* **enforcer:** define pre-ready exit health metric
* **enforcer:** Emit detailed memory metrics for isolates
* **enforcer:** fallback search in compatible data scopes
* **enforcer:** Remove not allowed attributes when filtering
* **enforcer:** simplify telemetry schema - remove ez_isolate_name
* **enforcer:** simplify telemetry schema - remove ez_isolate_type
* **enforcer:** simplify telemetry schema - remove ez_publisher_id
* **enforcer:** standardize telemetry attributes as scope attributes



### Bug Fixes

* **node:** Use tokio fs, spawn blocking as needed




## 0.12.0 (2026-06-02)


### Dependencies

* **deps:** Update DevKit to release-3.8.0


### EZ Policy Enforcer

* **enforcer:** Create offline metrics simulator skeleton
* **enforcer:** Enable metric filtering by default
* **enforcer:** Enable traces in all environments
* **enforcer:** restrict disable_metrics_filtering to debug mode
* **enforcer:** tag self-telemetry with component


### Features

* EzIsolateBridge support for shared mem payloads (streaming)
* IsolateEzBridge support for shared mem payloads (streaming)
* **metrics:** Enrich enforcer metrics
* **metrics:** Support BundleManifest in metrics_simulator


### Bug Fixes

* Apply MS_NOEXEC, MS_NOSUID, MS_NODEV to IPC shared memory mount
* **container:** Prevent arbitrary file creation via intermediate symlink
* **container:** Prevent arbitrary file truncation via malicious symlinks

## 0.11.0 (2026-05-27)


### Dependencies

* **deps:** Lock boring and boring-sys versions
* **deps:** Update DevKit to release-3.6.0
* **deps:** Update DevKit to release-3.7.0


### EZ Policy Enforcer

* **enforcer:** add allowed_attributes to proto
* **enforcer:** filter metric attributes
* **enforcer:** Implement file-based ShmSlabPool
* **enforcer:** Utilize /enforcer-writes for InvokeEzResponse
* **enforcer:** Utilize /isolate-writes for InvokeIsolateResponse


### mTLS

* **mtls:** Add extra unit tests for routing
* **mtls:** add EzBackendDependencies wrapper
* **mtls:** add fields to IsolateServiceInfo
* **mtls:** pass backend deps to Isolate
* **mtls:** qualify mTLS interceptor routing
* **mtls:** update service mapping logic


### Features

* Add support for ShmSlabPool env vars
* Enable wildcards in metrics allowlist
* **metrics:** Enrich isolate metrics
* optimize enforcer binary used in perf-mpm
* Process payloads received via shared memory in InvokeEzRequest
* Setup enforcer isolate shm slab buffers
* utilize ShmSlabPool for InvokeIsolateRequest payloads


### Bug Fixes

* Update OTel namespace from enforcer to encrypted_zone.enforcer

## 0.10.0 (2026-05-13)


### EZ Policy Enforcer

* **enforcer:** Adjust sampling rate


### Bug Fixes

* streaming deadlock in Isolate Junction proxy

## 0.9.0 (2026-05-11)


### Dependencies

* **deps:** Update DevKit to release-3.1.1
* **deps:** Update DevKit to release-3.2.0
* **deps:** Update DevKit to release-3.3.0
* **deps:** Update DevKit to release-3.4.0
* **deps:** Update DevKit to release-3.5.0


### EZ Policy Enforcer

* **enforcer:** Add parameter to avoid duplicate service name
* **enforcer:** Fix race condition in metrics startup + OTel endpoint bug
* **enforcer:** Make console_subscriber_port optional
* **enforcer:** Mount traces uds socket inside isolate
* **enforcer:** remove allowed_attributes field from AllowedMetric
* **enforcer:** Rename source metric attribute to ez_component_name
* **enforcer:** start UDS reception server to route Isolate metrics
* **enforcer:** Switch to EzHybridPayload as a no-op


### mTLS

* **mtls:** add TLS networking utilities to grpc_connector
* **mtls:** integrate mTLS into inbound gRPC handler
* **mtls:** integrate mTLS into outbound gRPC handler


### Features

* Add cc_proto/cpp_grpc_libs for diagnostics
* add flag to run isolates as unprivileged user
* Add HybridPayload definition to ez_payload.proto
* Add ShmSlotReference and ShmSlotData to ez_shm_payload.proto
* Add use_devkit Rule for Jeskit
* **container:** mount OTel metrics UDS into Isolate containers
* Enable devkit/gitlinks check during pre-commit
* ignore .agents/ dir used for Jetski Rules/Skills
* implement *secure* tmpfs mount at /tmp
* improve msg when rejecting isolate attempted comms
* **metrics:** add receiver for filtering and identity injection
* **node:** Improve health manager logging
* Remove options.proto


### Bug Fixes

* Allow different Isolates with binary & pub id
* forcefully disable otlp traces
* Reinstate otel_safe_endpoint flag
* Use unsafe endpoint for isolate metrics


### Documentation

* Fix links in README files

## 0.8.0 (2026-04-06)


### ⚠ BREAKING CHANGES

* Version ez_manifest.proto

### Dependencies

* **deps:** Update DevKit to release-2.11.0
* **deps:** Update DevKit to release-2.12.0
* **deps:** Update DevKit to release-2.13.0
* **deps:** Update DevKit to release-2.14.0
* **deps:** Update DevKit to release-2.15.0
* **deps:** Update DevKit to release-3.0.0


### Diagnostics

* **diagnostics:** add pprof cpu profiling
* **diagnostics:** gzip compress pprof CPU profile data
* **diagnostics:** implement Tokio profile aggregation
* **diagnostics:** implement TokioDiagnosticService
* **diagnostics:** integrate ConsoleHelper and DiagnosticService
* **diagnostics:** simplify exports_files for buf.yaml
* **diagnostics:** validate top_n in TokioDiagnosticService


### EZ Policy Enforcer

* **enforcer:** add diagnostic gRPC service
* **enforcer:** add diagnostics service proto definitions
* **enforcer:** add initialization metric to OTel pipeline
* **enforcer:** add logging and improve error reporting in diagnostics
* **enforcer:** add PprofService for CPU profiling
* **enforcer:** add PprofService to diagnostics API
* **enforcer:** Add tests for console_helper
* **enforcer:** Add tests for console-subscriber
* **enforcer:** Add traces directory in debug mode
* **enforcer:** Allow external call interception
* **enforcer:** Correct error reporting for ez_to_external streaming
* **enforcer:** Fileshare implementation
* **enforcer:** Fix ExponentialBackoff arguments ([5b30c57]( )), closes [/docs.rs/tokio-retry/latest/src/tokio_retry/strategy/exponential_backoff.rs.html#77]( )
* **enforcer:** Handle console subscriber bind failures gracefully
* **enforcer:** Implement console helper for tokio monitoring
* **enforcer:** implement on-demand CPU profiling with duration support
* **enforcer:** implement pprof CPU profiling in DiagnosticService
* **enforcer:** Integrate console-subscriber to the telemetry setup
* **enforcer:** Override default histogram bucket boundaries
* **enforcer:** Prevent intercept recursion
* **enforcer:** Propagate error context more
* **enforcer:** remove outdated external prefix and fix build issue
* **enforcer:** Remove scope validation from Ratified Isolate Manager
* **enforcer:** require positive duration for diagnostic profiles
* **enforcer:** update unary error message
* **enforcer:** use IPv6 and non-privileged port for tokio-console
* **enforcer:** Use oneof for diagnostics profile specification
* **enforcer:** use typed results for diagnostics profile response


### mTLS

* **mtls:** add boring ssl dependency
* **mtls:** Add ez mtls services for EZ to invoke
* **mtls:** add EzMtls client
* **mtls:** Add EzMtlsManager for mtls resources
* **mtls:** Add leaf key / csr loading from disk
* **mtls:** Add SPIFFE ID parsing logic
* **mtls:** Add SpiffePolicy
* **mtls:** Add test keys for mTLS
* **mtls:** Implement BoringTlsStream
* **mtls:** implement ssl acceptor and connector
* **mtls:** modify cert_chain to be repeated bytes
* **mtls:** Parse Manifest to get SNI info


### Features

* add 'reserved' annotations to protos
* add `cc_proto_library` for `manifest_proto`
* add `cc_proto_library` for `options_proto`
* Add `cc_proto_library` rule for `ez_service`
* Add cc targets to the ez_mtls proto
* Add cc_proto_library and cpp_grpc_libary targets
* add container run status metric
* Add DataScopeMetrics struct
* Add error test cases to IsolateEzService tests for internal paths
* Add ez_service_grpc_proto
* Add ez_to_ez test for missing payload case
* Add hash function for SNI name
* Add Isolate state metrics
* add IsolateMetricsPolicy manifest
* Add mTLS section to .versionrc.json
* Add observed proxy channels
* add perf commit type
* Add py_proto_library to SoT
* Add request/response_metadata fields
* add system resource metrics
* Add trace context benchmark
* correctly propogate error codes to client for unary ez-to-ez
* error unit test for remote bidi path in IsolateEzService
* error unit test for remote unary path in IsolateEzService
* expose Gauge metric points in test utils
* ez external metrics definitions
* EZ External proxy metrics instrumentation
* EZ External proxy metrics instrumentation
* fix internal streaming error propagation
* **fix:** Remove IsolateId check from RatifiedManager's validate_scope
* Improve tracing instrumentation
* Instrument trace spans in public_api and junction
* link script for g3 pre-submits in google_internal
* Metrics for the ez-to-ez inbound handler
* Minor optimization for health manager metrics
* Move trace function into trace_context lib
* **node:** Support debug level logs
* Observed proxy channels for ez2ez outbound
* propagate RPC errors from IsolateEzService for unary internal
* propogate original error message for ez-to-external bidi
* propogate original error message for ez-to-external unary
* proto update to propagate request_metadata to the isolate
* Remove go_package from ez_mtls.proto
* Remove go_package from Public API
* remove IsolateStatus from node
* remove IsolateStatus usage from isolate_ez_service
* remove SimpleStreamingWrapper from Junction
* Return correct error if first request in stream fails
* send correct error back to isolate for outbound stream
* Split off external_proxy_proto into own target
* Trace context propagation (receiving)
* Track container reset triggers via metrics
* use grpc_connector library in junction
* Version ez_manifest.proto


### Bug Fixes

* Disable flaky console_subscriber feature
* **node:** Package enforcer test tar file appropriately
* prefer expect_err over inspect_err in tests
* Propagate the current trace span to the isolate
* Reduce protobuf fields emitted in instrumented traces
* Simplify EZ MCP server installation
* update ez manifest proto descriptor path
* use next in line proto field for fileshare_handles
* Use path join() to construct paths


### Performance EZ Policy Enforcer

* **enforcer:** add GrpcChannelPool to grpc_connector library


### Documentation

* Add docs for tokio-console
* Correct instructions for compiling/running microbenchmarks
* Correct references to microbenchmark README.md
* **enforcer:** add profiling guide for enforcer
* **enforcer:** enable buf comment linting for diagnostics service
* **enforcer:** include debug artifacts in distribution
* Update Tokio Console guide to use devkit/build

## 0.7.0 (2026-02-18)


### Dependencies

* **deps:** Update DevKit to release-2.10.0


### Features

* Add metadata_headers to control_plane_metadata structure
* Add observed streams to IsolateEzService
* Deferred attribute setup for ObservedStream
* Interceptor works for reqs leaving Isolate
* Propagate deadline through remaining routes
* Propagate timeout/deadline for unary calls
* Propagate trace context via isolate RPCs


### Bug Fixes

* Increase sleep duration in container tests to reduce flake


### Documentation

* fix node readme formatting

## 0.6.0 (2026-02-09)


### Features

* Initial release

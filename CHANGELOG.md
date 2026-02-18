# Changelog

All notable changes to this project will be documented in this file. See [commit-and-tag-version](https://github.com/absolute-version/commit-and-tag-version) for commit guidelines.

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

workspace(name = "encrypted-zone")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Oak repository for OCI oci_runtime_bundle.
# Sadly Oak is not Bzlmod ready yet.
http_archive(
    name = "oak",
    sha256 = "c980e70cb535eac5185aee397fa577874ee37a38955161938cd89784ab5f33de",
    # commit 439c15a 2024-10-18
    strip_prefix = "oak-439c15a855fdb42067d4544535f836a19ee9f168",
    url = "https://github.com/project-oak/oak/archive/439c15a855fdb42067d4544535f836a19ee9f168.tar.gz",
)

load("@oak//bazel:repositories.bzl", "oak_toolchain_repositories")

oak_toolchain_repositories(oak_workspace_name = "oak")

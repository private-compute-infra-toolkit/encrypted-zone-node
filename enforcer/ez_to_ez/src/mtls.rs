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

use anyhow::{anyhow, Context, Result};
use arc_swap::ArcSwap;
use boring::pkey::{PKey, Private};
use boring::ssl::{SslAcceptor, SslConnector, SslMethod, SslVerifyMode};
use boring::x509::{store::X509StoreBuilder, X509};
use ez_mtls_proto::enforcer::v1::ez_mtls_service_client::EzMtlsServiceClient;
use ez_mtls_proto::enforcer::v1::{GetCertificateRequest, ReportSniRequest};
use grpc_connector::{GrpcChannelPool, DEFAULT_POOL_SIZE};
use manifest_proto::enforcer::v1::{ez_manifest::ManifestType, EzManifest};
use sha2::Digest;
use std::collections::HashSet;
use std::sync::Arc;
use tonic::transport::Channel;

const HASH_VERSION: &str = "a";

/// Generates a Server Name Indication (SNI) string based on the provided parameters.
///
/// The resulting SNI is designed to be compatible with DNS hostname parsing rules.
/// To achieve this, a secure identifier is generated using a SHA-256 checksum over the
/// `isolate_name`, `publisher_id`, and `operator_domain` (including their lengths to
/// prevent collision vulnerabilities). The first 16 bytes of this hash are hex-encoded,
/// producing a 32-character string that is safely embedded within the SNI along with
/// the `ez_instance_id` and `trust_domain` using DNS-valid delimiters (`-` and `.`).
pub fn sni(
    ez_instance_id: &str,
    isolate_name: &str,
    publisher_id: &str,
    operator_domain: &str,
    trust_domain: &str,
) -> String {
    let mut hasher = sha2::Sha256::new();
    sha2::Digest::update(&mut hasher, isolate_name.as_bytes());
    sha2::Digest::update(&mut hasher, format!("{}", isolate_name.len()).as_bytes());
    sha2::Digest::update(&mut hasher, "/".as_bytes());
    sha2::Digest::update(&mut hasher, publisher_id.as_bytes());
    sha2::Digest::update(&mut hasher, format!("{}", publisher_id.len()).as_bytes());
    sha2::Digest::update(&mut hasher, "/".as_bytes());
    sha2::Digest::update(&mut hasher, operator_domain.as_bytes());
    sha2::Digest::update(&mut hasher, format!("{}", operator_domain.len()).as_bytes());
    let hash = sha2::Digest::finalize(hasher);
    format!(
        "{}--{}-{}.{}",
        ez_instance_id,
        // {:x02x} converts the raw bytes into HEX format using 2 characters to
        // represent each byte therefore, the final length of the hash is 32.
        hash[..16].iter().map(|b| format!("{:02x}", b)).collect::<String>(),
        HASH_VERSION,
        trust_domain
    )
}

/// Extracts the isolate name and publisher ID pairs from an EzManifest.
fn extract_sni_params(manifest: &EzManifest) -> Vec<(&str, &str)> {
    if let Some(ManifestType::BundleManifest(bundle_manifest)) = &manifest.manifest_type {
        bundle_manifest.manifests.iter().flat_map(|m| extract_sni_params(m)).collect()
    } else {
        vec![(manifest.isolate_name.as_str(), manifest.publisher_id.as_str())]
    }
}

/// A generic stream that wraps UDS or TCP, and satisfies Tonic `Connected`.
///
/// This wrapper is required because `tokio_boring::SslStream` does not natively implement
/// `tonic::transport::server::Connected`, which Tonic's server expects to read peer
/// connection information (e.g., remote address). We delegate reads/writes to `SslStream`
/// and forward `ConnectInfo` from the underlying stream (UDS or TCP) to Tonic.
pub struct BoringTlsStream<S> {
    inner: tokio_boring::SslStream<S>,
}

impl<S> BoringTlsStream<S> {
    /// Creates a new `BoringTlsStream` wrapping the underlying `SslStream`.
    pub fn new(inner: tokio_boring::SslStream<S>) -> Self {
        Self { inner }
    }
}

impl<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> tokio::io::AsyncRead
    for BoringTlsStream<S>
{
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite
    for BoringTlsStream<S>
{
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// A manager for the EzMtlsService connection that holds the current certificate and SNIs.
#[derive(Clone)]
#[allow(dead_code)]
pub struct EzMtlsManager {
    // A channel pool is held for periodic interaction with the proxy.
    client_channel_pool: GrpcChannelPool,
    // Client for the EzMtlsService.
    client: EzMtlsServiceClient<Channel>,
    // Leaf private key for mTLS encryption.
    leaf_private_key: PKey<Private>,
    // Vector of X509 certificates.
    // The leaf certificate is at index 0, followed by intermediate certificates.
    cert_chain: Arc<ArcSwap<Vec<X509>>>,
    // Trust anchors for certificate verification.
    trust_anchors: Arc<ArcSwap<Vec<X509>>>,
    // Policy for SPIFFE URI authorization.
    policy: Arc<EzToEzPolicy>,
}

impl EzMtlsManager {
    /// Establishes a connection to the `EzMtlsService` through the specified address.
    ///
    /// The client will make initial requests to fetch the certificate and report SNIs.
    ///
    /// # Arguments
    ///
    /// * `mtls_key_path` - The file path to the leaf private key for mTLS.
    /// * `csr_path` - The file path to the certificate signing request (CSR) used for fetching the certificate.
    /// * `address` - The address of the `EzMtlsService` proxy to connect to.
    /// * `ez_manifest` - The EzManifest proto for the current EZ instance.
    pub async fn create(
        mtls_key_path: String,
        csr_path: String,
        address: String,
        ez_manifest: &EzManifest,
        policy: Arc<EzToEzPolicy>,
    ) -> Result<Self> {
        // TODO: Use a shared connect module in utils because it's repeated multiple times.
        let retry_delay_ms = std::env::var("PROXY_CONNECT_RETRY_DELAY_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(grpc_connector::DEFAULT_CONNECT_RETRY_DELAY_MS);
        let retry_count = std::env::var("PROXY_CONNECT_RETRY_COUNT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(grpc_connector::DEFAULT_CONNECT_RETRY_COUNT);
        let retry_scaling = std::env::var("PROXY_CONNECT_RETRY_SCALING")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(grpc_connector::DEFAULT_CONNECT_RETRY_SCALING);
        let client_channel_pool = GrpcChannelPool::new(
            address.to_string(),
            DEFAULT_POOL_SIZE,
            retry_count,
            retry_delay_ms,
            retry_scaling,
        )
        .await
        .map_err(|e| anyhow!("Failed to connect to EzToEz proxy: {}", e))?;
        let client = EzMtlsServiceClient::new(client_channel_pool.next_channel());
        // TODO: The private key will be generated in-memory instead of read from a file.
        let leaf_private_key_bytes = tokio::fs::read(mtls_key_path)
            .await
            .context("Failed to read leaf private key from path")?;
        let leaf_private_key = PKey::private_key_from_pem(&leaf_private_key_bytes)
            .context("Failed to parse leaf private key from pem")?;
        let csr = tokio::fs::read(csr_path).await.context("Failed to read leaf CSR from path")?;
        // TODO: Initialize trust anchors from a trust anchor file. Currently, we trust the fetched root certificate.
        let manager = Self {
            client_channel_pool,
            client,
            cert_chain: Arc::new(ArcSwap::from_pointee(vec![])),
            leaf_private_key,
            trust_anchors: Arc::new(ArcSwap::from_pointee(vec![])),
            policy,
        };

        manager
            .fetch_certificate(csr)
            .await
            .context("Failed to fetch preliminary mTLS certificates")?;

        // TODO: Parse the operator domain and trust domain from the leaf certificate.
        let sni_params = extract_sni_params(ez_manifest);
        let mut snis = Vec::new();
        for (isolate_name, publisher_id) in sni_params {
            snis.push(sni(
                // Empty means it'll match any request that doesn't specify an ez_instance_id.
                /* ez_instance_id= */
                "",
                isolate_name,
                publisher_id,
                "placeholder-operator",
                "placeholder-domain",
            ));
        }

        manager.report_snis(snis).await.context("Failed to report preliminary SNIs")?;
        Ok(manager)
    }

    /// Fetches a certificate from the mTLS service using the given CSR, and stores it internally.
    pub async fn fetch_certificate(&self, csr: Vec<u8>) -> Result<()> {
        let req = GetCertificateRequest { csr, evidence: None, endorsements: None };
        let mut client = self.client.clone();
        let resp = client.get_certificate(tonic::Request::new(req)).await.map_err(|e| {
            anyhow::anyhow!("Failed to fetch certificate from EzMtlsService: {}", e)
        })?;

        let mut parsed_certs = Vec::new();
        for cert in resp.into_inner().certs {
            parsed_certs.push(X509::from_der(&cert)?);
        }

        // TODO: Remove this. Initialize trust anchors from a trust anchor file.
        if let Some(last_cert) = parsed_certs.last() {
            let mut anchors = self.trust_anchors.load().as_ref().clone();
            anchors.push(last_cert.clone());
            self.trust_anchors.store(Arc::new(anchors));
        }

        self.cert_chain.store(Arc::new(parsed_certs));
        Ok(())
    }

    /// Reports the provided SNI strings to the mTLS service to update the routing table.
    pub async fn report_snis(&self, snis: Vec<String>) -> Result<()> {
        let req = ReportSniRequest { sni: snis };
        let mut client = self.client.clone();
        client
            .report_sni(tonic::Request::new(req))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to report SNIs to EzMtlsService: {}", e))?;
        Ok(())
    }

    /// Helper function to build the X509 store from trust anchors.
    fn build_x509_store(&self) -> Result<boring::x509::store::X509Store> {
        let mut store = X509StoreBuilder::new()?;
        let anchors = self.trust_anchors.load();
        for ca_cert in &**anchors {
            store.add_cert(ca_cert.clone())?;
        }
        Ok(store.build())
    }

    /// Creates a TLS acceptor from the stored certificate chain for server side connections.
    pub async fn create_tls_acceptor(&self) -> anyhow::Result<SslAcceptor> {
        let mut acceptor = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls())?;
        acceptor.set_private_key(&self.leaf_private_key)?;

        {
            let cert_chain_guard = self.cert_chain.load();
            if cert_chain_guard.is_empty() {
                anyhow::bail!("Certificate chain not fetched yet");
            }
            let certs = &**cert_chain_guard;

            // Leaf certificate is at index 0.
            let leaf_cert =
                certs.first().ok_or_else(|| anyhow!("No leaf certificate found in chain"))?;
            acceptor.set_certificate(leaf_cert)?;

            // Set intermediate certificates in the chain.
            for cert in certs.iter().skip(1) {
                acceptor.add_extra_chain_cert(cert.clone())?;
            }
        } // cert_chain_guard is dropped here

        let store = self.build_x509_store()?;
        acceptor.set_verify_cert_store(store)?;

        let verify_mode = SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT;
        acceptor.set_verify_callback(
            verify_mode,
            build_verify_spiffe_uri_callback(self.policy.clone()),
        );
        Ok(acceptor.build())
    }

    /// Creates a TLS connector for outbound client connections.
    pub async fn create_tls_connector(&self) -> anyhow::Result<SslConnector> {
        let mut connector = SslConnector::builder(SslMethod::tls())?;
        connector.set_private_key(&self.leaf_private_key)?;

        {
            let cert_chain_guard = self.cert_chain.load();
            if cert_chain_guard.is_empty() {
                anyhow::bail!("Certificate chain not fetched yet");
            }
            let certs = &**cert_chain_guard;

            let leaf_cert =
                certs.first().ok_or_else(|| anyhow!("No leaf certificate found in chain"))?;
            connector.set_certificate(leaf_cert)?;

            for cert in certs.iter().skip(1) {
                connector.add_extra_chain_cert(cert.clone())?;
            }
        } // cert_chain_guard is dropped here

        let store = self.build_x509_store()?;
        connector.set_verify_cert_store(store)?;

        // Same verification as acceptor but without fail_if_no_peer_cert (the server provides it).
        connector.set_verify_callback(
            SslVerifyMode::PEER,
            build_verify_spiffe_uri_callback(self.policy.clone()),
        );
        Ok(connector.build())
    }
}

/// Represents the parsed components of a SPIFFE URI.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SpiffeUri {
    /// The trust domain of the SPIFFE URI.
    pub trust_domain: String,
    /// The operator domain.
    pub operator_domain: String,
    /// The publisher ID.
    pub publisher_id: String,
    /// The workload name.
    pub workload_name: String,
}

impl SpiffeUri {
    /// Parses a strictly formatted SPIFFE URI into its logical components.
    /// Expected format: spiffe://<trust_domain>/operator/<operator_domain>/publisher/<publisher_id>/workload/<workload_name>
    pub fn new(uri: &str) -> anyhow::Result<Self> {
        let without_scheme = uri
            .strip_prefix("spiffe://")
            .ok_or_else(|| anyhow::anyhow!("Missing 'spiffe://' scheme"))?;
        // Find the indices of each delimiter
        let op_idx = without_scheme
            .find("/operator/")
            .ok_or_else(|| anyhow::anyhow!("Missing '/operator/' tag"))?;
        let pub_idx = without_scheme
            .find("/publisher/")
            .ok_or_else(|| anyhow::anyhow!("Missing '/publisher/' tag"))?;
        let work_idx = without_scheme
            .find("/workload/")
            .ok_or_else(|| anyhow::anyhow!("Missing '/workload/' tag"))?;

        // Validate that the tags appear in the correct sequential order
        if !(op_idx < pub_idx && pub_idx < work_idx) {
            anyhow::bail!("Tags are out of correct sequential order");
        }

        let trust_domain = &without_scheme[..op_idx];
        let operator_domain = &without_scheme[op_idx + "/operator/".len()..pub_idx];
        let publisher_id = &without_scheme[pub_idx + "/publisher/".len()..work_idx];
        let workload_name = &without_scheme[work_idx + "/workload/".len()..];

        Ok(Self {
            trust_domain: trust_domain.to_string(),
            operator_domain: operator_domain.to_string(),
            publisher_id: publisher_id.to_string(),
            workload_name: workload_name.to_string(),
        })
    }
}

/// Helper factory that builds a callback verifying if a peer's certificate contains exactly 1 SPIFFE URI SAN
/// and if it matches the allowlisted policy.
fn build_verify_spiffe_uri_callback(
    policy: Arc<EzToEzPolicy>,
) -> impl Fn(bool, &mut boring::x509::X509StoreContextRef) -> bool + Sync + Send + 'static {
    move |ok: bool, ctx: &mut boring::x509::X509StoreContextRef| -> bool {
        if !ok {
            return false; // Trust chain or signature failed
        }

        // Intermediate certs don't need SAN checks. Only evaluate depth 0 (leaf).
        if ctx.error_depth() != 0 {
            return true;
        }

        if let Some(cert) = ctx.current_cert() {
            if let Some(sans) = cert.subject_alt_names() {
                let mut uri_count = 0;
                for san in sans {
                    if let Some(uri) = san.uri() {
                        uri_count += 1;
                        if uri_count > 1 {
                            log::warn!(
                                "Multiple URI SANs found. SPIFFE SVID dictates exactly one."
                            );
                            ctx.set_error(Err(
                                boring::x509::X509VerifyError::APPLICATION_VERIFICATION,
                            ));
                            return false;
                        }

                        // Extract and check fields
                        match SpiffeUri::new(uri) {
                            Ok(parsed) => {
                                if policy.verify(&parsed) {
                                    log::debug!("SPIFFE URI authorized correctly: {}", uri);
                                    return true;
                                } else {
                                    log::warn!("SPIFFE URI rejected by policy: {}", uri);
                                    ctx.set_error(Err(
                                        boring::x509::X509VerifyError::APPLICATION_VERIFICATION,
                                    ));
                                    return false;
                                }
                            }
                            Err(e) => {
                                log::warn!("Malformed SPIFFE URI structure: {} - {}", uri, e);
                                ctx.set_error(Err(
                                    boring::x509::X509VerifyError::APPLICATION_VERIFICATION,
                                ));
                                return false;
                            }
                        }
                    }
                }
                if uri_count == 0 {
                    log::warn!("No URI SAN found in certificate. SPIFFE requires a URI.");
                    ctx.set_error(Err(boring::x509::X509VerifyError::APPLICATION_VERIFICATION));
                    return false;
                }
            }
        }

        log::warn!("Failed to extract SAN extensions from certificate.");
        ctx.set_error(Err(boring::x509::X509VerifyError::APPLICATION_VERIFICATION));
        false
    }
}

/// Policy defining allowlisted components for SPIFFE URI authorization.
/// We currently allow any combination of trust domains, operator domains, publisher IDs, and workload names configured in the policy.
/// However, this may change in the future.
pub struct EzToEzPolicy {
    pub allowed_trust_domains: HashSet<String>,
    pub allowed_operator_domains: HashSet<String>,
    pub allowed_publisher_ids: HashSet<String>,
    pub allowed_workload_names: HashSet<String>,
}

impl EzToEzPolicy {
    /// Verifies if the parsed components are allowlisted by this policy.
    pub fn verify(&self, uri: &SpiffeUri) -> bool {
        self.allowed_trust_domains.contains(&uri.trust_domain)
            && self.allowed_operator_domains.contains(&uri.operator_domain)
            && self.allowed_publisher_ids.contains(&uri.publisher_id)
            && self.allowed_workload_names.contains(&uri.workload_name)
    }
}

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
use grpc_connector::GrpcChannelPool;
use manifest_proto::enforcer::v1::{ez_manifest::ManifestType, EzManifest};
use sha2::Digest;
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

// Connected is required by serve_with_incoming.
impl<S: tonic::transport::server::Connected> tonic::transport::server::Connected
    for BoringTlsStream<S>
{
    type ConnectInfo = S::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.inner.get_ref().connect_info()
    }
}

#[derive(Clone, Debug)]
pub struct EzMtlsManagerConfig {
    // TODO: All keys and CSRs will be generated in enforcer memory.
    // Path to read the mTLS private key in der format.
    pub mtls_key_path: String,
    // Path to read the CSR in der format.
    pub csr_path: String,
    // Address to talk to EZ proxy.
    pub proxy_address: String,
    // EzManifest to parse for SNI fields.
    pub ez_manifest: EzManifest,
}

/// A manager for the EzMtlsService connection that holds the current certificate and SNIs.
#[derive(Clone)]
pub struct EzMtlsManager {
    #[allow(dead_code)]
    config: EzMtlsManagerConfig,

    // Certificates will be fetched periodically every 24 hours.
    // Therefore, we should keep the connection.
    // A channel pool is held for periodic interaction with the proxy.
    #[allow(dead_code)]
    client_channel_pool: GrpcChannelPool,
    // Client for the EzMtlsService.
    client: EzMtlsServiceClient<Channel>,

    // Leaf private key for mTLS encryption.
    leaf_private_key: PKey<Private>,
    // Certificate signing request in der format.
    csr: Vec<u8>,
    // Vector of X509 certificates.
    // The leaf certificate is at index 0, followed by intermediate certificates.
    cert_chain: Arc<ArcSwap<Vec<X509>>>,
    // Trust anchors for certificate verification.
    trust_anchors: Arc<ArcSwap<Vec<X509>>>,
    // The SPIFFE URI of this manager parsed from the leaf certificate.
    spiffe_identity: SpiffeUri,
}

impl EzMtlsManager {
    /// Builds and initializes a new `EzMtlsManager`.
    pub async fn build(config: EzMtlsManagerConfig) -> Result<Self> {
        let (client_channel_pool, mut client) = Self::create_client(&config).await?;
        let (leaf_private_key, csr) = Self::load_keys(&config).await?;
        let (cert_chain, trust_anchors) = Self::fetch_certs(&mut client, &csr).await?;
        let spiffe_identity = Self::parse_spiffe_id(&cert_chain)?;
        Self::report_initial_snis_internal(&mut client, &spiffe_identity, &config.ez_manifest)
            .await?;
        Ok(Self {
            config,
            client_channel_pool,
            client,
            leaf_private_key,
            csr,
            cert_chain: Arc::new(ArcSwap::from_pointee(cert_chain)),
            trust_anchors: Arc::new(ArcSwap::from_pointee(trust_anchors)),
            spiffe_identity,
        })
    }

    /// Returns the SPIFFE identity of this manager.
    pub fn spiffe_identity(&self) -> SpiffeUri {
        self.spiffe_identity.clone()
    }

    /// Creates the gRPC client and channel pool.
    async fn create_client(
        config: &EzMtlsManagerConfig,
    ) -> Result<(GrpcChannelPool, EzMtlsServiceClient<Channel>)> {
        let client_channel_pool = GrpcChannelPool::new_from_env(&config.proxy_address)
            .await
            .map_err(|e| anyhow!("Failed to connect to EzToEz proxy: {}", e))?;
        let client = EzMtlsServiceClient::new(client_channel_pool.next_channel());
        Ok((client_channel_pool, client))
    }

    /// Reads the leaf private key and CSR from disk.
    async fn load_keys(config: &EzMtlsManagerConfig) -> Result<(PKey<Private>, Vec<u8>)> {
        let leaf_private_key_bytes = tokio::fs::read(&config.mtls_key_path)
            .await
            .context("Failed to read leaf private key from path")?;
        let leaf_private_key = PKey::private_key_from_pem(&leaf_private_key_bytes)
            .context("Failed to parse leaf private key from pem")?;

        let csr =
            tokio::fs::read(&config.csr_path).await.context("Failed to read leaf CSR from path")?;

        Ok((leaf_private_key, csr))
    }

    /// Fetches initial certificates.
    async fn fetch_certs(
        client: &mut EzMtlsServiceClient<Channel>,
        csr: &[u8],
    ) -> Result<(Vec<X509>, Vec<X509>)> {
        let req = GetCertificateRequest { csr: csr.to_vec(), evidence: None, endorsements: None };
        let resp = client.get_certificate(tonic::Request::new(req)).await.map_err(|e| {
            anyhow::anyhow!("Failed to fetch certificate from EzMtlsService: {}", e)
        })?;

        let mut parsed_certs = Vec::new();
        for cert in resp.into_inner().certs {
            parsed_certs.push(X509::from_der(&cert)?);
        }

        let mut trust_anchors = Vec::new();
        if let Some(last_cert) = parsed_certs.last() {
            trust_anchors.push(last_cert.clone());
        }

        Ok((parsed_certs, trust_anchors))
    }

    /// Parses the SPIFFE ID from the leaf certificate.
    fn parse_spiffe_id(cert_chain: &[X509]) -> Result<SpiffeUri> {
        let leaf_cert = cert_chain.first().context("No leaf certificate found in chain")?;
        let spiffe_identity = Self::extract_spiffe_uri_from_cert(leaf_cert)
            .context("Failed to extract SPIFFE ID from leaf certificate")?;
        Ok(spiffe_identity)
    }

    /// Reports initial SNIs to the proxy.
    async fn report_initial_snis_internal(
        client: &mut EzMtlsServiceClient<Channel>,
        spiffe_identity: &SpiffeUri,
        ez_manifest: &EzManifest,
    ) -> Result<()> {
        let sni_params = extract_sni_params(ez_manifest);
        let mut snis = Vec::new();
        for (isolate_name, publisher_id) in sni_params {
            snis.push(sni(
                "",
                isolate_name,
                publisher_id,
                &spiffe_identity.operator_domain,
                &spiffe_identity.trust_domain,
            ));
        }

        let req = ReportSniRequest { sni: snis };
        client
            .report_sni(tonic::Request::new(req))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to report SNIs to EzMtlsService: {}", e))?;
        Ok(())
    }

    /// Helper function to extract exactly one SPIFFE URI from a certificate.
    fn extract_spiffe_uri_from_cert(cert: &boring::x509::X509Ref) -> anyhow::Result<SpiffeUri> {
        let sans = cert
            .subject_alt_names()
            .ok_or_else(|| anyhow::anyhow!("Failed to extract SAN extensions from certificate"))?;

        let mut uris = sans.into_iter().filter_map(|san| san.uri().map(|s| s.to_string()));
        let first_uri = uris.next().ok_or_else(|| anyhow::anyhow!("No URI SAN found"))?;
        if uris.next().is_some() {
            anyhow::bail!("Multiple URI SANs found in certificate, SPIFFE requires exactly one.")
        }
        SpiffeUri::new(&first_uri).context("failed to parse spiffe URI from cert")
    }

    /// Helper function to verify that a certificate contains exactly one SPIFFE URI
    /// and that it matches the expected identity.
    fn verify_certificate_spiffe_identity(
        cert: &boring::x509::X509Ref,
        expected_identity: &SpiffeUri,
    ) -> Result<(), boring::x509::X509VerifyError> {
        match Self::extract_spiffe_uri_from_cert(cert) {
            Ok(parsed) => {
                if &parsed == expected_identity {
                    Ok(())
                } else {
                    log::warn!("SPIFFE URI does not match expected identity");
                    Err(boring::x509::X509VerifyError::APPLICATION_VERIFICATION)
                }
            }
            Err(e) => {
                log::warn!("Failed to extract SPIFFE ID: {}", e);
                Err(boring::x509::X509VerifyError::APPLICATION_VERIFICATION)
            }
        }
    }

    /// Helper factory that builds a callback verifying if a peer's certificate contains exactly 1 SPIFFE URI SAN
    /// and if it matches the expected spiffe id.
    ///
    /// In the future we may extend this to allow various inputs instead.
    fn build_verify_spiffe_uri_callback(
        &self,
        expected_identity: SpiffeUri,
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
                if let Err(e) = Self::verify_certificate_spiffe_identity(cert, &expected_identity) {
                    ctx.set_error(Err(e));
                    return false;
                }
                return true;
            }

            log::warn!("Failed to get current certificate from context.");
            ctx.set_error(Err(boring::x509::X509VerifyError::APPLICATION_VERIFICATION));
            false
        }
    }

    /// Fetches a certificate from the mTLS service using the CSR path in config, and stores it internally.
    pub async fn fetch_certificate(&self) -> Result<()> {
        let mut client = self.client.clone();
        let (parsed_certs, new_anchors) = Self::fetch_certs(&mut client, &self.csr).await?;

        self.cert_chain.store(Arc::new(parsed_certs));

        if let Some(last_cert) = new_anchors.first() {
            let mut anchors = self.trust_anchors.load().as_ref().clone();
            anchors.push(last_cert.clone());
            self.trust_anchors.store(Arc::new(anchors));
        }
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

        // TODO: In the future, we may have multiple acceptable peer roles for incoming request.
        let verify_mode = SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT;
        acceptor.set_verify_callback(
            verify_mode,
            self.build_verify_spiffe_uri_callback(self.spiffe_identity.clone()),
        );
        Ok(acceptor.build())
    }

    /// Creates a TLS connector for outbound client connections with a specific expected identity.
    async fn create_tls_connector_internal(
        &self,
        expected_identity: SpiffeUri,
    ) -> anyhow::Result<SslConnector> {
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
            self.build_verify_spiffe_uri_callback(expected_identity),
        );
        connector.set_verify_depth(2);
        Ok(connector.build())
    }

    /// Returns a factory that can create TLS connectors for specific targets.
    pub fn get_connector_factory(&self) -> TlsConnectorFactory {
        TlsConnectorFactory { manager: self.clone() }
    }
}

/// A factory that can create TLS connectors for specific targets.
#[derive(Clone)]
pub struct TlsConnectorFactory {
    manager: EzMtlsManager,
}

impl TlsConnectorFactory {
    /// Creates a TLS connector for a specific target operator domain.
    ///
    /// TODO: We may add different fields to construct the connector in the future.
    pub async fn create_connector(&self, operator_domain: &str) -> anyhow::Result<SslConnector> {
        let expected_identity = SpiffeUri {
            trust_domain: self.manager.spiffe_identity.trust_domain.clone(),
            operator_domain: operator_domain.to_string(),
            publisher_id: self.manager.spiffe_identity.publisher_id.clone(),
            workload_name: self.manager.spiffe_identity.workload_name.clone(),
        };
        self.manager.create_tls_connector_internal(expected_identity).await
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
        const OPERATOR_TAG: &str = "/operator/";
        const PUBLISHER_TAG: &str = "/publisher/";
        const WORKLOAD_TAG: &str = "/workload/";

        let without_scheme = uri
            .strip_prefix("spiffe://")
            .ok_or_else(|| anyhow::anyhow!("Missing 'spiffe://' scheme"))?;
        // Find the indices of each delimiter
        let op_idx = without_scheme
            .find(OPERATOR_TAG)
            .ok_or_else(|| anyhow::anyhow!("Missing '/operator/' tag"))?;
        let pub_idx = without_scheme
            .find(PUBLISHER_TAG)
            .ok_or_else(|| anyhow::anyhow!("Missing '/publisher/' tag"))?;
        let work_idx = without_scheme
            .find(WORKLOAD_TAG)
            .ok_or_else(|| anyhow::anyhow!("Missing '/workload/' tag"))?;

        // Validate that the tags appear in the correct sequential order
        if !(op_idx < pub_idx && pub_idx < work_idx) {
            anyhow::bail!("Tags are out of correct sequential order");
        }

        let trust_domain = &without_scheme[..op_idx];
        let operator_domain = &without_scheme[op_idx + OPERATOR_TAG.len()..pub_idx];
        let publisher_id = &without_scheme[pub_idx + PUBLISHER_TAG.len()..work_idx];
        let workload_name = &without_scheme[work_idx + WORKLOAD_TAG.len()..];

        Ok(Self {
            trust_domain: trust_domain.to_string(),
            operator_domain: operator_domain.to_string(),
            publisher_id: publisher_id.to_string(),
            workload_name: workload_name.to_string(),
        })
    }
}

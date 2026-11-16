// Stream wrapper.

use rustls_pemfile::{certs, read_one, Item};
use std::iter;
use std::path::Path;
use std::sync::Arc;
use tokio_rustls::rustls::{
    self,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{self, CryptoProvider},
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime},
    DigitallySignedStruct, SignatureScheme,
};
use tokio_rustls::TlsAcceptor;

use crate::config::get_config;
use crate::errors::Error;

/// The crypto provider for every TLS config pgcat builds: rustls' ring
/// provider, with its default cipher suites and key exchange groups.
///
/// Configs name it explicitly instead of relying on a process-wide default,
/// which rustls can't pick when more than one provider is compiled in.
pub fn crypto_provider() -> Arc<CryptoProvider> {
    Arc::new(crypto::ring::default_provider())
}

// TLS
pub fn load_certs(path: &Path) -> std::io::Result<Vec<CertificateDer<'static>>> {
    certs(&mut std::io::BufReader::new(std::fs::File::open(path)?))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid cert"))
}

pub fn load_keys(path: &Path) -> std::io::Result<Vec<PrivateKeyDer<'static>>> {
    let mut rd = std::io::BufReader::new(std::fs::File::open(path)?);

    iter::from_fn(|| read_one(&mut rd).transpose())
        .filter_map(|item| match item {
            Err(err) => Some(Err(err)),
            Ok(Item::Pkcs1Key(key)) => Some(Ok(key.into())),
            Ok(Item::Sec1Key(key)) => Some(Ok(key.into())),
            Ok(Item::Pkcs8Key(key)) => Some(Ok(key.into())),
            _ => None,
        })
        .collect()
}

pub struct Tls {
    pub acceptor: TlsAcceptor,
}

impl Tls {
    pub fn new() -> Result<Self, Error> {
        let config = get_config();

        let certs = match load_certs(Path::new(&config.general.tls_certificate.unwrap())) {
            Ok(certs) => certs,
            Err(_) => return Err(Error::TlsError),
        };

        let mut keys = match load_keys(Path::new(&config.general.tls_private_key.unwrap())) {
            Ok(keys) => keys,
            Err(_) => return Err(Error::TlsError),
        };

        let config = match rustls::ServerConfig::builder_with_provider(crypto_provider())
            .with_safe_default_protocol_versions()
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))
            .and_then(|builder| {
                builder
                    .with_no_client_auth()
                    .with_single_cert(certs, keys.remove(0))
                    .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))
            }) {
            Ok(c) => c,
            Err(_) => return Err(Error::TlsError),
        };

        Ok(Tls {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        })
    }
}

/// Accepts any server certificate. Handshake signatures are still checked
/// against the certificate's key, as rustls does by default.
#[derive(Debug)]
pub struct NoCertificateVerification {
    provider: Arc<CryptoProvider>,
}

impl NoCertificateVerification {
    pub fn new() -> Self {
        NoCertificateVerification {
            provider: crypto_provider(),
        }
    }
}

impl Default for NoCertificateVerification {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    // ==================== load_certs Tests ====================

    #[test]
    fn test_load_certs_valid_file() {
        let cert_path = PathBuf::from(".circleci/server.cert");
        if cert_path.exists() {
            let result = load_certs(&cert_path);
            assert!(result.is_ok());
            let certs = result.unwrap();
            assert!(!certs.is_empty());
        }
    }

    #[test]
    fn test_load_certs_nonexistent_file() {
        let cert_path = PathBuf::from("nonexistent/path/cert.pem");
        let result = load_certs(&cert_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_load_certs_invalid_file() {
        // Try loading a non-certificate file
        let invalid_path = PathBuf::from("Cargo.toml");
        if invalid_path.exists() {
            let result = load_certs(&invalid_path);
            // Should either error or return empty/invalid certs
            match result {
                Ok(certs) => assert!(certs.is_empty()),
                Err(_) => (), // Error is also acceptable
            }
        }
    }

    // ==================== load_keys Tests ====================

    #[test]
    fn test_load_keys_valid_file() {
        let key_path = PathBuf::from(".circleci/server.key");
        if key_path.exists() {
            let result = load_keys(&key_path);
            assert!(result.is_ok());
            let keys = result.unwrap();
            assert!(!keys.is_empty());
        }
    }

    #[test]
    fn test_load_keys_nonexistent_file() {
        let key_path = PathBuf::from("nonexistent/path/key.pem");
        let result = load_keys(&key_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_load_keys_invalid_file() {
        // Try loading a non-key file
        let invalid_path = PathBuf::from("Cargo.toml");
        if invalid_path.exists() {
            let result = load_keys(&invalid_path);
            // Should succeed but with empty keys
            assert!(result.is_ok());
            let keys = result.unwrap();
            assert!(keys.is_empty());
        }
    }

    // ==================== NoCertificateVerification Tests ====================

    #[test]
    fn test_no_certificate_verification_always_succeeds() {
        let verifier = NoCertificateVerification::new();

        // Create a dummy certificate
        let cert_data = vec![0u8; 100];
        let cert = CertificateDer::from(cert_data);

        // Create a dummy server name
        let server_name = ServerName::try_from("example.com").unwrap();

        let result = verifier.verify_server_cert(&cert, &[], &server_name, &[], UnixTime::now());

        assert!(result.is_ok());
    }

    #[test]
    fn test_no_certificate_verification_with_intermediates() {
        let verifier = NoCertificateVerification::new();

        let cert_data = vec![0u8; 100];
        let cert = CertificateDer::from(cert_data);
        let intermediate1 = CertificateDer::from(vec![1u8; 50]);
        let intermediate2 = CertificateDer::from(vec![2u8; 50]);

        let server_name = ServerName::try_from("test.local").unwrap();

        let result = verifier.verify_server_cert(
            &cert,
            &[intermediate1, intermediate2],
            &server_name,
            &[],
            UnixTime::now(),
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_no_certificate_verification_with_ocsp_response() {
        let verifier = NoCertificateVerification::new();

        let cert = CertificateDer::from(vec![0u8; 100]);
        let server_name = ServerName::try_from("example.org").unwrap();
        let ocsp_response = vec![1u8, 2u8, 3u8, 4u8];

        let result =
            verifier.verify_server_cert(&cert, &[], &server_name, &ocsp_response, UnixTime::now());

        assert!(result.is_ok());
    }

    #[test]
    fn test_no_certificate_verification_offers_provider_schemes() {
        let verifier = NoCertificateVerification::new();
        let schemes = verifier.supported_verify_schemes();

        assert!(!schemes.is_empty());
        assert_eq!(
            schemes,
            crypto_provider()
                .signature_verification_algorithms
                .supported_schemes()
        );
    }

    // ==================== Certificate/PrivateKey Type Tests ====================

    #[test]
    fn test_certificate_creation() {
        let data = vec![1u8, 2u8, 3u8, 4u8, 5u8];
        let cert = CertificateDer::from(data.clone());
        assert_eq!(cert.as_ref(), &data[..]);
    }

    #[test]
    fn test_private_key_creation() {
        let data = vec![10u8, 20u8, 30u8, 40u8, 50u8];
        let key = PrivateKeyDer::Pkcs8(data.clone().into());
        assert_eq!(key.secret_der(), &data[..]);
    }

    // ==================== Integration-style Tests ====================

    #[test]
    fn test_load_certs_and_keys_together() {
        let cert_path = PathBuf::from(".circleci/server.cert");
        let key_path = PathBuf::from(".circleci/server.key");

        if cert_path.exists() && key_path.exists() {
            let certs = load_certs(&cert_path).unwrap();
            let keys = load_keys(&key_path).unwrap();

            assert!(!certs.is_empty(), "Should have loaded certificates");
            assert!(!keys.is_empty(), "Should have loaded keys");
        }
    }

    #[test]
    fn test_server_name_creation() {
        // Test various server names
        let name1 = ServerName::try_from("example.com");
        assert!(name1.is_ok());

        let name2 = ServerName::try_from("localhost");
        assert!(name2.is_ok());

        let name3 = ServerName::try_from("192.168.1.1");
        // IP addresses should work with IpAddress variant
        assert!(name3.is_ok());
    }
}

// Stream wrapper.

use rustls_pemfile::{certs, read_one, Item};
use std::iter;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;
use tokio_rustls::rustls::{
    self,
    client::{ServerCertVerified, ServerCertVerifier},
    Certificate, PrivateKey, ServerName,
};
use tokio_rustls::TlsAcceptor;

use crate::config::get_config;
use crate::errors::Error;

// TLS
pub fn load_certs(path: &Path) -> std::io::Result<Vec<Certificate>> {
    certs(&mut std::io::BufReader::new(std::fs::File::open(path)?))
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid cert"))
        .map(|mut certs| certs.drain(..).map(Certificate).collect())
}

pub fn load_keys(path: &Path) -> std::io::Result<Vec<PrivateKey>> {
    let mut rd = std::io::BufReader::new(std::fs::File::open(path)?);

    iter::from_fn(|| read_one(&mut rd).transpose())
        .filter_map(|item| match item {
            Err(err) => Some(Err(err)),
            Ok(Item::RSAKey(key)) => Some(Ok(PrivateKey(key))),
            Ok(Item::ECKey(key)) => Some(Ok(PrivateKey(key))),
            Ok(Item::PKCS8Key(key)) => Some(Ok(PrivateKey(key))),
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

        let config = match rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(certs, keys.remove(0))
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))
        {
            Ok(c) => c,
            Err(_) => return Err(Error::TlsError),
        };

        Ok(Tls {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        })
    }
}

pub struct NoCertificateVerification;

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &Certificate,
        _intermediates: &[Certificate],
        _server_name: &ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: SystemTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
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
        let verifier = NoCertificateVerification;

        // Create a dummy certificate
        let cert_data = vec![0u8; 100];
        let cert = Certificate(cert_data);

        // Create a dummy server name
        let server_name = ServerName::try_from("example.com").unwrap();

        // Empty iterator for SCTs
        let mut scts_iter = std::iter::empty::<&[u8]>();

        let result = verifier.verify_server_cert(
            &cert,
            &[],
            &server_name,
            &mut scts_iter,
            &[],
            SystemTime::now(),
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_no_certificate_verification_with_intermediates() {
        let verifier = NoCertificateVerification;

        let cert_data = vec![0u8; 100];
        let cert = Certificate(cert_data);
        let intermediate1 = Certificate(vec![1u8; 50]);
        let intermediate2 = Certificate(vec![2u8; 50]);

        let server_name = ServerName::try_from("test.local").unwrap();
        let mut scts_iter = std::iter::empty::<&[u8]>();

        let result = verifier.verify_server_cert(
            &cert,
            &[intermediate1, intermediate2],
            &server_name,
            &mut scts_iter,
            &[],
            SystemTime::now(),
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_no_certificate_verification_with_ocsp_response() {
        let verifier = NoCertificateVerification;

        let cert = Certificate(vec![0u8; 100]);
        let server_name = ServerName::try_from("example.org").unwrap();
        let mut scts_iter = std::iter::empty::<&[u8]>();
        let ocsp_response = vec![1u8, 2u8, 3u8, 4u8];

        let result = verifier.verify_server_cert(
            &cert,
            &[],
            &server_name,
            &mut scts_iter,
            &ocsp_response,
            SystemTime::now(),
        );

        assert!(result.is_ok());
    }

    // ==================== Certificate/PrivateKey Type Tests ====================

    #[test]
    fn test_certificate_creation() {
        let data = vec![1u8, 2u8, 3u8, 4u8, 5u8];
        let cert = Certificate(data.clone());
        assert_eq!(cert.0, data);
    }

    #[test]
    fn test_private_key_creation() {
        let data = vec![10u8, 20u8, 30u8, 40u8, 50u8];
        let key = PrivateKey(data.clone());
        assert_eq!(key.0, data);
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

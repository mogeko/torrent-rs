//! TLS configuration and utilities.
//!
//! Centralized TLS setup using rustls with platform native root
//! certificates. Shared by all modules that need HTTPS/TLS: HTTP
//! tracker (BEP 3), web seed download (BEP 19), etc.

use std::sync::Arc;

use rustls::{ClientConfig, RootCertStore};
use rustls_native_certs::load_native_certs;
use tokio_rustls::TlsConnector;

use crate::error::Error;

/// Build a TLS connector with platform native root certificates.
///
/// Uses `rustls-native-certs` to load the OS trust store. The
/// returned [`TlsConnector`] can be cloned cheaply (Arc-based).
///
/// This is the single place where TLS configuration lives. Any
/// future TLS changes (client certificates, cipher suite tuning,
/// certificate pinning) should be added here.
pub(crate) fn build_tls_connector() -> Result<TlsConnector, Error> {
    let mut root_store = RootCertStore::empty();

    for cert in load_native_certs().certs {
        root_store.add(cert).map_err(Error::invalid_input)?;
    }

    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(TlsConnector::from(Arc::new(config)))
}

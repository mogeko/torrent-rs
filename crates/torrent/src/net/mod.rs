//! Network primitives shared across protocol modules.
//!
//! This module provides transport-layer (TLS) and application-layer
//! (HTTP) utilities used by the HTTP tracker (BEP 3), web seed
//! download (BEP 19), and future network components.
//!
//! # Module Layout
//!
//! - [`tls`] — TLS connector construction (platform root certs)
//! - [`http`] — purpose-built HTTP/1.1 client

pub(crate) mod http;
pub(crate) mod tls;

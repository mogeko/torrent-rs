//! Internal HTTP/1.1 client for BitTorrent protocols.
//!
//! Shared by HTTP tracker (BEP 3) and web seed download (BEP 19).
//! Purpose-built for BT use cases — not a general-purpose HTTP client.
//!
//! Features:
//! - HTTP GET with optional `Range` header
//! - Automatic HTTPS via TLS (`tokio-rustls`)
//! - TCP_NODELAY for low-latency connections
//! - Response size capping (anti-DoS)
//! - Redirect resolution helper

use std::time::Duration;

use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpSocket, lookup_host};
use tokio_rustls::TlsConnector;
use url::Url;

use crate::error::{Error, ErrorKind};

use super::tls::build_tls_connector;

/// Maximum number of redirects to follow before giving up.
pub(crate) const MAX_REDIRECTS: u32 = 5;

/// Maximum response size to guard against malicious or buggy servers (256 KB).
pub(crate) const MAX_RESPONSE_SIZE: u64 = 256 * 1024;

// ── Internal stream trait ──────────────────────────────────────────

/// Internal trait to unify plain TCP and TLS streams into `Box<dyn …>`.
trait HttpStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> HttpStream for T {}

// ── HttpClient ─────────────────────────────────────────────────────

/// A purpose-built HTTP/1.1 client for BitTorrent use cases.
///
/// Supports plain `http://` (TCP) and `https://` (TLS). Provides both
/// basic GET and ranged GET for partial content downloads (BEP 19 web
/// seed).
pub(crate) struct HttpClient {
    /// Per-request timeout (connect + send + receive).
    timeout: Duration,
}

impl HttpClient {
    /// Create a new HTTP client with the given timeout.
    pub fn new(timeout: Duration) -> Self {
        HttpClient { timeout }
    }

    /// HTTP GET request without a `Range` header.
    ///
    /// Returns the full response body (capped at [`MAX_RESPONSE_SIZE`]).
    /// Used by the HTTP tracker for announces.
    pub async fn get(&self, url: &Url, path_and_query: &str) -> Result<Vec<u8>, Error> {
        let tls = if url.scheme() == "https" {
            Some(build_tls_connector()?)
        } else {
            None
        };
        self.send_request(url, &tls, path_and_query, None).await
    }

    /// HTTP GET with a `Range: bytes=start-end` header.
    ///
    /// Used by web seed download (BEP 19) to fetch partial file
    /// content. Returns the body bytes for the requested range
    /// (HTTP headers are stripped).
    pub async fn get_with_range(
        &self, url: &Url, path_and_query: &str, range_start: u64, range_end: u64,
    ) -> Result<Vec<u8>, Error> {
        let tls = if url.scheme() == "https" {
            Some(build_tls_connector()?)
        } else {
            None
        };
        let range = Some((range_start, range_end));
        let raw = self.send_request(url, &tls, path_and_query, range).await?;
        Ok(Self::body_from_response(&raw)?.to_vec())
    }

    /// Core request implementation: TCP connect, optional TLS, send
    /// request, read response (capped).
    async fn send_request(
        &self, url: &Url, tls: &Option<TlsConnector>, path_and_query: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Vec<u8>, Error> {
        let host = url
            .host_str()
            .ok_or(Error::new(ErrorKind::InvalidInput))?
            .to_owned();
        let port = url.port_or_known_default().unwrap_or(80);
        let tls = tls.clone();
        let path_and_query = path_and_query.to_owned();
        let timeout = self.timeout;

        let response = tokio::time::timeout(timeout, async move {
            let addrs = match lookup_host((&*host, port)).await {
                Ok(a) => a,
                Err(e) => return Err(Error::tracker_failed(e)),
            };

            // Try each resolved address until one connects.
            let (mut tcp_stream, mut last_err) = (None, None);
            for addr in addrs {
                let socket = if addr.is_ipv4() {
                    TcpSocket::new_v4()
                } else {
                    TcpSocket::new_v6()
                }
                .map_err(Error::tracker_failed)?;

                socket.set_nodelay(true).map_err(Error::tracker_failed)?;

                match socket.connect(addr).await {
                    Ok(s) => {
                        tcp_stream = Some(s);
                        break;
                    }
                    Err(e) => last_err = Some(Error::tracker_failed(e)),
                }
            }

            let Some(tcp_stream) = tcp_stream else {
                return Err(last_err.unwrap_or(Error::new(ErrorKind::TrackerRequestFailed)));
            };

            let mut stream: Box<dyn HttpStream> = if let Some(ref connector) = tls {
                let domain = ServerName::try_from(host.clone()).map_err(Error::invalid_input)?;
                let tls_stream = match connector.connect(domain, tcp_stream).await {
                    Ok(ts) => ts,
                    Err(e) => return Err(Error::tracker_failed(e)),
                };

                Box::new(tls_stream)
            } else {
                Box::new(tcp_stream)
            };

            // Build the HTTP request
            let mut request = format!(
                "GET {} HTTP/1.1\r\nHost: {}\r\nUser-Agent: torrent-rs/0.1.0\r\nAccept-Encoding: identity\r\nConnection: close\r\n",
                path_and_query, host
            );

            if let Some((start, end)) = range {
                request.push_str(&format!("Range: bytes={}-{}\r\n", start, end));
            }

            request.push_str("\r\n");

            if let Err(e) = stream.write_all(request.as_bytes()).await {
                return Err(Error::tracker_failed(e));
            }

            let mut buf = Vec::new();
            let mut limited = AsyncReadExt::take(&mut stream, MAX_RESPONSE_SIZE);

            if let Err(e) = limited.read_to_end(&mut buf).await {
                return Err(Error::tracker_failed(e));
            }

            Ok(buf)
        });

        match response.await {
            Ok(Ok(buf)) => Ok(buf),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Error::new(ErrorKind::TrackerRequestFailed)),
        }
    }

    /// Split HTTP response into body bytes (strips headers at `\r\n\r\n`).
    ///
    /// Returns just the body portion after the header separator.
    /// Returns an error if the separator is not found (malformed response).
    fn body_from_response(buf: &[u8]) -> Result<&[u8], Error> {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            Ok(&buf[pos + 4..])
        } else {
            Err(Error::new(ErrorKind::TrackerInvalidResponse))
        }
    }
}

// ── Redirect resolution ────────────────────────────────────────────

/// Resolve a redirect `Location` header value against a base URL.
///
/// Handles absolute URLs (e.g. `http://new.example.com/announce`),
/// relative paths (e.g. `/announce` or `announce`), and scheme-relative
/// URLs (e.g. `//new.example.com/announce`).
///
/// Returns an error if the resulting URL uses an unsupported scheme
/// (anything other than `http` or `https`).
pub(crate) fn resolve_redirect_url(base: &Url, location: &str) -> Result<Url, Error> {
    let new_url = Url::options()
        .base_url(Some(base))
        .parse(location)
        .map_err(|_| Error::new(ErrorKind::TrackerProtocolError))?;

    match new_url.scheme() {
        "http" | "https" => Ok(new_url),
        _ => {
            tracing::warn!(
                "HTTP redirect: unsupported scheme '{}' in redirect URL {}",
                new_url.scheme(),
                new_url,
            );
            Err(Error::new(ErrorKind::TrackerProtocolError))
        }
    }
}

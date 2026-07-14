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

use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpSocket, lookup_host};
use tokio_rustls::TlsConnector;

use crate::error::{Error, ErrorKind};

use super::tls::build_tls_connector;
use super::{IntoUrl, Url};

/// Maximum number of redirects to follow before giving up.
pub(crate) const MAX_REDIRECTS: u32 = 5;

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
    /// Maximum response size to read (`None` = no cap, reads until EOF).
    max_response: Option<u64>,
}

impl HttpClient {
    /// Create a new HTTP client with no response size cap.
    ///
    /// Use [`with_max_response`](Self::with_max_response) to set a cap.
    pub fn new() -> Self {
        HttpClient { max_response: None }
    }

    /// Create a new HTTP client with a response size cap.
    ///
    /// Responses larger than `max_response` bytes are silently
    /// truncated.  Used by web seed download (BEP 19) to prevent
    /// unbounded reads from large files.
    pub fn with_max_response(max_response: u64) -> Self {
        HttpClient {
            max_response: Some(max_response),
        }
    }

    /// HTTP GET request without a `Range` header.
    ///
    /// The `path_and_query` for the request line is derived from `url`
    /// (path + optional query string). Returns the full response body.
    ///
    /// Used by the HTTP tracker for announces.  Callers are expected
    /// to wrap this with [`tokio::time::timeout`] if they need a
    /// deadline.
    pub async fn get(&self, url: impl IntoUrl) -> Result<Vec<u8>, Error> {
        let url = url.into_url()?;
        let tls = if url.scheme() == "https" {
            Some(build_tls_connector()?)
        } else {
            None
        };
        self.send_request("GET", &url, &tls, None).await
    }

    /// HTTP GET with a `Range: bytes=start-end` header.
    ///
    /// The `path_and_query` for the request line is derived from `url`.
    /// Returns the body bytes for the requested range (HTTP headers
    /// are stripped).
    ///
    /// Used by web seed download (BEP 19) to fetch partial file content.
    /// Callers are expected to wrap this with [`tokio::time::timeout`]
    /// if they need a deadline.
    pub async fn get_with_range(
        &self, url: impl IntoUrl, range_start: u64, range_end: u64,
    ) -> Result<Vec<u8>, Error> {
        let url = url.into_url()?;
        let tls = if url.scheme() == "https" {
            Some(build_tls_connector()?)
        } else {
            None
        };
        let range = Some((range_start, range_end));
        let raw = self.send_request("GET", &url, &tls, range).await?;
        Ok(Self::body_from_response(&raw)?.to_vec())
    }

    /// `method` is the HTTP method string (e.g. `"GET"`).
    /// `range` adds a `Range: bytes=start-end` header when `Some`.
    ///
    /// `path_and_query` for the HTTP request line is derived from
    /// `url` via [`path_and_query_from_url`].
    async fn send_request(
        &self, method: &str, url: &Url, tls: &Option<TlsConnector>, range: Option<(u64, u64)>,
    ) -> Result<Vec<u8>, Error> {
        let host = url
            .host_str()
            .ok_or(Error::new(ErrorKind::InvalidInput))?
            .to_owned();
        let port = url.port_or_known_default().unwrap_or(80);
        let method = method.to_owned();
        let tls = tls.clone();
        let path_and_query = path_and_query_from_url(url);

        let addrs = match lookup_host((&*host, port)).await {
            Ok(a) => a,
            Err(e) => return Err(Error::io(e)),
        };

        // Try each resolved address until one connects.
        let (mut tcp_stream, mut last_err) = (None, None);
        for addr in addrs {
            let socket = if addr.is_ipv4() {
                TcpSocket::new_v4()
            } else {
                TcpSocket::new_v6()
            }
            .map_err(Error::io)?;

            socket.set_nodelay(true).map_err(Error::io)?;

            match socket.connect(addr).await {
                Ok(s) => {
                    tcp_stream = Some(s);
                    break;
                }
                Err(e) => last_err = Some(Error::io(e)),
            }
        }

        let Some(tcp_stream) = tcp_stream else {
            return Err(last_err.unwrap_or(Error::new(ErrorKind::Io)));
        };

        let mut stream: Box<dyn HttpStream> = if let Some(ref connector) = tls {
            let domain = ServerName::try_from(host.clone()).map_err(Error::invalid_input)?;
            let tls_stream = match connector.connect(domain, tcp_stream).await {
                Ok(ts) => ts,
                Err(e) => return Err(Error::io(e)),
            };
            Box::new(tls_stream)
        } else {
            Box::new(tcp_stream)
        };

        // Build the HTTP request line and headers
        let mut request = format!(
            "{method} {path_and_query} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: torrent-rs/0.1.0\r\nAccept-Encoding: identity\r\nConnection: close\r\n",
        );

        if let Some((start, end)) = range {
            request.push_str(&format!("Range: bytes={start}-{end}\r\n"));
        }

        request.push_str("\r\n");

        if let Err(e) = stream.write_all(request.as_bytes()).await {
            return Err(Error::io(e));
        }

        let mut buf = Vec::new();
        match self.max_response {
            Some(cap) => {
                let mut limited = AsyncReadExt::take(&mut stream, cap);
                limited.read_to_end(&mut buf).await.map_err(Error::io)?;
            }
            None => {
                stream.read_to_end(&mut buf).await.map_err(Error::io)?;
            }
        }

        Ok(buf)
    }

    /// Split HTTP response into body bytes (strips headers at `\r\n\r\n`).
    ///
    /// Returns just the body portion after the header separator.
    /// Returns an error if the separator is not found (malformed response).
    fn body_from_response(buf: &[u8]) -> Result<&[u8], Error> {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            Ok(&buf[pos + 4..])
        } else {
            Err(Error::new(ErrorKind::Protocol))
        }
    }
}

// ── Helpers ────────────────────────────────────────────────────────

/// Extract the `path_and_query` string from a URL for the HTTP
/// request line.
///
/// Returns `"/path"` when there is no query, or `"/path?query"`
/// when a query string is present.
fn path_and_query_from_url(url: &Url) -> String {
    match url.query() {
        Some(q) => format!("{}?{}", url.path(), q),
        None => url.path().to_string(),
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

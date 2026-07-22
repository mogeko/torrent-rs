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

use http::{Request, Response};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpSocket, lookup_host};

use crate::error::{Error, ErrorKind};

use super::Url;
use super::tls::build_tls_connector;

/// Maximum number of redirects to follow before giving up.
pub(crate) const MAX_REDIRECTS: u32 = 5;

// ── Internal stream trait ──────────────────────────────────────────

/// Internal trait to unify plain TCP and TLS streams into `Box<dyn …>`.
trait HttpStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> HttpStream for T {}

// ── HttpClient ─────────────────────────────────────────────────────

/// A purpose-built HTTP/1.1 client for BitTorrent use cases.
///
/// Supports plain `http://` (TCP) and `https://` (TLS).
/// Each [`send_request`](HttpClient::send_request) performs
/// independent DNS resolution + TCP connect + TLS + HTTP exchange.
///
/// The client is stateless — timeout should be applied at the
/// call site via [`tokio::time::timeout`].
#[derive(Clone)]
pub(crate) struct HttpClient;

impl HttpClient {
    /// Create a new HTTP client.
    pub fn new() -> Self {
        HttpClient
    }

    /// Send an HTTP request — DNS → TCP → TLS → send → receive → parse.
    ///
    /// Accepts a standard [`http::Request<Vec<u8>>`] and returns an
    /// [`http::Response<Vec<u8>>`].  The request body is ignored for GET
    /// (always sent as empty).  Timeout is **not** applied here — wrap
    /// with [`tokio::time::timeout`] at the call site.
    pub(crate) async fn send_request(
        &self, req: Request<Vec<u8>>,
    ) -> Result<Response<Vec<u8>>, Error> {
        let uri = req.uri().clone();
        let host = uri
            .host()
            .ok_or(Error::new(ErrorKind::InvalidInput))?
            .to_owned();
        let port = uri
            .port_u16()
            .unwrap_or(if uri.scheme_str() == Some("https") {
                443
            } else {
                80
            });
        let path_and_query = match uri.path_and_query() {
            Some(pq) => pq.as_str().to_owned(),
            None => "/".to_owned(),
        };
        let tls = if uri.scheme_str() == Some("https") {
            Some(build_tls_connector()?)
        } else {
            None
        };

        // ── DNS + TCP connect ──
        let addrs = lookup_host((&*host, port)).await.map_err(Error::io)?;

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

        // ── TLS handshake (if https) ──
        let mut stream: Box<dyn HttpStream> = if let Some(ref connector) = tls {
            let domain = ServerName::try_from(host.clone()).map_err(Error::invalid_input)?;
            let tls_stream = connector
                .connect(domain, tcp_stream)
                .await
                .map_err(Error::io)?;
            Box::new(tls_stream)
        } else {
            Box::new(tcp_stream)
        };

        // ── Build HTTP request line from http::Request ──
        let method = req.method().as_str();
        let mut http_req = format!("{method} {path_and_query} HTTP/1.1\r\nHost: {host}\r\n",);

        // Copy headers from the http::Request
        for (name, value) in req.headers() {
            if let Ok(v) = value.to_str() {
                http_req.push_str(&format!("{}: {v}\r\n", name.as_str()));
            }
        }

        // Default headers if not overridden
        if !req.headers().contains_key("user-agent") {
            http_req.push_str("User-Agent: torrent-rs/0.1.0\r\n");
        }
        if !req.headers().contains_key("accept-encoding") {
            http_req.push_str("Accept-Encoding: identity\r\n");
        }
        http_req.push_str("Connection: close\r\n");
        http_req.push_str("\r\n");

        stream
            .write_all(http_req.as_bytes())
            .await
            .map_err(Error::io)?;

        // ── Read response ──
        let max_response = req
            .headers()
            .get("x-max-response")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());

        let mut buf = Vec::new();
        match max_response {
            Some(cap) => {
                let mut limited = AsyncReadExt::take(&mut stream, cap);
                limited.read_to_end(&mut buf).await.map_err(Error::io)?;
            }
            None => {
                stream.read_to_end(&mut buf).await.map_err(Error::io)?;
            }
        }

        // ── Parse HTTP response ──
        parse_http_response(&buf)
    }
}

/// Minimal HTTP/1.1 response parser — extracts status code, headers, and body
/// into an [`http::Response<Vec<u8>>`].
fn parse_http_response(raw: &[u8]) -> Result<Response<Vec<u8>>, Error> {
    let header_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or(Error::new(ErrorKind::Protocol))?;

    let header_bytes = &raw[..header_end];
    let body = raw[header_end + 4..].to_vec();

    let header_str =
        std::str::from_utf8(header_bytes).map_err(|_| Error::new(ErrorKind::Protocol))?;

    let mut lines = header_str.lines();
    let status_line = lines.next().unwrap_or("");

    // Parse "HTTP/1.1 200 OK"
    let code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(500);

    let mut resp = Response::builder().status(code);

    // Parse headers
    for line in lines {
        if let Some((name, value)) = line.split_once(": ") {
            resp = resp.header(name, value);
        }
    }

    resp.body(body).map_err(|_| Error::new(ErrorKind::Protocol))
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

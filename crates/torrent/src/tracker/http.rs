use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpSocket, lookup_host};
use tokio_rustls::TlsConnector;

use crate::error::{Error, ErrorKind};

use super::{AnnounceEvent, AnnounceRequest, AnnounceResponse, IntoUrl, Url};

/// Timeout for HTTP tracker connect + request + response read.
use super::DEFAULT_TIMEOUT;

/// Maximum number of redirects to follow before giving up.
const MAX_REDIRECTS: u32 = 5;

/// Maximum response size to guard against malicious or buggy trackers (256 KB).
const MAX_RESPONSE_SIZE: u64 = 256 * 1024;

/// Internal trait to unify plain TCP and TLS streams into a single `Box<dyn …>`.
trait TrackerStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> TrackerStream for T {}

/// HTTP tracker client (BEP 3, BEP 23).
///
/// Supports both `http://` (plain TCP) and `https://` (TLS via `tokio-rustls`).
pub struct HttpTracker {
    url: Url,
    /// Pre-extracted host string to avoid `Box::leak` on every announce.
    host: String,
    /// Port to connect to (80 for http, 443 for https, or URL-specified).
    port: u16,
    /// TLS connector for `https://` URLs; `None` for plain `http://`.
    tls: Option<TlsConnector>,
    /// Per-request timeout.
    timeout: Duration,
}

impl fmt::Debug for HttpTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpTracker")
            .field("url", &self.url)
            .field("tls", &self.tls.is_some())
            .finish()
    }
}

impl Clone for HttpTracker {
    fn clone(&self) -> Self {
        HttpTracker {
            url: self.url.clone(),
            host: self.host.clone(),
            port: self.port,
            tls: self.tls.clone(),
            timeout: self.timeout,
        }
    }
}

impl HttpTracker {
    /// Create a new HTTP tracker client with the default 15 s timeout.
    ///
    /// `url` must be a full announce URL (e.g. `http://tracker.example.com:6969/announce`
    /// or `https://tracker.example.com/announce`). Automatically detects TLS.
    /// Accepts `&str`, `String`, `&String`, or `Url`.
    pub fn new(url: impl IntoUrl) -> Result<Self, Error> {
        HttpTracker::with_timeout(url, DEFAULT_TIMEOUT)
    }

    /// Create a new HTTP tracker client with a custom timeout.
    pub fn with_timeout(url: impl IntoUrl, timeout: Duration) -> Result<Self, Error> {
        let url = url.into_url()?;
        let host = url
            .host_str()
            .ok_or(Error::new(ErrorKind::InvalidInput))?
            .to_owned();
        let port = url.port_or_known_default().unwrap_or(80);
        let tls = if url.scheme() == "https" {
            Some(build_tls_connector()?)
        } else {
            None
        };
        Ok(HttpTracker {
            url,
            host,
            port,
            tls,
            timeout,
        })
    }

    /// Returns the tracker's URL.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Announce to the HTTP tracker, following redirects (301, 302) up to
    /// `MAX_REDIRECTS` times.
    pub async fn announce(&self, req: &AnnounceRequest) -> Result<AnnounceResponse, Error> {
        tracing::info!("HTTP announce to {} (event: {:?})", self.url, req.event);

        let mut current_url = self.url.clone();
        let mut tls = self.tls.clone();
        let mut redirects_remaining = MAX_REDIRECTS;

        loop {
            let path_and_query = format!("{}?{}", current_url.path(), build_query_string(req));

            let buf = send_http_request(&current_url, &tls, &path_and_query, self.timeout).await?;

            // Parse HTTP response: find "\r\n\r\n" separator
            let Some(header_end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
                tracing::warn!("HTTP announce: missing header separator");
                return Err(Error::new(ErrorKind::TrackerInvalidResponse));
            };

            let body = &buf[header_end + 4..];

            // Parse status code from first line
            let headers_str = std::str::from_utf8(&buf[..header_end])
                .map_err(|_| Error::new(ErrorKind::TrackerInvalidResponse))?;
            let first_line = headers_str.lines().next().unwrap_or("");
            let status_code = first_line.split_whitespace().nth(1).unwrap_or("");

            match status_code {
                "301" | "302" => {
                    redirects_remaining -= 1;
                    if redirects_remaining == 0 {
                        tracing::warn!("HTTP announce: too many redirects");
                        return Err(Error::new(ErrorKind::TrackerRequestFailed));
                    }

                    let location = headers_str
                        .lines()
                        .find(|l| l.to_ascii_lowercase().starts_with("location: "))
                        .and_then(|l| l.split_once(": ").map(|x| x.1))
                        .map(|l| l.trim())
                        .unwrap_or("");
                    if location.is_empty() {
                        return Err(Error::new(ErrorKind::TrackerProtocolError));
                    }

                    let new_url = resolve_redirect_url(&current_url, location)?;
                    tracing::info!(
                        "HTTP redirect #{}/{}: {} -> {}",
                        MAX_REDIRECTS - redirects_remaining,
                        MAX_REDIRECTS,
                        current_url,
                        new_url,
                    );

                    // If redirect changes scheme to https, lazily build TLS connector.
                    if new_url.scheme() == "https" && tls.is_none() {
                        tls = Some(build_tls_connector()?);
                    } else if new_url.scheme() == "http" {
                        tls = None;
                    }

                    current_url = new_url;
                    continue;
                }
                "200" => return AnnounceResponse::from_bencode(body),
                _ => {
                    tracing::warn!("HTTP announce: unexpected status {}", status_code);
                    return Err(Error::new(ErrorKind::TrackerRequestFailed));
                }
            }
        }
    }
}

/// Build a TLS connector with system root certificates.
fn build_tls_connector() -> Result<TlsConnector, Error> {
    let mut root_store = rustls::RootCertStore::empty();

    let native_certs = rustls_native_certs::load_native_certs();
    for cert in native_certs.certs {
        root_store.add(cert).map_err(Error::invalid_input)?;
    }

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(TlsConnector::from(Arc::new(config)))
}

/// Send an HTTP GET request to the given URL and return the raw response bytes.
///
/// Handles TCP connection, optional TLS wrapping, request sending, and
/// response reading (capped at [`MAX_RESPONSE_SIZE`]).
async fn send_http_request(
    url: &Url, tls: &Option<TlsConnector>, path_and_query: &str, timeout: Duration,
) -> Result<Vec<u8>, Error> {
    let host = url
        .host_str()
        .ok_or(Error::new(ErrorKind::InvalidInput))?
        .to_owned();
    let port = url.port_or_known_default().unwrap_or(80);
    let tls = tls.clone();
    let path_and_query = path_and_query.to_owned();

    let response = tokio::time::timeout(timeout, async move {
        let addrs = lookup_host((&*host, port))
            .await
            .map_err(Error::tracker_failed)?;

        // Try each resolved address (IPv4 and/or IPv6) until one connects.
        // This mirrors TcpStream::connect((host, port)) which iterates all
        // resolved addresses, but also allows us to set TCP_NODELAY.
        let mut last_err = None;
        let mut tcp_stream = None;
        for addr in addrs {
            let socket = if addr.is_ipv4() {
                TcpSocket::new_v4()
            } else {
                TcpSocket::new_v6()
            }
            .map_err(Error::tracker_failed)?;

            socket
                .set_nodelay(true)
                .map_err(Error::tracker_failed)?;

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

        let mut stream: Box<dyn TrackerStream> = if let Some(ref connector) = tls {
            use rustls::pki_types::ServerName;

            let domain = ServerName::try_from(host.clone())
                .map_err(Error::invalid_input)?;
            let tls_stream = connector
                .connect(domain, tcp_stream)
                .await
                .map_err(Error::tracker_failed)?;
            Box::new(tls_stream)
        } else {
            Box::new(tcp_stream)
        };

        let request = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nUser-Agent: torrent-rs/0.1.0\r\nAccept-Encoding: identity\r\nConnection: close\r\n\r\n",
            path_and_query, host
        );

        stream
            .write_all(request.as_bytes())
            .await
            .map_err(Error::tracker_failed)?;

        let mut buf = Vec::new();
        let mut limited = AsyncReadExt::take(&mut stream, MAX_RESPONSE_SIZE);

        limited
            .read_to_end(&mut buf)
            .await
            .map_err(Error::tracker_failed)?;

        Ok(buf)
    })
    .await;

    match response {
        Ok(Ok(buf)) => Ok(buf),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(Error::new(ErrorKind::TrackerRequestFailed)),
    }
}

/// Resolve a redirect `Location` header value against a base URL.
///
/// Handles absolute URLs (e.g. `http://new.example.com/announce`),
/// relative paths (e.g. `/announce` or `announce`), and scheme-relative
/// URLs (e.g. `//new.example.com/announce`).
///
/// Returns an error if the resulting URL uses an unsupported scheme
/// (anything other than `http` or `https`).
fn resolve_redirect_url(base: &Url, location: &str) -> Result<Url, Error> {
    // url::Url::options().base_url() follows RFC 3986 reference resolution.
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

/// Build the query string with correct percent-encoding for binary fields
/// (info_hash, peer_id).
fn build_query_string(req: &AnnounceRequest) -> String {
    use url::form_urlencoded::byte_serialize;

    let mut q = String::new();

    q.push_str("info_hash=");
    q.push_str(&byte_serialize(&req.info_hash).collect::<String>());

    q.push_str("&peer_id=");
    q.push_str(&byte_serialize(&req.peer_id.0).collect::<String>());

    q.push_str(&format!("&port={}", req.port));
    q.push_str(&format!("&uploaded={}", req.uploaded));
    q.push_str(&format!("&downloaded={}", req.downloaded));
    q.push_str(&format!("&left={}", req.left));

    if req.compact {
        q.push_str("&compact=1");
    }

    let event_str = match req.event {
        AnnounceEvent::Started => "started",
        AnnounceEvent::Stopped => "stopped",
        AnnounceEvent::Completed => "completed",
        AnnounceEvent::None => "empty",
        _ => "empty", // unknown future variants
    };
    q.push_str("&event=");
    q.push_str(event_str);

    if let Some(numwant) = req.numwant {
        q.push_str(&format!("&numwant={}", numwant));
    }
    if let Some(key) = req.key {
        q.push_str(&format!("&key={}", key));
    }
    if let Some(ref trackerid) = req.trackerid {
        q.push_str("&trackerid=");
        q.push_str(trackerid);
    }
    if let Some(ip) = req.ip {
        q.push_str(&format!("&ip={ip}"));
    }
    if let Some(ipv6) = req.ipv6 {
        q.push_str(&format!("&ipv6={ipv6}"));
    }

    q
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::PeerId;

    #[test]
    fn test_build_query_string() {
        let mut req = AnnounceRequest::new([0x01; 20], PeerId::random(), 6881);
        req.left = 1024;
        req.event = AnnounceEvent::Started;
        let q = build_query_string(&req);
        assert!(q.starts_with("info_hash="));
        assert!(q.contains("&peer_id="));
        assert!(q.contains("&port=6881"));
        assert!(q.contains("&compact=1"));
        assert!(q.contains("&event=started"));
        assert!(q.contains("&left=1024"));
    }

    #[test]
    fn test_new_invalid_url() {
        assert!(HttpTracker::new("not-a-valid-url").is_err());
    }

    #[test]
    fn test_build_query_string_binary_info_hash() {
        let info_hash = [
            0x00, 0x01, 0x7F, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let mut req = AnnounceRequest::new(info_hash, PeerId::random(), 6881);
        req.compact = false;
        req.numwant = None;
        req.left = 100;
        let q = build_query_string(&req);
        // Binary bytes should be percent-encoded by byte_serialize
        assert!(q.contains("%00%01%7F%FF"));
    }

    // ── resolve_redirect_url tests ──────────────────────────────

    #[test]
    fn redirect_absolute_url() {
        let base = Url::parse("http://tracker.example.com:6969/announce").unwrap();
        let resolved = resolve_redirect_url(&base, "http://new.example.com/announce").unwrap();
        assert_eq!(resolved.as_str(), "http://new.example.com/announce");
    }

    #[test]
    fn redirect_relative_path() {
        let base = Url::parse("http://tracker.example.com:6969/announce").unwrap();
        let resolved = resolve_redirect_url(&base, "/new-announce").unwrap();
        assert_eq!(
            resolved.as_str(),
            "http://tracker.example.com:6969/new-announce"
        );
    }

    #[test]
    fn redirect_https_to_http() {
        let base = Url::parse("https://tracker.example.com/announce").unwrap();
        let resolved = resolve_redirect_url(&base, "http://other.example.com/announce").unwrap();
        assert_eq!(resolved.as_str(), "http://other.example.com/announce");
    }

    #[test]
    fn redirect_http_to_https() {
        let base = Url::parse("http://tracker.example.com/announce").unwrap();
        let resolved = resolve_redirect_url(&base, "https://tracker.example.com/announce").unwrap();
        assert_eq!(resolved.as_str(), "https://tracker.example.com/announce");
    }

    #[test]
    fn redirect_rejects_udp_scheme() {
        let base = Url::parse("http://tracker.example.com/announce").unwrap();
        assert!(resolve_redirect_url(&base, "udp://tracker.example.com:6969").is_err());
    }

    #[test]
    fn redirect_empty_location_is_error() {
        // resolve_redirect_url is not called with empty string (caller guards that),
        // but url::Url::parse("") against a base returns the base itself.
        let base = Url::parse("http://tracker.example.com/announce").unwrap();
        // An empty string resolves to the base URL, which is http → still valid.
        let resolved = resolve_redirect_url(&base, "").unwrap();
        assert_eq!(resolved.as_str(), "http://tracker.example.com/announce");
    }

    #[test]
    fn redirect_scheme_relative() {
        let base = Url::parse("http://tracker.example.com/announce").unwrap();
        let resolved = resolve_redirect_url(&base, "//other.example.com/announce").unwrap();
        assert_eq!(resolved.as_str(), "http://other.example.com/announce");
    }
}

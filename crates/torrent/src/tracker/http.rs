use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::{Error, ErrorKind};
use crate::tracker::{AnnounceEvent, AnnounceRequest, AnnounceResponse, IntoUrl, Url};

/// Timeout for HTTP tracker connect + request + response read.
const TIMEOUT: Duration = Duration::from_secs(15);

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
    /// TLS connector for `https://` URLs; `None` for plain `http://`.
    tls: Option<tokio_rustls::TlsConnector>,
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
            tls: self.tls.clone(),
        }
    }
}

impl HttpTracker {
    /// Create a new HTTP tracker client.
    ///
    /// `url` must be a full announce URL (e.g. `http://tracker.example.com:6969/announce`
    /// or `https://tracker.example.com/announce`). Automatically detects TLS.
    /// Accepts `&str`, `String`, `&String`, or `Url`.
    pub fn new(url: impl IntoUrl) -> Result<Self, Error> {
        let url = url.into_url()?;
        let tls = if url.scheme() == "https" {
            Some(build_tls_connector()?)
        } else {
            None
        };
        Ok(HttpTracker { url, tls })
    }

    /// Returns the tracker's URL.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Announce to the HTTP tracker.
    pub async fn announce(&self, req: &AnnounceRequest) -> Result<AnnounceResponse, Error> {
        tracing::info!("HTTP announce to {} (event: {:?})", self.url, req.event);
        // Build path + query string (avoid intermediate Url clone).
        let path_and_query = format!("{}?{}", self.url.path(), build_query_string(req));

        // Owned copy for the async block (TlsConnector::connect requires 'static).
        let host: &'static str = Box::leak(
            self.url
                .host_str()
                .ok_or(Error::new(ErrorKind::InvalidInput))?
                .to_owned()
                .into_boxed_str(),
        );
        // Use the correct default port for the scheme (80 for http, 443 for https).
        let port = self.url.port_or_known_default().unwrap_or(80);

        // Build host header
        let host_header = match self.url.port() {
            Some(p) => format!("{}:{}", host, p),
            None => host.to_string(),
        };

        // Build HTTP GET request
        let request = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nAccept-Encoding: identity\r\nConnection: close\r\n\r\n",
            path_and_query, host_header
        );

        // Perform the full HTTP round-trip with a single timeout guard.
        // Clone TLS connector before the async block to avoid borrowing self.
        let tls = self.tls.clone();
        let response = tokio::time::timeout(TIMEOUT, async move {
            let tcp_stream = TcpStream::connect((host, port))
                .await
                .map_err(Error::tracker_failed)?;

            // Conditionally wrap the TCP stream in TLS for `https://` URLs.
            let mut stream: Box<dyn TrackerStream> = if let Some(ref connector) = tls {
                let domain = rustls::pki_types::ServerName::try_from(host)
                    .map_err(|_| Error::new(ErrorKind::InvalidInput))?;
                let tls_stream = connector
                    .connect(domain, tcp_stream)
                    .await
                    .map_err(Error::tracker_failed)?;
                Box::new(tls_stream)
            } else {
                Box::new(tcp_stream)
            };

            stream
                .write_all(request.as_bytes())
                .await
                .map_err(Error::tracker_failed)?;

            // Limit read size to prevent OOM.
            let mut buf = Vec::new();
            let mut limited = AsyncReadExt::take(&mut stream, MAX_RESPONSE_SIZE);

            limited
                .read_to_end(&mut buf)
                .await
                .map_err(Error::tracker_failed)?;

            Ok(buf)
        })
        .await;

        let buf = match response {
            Ok(Ok(buf)) => buf,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(Error::new(ErrorKind::TrackerRequestFailed)),
        };

        // Parse HTTP response: find "\r\n\r\n" separator
        let Some(header_end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
            tracing::warn!("HTTP announce: missing header separator");
            return Err(Error::new(ErrorKind::TrackerInvalidResponse));
        };

        let body = &buf[header_end + 4..];

        // Check for HTTP redirect (301, 302)
        let headers_str = std::str::from_utf8(&buf[..header_end])
            .map_err(|_| Error::new(ErrorKind::TrackerInvalidResponse))?;
        let first_line = headers_str.lines().next().unwrap_or("");
        let status_code = first_line.split_whitespace().nth(1).unwrap_or("");

        match status_code {
            "301" | "302" => Err(Error::new(ErrorKind::TrackerProtocolError)),
            "200" => AnnounceResponse::from_bencode(body),
            _ => Err(Error::new(ErrorKind::TrackerRequestFailed)),
        }
    }
}

/// Build a TLS connector with system root certificates.
fn build_tls_connector() -> Result<tokio_rustls::TlsConnector, Error> {
    let mut root_store = rustls::RootCertStore::empty();

    let native_certs = rustls_native_certs::load_native_certs();
    for cert in native_certs.certs {
        root_store.add(cert).map_err(Error::invalid_input)?;
    }

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
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
}

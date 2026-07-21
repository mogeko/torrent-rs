use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio_rustls::TlsConnector;
use tower::Service;

use crate::error::{Error, ErrorKind};
use crate::net::http::{HttpClient, MAX_REDIRECTS, resolve_redirect_url};
use crate::net::tls::build_tls_connector;
use crate::{IntoUrl, Url};

use super::{AnnounceEvent, AnnounceRequest, AnnounceResponse};

/// Maximum response size to guard against malicious or buggy servers (256 KB).
pub(crate) const MAX_RESPONSE_SIZE: u64 = 256 * 1024;

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
        }
    }
}

impl HttpTracker {
    /// Create a new HTTP tracker client.
    ///
    /// `url` must be a full announce URL (e.g. `http://tracker.example.com:6969/announce`
    /// or `https://tracker.example.com/announce`). Automatically detects TLS.
    /// Accepts `&str`, `String`, `&String`, or `Url`.
    ///
    /// Timeout is not embedded — apply [`tower::timeout::TimeoutLayer`]
    /// at the call site via [`tower::ServiceBuilder`].
    pub fn new(url: impl IntoUrl) -> Result<Self, Error> {
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
        })
    }

    /// Returns the tracker's URL.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Announce to the HTTP tracker, following redirects (301, 302) up to
    /// `MAX_REDIRECTS` times.
    pub async fn announce(&self, req: AnnounceRequest) -> Result<AnnounceResponse, Error> {
        tracing::info!("HTTP announce to {} (event: {:?})", self.url, req.event);

        let mut current_url = self.url.clone();
        let mut tls = self.tls.clone();
        let mut redirects_remaining = MAX_REDIRECTS;
        let mut client = HttpClient::new();

        loop {
            let mut announce_url = current_url.clone();
            announce_url.set_query(Some(&build_query_string(&req)));

            let http_req = http::Request::get(announce_url.as_str())
                .header("x-max-response", MAX_RESPONSE_SIZE.to_string())
                .body(vec![])
                .map_err(|_| Error::new(ErrorKind::TrackerInvalidResponse))?;

            let resp = Service::call(&mut client, http_req)
                .await
                .map_err(Error::io)?;

            let status_code = resp.status().as_u16();
            let location = resp
                .headers()
                .get(http::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_owned());
            let body = resp.into_body();

            match status_code {
                301 | 302 => {
                    redirects_remaining -= 1;
                    if redirects_remaining == 0 {
                        tracing::warn!("HTTP announce: too many redirects");
                        return Err(Error::new(ErrorKind::TrackerRequestFailed));
                    }

                    let location = location.unwrap_or_default();
                    if location.is_empty() {
                        return Err(Error::new(ErrorKind::TrackerProtocolError));
                    }

                    let new_url = resolve_redirect_url(&current_url, &location)?;
                    tracing::info!(
                        "HTTP redirect #{}/{}: {} -> {}",
                        MAX_REDIRECTS - redirects_remaining,
                        MAX_REDIRECTS,
                        current_url,
                        new_url,
                    );

                    if new_url.scheme() == "https" && tls.is_none() {
                        tls = Some(build_tls_connector()?);
                    } else if new_url.scheme() == "http" {
                        tls = None;
                    }

                    current_url = new_url;
                }
                200 => return AnnounceResponse::from_bencode(&body),
                _ => {
                    tracing::warn!("HTTP announce: unexpected status {}", status_code);
                    return Err(Error::new(ErrorKind::TrackerRequestFailed));
                }
            }
        }
    }
}

/// Tower [`Service`] implementation for HTTP tracker announces.
///
/// This is a **raw** service — no timeout or retry is applied here.
/// Wrap with [`tower::ServiceBuilder`] at the call site to add
/// timeout, retry, rate-limiting, etc.:
///
/// ```ignore
/// use tower::{Service, ServiceBuilder};
///
/// let mut svc = ServiceBuilder::new()
///     .timeout(Duration::from_secs(15))
///     .service(tracker);
/// let resp = svc.call(req).await?;
/// ```
impl Service<AnnounceRequest> for HttpTracker {
    type Response = AnnounceResponse;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: AnnounceRequest) -> Self::Future {
        let this = self.clone();
        Box::pin(async move { this.announce(req).await })
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

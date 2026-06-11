use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, lookup_host};

use crate::error::{Error, ErrorKind};
use crate::tracker::{AnnounceEvent, AnnounceRequest, AnnounceResponse, IntoUrl, Url};

/// Timeout for HTTP tracker connect + request + response read.
const TIMEOUT: Duration = Duration::from_secs(15);

/// Maximum response size to guard against malicious or buggy trackers (256 KB).
const MAX_RESPONSE_SIZE: u64 = 256 * 1024;

/// HTTP tracker client (BEP 3).
#[derive(Debug, Clone)]
pub struct HttpTracker {
    url: Url,
}

impl HttpTracker {
    /// Create a new HTTP tracker client.
    ///
    /// `url` must be a full announce URL (e.g. `http://tracker.example.com:6969/announce`).
    /// Accepts `&str`, `String`, `&String`, or `Url`.
    pub fn new(url: impl IntoUrl) -> Result<Self, Error> {
        Ok(HttpTracker {
            url: url.into_url()?,
        })
    }

    /// Announce to the HTTP tracker.
    pub async fn announce(&self, req: &AnnounceRequest) -> Result<AnnounceResponse, Error> {
        // Build path + query string (avoid intermediate Url clone).
        let path_and_query = format!("{}?{}", self.url.path(), build_query_string(req));

        // Resolve hostname asynchronously (avoid blocking the tokio runtime).
        let Some(host) = self.url.host_str() else {
            return Err(Error::new(ErrorKind::InvalidInput));
        };
        let port = self.url.port().unwrap_or(80);

        let addr = lookup_host((host, port))
            .await
            .map_err(|e| Error::with_source(ErrorKind::TrackerRequestFailed, e))?
            .next()
            .ok_or(Error::new(ErrorKind::TrackerRequestFailed))?;

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
        let response = tokio::time::timeout(TIMEOUT, async {
            let mut stream = TcpStream::connect(addr)
                .await
                .map_err(|e| Error::with_source(ErrorKind::TrackerRequestFailed, e))?;

            stream
                .write_all(request.as_bytes())
                .await
                .map_err(|e| Error::with_source(ErrorKind::TrackerRequestFailed, e))?;

            // Limit read size to prevent OOM.
            let mut buf = Vec::new();
            let mut limited = AsyncReadExt::take(&mut stream, MAX_RESPONSE_SIZE);

            limited
                .read_to_end(&mut buf)
                .await
                .map_err(|e| Error::with_source(ErrorKind::TrackerRequestFailed, e))?;

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

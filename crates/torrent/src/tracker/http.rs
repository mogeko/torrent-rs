use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::{Error, ErrorKind};
use crate::tracker::{AnnounceEvent, AnnounceRequest, AnnounceResponse, IntoUrl, Url};

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
        let announce_url = build_announce_url(&self.url, req);

        // Resolve host:port (supports DNS via ToSocketAddrs)
        let host = self
            .url
            .host_str()
            .ok_or(Error::new(ErrorKind::InvalidInput))?;
        let port = self.url.port().unwrap_or(80);

        let mut stream = TcpStream::connect((host, port))
            .await
            .map_err(|e| Error::with_source(ErrorKind::TrackerRequestFailed, e))?;

        // Build host header
        let host_header = match self.url.port() {
            Some(p) => format!("{}:{}", host, p),
            None => host.to_string(),
        };

        // Build HTTP GET request
        let path_and_query = announce_url[url::Position::BeforePath..].to_string();
        let request = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nAccept-Encoding: identity\r\nConnection: close\r\n\r\n",
            path_and_query, host_header
        );
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| Error::with_source(ErrorKind::TrackerRequestFailed, e))?;

        // Read response
        let mut buf = Vec::new();
        stream
            .read_to_end(&mut buf)
            .await
            .map_err(|e| Error::with_source(ErrorKind::TrackerRequestFailed, e))?;

        // Parse HTTP response: find "\r\n\r\n" separator
        let header_end = buf
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .ok_or(Error::new(ErrorKind::TrackerInvalidResponse))?;

        let body = &buf[header_end + 4..];

        // Check for HTTP redirect (301, 302)
        let headers_str = std::str::from_utf8(&buf[..header_end])
            .map_err(|_| Error::new(ErrorKind::TrackerInvalidResponse))?;
        let first_line = headers_str.lines().next().unwrap_or("");
        if first_line.contains("301") || first_line.contains("302") {
            return Err(Error::new(ErrorKind::TrackerProtocolError));
        }

        if !first_line.contains("200") {
            return Err(Error::new(ErrorKind::TrackerRequestFailed));
        }

        AnnounceResponse::from_bencode(body)
    }
}

/// Build the full announce URL with query parameters.
fn build_announce_url(base: &Url, req: &AnnounceRequest) -> Url {
    let mut url = base.clone();

    let query = build_query_string(req);
    url.set_query(Some(&query));

    url
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
    fn test_build_announce_url() {
        let base = Url::parse("http://tracker.example.com:6969/announce").unwrap();
        let req = AnnounceRequest {
            info_hash: [0x01; 20],
            peer_id: PeerId::random(),
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: 1024,
            event: AnnounceEvent::Started,
            compact: true,
            numwant: Some(50),
            key: None,
            trackerid: None,
        };
        let url = build_announce_url(&base, &req);
        assert_eq!(url.host_str().unwrap(), "tracker.example.com");
        assert_eq!(url.port().unwrap(), 6969);
        assert!(url.as_str().contains("info_hash="));
        assert!(url.as_str().contains("&peer_id="));
        assert!(url.as_str().contains("&port=6881"));
        assert!(url.as_str().contains("&compact=1"));
        assert!(url.as_str().contains("&event=started"));
    }

    #[test]
    fn test_new_invalid_url() {
        assert!(HttpTracker::new("not-a-valid-url").is_err());
    }

    #[test]
    fn test_build_announce_url_binary_info_hash() {
        let base = Url::parse("http://tracker.example.com/announce").unwrap();
        let req = AnnounceRequest {
            info_hash: [
                0x00, 0x01, 0x7F, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ],
            peer_id: PeerId::random(),
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: 100,
            event: AnnounceEvent::None,
            compact: false,
            numwant: None,
            key: None,
            trackerid: None,
        };
        let url = build_announce_url(&base, &req);
        // Binary bytes should be percent-encoded by byte_serialize
        assert!(url.as_str().contains("%00%01%7F%FF"));
    }
}

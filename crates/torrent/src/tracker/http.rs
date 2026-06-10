use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::{Error, ErrorKind};
use crate::tracker::{AnnounceEvent, AnnounceRequest, AnnounceResponse};

/// HTTP tracker client (BEP 3).
pub struct HttpTracker {
    announce_url: String,
}

impl HttpTracker {
    /// Create a new HTTP tracker client.
    pub fn new(announce_url: &str) -> Self {
        HttpTracker {
            announce_url: announce_url.to_string(),
        }
    }

    /// Announce to the HTTP tracker.
    pub async fn announce(&self, req: &AnnounceRequest) -> Result<AnnounceResponse, Error> {
        let url = build_announce_url(&self.announce_url, req);
        let addr = parse_host_port(&self.announce_url)?;

        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|e| Error::with_source(ErrorKind::TrackerRequestFailed, e))?;

        // Build HTTP GET request
        let request = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nAccept-Encoding: identity\r\nConnection: close\r\n\r\n",
            get_path_and_query(&url),
            get_host(&self.announce_url)?
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
fn build_announce_url(base: &str, req: &AnnounceRequest) -> String {
    let mut url = base.to_string();

    // Determine separator
    if url.contains('?') {
        url.push('&');
    } else {
        url.push('?');
    }

    url.push_str(&format!("info_hash={}", url_encode_binary(&req.info_hash)));
    url.push_str(&format!("&peer_id={}", url_encode_binary(&req.peer_id.0)));
    url.push_str(&format!("&port={}", req.port));
    url.push_str(&format!("&uploaded={}", req.uploaded));
    url.push_str(&format!("&downloaded={}", req.downloaded));
    url.push_str(&format!("&left={}", req.left));

    if req.compact {
        url.push_str("&compact=1");
    }

    let event_str = match req.event {
        AnnounceEvent::Started => "started",
        AnnounceEvent::Stopped => "stopped",
        AnnounceEvent::Completed => "completed",
        AnnounceEvent::None => "empty",
    };
    url.push_str(&format!("&event={}", event_str));

    if let Some(numwant) = req.numwant {
        url.push_str(&format!("&numwant={}", numwant));
    }
    if let Some(key) = req.key {
        url.push_str(&format!("&key={}", key));
    }
    if let Some(ref trackerid) = req.trackerid {
        url.push_str(&format!("&trackerid={}", trackerid));
    }

    url
}

/// URL-encode arbitrary binary data (percent encoding).
fn url_encode_binary(data: &[u8]) -> String {
    let mut result = String::with_capacity(data.len() * 3);
    for &byte in data {
        // Unreserved characters
        if byte.is_ascii_alphanumeric()
            || byte == b'-'
            || byte == b'_'
            || byte == b'.'
            || byte == b'~'
        {
            result.push(byte as char);
        } else {
            result.push_str(&format!("%{:02X}", byte));
        }
    }
    result
}

/// Get the path and query portion of a URL.
fn get_path_and_query(url: &str) -> String {
    // Find the third '/' (after scheme://)
    let without_scheme = url.split("://").nth(1).unwrap_or(url);
    let idx = without_scheme.find('/').unwrap_or(without_scheme.len());
    without_scheme[idx..].to_string()
}

/// Extract the host header value from a URL.
fn get_host(url: &str) -> Result<String, Error> {
    let without_scheme = url.split("://").nth(1).unwrap_or(url);
    let idx = without_scheme.find('/').unwrap_or(without_scheme.len());
    Ok(without_scheme[..idx].to_string())
}

/// Extract host and port from a URL for TCP connection.
fn parse_host_port(url: &str) -> Result<SocketAddr, Error> {
    let host_str = get_host(url)?;
    // Check for explicit port
    let (host, port) = if let Some(idx) = host_str.rfind(':') {
        let maybe_port = &host_str[idx + 1..];
        if maybe_port.chars().all(|c| c.is_ascii_digit()) {
            let port: u16 = maybe_port
                .parse()
                .map_err(|_| Error::new(ErrorKind::InvalidInput))?;
            (&host_str[..idx], port)
        } else {
            // IPv6 address
            (host_str.as_str(), 80)
        }
    } else {
        (host_str.as_str(), 80)
    };

    // Remove brackets from IPv6
    let host = host.trim_start_matches('[').trim_end_matches(']');

    // Resolve
    let addr = format!("{}:{}", host, port)
        .parse()
        .map_err(|_| Error::new(ErrorKind::InvalidInput))?;

    Ok(addr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::PeerId;

    #[test]
    fn test_build_announce_url() {
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
        let url = build_announce_url("http://tracker.example.com:6969/announce", &req);
        assert!(url.starts_with("http://tracker.example.com:6969/announce?"));
        assert!(url.contains("info_hash="));
        assert!(url.contains("&peer_id="));
        assert!(url.contains("&port=6881"));
        assert!(url.contains("&compact=1"));
        assert!(url.contains("&event=started"));
    }

    #[test]
    fn test_url_encode_binary() {
        // All-zero bytes
        let encoded = url_encode_binary(&[0x00, 0x01, 0x7F, 0xFF]);
        assert_eq!(encoded, "%00%01%7F%FF");
    }

    #[test]
    fn test_url_encode_printable() {
        let encoded = url_encode_binary(b"hello");
        assert_eq!(encoded, "hello");
    }

    #[test]
    fn test_get_host() {
        assert_eq!(
            get_host("http://tracker.example.com:6969/announce").unwrap(),
            "tracker.example.com:6969"
        );
    }
}

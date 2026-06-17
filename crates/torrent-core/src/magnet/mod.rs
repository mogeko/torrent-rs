//! Magnet URI parsing (BEP 9).
//!
//! Magnet URIs identify torrents by their info hash without requiring
//! a `.torrent` file. This module supports both hex (40 chars) and
//! base32 (32 chars) encoded info hashes, plus display name and
//! tracker parameters.
//!
//! # Key Types
//!
//! - [`MagnetUri`] — the parsed URI, implementing `FromStr` and `Display`
//! - [`InfoHash`] — a 20-byte SHA-1 hash with its original encoded form
//!
//! # Examples
//!
//! ```
//! use std::str::FromStr;
//! use torrent_core::magnet::MagnetUri;
//!
//! let uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
//! let magnet = MagnetUri::from_str(uri).unwrap();
//! assert_eq!(magnet.info_hashes.len(), 1);
//! ```

use std::fmt::{self, Write};
use std::str::FromStr;

use crate::error::{Error, ErrorKind};
use crate::metainfo::{Metainfo, Mode};

/// A parsed magnet URI (BEP 9).
///
/// Magnet URIs provide a way to identify torrents by their info hash
/// without requiring a `.torrent` file. The format is:
///
/// ```text
/// magnet:?xt=urn:btih:<info_hash>&dn=<name>&tr=<tracker_url>&...
/// ```
///
/// Both hex (40 characters) and base32 (32 characters) encoded info
/// hashes are supported.
///
/// # Examples
///
/// ```
/// use std::str::FromStr;
/// use torrent_core::magnet::MagnetUri;
///
/// let uri = "magnet:?xt=urn:btih:\
///     0123456789abcdef0123456789abcdef01234567\
///     &dn=ubuntu-24.04\
///     &tr=http://tracker.example.com/announce";
///
/// let magnet = MagnetUri::from_str(uri).unwrap();
/// assert_eq!(magnet.info_hashes.len(), 1);
/// assert_eq!(magnet.display_name.as_deref(), Some("ubuntu-24.04"));
/// assert_eq!(magnet.trackers.len(), 1);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MagnetUri {
    /// Exact Topic — info hashes (at least one required).
    pub info_hashes: Vec<InfoHash>,
    /// Display name (optional, from `dn` parameter).
    pub display_name: Option<String>,
    /// Tracker URLs (from `tr` parameters).
    pub trackers: Vec<String>,
    /// Web seed URLs (from `ws` parameter, BEP 19).
    pub web_seeds: Vec<String>,
    /// Exact Source — SHA-1 hash of the entire file (from `xs` parameter).
    pub exact_source: Option<String>,
    /// Acceptable Source (from `as` parameter).
    pub acceptable_source: Option<String>,
    /// Keyword topic (from `kt` parameter).
    pub keyword_topic: Option<String>,
    /// Manifest topic (from `mt` parameter).
    pub manifest_topic: Option<String>,
    /// Exact length in bytes (from `xl` parameter, BEP 9).
    pub exact_length: Option<u64>,
}

/// An info hash extracted from a magnet URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InfoHash {
    /// The 20-byte SHA-1 hash.
    pub bytes: [u8; 20],
    /// The original encoded form (for round-trip fidelity).
    pub raw: String,
}

impl FromStr for MagnetUri {
    type Err = Error;

    /// Parse a magnet URI string.
    ///
    /// Format: `magnet:?xt=urn:btih:<info_hash>&dn=<name>&tr=<tracker>&...`
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        tracing::debug!("parsing magnet URI: {}", s);
        let s = s.trim();

        // Check prefix (case-insensitive)
        let body = s
            .strip_prefix("magnet:")
            .or_else(|| s.strip_prefix("MAGNET:"))
            .ok_or(Error::new(ErrorKind::InvalidInput))?
            .strip_prefix('?')
            .ok_or(Error::new(ErrorKind::InvalidInput))?;

        let mut info_hashes = Vec::new();
        let mut display_name = None;
        let mut trackers = Vec::new();
        let mut web_seeds = Vec::new();
        let mut exact_source = None;
        let mut acceptable_source = None;
        let mut keyword_topic = None;
        let mut manifest_topic = None;
        let mut exact_length = None;

        for param in body.split('&') {
            if param.is_empty() {
                continue;
            }
            let (key, value) = match param.split_once('=') {
                Some((k, v)) => (k, v),
                None => continue,
            };

            match key {
                "xt" => {
                    if let Some(hash) = parse_xt(value) {
                        info_hashes.push(hash);
                    }
                }
                "dn" => {
                    display_name = Some(url_decode(value));
                }
                "tr" => {
                    trackers.push(url_decode(value));
                }
                "ws" => {
                    web_seeds.push(url_decode(value));
                }
                "xs" => {
                    exact_source = Some(url_decode(value));
                }
                "as" => {
                    acceptable_source = Some(url_decode(value));
                }
                "kt" => {
                    keyword_topic = Some(url_decode(value));
                }
                "mt" => {
                    manifest_topic = Some(url_decode(value));
                }
                "xl" => {
                    exact_length = value.parse::<u64>().ok();
                }
                _ => {
                    // Unknown parameters are ignored per BEP 9
                }
            }
        }

        if info_hashes.is_empty() {
            return Err(Error::new(ErrorKind::InvalidInput));
        }

        Ok(MagnetUri {
            info_hashes,
            display_name,
            trackers,
            web_seeds,
            exact_source,
            acceptable_source,
            keyword_topic,
            manifest_topic,
            exact_length,
        })
    }
}

impl From<&Metainfo> for MagnetUri {
    /// Create a magnet URI from torrent metadata (BEP 9).
    fn from(meta: &Metainfo) -> Self {
        let ih = meta.info_hash();
        MagnetUri {
            info_hashes: vec![InfoHash {
                bytes: ih,
                raw: hex_encode(ih),
            }],
            display_name: Some(match &meta.info.mode {
                Mode::Single { name, .. } | Mode::Multiple { name, .. } => name.clone(),
            }),
            exact_length: Some(meta.info.total_size()),
            trackers: std::iter::once(meta.announce.clone())
                .chain(meta.announce_list.iter().flatten().cloned())
                .collect(),
            web_seeds: Vec::new(),
            exact_source: None,
            acceptable_source: None,
            keyword_topic: None,
            manifest_topic: None,
        }
    }
}

impl fmt::Display for MagnetUri {
    /// Re-serialize to magnet URI format with RFC 3986 percent-encoding.
    ///
    /// Uses the original `raw` form for info hashes to preserve encoding.
    /// String values (`dn`, `tr`, `ws`, `xs`, `as`, `kt`, `mt`) are
    /// percent-encoded so the output is a valid ASCII URI.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "magnet:?")?;

        let mut first = true;

        // xt parameters (info hashes)
        for xt in &self.info_hashes {
            if !first {
                write!(f, "&")?;
            }
            write!(f, "xt=urn:btih:{}", xt.raw)?;
            first = false;
        }

        // dn
        if let Some(ref dn) = self.display_name {
            if !first {
                write!(f, "&")?;
            }
            write!(f, "dn={}", url_encode(dn))?;
            first = false;
        }

        // tr
        for tr in &self.trackers {
            if !first {
                write!(f, "&")?;
            }
            write!(f, "tr={}", url_encode(tr))?;
            first = false;
        }

        // ws
        for ws in &self.web_seeds {
            write!(f, "&ws={}", url_encode(ws))?;
        }

        // xs
        if let Some(ref xs) = self.exact_source {
            write!(f, "&xs={}", url_encode(xs))?;
        }

        // as
        if let Some(ref a) = self.acceptable_source {
            write!(f, "&as={}", url_encode(a))?;
        }

        // kt
        if let Some(ref kt) = self.keyword_topic {
            write!(f, "&kt={}", url_encode(kt))?;
        }

        // mt
        if let Some(ref mt) = self.manifest_topic {
            write!(f, "&mt={}", url_encode(mt))?;
        }

        // xl
        if let Some(xl) = self.exact_length {
            write!(f, "&xl={}", xl)?;
        }

        Ok(())
    }
}

impl MagnetUri {
    /// Return the primary info hash (first `xt` parameter).
    pub fn primary_info_hash(&self) -> &[u8; 20] {
        &self.info_hashes[0].bytes
    }
}

/// Parse an `xt` value of the form `urn:btih:<info_hash>`.
fn parse_xt(value: &str) -> Option<InfoHash> {
    let hash_str = value.strip_prefix("urn:btih:")?;
    let raw = hash_str.to_string();

    let bytes = if hash_str.len() == 40 {
        // Hex-encoded (40 chars → 20 bytes)
        hex_decode(hash_str).ok()
    } else if hash_str.len() == 32 {
        // Base32-encoded (32 chars → 20 bytes)
        base32_decode(hash_str).ok()
    } else {
        return None;
    }?;

    Some(InfoHash { bytes, raw })
}

/// Encode 20 bytes as a hex string.
#[doc(hidden)]
pub fn hex_encode(bytes: [u8; 20]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Decode a hex string to 20 bytes.
fn hex_decode(s: &str) -> Result<[u8; 20], Error> {
    if s.len() != 40 {
        return Err(Error::new(ErrorKind::InvalidInput));
    }
    let mut out = [0u8; 20];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hi = hex_val(chunk[0])?;
        let lo = hex_val(chunk[1])?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

fn hex_val(b: u8) -> Result<u8, Error> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(Error::new(ErrorKind::InvalidInput)),
    }
}

/// Decode a base32 (RFC 4648) string to 20 bytes.
fn base32_decode(s: &str) -> Result<[u8; 20], Error> {
    if s.len() != 32 {
        return Err(Error::new(ErrorKind::InvalidInput));
    }
    let mut out = [0u8; 20];
    let bytes = s.as_bytes();

    // RFC 4648 base32: 32 characters → 20 bytes
    // Process in 4 groups of 8 characters, each producing 5 bytes
    for chunk_idx in 0..4 {
        let offset = chunk_idx * 8;
        let mut buf: u64 = 0;
        for j in 0..8 {
            let c = bytes[offset + j];
            let val = base32_val(c)?;
            buf = (buf << 5) | val as u64;
        }
        // Extract 5 bytes from the 40-bit buffer
        let dst = chunk_idx * 5;
        out[dst] = ((buf >> 32) & 0xFF) as u8;
        out[dst + 1] = ((buf >> 24) & 0xFF) as u8;
        out[dst + 2] = ((buf >> 16) & 0xFF) as u8;
        out[dst + 3] = ((buf >> 8) & 0xFF) as u8;
        out[dst + 4] = (buf & 0xFF) as u8;
    }
    Ok(out)
}

fn base32_val(c: u8) -> Result<u8, Error> {
    match c {
        b'A'..=b'Z' => Ok(c - b'A'),
        b'a'..=b'z' => Ok(c - b'a'),
        b'2'..=b'7' => Ok(c - b'2' + 26),
        _ => Err(Error::new(ErrorKind::InvalidInput)),
    }
}

/// URL percent-decoding with proper UTF-8 handling.
///
/// Accumulates percent-decoded bytes, then decodes as UTF-8
/// (replacing invalid sequences with U+FFFD).
fn url_decode(s: &str) -> String {
    let mut buf = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Ok(hi), Ok(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2]))
        {
            buf.push((hi << 4) | lo);
            i += 3;
            continue;
        }
        buf.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&buf).into_owned()
}

/// URL percent-encoding per RFC 3986.
///
/// Encodes all bytes outside the unreserved set (`A-Za-z0-9-._~`).
fn url_encode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                result.push(*b as char);
            }
            _ => {
                write!(result, "%{:02X}", b).unwrap();
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_decode_valid() {
        let result = hex_decode("0123456789abcdef0123456789abcdef01234567").unwrap();
        assert_eq!(result[0], 0x01);
        assert_eq!(result[1], 0x23);
        assert_eq!(result[19], 0x67);
    }

    #[test]
    fn hex_decode_invalid_length() {
        assert!(hex_decode("abc").is_err());
    }

    #[test]
    fn base32_decode_valid() {
        let result = base32_decode("64wsmv3zsbx5fve2sn5zxdq5w22lfpxy").unwrap();
        assert_eq!(result.len(), 20);
    }

    #[test]
    fn base32_decode_invalid_length() {
        assert!(base32_decode("abc").is_err());
    }

    #[test]
    fn url_decode_percent() {
        assert_eq!(url_decode("hello%20world"), "hello world");
    }

    #[test]
    fn url_decode_no_encoding() {
        assert_eq!(url_decode("hello world"), "hello world");
    }

    #[test]
    fn parse_xl_parameter() {
        use std::str::FromStr;
        let uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&xl=1024";
        let magnet = MagnetUri::from_str(uri).unwrap();
        assert_eq!(magnet.exact_length, Some(1024));
    }

    #[test]
    fn metainfo_to_magnet() {
        use crate::metainfo::{Bytes, Info, Metainfo, Mode, RawInfo};

        let info = Info {
            piece_length: 262144,
            pieces: vec![[0u8; 20]],
            mode: Mode::Single {
                name: "test.txt".into(),
                length: 1024,
            },
            raw_info: RawInfo::Bytes(Bytes::from_static(b"d4:infod...e")),
        };
        let meta = Metainfo {
            announce: "http://tracker.example.com/announce".into(),
            announce_list: vec![],
            info,
            creation_date: None,
            comment: None,
            created_by: None,
            encoding: None,
        };

        let magnet = MagnetUri::from(&meta);
        assert_eq!(magnet.info_hashes.len(), 1);
        assert_eq!(magnet.display_name.as_deref(), Some("test.txt"));
        assert_eq!(magnet.exact_length, Some(1024));
        assert_eq!(magnet.trackers.len(), 1);
        assert_eq!(magnet.trackers[0], "http://tracker.example.com/announce");
    }

    // --- Additional parameter tests ---

    #[test]
    fn parse_magnet_ws() {
        let uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567\
            &ws=http://example.com/file";
        let magnet = MagnetUri::from_str(uri).unwrap();
        assert_eq!(magnet.web_seeds, vec!["http://example.com/file"]);
    }

    #[test]
    fn parse_magnet_xs() {
        let uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567\
            &xs=urn:sha1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let magnet = MagnetUri::from_str(uri).unwrap();
        assert_eq!(
            magnet.exact_source.as_deref(),
            Some("urn:sha1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        );
    }

    #[test]
    fn parse_magnet_as() {
        let uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567\
            &as=http://alt.example.com/file";
        let magnet = MagnetUri::from_str(uri).unwrap();
        assert_eq!(
            magnet.acceptable_source.as_deref(),
            Some("http://alt.example.com/file")
        );
    }

    #[test]
    fn parse_magnet_kt_mt() {
        let uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567\
            &kt=keyword1+keyword2&mt=http://manifest.example.com";
        let magnet = MagnetUri::from_str(uri).unwrap();
        assert_eq!(magnet.keyword_topic.as_deref(), Some("keyword1+keyword2"));
        assert_eq!(
            magnet.manifest_topic.as_deref(),
            Some("http://manifest.example.com")
        );
    }

    #[test]
    fn parse_magnet_all_params() {
        let uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567\
            &dn=Test+File\
            &tr=http://t1.com/ann\
            &tr=http://t2.com/ann\
            &ws=http://webseed.example.com/data\
            &xs=urn:sha1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\
            &as=http://alt.example.com/data\
            &kt=test+keyword\
            &mt=http://manifest.example.com\
            &xl=4096";
        let magnet = MagnetUri::from_str(uri).unwrap();
        assert_eq!(magnet.info_hashes.len(), 1);
        assert_eq!(magnet.display_name.as_deref(), Some("Test+File"));
        assert_eq!(magnet.trackers.len(), 2);
        assert_eq!(magnet.web_seeds.len(), 1);
        assert!(magnet.exact_source.is_some());
        assert!(magnet.acceptable_source.is_some());
        assert!(magnet.keyword_topic.is_some());
        assert!(magnet.manifest_topic.is_some());
        assert_eq!(magnet.exact_length, Some(4096));
    }

    #[test]
    fn display_all_params() {
        let uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567\
            &dn=Test%20File\
            &tr=http%3A%2F%2Ft.com%2Fann\
            &ws=http%3A%2F%2Fweb.example.com\
            &xs=urn%3Asha1%3AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\
            &as=http%3A%2F%2Falt.example.com\
            &kt=k\
            &mt=http%3A%2F%2Fm.example.com\
            &xl=2048";
        let magnet = MagnetUri::from_str(uri).unwrap();
        let displayed = magnet.to_string();
        // All values should be percent-encoded
        assert!(displayed.contains("dn=Test%20File"));
        assert!(displayed.contains("tr=http%3A%2F%2Ft.com%2Fann"));
        assert!(displayed.contains("ws=http%3A%2F%2Fweb.example.com"));
        // xt hash is raw hex, not escaped (no % chars to escape)
        assert!(displayed.contains("xt=urn:btih:0123456789abcdef0123456789abcdef01234567"));
        assert!(displayed.contains("xl=2048"));
    }

    #[test]
    fn roundtrip_percent_encoded() {
        // A URI with special characters that need encoding
        let uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567\
            &dn=Test%20File%21\
            &tr=http%3A%2F%2Ft.com%3A8080%2Fann%3Fkey%3Dval";
        let magnet = MagnetUri::from_str(uri).unwrap();
        // Round-trip: the structure should be identical
        let magnet2 = magnet.to_string().parse::<MagnetUri>().unwrap();
        assert_eq!(magnet, magnet2);
    }

    #[test]
    fn roundtrip_unicode_dn() {
        // Unicode in dn — survives round-trip via percent-encoding
        let uri = "magnet:?xt=urn:btih:cccccccccccccccccccccccccccccccccccccccc\
            &dn=%E2%98%83%20snowman"; // ☃ snowman
        let magnet = MagnetUri::from_str(uri).unwrap();
        let encoded = magnet.to_string();
        // Should be re-encoded as ASCII
        assert!(encoded.is_ascii());
        assert!(encoded.contains("dn=%E2%98%83%20snowman"));
        // Values must survive the trip
        let magnet2 = encoded.parse::<MagnetUri>().unwrap();
        assert_eq!(magnet, magnet2);
    }

    // --- Malformed xt ---

    #[test]
    fn reject_xt_wrong_prefix() {
        let uri = "magnet:?xt=urn:sha1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        assert!(MagnetUri::from_str(uri).is_err());
    }

    #[test]
    fn reject_xt_hex_wrong_length() {
        // 39 chars (must be exactly 40)
        let uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef0123456";
        assert!(MagnetUri::from_str(uri).is_err());
    }

    #[test]
    fn reject_xt_base32_wrong_length() {
        // 31 chars (must be exactly 32)
        let uri = "magnet:?xt=urn:btih:64wsmv3zsbx5fve2sn5zxdq5w22lfpx";
        assert!(MagnetUri::from_str(uri).is_err());
    }

    #[test]
    fn reject_xt_invalid_length() {
        // completely wrong length
        let uri = "magnet:?xt=urn:btih:short";
        assert!(MagnetUri::from_str(uri).is_err());
    }

    // --- URL decode edge cases ---

    #[test]
    fn url_decode_multiple_percents() {
        assert_eq!(url_decode("hello%20world%21"), "hello world!");
    }

    #[test]
    fn url_decode_incomplete_percent() {
        // solitary % at end should be left as-is
        assert_eq!(url_decode("hello%"), "hello%");
    }

    #[test]
    fn url_decode_truncated_percent() {
        // %2 at end (only 1 hex digit) should be left as-is
        assert_eq!(url_decode("hello%2"), "hello%2");
    }

    #[test]
    fn url_decode_invalid_hex() {
        // %ZZ is not valid hex
        assert_eq!(url_decode("hello%ZZworld"), "hello%ZZworld");
    }

    #[test]
    fn url_decode_partial_hex() {
        // %2g — only first char is valid hex
        assert_eq!(url_decode("hello%2gworld"), "hello%2gworld");
    }

    // --- primary_info_hash ---

    #[test]
    fn primary_info_hash_returns_first() {
        let uri = "magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\
            &xt=urn:btih:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let magnet = MagnetUri::from_str(uri).unwrap();
        let primary = magnet.primary_info_hash();
        assert_eq!(
            primary,
            &[
                0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
            ]
        );
    }

    #[test]
    fn magnet_with_percent_encoded_dn() {
        let uri = "magnet:?xt=urn:btih:cccccccccccccccccccccccccccccccccccccccc\
            &dn=%E2%98%83%20snowman"; // ☃ snowman
        let magnet = MagnetUri::from_str(uri).unwrap();
        let name = magnet.display_name.unwrap();
        assert_eq!(name, "\u{2603} snowman");
    }
}

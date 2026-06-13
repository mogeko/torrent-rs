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

use std::fmt;
use std::str::FromStr;

use crate::error::{Error, ErrorKind};

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
        })
    }
}

impl fmt::Display for MagnetUri {
    /// Re-serialize to magnet URI format.
    ///
    /// Uses the original `raw` form for info hashes to preserve encoding.
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
            write!(f, "dn={}", dn)?;
            first = false;
        }

        // tr
        for tr in &self.trackers {
            if !first {
                write!(f, "&")?;
            }
            write!(f, "tr={}", tr)?;
            first = false;
        }

        // ws
        for ws in &self.web_seeds {
            if !first {
                write!(f, "&")?;
            }
            write!(f, "ws={}", ws)?;
            first = false;
        }

        // xs
        if let Some(ref xs) = self.exact_source {
            if !first {
                write!(f, "&")?;
            }
            write!(f, "xs={}", xs)?;
            first = false;
        }

        // as
        if let Some(ref a) = self.acceptable_source {
            if !first {
                write!(f, "&")?;
            }
            write!(f, "as={}", a)?;
            first = false;
        }

        // kt
        if let Some(ref kt) = self.keyword_topic {
            if !first {
                write!(f, "&")?;
            }
            write!(f, "kt={}", kt)?;
            first = false;
        }

        // mt
        if let Some(ref mt) = self.manifest_topic {
            if !first {
                write!(f, "&")?;
            }
            write!(f, "mt={}", mt)?;
        }

        Ok(())
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

/// Simple URL percent-decoding.
fn url_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Ok(hi), Ok(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2]))
        {
            result.push((hi << 4 | lo) as char);
            i += 3;
            continue;
        }
        result.push(bytes[i] as char);
        i += 1;
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
}

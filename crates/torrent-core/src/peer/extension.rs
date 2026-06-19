use std::collections::HashMap;

use crate::bencode::{Bencode, Bytes, dict_get, dict_get_bytes, dict_get_int};
use crate::error::{Error, ErrorKind};

/// LTEP (LibTorrent Extension Protocol) extension negotiation handshake (BEP 10).
///
/// After the BEP 3 TCP handshake, if both peers set bit 63 in their reserved
/// bytes, they exchange [`ExtensionNegotiation`] messages to agree on which
/// extensions are supported and their assigned message IDs.
///
/// The negotiation is sent as a `PeerMessage::Extended { ext_id: 0, data }`
/// where `data` is the bencoded form of this structure.
///
/// # Wire format (bencoded)
///
/// ```text
/// d
///   1:m d
///     6:ut_pex i1e
///     9:ut_metadata i2e
///   e
///   1:v 14:torrent.rs 0.1
///   6:yourip 4:\x7f\x00\x00\x01
/// e
/// ```
///
/// # Examples
///
/// ```
/// use torrent_core::peer::ExtensionNegotiation;
///
/// let mut neg = ExtensionNegotiation::new();
/// neg.add_extension("ut_pex", 1);
/// let ben = neg.to_bencode();
/// let parsed = ExtensionNegotiation::from_bencode(&ben).unwrap();
/// assert_eq!(parsed.m.get("ut_pex"), Some(&1u8));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionNegotiation {
    /// Extension name → assigned message ID mapping.
    ///
    /// Required. Extension ID 0 is reserved for the handshake itself
    /// and must not appear in this map.
    pub m: HashMap<String, u8>,
    /// Client name and version string (optional, e.g. `"torrent.rs 0.1.0"`).
    pub v: Option<String>,
    /// Your IP address as seen by the remote peer (optional, informational).
    pub yourip: Option<String>,
    /// Metadata size in bytes (optional, for BEP 9 metadata exchange).
    pub metadata_size: Option<i64>,
    /// Maximum number of outstanding requests (optional, BEP 10 reqq).
    pub reqq: Option<i64>,
}

impl ExtensionNegotiation {
    /// Create an empty extension negotiation with no extensions registered.
    pub fn new() -> Self {
        ExtensionNegotiation {
            m: HashMap::new(),
            v: None,
            yourip: None,
            metadata_size: None,
            reqq: None,
        }
    }

    /// Register an extension with the given message ID.
    ///
    /// Extension ID 0 means disabled per BEP 10; use IDs ≥ 1 for enabled
    /// extensions.
    pub fn add_extension(&mut self, name: impl Into<String>, id: u8) {
        debug_assert!(
            id != 0,
            "extension ID 0 means disabled per BEP 10; use IDs >= 1 for enabled extensions"
        );
        self.m.insert(name.into(), id);
    }

    /// Serialize to a bencoded dictionary.
    ///
    /// Keys are sorted lexicographically per BEP 3. Optional fields
    /// are omitted when `None`.
    pub fn to_bencode(&self) -> Bencode {
        let mut entries: Vec<(Bytes, Bencode)> = Vec::with_capacity(5);

        // "m" — required: extension name → id mapping
        let mut m_entries: Vec<(Bytes, Bencode)> = self
            .m
            .iter()
            .map(|(name, &id)| {
                (
                    Bytes::copy_from_slice(name.as_bytes()),
                    Bencode::Integer(i64::from(id)),
                )
            })
            .collect();
        m_entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        entries.push((Bytes::from("m"), Bencode::Dict(m_entries)));

        if let Some(ref v) = self.v {
            entries.push((
                Bytes::from("v"),
                Bencode::Bytes(Bytes::copy_from_slice(v.as_bytes())),
            ));
        }

        if let Some(ref ip) = self.yourip {
            entries.push((
                Bytes::from("yourip"),
                Bencode::Bytes(Bytes::copy_from_slice(ip.as_bytes())),
            ));
        }

        if let Some(size) = self.metadata_size {
            entries.push((Bytes::from("metadata_size"), Bencode::Integer(size)));
        }

        if let Some(r) = self.reqq {
            entries.push((Bytes::from("reqq"), Bencode::Integer(r)));
        }

        entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        Bencode::Dict(entries)
    }

    /// Deserialize from a bencoded dictionary.
    ///
    /// Returns an error if `val` is not a dictionary or the required
    /// `"m"` field is missing or malformed.
    pub fn from_bencode(val: &Bencode) -> Result<Self, Error> {
        // Verify this is a dictionary
        if !matches!(val, Bencode::Dict(_)) {
            return Err(Error::new(ErrorKind::PeerInvalidExtendedMessage));
        }

        // "m" — required sub-dict
        let m: HashMap<String, u8> = match dict_get(val, b"m") {
            Some(Bencode::Dict(m_entries)) => {
                let mut map = HashMap::with_capacity(m_entries.len());
                for (key, value) in m_entries {
                    let name = String::from_utf8(key.to_vec())
                        .map_err(|_| Error::new(ErrorKind::PeerInvalidExtendedMessage))?;
                    let id = match value {
                        Bencode::Integer(i) => u8::try_from(*i)
                            .map_err(|_| Error::new(ErrorKind::PeerInvalidExtendedMessage))?,
                        _ => return Err(Error::new(ErrorKind::PeerInvalidExtendedMessage)),
                    };
                    // BEP 10: ID 0 means the extension is disabled/not supported
                    if id == 0 {
                        continue;
                    }
                    map.insert(name, id);
                }
                map
            }
            Some(_) => return Err(Error::new(ErrorKind::PeerInvalidExtendedMessage)),
            None => return Err(Error::new(ErrorKind::PeerInvalidExtendedMessage)),
        };

        let v = dict_get_bytes(val, b"v").and_then(|b| String::from_utf8(b.to_vec()).ok());

        let yourip =
            dict_get_bytes(val, b"yourip").and_then(|b| String::from_utf8(b.to_vec()).ok());

        let metadata_size = dict_get_int(val, b"metadata_size");

        let reqq = dict_get_int(val, b"reqq");

        Ok(ExtensionNegotiation {
            m,
            v,
            yourip,
            metadata_size,
            reqq,
        })
    }
}

impl Default for ExtensionNegotiation {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bencode::encode;

    #[test]
    fn to_bencode_empty() {
        let neg = ExtensionNegotiation::new();
        let ben = neg.to_bencode();
        let encoded = encode(&ben);
        // {"m":{}}
        assert_eq!(encoded, b"d1:mdee");
    }

    #[test]
    fn to_bencode_with_pex() {
        let mut neg = ExtensionNegotiation::new();
        neg.add_extension("ut_pex", 1);
        let ben = neg.to_bencode();
        // Parse back and check
        let parsed = ExtensionNegotiation::from_bencode(&ben).unwrap();
        assert_eq!(parsed.m.get("ut_pex"), Some(&1u8));
    }

    #[test]
    fn to_bencode_full() {
        let mut neg = ExtensionNegotiation::new();
        neg.add_extension("ut_pex", 1);
        neg.v = Some("torrent.rs 0.1.0".into());
        let ben = neg.to_bencode();
        let parsed = ExtensionNegotiation::from_bencode(&ben).unwrap();
        assert_eq!(parsed.m.get("ut_pex"), Some(&1u8));
        assert_eq!(parsed.v.as_deref(), Some("torrent.rs 0.1.0"));
    }

    #[test]
    fn to_bencode_with_multiple_extensions() {
        let mut neg = ExtensionNegotiation::new();
        neg.add_extension("ut_pex", 1);
        neg.add_extension("ut_metadata", 2);
        neg.reqq = Some(512);
        let ben = neg.to_bencode();
        let parsed = ExtensionNegotiation::from_bencode(&ben).unwrap();
        assert_eq!(parsed.m.get("ut_pex"), Some(&1u8));
        assert_eq!(parsed.m.get("ut_metadata"), Some(&2u8));
        assert_eq!(parsed.reqq, Some(512));
    }

    #[test]
    fn from_bencode_empty_m() {
        // {"m":{}}
        let (val, _) = crate::bencode::decode(b"d1:mdee").unwrap();
        let neg = ExtensionNegotiation::from_bencode(&val).unwrap();
        assert!(neg.m.is_empty());
    }

    #[test]
    fn from_bencode_missing_m() {
        // {"v":"foo"} — no "m" key
        let (val, _) = crate::bencode::decode(b"d1:v3:fooe").unwrap();
        assert!(ExtensionNegotiation::from_bencode(&val).is_err());
    }

    #[test]
    fn from_bencode_with_pex() {
        let mut neg = ExtensionNegotiation::new();
        neg.add_extension("ut_pex", 1);
        neg.v = Some("test".into());
        let ben = neg.to_bencode();
        let parsed = ExtensionNegotiation::from_bencode(&ben).unwrap();
        assert_eq!(parsed.m.get("ut_pex"), Some(&1u8));
        assert_eq!(parsed.v.as_deref(), Some("test"));
    }

    #[test]
    fn from_bencode_not_a_dict() {
        let val = Bencode::Integer(42);
        assert!(ExtensionNegotiation::from_bencode(&val).is_err());
    }

    #[test]
    fn from_bencode_m_not_a_dict() {
        // {"m":42}
        let (val, _) = crate::bencode::decode(b"d1:mi42ee").unwrap();
        assert!(ExtensionNegotiation::from_bencode(&val).is_err());
    }

    #[test]
    fn roundtrip() {
        let mut neg = ExtensionNegotiation::new();
        neg.add_extension("ut_pex", 1);
        neg.add_extension("ut_metadata", 3);
        neg.v = Some("torrent.rs 0.1.0".into());
        neg.yourip = Some("127.0.0.1".into());
        neg.metadata_size = Some(0);
        neg.reqq = Some(512);
        let ben = neg.to_bencode();
        let parsed = ExtensionNegotiation::from_bencode(&ben).unwrap();
        assert_eq!(neg, parsed);
    }
}

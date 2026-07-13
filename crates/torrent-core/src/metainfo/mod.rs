//! .torrent file parsing and metadata (BEP 3, 12, 52).
//!
//! The metainfo module parses `.torrent` files into structured data,
//! supporting both single-file and multi-file modes. The `info_hash`
//! is computed as `SHA-1(bencoded_info_dict)`, serving as the
//! torrent's unique identifier.
//!
//! # Key Types
//!
//! - [`Metainfo`] — the top-level parsed torrent
//! - [`Info`] — the `info` dictionary with piece hashes and file layout
//! - [`Mode`] — single-file vs multi-file layout
//! - [`FileInfo`] — per-file metadata in multi-file mode
//! - [`from_bytes`] — parse raw bencoded `.torrent` data
//!
//! # Examples
//!
//! ```no_run
//! use torrent_core::metainfo::from_bytes;
//!
//! let data = std::fs::read("debian.torrent").unwrap();
//! let meta = from_bytes(&data).unwrap();
//! println!("Info hash: {:x?}", meta.info_hash());
//! println!("Pieces: {}", meta.info.num_pieces());
//! ```

pub mod builder;
mod parse;

// Re-export Bytes so callers can construct `RawInfo::Bytes(...)` without
// adding `bytes` as a direct dependency.
pub use crate::bencode::Bytes;

pub use self::builder::MetainfoBuilder;

use sha1::{Digest, Sha1};

use crate::bencode::{self, Bencode};
use crate::error::Error;

/// Represents a parsed `.torrent` file (BEP 3).
///
/// A `Metainfo` is the result of parsing a `.torrent` file's bencoded content
/// via [`from_bytes`]. It contains tracker URLs, file metadata, and
/// the raw info dict bytes needed for computing the torrent's
/// unique [`info_hash`](Metainfo::info_hash).
///
/// # Examples
///
/// ```no_run
/// use torrent_core::metainfo::from_bytes;
///
/// let data = std::fs::read("debian.torrent").unwrap();
/// let meta = from_bytes(&data).unwrap();
/// println!("Info hash: {:x?}", meta.info_hash());
/// println!("Announce: {}", meta.announce);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Metainfo {
    /// The URL of the tracker.
    pub announce: String,
    /// Alternative tracker tiers (BEP 12).
    pub announce_list: Vec<Vec<String>>,
    /// The info dictionary containing file metadata.
    pub info: Info,
    /// Web seed URLs from the `url-list` key (BEP 19).
    ///
    /// Each URL points to a standard HTTP/FTP server hosting the
    /// torrent's files. URLs ending with `/` are directory URLs —
    /// the client appends the file path. Explicit file URLs are
    /// used as-is.
    pub url_list: Vec<String>,
    /// HTTP seed URLs from the `httpseeds` key (BEP 17, Draft).
    ///
    /// These require a server-side script. Parsed for forward
    /// compatibility but not yet used for download.
    pub httpseeds: Vec<String>,
    /// Unix timestamp of creation (optional).
    pub creation_date: Option<i64>,
    /// Free-form comment (optional).
    pub comment: Option<String>,
    /// Name of the program used to create the torrent (optional).
    pub created_by: Option<String>,
    /// Encoding used for string values (optional, historic).
    pub encoding: Option<String>,
}

/// Either the raw bencoded `info` dict bytes or a pre-computed hash.
///
/// From a `.torrent` file we get the full bytes (`Bytes`), and `info_hash()`
/// computes SHA-1 from them. From a magnet URI we only have the hash itself
/// (`Hash`) — see BEP 9.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RawInfo {
    /// Full bencoded info dict bytes (from a `.torrent` file).
    Bytes(Bytes),
    /// Pre-computed 20-byte info hash (from a magnet URI, no raw bytes).
    Hash([u8; 20]),
}

/// The `info` dictionary from a `.torrent` file.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Info {
    /// Length of each piece in bytes.
    pub piece_length: u64,
    /// Concatenated SHA-1 hashes, one 20-byte hash per piece.
    pub pieces: Vec<[u8; 20]>,
    /// Whether this is a single-file or multi-file torrent.
    pub mode: Mode,
    /// Raw info dict bytes or pre-computed hash (from magnet URI).
    pub raw_info: RawInfo,
}

/// File layout mode for a torrent.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Mode {
    /// Single file mode: just one file.
    Single {
        /// The name of the file.
        name: String,
        /// The length of the file in bytes.
        length: u64,
    },
    /// Multi-file mode: multiple files under a root directory.
    Multiple {
        /// The name of the root directory.
        name: String,
        /// The list of files.
        files: Vec<FileInfo>,
    },
}

/// A single file entry in a multi-file torrent (BEP 52).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FileInfo {
    /// File length in bytes.
    pub length: u64,
    /// Path components (e.g., `["dir", "subdir", "file.txt"]`).
    pub path: Vec<String>,
}

impl Metainfo {
    /// Calculate the 20-byte info hash (SHA-1 of the bencoded info dict).
    ///
    /// This is the torrent's unique identifier used in tracker requests,
    /// DHT lookups, and magnet links.
    ///
    /// If [`RawInfo::Hash`] is stored (e.g. from a magnet URI), that
    /// value is returned directly instead of computing SHA-1.
    pub fn info_hash(&self) -> [u8; 20] {
        match &self.info.raw_info {
            RawInfo::Hash(h) => *h,
            RawInfo::Bytes(raw) => {
                let mut hasher = Sha1::new();
                hasher.update(raw);
                hasher.finalize().into()
            }
        }
    }

    /// Serialize back to bencoded bytes (the `.torrent` file format).
    ///
    /// Returns `None` if this is a magnet-URI stub (`RawInfo::Hash`)
    /// whose raw info dict bytes are not available.
    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        let raw_info = match &self.info.raw_info {
            RawInfo::Bytes(raw) => raw.clone(),
            RawInfo::Hash(_) => return None,
        };

        let mut entries: Vec<(Bytes, Bencode)> = Vec::new();

        // announce
        entries.push((
            Bytes::from("announce"),
            Bencode::Bytes(Bytes::copy_from_slice(self.announce.as_bytes())),
        ));

        // announce-list (BEP 12) — only if non-empty
        if !self.announce_list.is_empty() {
            let tiers: Vec<Bencode> = self
                .announce_list
                .iter()
                .map(|tier| {
                    Bencode::List(
                        tier.iter()
                            .map(|url| Bencode::Bytes(Bytes::copy_from_slice(url.as_bytes())))
                            .collect(),
                    )
                })
                .collect();
            entries.push((Bytes::from("announce-list"), Bencode::List(tiers)));
        }

        // Optional fields
        if let Some(date) = self.creation_date {
            entries.push((Bytes::from("creation date"), Bencode::Integer(date)));
        }
        if let Some(ref c) = self.comment {
            entries.push((
                Bytes::from("comment"),
                Bencode::Bytes(Bytes::copy_from_slice(c.as_bytes())),
            ));
        }
        if let Some(ref cb) = self.created_by {
            entries.push((
                Bytes::from("created by"),
                Bencode::Bytes(Bytes::copy_from_slice(cb.as_bytes())),
            ));
        }
        if let Some(ref enc) = self.encoding {
            entries.push((
                Bytes::from("encoding"),
                Bencode::Bytes(Bytes::copy_from_slice(enc.as_bytes())),
            ));
        }

        // Build the outer dict manually so we can splice in the raw info
        // dict bytes verbatim (they are already bencoded).
        let mut out: Vec<u8> = Vec::new();
        out.push(b'd');

        // Sort keys per BEP 3
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        for (key, val) in entries {
            out.extend_from_slice(&bencode::encode(&Bencode::Bytes(key)));
            out.extend_from_slice(&bencode::encode(&val));
        }

        // Insert info dict raw bytes verbatim
        let info_key = bencode::encode(&Bencode::Bytes(Bytes::from("info")));
        out.extend_from_slice(&info_key);
        out.extend_from_slice(&raw_info);

        out.push(b'e');
        Some(out)
    }
}

impl Info {
    /// Replace the raw info bytes (e.g. after a BEP 9 metadata exchange).
    ///
    /// Only valid when `raw_info` is currently [`RawInfo::Hash`].
    /// The caller MUST verify that `SHA-1(raw)` matches the cached hash.
    pub fn set_raw_bytes(&mut self, raw: Bytes) {
        self.raw_info = RawInfo::Bytes(raw);
    }

    /// Returns the total size of all files in bytes.
    ///
    /// For single-file torrents, this is the file length.
    /// For multi-file torrents, this is the sum of all file lengths.
    pub fn total_size(&self) -> u64 {
        match &self.mode {
            Mode::Single { length, .. } => *length,
            Mode::Multiple { files, .. } => files.iter().map(|f| f.length).sum(),
        }
    }

    /// Returns the number of pieces.
    ///
    /// Each piece corresponds to a 20-byte SHA-1 hash in the `pieces` field.
    pub fn num_pieces(&self) -> usize {
        self.pieces.len()
    }

    /// Map each file to its byte offset in the torrent byte stream.
    ///
    /// For single-file torrents, returns a single [`FileOffset`] at offset 0.
    /// For multi-file torrents, each file's offset is the sum of previous
    /// file lengths.
    ///
    /// Used by web seed (BEP 19) to construct per-file HTTP URLs for
    /// directory-style web seed URLs.
    pub fn file_offsets(&self) -> Vec<FileOffset> {
        match &self.mode {
            Mode::Single { name, length } => {
                vec![FileOffset {
                    offset: 0,
                    length: *length,
                    path: vec![name.clone()],
                }]
            }
            Mode::Multiple { files, .. } => {
                let mut offset = 0u64;
                files
                    .iter()
                    .map(|f| {
                        let fo = FileOffset {
                            offset,
                            length: f.length,
                            path: f.path.clone(),
                        };
                        offset += f.length;
                        fo
                    })
                    .collect()
            }
        }
    }
}

/// A file's position and identity within a torrent's byte stream.
///
/// Used by web seed (BEP 19) to map byte ranges to HTTP request URLs.
/// For single-file torrents, there is one entry at offset 0 with the
/// file's name. For multi-file torrents, each entry represents a file
/// in the torrent with its byte offset and path components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileOffset {
    /// Byte offset of this file in the torrent byte stream.
    pub offset: u64,
    /// Length of this file in bytes.
    pub length: u64,
    /// Path components (e.g. `["dir", "file.txt"]`).
    pub path: Vec<String>,
}

/// Try to parse [`Metainfo`] from raw bencoded bytes (BEP 3).
///
/// This is the standard `TryFrom` entry point. A convenience wrapper
/// [`from_bytes`] is also provided for call sites where a free function
/// reads more naturally.
///
/// # Errors
///
/// Returns [`Error`] if the data is not valid bencode or if required
/// metainfo fields are missing or invalid.
impl TryFrom<&[u8]> for Metainfo {
    type Error = Error;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        self::parse::from_bytes(data)
    }
}

/// Parse [`Metainfo`] from a borrowed byte array of any size.
impl<const N: usize> TryFrom<&[u8; N]> for Metainfo {
    type Error = Error;

    fn try_from(data: &[u8; N]) -> Result<Self, Self::Error> {
        self::parse::from_bytes(data)
    }
}

/// Parse [`Metainfo`] from a shared reference to a byte vector.
///
/// This complements [`TryFrom<&[u8]>`] so that callers can write
/// `Metainfo::try_from(&vec)` without manually slicing.
impl TryFrom<&Vec<u8>> for Metainfo {
    type Error = Error;

    fn try_from(data: &Vec<u8>) -> Result<Self, Self::Error> {
        self::parse::from_bytes(data.as_slice())
    }
}

/// Parse [`Metainfo`] from an owned byte vector.
///
/// This allows `data.try_into()` where `data: Vec<u8>`.
impl TryFrom<Vec<u8>> for Metainfo {
    type Error = Error;

    fn try_from(data: Vec<u8>) -> Result<Self, Self::Error> {
        self::parse::from_bytes(&data)
    }
}

/// Parse a `Metainfo` from raw bencoded bytes (the contents of a `.torrent` file).
///
/// This is a convenience wrapper around [`Metainfo::try_from`].
///
/// # Examples
///
/// ```no_run
/// use torrent_core::metainfo::from_bytes;
///
/// let data = std::fs::read("debian.torrent").unwrap();
/// let meta = from_bytes(&data).unwrap();
/// println!("Info hash: {:x?}", meta.info_hash());
/// ```
pub fn from_bytes(data: &[u8]) -> Result<Metainfo, Error> {
    data.try_into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_single() -> (Metainfo, Vec<u8>) {
        let info = Bencode::Dict(vec![
            (Bytes::from("name"), Bencode::Bytes(Bytes::from("f.txt"))),
            (Bytes::from("piece length"), Bencode::Integer(16)),
            (Bytes::from("length"), Bencode::Integer(32)),
            (
                Bytes::from("pieces"),
                Bencode::Bytes(Bytes::from(vec![0u8; 20])),
            ),
        ]);
        let root = Bencode::Dict(vec![
            (
                Bytes::from("announce"),
                Bencode::Bytes(Bytes::from("http://t.com/a")),
            ),
            (Bytes::from("info"), info),
        ]);
        let data = bencode::encode(&root);
        let meta = Metainfo::try_from(&data).unwrap();
        (meta, data)
    }

    #[test]
    fn to_bytes_roundtrip() {
        let (meta, original) = make_single();
        let re_encoded = meta.to_bytes().expect("should have raw bytes");
        assert_eq!(re_encoded, original);
    }

    #[test]
    fn to_bytes_magnet_stub_returns_none() {
        let meta = Metainfo {
            announce: String::new(),
            announce_list: vec![],
            info: Info {
                piece_length: 0,
                pieces: vec![],
                mode: Mode::Single {
                    name: "stub".into(),
                    length: 0,
                },
                raw_info: RawInfo::Hash([0u8; 20]),
            },
            url_list: vec![],
            httpseeds: vec![],
            creation_date: None,
            comment: None,
            created_by: None,
            encoding: None,
        };
        assert!(meta.to_bytes().is_none());
    }

    #[test]
    fn set_raw_bytes_then_to_bytes() {
        let (meta, _) = make_single();

        // Simulate: start with Hash, then set raw bytes (metadata exchange)
        let mut stub = Metainfo {
            announce: "http://t.com/a".into(),
            announce_list: vec![],
            info: Info {
                piece_length: 16,
                pieces: vec![[0u8; 20]],
                mode: Mode::Single {
                    name: "f.txt".into(),
                    length: 32,
                },
                raw_info: RawInfo::Hash([0u8; 20]),
            },
            url_list: vec![],
            httpseeds: vec![],
            creation_date: None,
            comment: None,
            created_by: None,
            encoding: None,
        };
        assert!(stub.to_bytes().is_none());

        // After metadata exchange: set the raw bytes from the original
        let raw = match &meta.info.raw_info {
            RawInfo::Bytes(b) => b.clone(),
            _ => unreachable!(),
        };
        stub.info.set_raw_bytes(raw);
        let re_encoded = stub.to_bytes().expect("should have raw bytes now");
        let re_parsed = Metainfo::try_from(&re_encoded).unwrap();
        assert_eq!(re_parsed.info_hash(), stub.info_hash());
    }

    #[test]
    fn to_bytes_preserves_optional_fields() {
        let info = Bencode::Dict(vec![
            (Bytes::from("name"), Bencode::Bytes(Bytes::from("x"))),
            (Bytes::from("piece length"), Bencode::Integer(32)),
            (Bytes::from("length"), Bencode::Integer(64)),
            (
                Bytes::from("pieces"),
                Bencode::Bytes(Bytes::from(vec![0u8; 20])),
            ),
        ]);
        let root = Bencode::Dict(vec![
            (
                Bytes::from("announce"),
                Bencode::Bytes(Bytes::from("http://t.com/a")),
            ),
            (Bytes::from("comment"), Bencode::Bytes(Bytes::from("test"))),
            (
                Bytes::from("created by"),
                Bencode::Bytes(Bytes::from("tool")),
            ),
            (Bytes::from("creation date"), Bencode::Integer(1000)),
            (
                Bytes::from("encoding"),
                Bencode::Bytes(Bytes::from("UTF-8")),
            ),
            (Bytes::from("info"), info),
        ]);
        let data = bencode::encode(&root);
        let meta = Metainfo::try_from(&data).unwrap();
        assert_eq!(meta.comment.as_deref(), Some("test"));
        assert_eq!(meta.created_by.as_deref(), Some("tool"));
        assert_eq!(meta.creation_date, Some(1000));
        assert_eq!(meta.encoding.as_deref(), Some("UTF-8"));

        // Round-trip
        let re_encoded = meta.to_bytes().unwrap();
        let re_parsed = Metainfo::try_from(&re_encoded).unwrap();
        assert_eq!(re_parsed.comment.as_deref(), Some("test"));
        assert_eq!(re_parsed.created_by.as_deref(), Some("tool"));
    }
}

#[cfg(all(test, feature = "serde"))]
mod serde_tests {
    use super::*;

    #[test]
    fn metainfo_roundtrip_single_file() {
        let raw = RawInfo::Bytes(Bytes::from_static(b"d4:infod...e"));
        let info = Info {
            piece_length: 262144,
            pieces: vec![[0x42u8; 20]],
            mode: Mode::Single {
                name: "test.txt".into(),
                length: 1024,
            },
            raw_info: raw,
        };
        let meta = Metainfo {
            announce: "http://tracker.example.com/announce".into(),
            announce_list: vec![vec!["http://t2.com/ann".into()]],
            info,
            url_list: vec![],
            httpseeds: vec![],
            creation_date: Some(1672531200),
            comment: Some("test torrent".into()),
            created_by: Some("torrent-rs".into()),
            encoding: Some("UTF-8".into()),
        };

        let json = serde_json::to_string(&meta).unwrap();
        let back: Metainfo = serde_json::from_str(&json).unwrap();

        assert_eq!(back.announce, meta.announce);
        assert_eq!(back.announce_list, meta.announce_list);
        assert_eq!(back.info.piece_length, meta.info.piece_length);
        assert_eq!(back.info.pieces, meta.info.pieces);
        assert_eq!(back.creation_date, meta.creation_date);
        assert_eq!(back.comment.as_deref(), Some("test torrent"));
        assert_eq!(back.created_by.as_deref(), Some("torrent-rs"));
        assert_eq!(back.encoding.as_deref(), Some("UTF-8"));
    }

    #[test]
    fn metainfo_roundtrip_multi_file() {
        let info = Info {
            piece_length: 65536,
            pieces: vec![[0u8; 20]],
            mode: Mode::Multiple {
                name: "my_data".into(),
                files: vec![
                    FileInfo {
                        length: 100,
                        path: vec!["dir".into(), "a.txt".into()],
                    },
                    FileInfo {
                        length: 200,
                        path: vec!["dir".into(), "b.txt".into()],
                    },
                ],
            },
            raw_info: RawInfo::Hash([0xAB; 20]),
        };
        let meta = Metainfo {
            announce: "http://t.com/a".into(),
            announce_list: vec![],
            info,
            url_list: vec![],
            httpseeds: vec![],
            creation_date: None,
            comment: None,
            created_by: None,
            encoding: None,
        };

        let json = serde_json::to_string(&meta).unwrap();
        let back: Metainfo = serde_json::from_str(&json).unwrap();

        match &back.info.mode {
            Mode::Multiple { name, files } => {
                assert_eq!(name, "my_data");
                assert_eq!(files.len(), 2);
                assert_eq!(files[0].length, 100);
                assert_eq!(files[0].path, vec!["dir", "a.txt"]);
                assert_eq!(files[1].length, 200);
                assert_eq!(files[1].path, vec!["dir", "b.txt"]);
            }
            _ => panic!("expected Multiple mode"),
        }
    }

    #[test]
    fn magnet_origin_roundtrip() {
        // Simulates a Metainfo created from a magnet URI (minimal fields)
        let meta = Metainfo {
            announce: String::new(),
            announce_list: vec![],
            info: Info {
                piece_length: 0,
                pieces: vec![],
                mode: Mode::Single {
                    name: "magnet-origin".into(),
                    length: 0,
                },
                raw_info: RawInfo::Hash([0x11; 20]),
            },
            url_list: vec![],
            httpseeds: vec![],
            creation_date: None,
            comment: None,
            created_by: None,
            encoding: None,
        };

        let json = serde_json::to_string(&meta).unwrap();
        let back: Metainfo = serde_json::from_str(&json).unwrap();

        assert_eq!(back.info_hash(), meta.info_hash());
        assert_eq!(back.announce, "");
        assert_eq!(back.announce_list.len(), 0);
    }

    // ── file_offsets tests (BEP 19 web seed) ─────────────────────

    #[test]
    fn file_offsets_single_file() {
        let info = Info {
            piece_length: 256,
            pieces: vec![[0u8; 20]; 4],
            mode: Mode::Single {
                name: "data.bin".into(),
                length: 1000,
            },
            raw_info: RawInfo::Hash([0u8; 20]),
        };
        let offsets = info.file_offsets();
        assert_eq!(offsets.len(), 1);
        assert_eq!(offsets[0].offset, 0);
        assert_eq!(offsets[0].length, 1000);
        assert_eq!(offsets[0].path, vec!["data.bin"]);
    }

    #[test]
    fn file_offsets_multi_file() {
        let info = Info {
            piece_length: 256,
            pieces: vec![[0u8; 20]; 3],
            mode: Mode::Multiple {
                name: "root".into(),
                files: vec![
                    FileInfo {
                        length: 100,
                        path: vec!["a.txt".into()],
                    },
                    FileInfo {
                        length: 200,
                        path: vec!["sub".into(), "b.txt".into()],
                    },
                    FileInfo {
                        length: 50,
                        path: vec!["c.txt".into()],
                    },
                ],
            },
            raw_info: RawInfo::Hash([0u8; 20]),
        };
        let offsets = info.file_offsets();
        assert_eq!(offsets.len(), 3);
        assert_eq!(offsets[0].offset, 0);
        assert_eq!(offsets[0].length, 100);
        assert_eq!(offsets[0].path, vec!["a.txt"]);
        assert_eq!(offsets[1].offset, 100);
        assert_eq!(offsets[1].length, 200);
        assert_eq!(offsets[1].path, vec!["sub", "b.txt"]);
        assert_eq!(offsets[2].offset, 300);
        assert_eq!(offsets[2].length, 50);
        assert_eq!(offsets[2].path, vec!["c.txt"]);
    }
}

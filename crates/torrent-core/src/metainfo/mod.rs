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

mod parse;

use bytes::Bytes;
use sha1::{Digest, Sha1};

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
pub struct Metainfo {
    /// The URL of the tracker.
    pub announce: String,
    /// Alternative tracker tiers (BEP 12).
    pub announce_list: Vec<Vec<String>>,
    /// The info dictionary containing file metadata.
    pub info: Info,
    /// Unix timestamp of creation (optional).
    pub creation_date: Option<i64>,
    /// Free-form comment (optional).
    pub comment: Option<String>,
    /// Name of the program used to create the torrent (optional).
    pub created_by: Option<String>,
    /// Encoding used for string values (optional, historic).
    pub encoding: Option<String>,
}

/// The `info` dictionary from a `.torrent` file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Info {
    /// Length of each piece in bytes.
    pub piece_length: u64,
    /// Concatenated SHA-1 hashes, one 20-byte hash per piece.
    pub pieces: Vec<[u8; 20]>,
    /// Whether this is a single-file or multi-file torrent.
    pub mode: Mode,
    /// The raw bencoded `info` dict bytes — needed for info_hash calculation.
    pub raw_info: Bytes,
}

/// File layout mode for a torrent.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub fn info_hash(&self) -> [u8; 20] {
        let mut hasher = Sha1::new();
        hasher.update(&self.info.raw_info);
        hasher.finalize().into()
    }

    /// Serialize back to bencoded bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        unimplemented!("Metainfo::to_bytes is not yet supported")
    }
}

impl Info {
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

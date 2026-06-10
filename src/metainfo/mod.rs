mod parse;

use bytes::Bytes;

/// Represents a parsed `.torrent` file (BEP 3).
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
        use sha1::{Digest, Sha1};
        let mut hasher = Sha1::new();
        hasher.update(&self.info.raw_info);
        hasher.finalize().into()
    }

    /// Serialize back to bencoded bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        // TODO: implement serialization in Phase 2.1+
        // For now, this is a placeholder.
        todo!("Metainfo::to_bytes not yet implemented")
    }
}

impl Info {
    /// Returns the total size of all files in bytes.
    pub fn total_size(&self) -> u64 {
        match &self.mode {
            Mode::Single { length, .. } => *length,
            Mode::Multiple { files, .. } => files.iter().map(|f| f.length).sum(),
        }
    }

    /// Returns the number of pieces.
    pub fn num_pieces(&self) -> usize {
        self.pieces.len()
    }
}

pub use parse::from_bytes;

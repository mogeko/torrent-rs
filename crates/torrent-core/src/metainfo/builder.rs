//! [`MetainfoBuilder`] — constructs a [`Metainfo`] from raw data by
//! computing SHA-1 piece hashes.
//!
//! This is the "create torrent" side of the metainfo module — feed raw
//! file bytes via [`add_data`](MetainfoBuilder::add_data) and call
//! [`finish`](MetainfoBuilder::finish) to obtain a complete [`Metainfo`]
//! suitable for serialization via [`Metainfo::to_bytes`].

use sha1::{Digest, Sha1};

use crate::bencode::{self, Bencode, Bytes};
use crate::metainfo::{Info, Metainfo, Mode, RawInfo};

/// Builds a [`Metainfo`] from raw data by computing SHA-1 piece hashes.
///
/// `MetainfoBuilder` is the entry point for creating new `.torrent` files.
/// Feed file contents via [`add_data`](Self::add_data) — the builder
/// accumulates bytes, hashes them at `piece_length` boundaries, and
/// stores the resulting 20-byte SHA-1 piece hashes.  Call
/// [`finish`](Self::finish) to produce a complete [`Metainfo`] with
/// a properly bencoded `info` dict and [`RawInfo::Bytes`].
///
/// # Examples
///
/// ```
/// use torrent_core::metainfo::{MetainfoBuilder, Mode};
///
/// let data = b"hello world this is a test of the piece hashing system";
/// let mut builder = MetainfoBuilder::new(32);
/// builder.add_data(data);
///
/// let meta = builder.finish(
///     "http://tracker.example.com/announce".into(),
///     Mode::Single {
///         name: "test.txt".into(),
///         length: data.len() as u64,
///     },
/// );
///
/// assert!(meta.to_bytes().is_some());
/// assert_eq!(meta.info.num_pieces(), 2); // 52 bytes ÷ 32 = 1 full + 1 partial
/// ```
pub struct MetainfoBuilder {
    piece_length: u32,
    /// Accumulated bytes not yet at a piece boundary.
    buf: Vec<u8>,
    /// Completed SHA-1 piece hashes.
    pieces: Vec<[u8; 20]>,
    /// Total bytes fed via [`add_data`].
    total_length: u64,
}

impl MetainfoBuilder {
    /// Create a new builder with the given piece length.
    ///
    /// `piece_length` is the number of bytes per piece. Common values:
    /// 256 KiB (262144), 512 KiB (524288), 1 MiB (1048576).
    ///
    /// # Panics
    ///
    /// Panics if `piece_length` is 0.
    pub fn new(piece_length: u32) -> Self {
        debug_assert!(piece_length > 0, "piece_length must be positive");
        Self {
            piece_length,
            buf: Vec::new(),
            pieces: Vec::new(),
            total_length: 0,
        }
    }

    /// Feed raw file data to the builder.
    ///
    /// Call this once per file (or per chunk for large files). The
    /// builder accumulates bytes internally. When the accumulated buffer
    /// reaches `piece_length`, a full piece is hashed and the hash is
    /// stored. Remaining bytes are kept for the next call or for the
    /// final piece in [`finish`](Self::finish).
    ///
    /// This method is infallible and can be called any number of times
    /// before [`finish`](Self::finish).
    pub fn add_data(&mut self, data: &[u8]) {
        self.total_length += data.len() as u64;
        self.buf.extend_from_slice(data);

        let piece_len = self.piece_length as usize;
        while self.buf.len() >= piece_len {
            // Drain exactly one full piece from the front of the buffer.
            let piece_data: Vec<u8> = self.buf.drain(..piece_len).collect();
            let hash: [u8; 20] = Sha1::digest(&piece_data).into();
            self.pieces.push(hash);
        }
    }

    /// Finalize the builder, producing a complete [`Metainfo`].
    ///
    /// Any remaining data in the buffer (less than `piece_length`) is
    /// hashed as the final piece. If no data was ever added (empty
    /// file), no pieces are produced — this is a valid torrent per
    /// BEP 3.
    ///
    /// The resulting [`Metainfo`] has [`RawInfo::Bytes`] populated,
    /// so [`Metainfo::to_bytes`] and [`Metainfo::info_hash`] both work.
    pub fn finish(self, announce: String, mode: Mode) -> Metainfo {
        let mut pieces = self.pieces;

        // Hash any remaining partial piece
        if !self.buf.is_empty() {
            let hash: [u8; 20] = Sha1::digest(&self.buf).into();
            pieces.push(hash);
        }

        // Concatenate all 20-byte hashes into a flat byte string
        let pieces_bytes: Vec<u8> = pieces.iter().flat_map(|h| h.iter().copied()).collect();
        let pieces_bytes = Bytes::from(pieces_bytes);

        // Bencode the info dict
        let info_dict = build_info_dict(self.piece_length as u64, &pieces_bytes, &mode);
        let raw_info_bytes = bencode::encode(&info_dict);
        let raw_info = RawInfo::Bytes(Bytes::from(raw_info_bytes));

        let info = Info {
            piece_length: self.piece_length as u64,
            pieces,
            mode,
            raw_info,
        };

        Metainfo {
            announce,
            announce_list: Vec::new(),
            info,
            creation_date: None,
            comment: None,
            created_by: None,
            encoding: None,
        }
    }

    /// Returns the total number of bytes fed via [`add_data`](Self::add_data).
    pub fn total_length(&self) -> u64 {
        self.total_length
    }

    /// Returns the number of completed pieces (full piece_length chunks
    /// already hashed). Does not include the partial final piece that
    /// will be hashed in [`finish`](Self::finish).
    pub fn num_completed_pieces(&self) -> usize {
        self.pieces.len()
    }
}

/// Build the bencoded `info` dictionary for a torrent.
///
/// Returns a [`Bencode::Dict`] with the standard BEP 3 keys:
/// `length` (single-file only), `name`, `piece length`, `pieces`,
/// and `files` (multi-file only). Keys are sorted lexicographically
/// by [`bencode::encode`].
fn build_info_dict(piece_length: u64, pieces_bytes: &Bytes, mode: &Mode) -> Bencode {
    let mut entries: Vec<(Bytes, Bencode)> = Vec::new();

    match mode {
        Mode::Single { name, length } => {
            entries.push((Bytes::from("length"), Bencode::Integer(*length as i64)));
            entries.push((
                Bytes::from("name"),
                Bencode::Bytes(Bytes::copy_from_slice(name.as_bytes())),
            ));
        }
        Mode::Multiple { name, files } => {
            let file_entries: Vec<Bencode> = files
                .iter()
                .map(|f| {
                    let path: Vec<Bencode> = f
                        .path
                        .iter()
                        .map(|p| Bencode::Bytes(Bytes::copy_from_slice(p.as_bytes())))
                        .collect();
                    Bencode::Dict(vec![
                        (Bytes::from("length"), Bencode::Integer(f.length as i64)),
                        (Bytes::from("path"), Bencode::List(path)),
                    ])
                })
                .collect();
            entries.push((Bytes::from("files"), Bencode::List(file_entries)));
            entries.push((
                Bytes::from("name"),
                Bencode::Bytes(Bytes::copy_from_slice(name.as_bytes())),
            ));
        }
    }

    entries.push((
        Bytes::from("piece length"),
        Bencode::Integer(piece_length as i64),
    ));
    entries.push((Bytes::from("pieces"), Bencode::Bytes(pieces_bytes.clone())));

    Bencode::Dict(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metainfo::FileInfo;
    use crate::metainfo::from_bytes;

    #[test]
    fn empty_data_produces_zero_pieces() {
        let builder = MetainfoBuilder::new(256 * 1024);
        let meta = builder.finish(
            "http://example.com/announce".into(),
            Mode::Single {
                name: "empty.dat".into(),
                length: 0,
            },
        );

        assert_eq!(meta.info.pieces.len(), 0);
        assert_eq!(meta.info.num_pieces(), 0);
        assert!(meta.to_bytes().is_some());

        let bytes = meta.to_bytes().unwrap();
        let parsed = from_bytes(&bytes).unwrap();
        assert_eq!(parsed.info_hash(), meta.info_hash());
    }

    #[test]
    fn data_less_than_piece_length() {
        let data = b"hello world";
        let mut builder = MetainfoBuilder::new(1024);
        builder.add_data(data);

        let meta = builder.finish(
            "http://example.com/announce".into(),
            Mode::Single {
                name: "small.txt".into(),
                length: data.len() as u64,
            },
        );

        // All data fits in one partial piece
        assert_eq!(meta.info.num_pieces(), 1);
        assert_eq!(meta.info.pieces.len(), 1);

        // Round-trip
        let bytes = meta.to_bytes().unwrap();
        let parsed = from_bytes(&bytes).unwrap();
        assert_eq!(parsed.info_hash(), meta.info_hash());
        assert_eq!(parsed.info.pieces, meta.info.pieces);
    }

    #[test]
    fn data_exactly_one_piece() {
        let piece_length: u32 = 64;
        let data = vec![0xABu8; piece_length as usize];
        let mut builder = MetainfoBuilder::new(piece_length);
        builder.add_data(&data);

        let meta = builder.finish(
            "http://example.com/announce".into(),
            Mode::Single {
                name: "exact.dat".into(),
                length: data.len() as u64,
            },
        );

        assert_eq!(meta.info.num_pieces(), 1);
        assert_eq!(meta.info.total_size(), piece_length as u64);

        // Round-trip
        let bytes = meta.to_bytes().unwrap();
        let parsed = from_bytes(&bytes).unwrap();
        assert_eq!(parsed.info_hash(), meta.info_hash());
    }

    #[test]
    fn multiple_pieces_with_partial_last() {
        let piece_length: u32 = 16;
        let data = b"abcdefghijklmnopqrstuvwxyz12345"; // 31 bytes → 1 full + 1 partial
        let mut builder = MetainfoBuilder::new(piece_length);
        builder.add_data(data);

        let meta = builder.finish(
            "http://example.com/announce".into(),
            Mode::Single {
                name: "multi.dat".into(),
                length: data.len() as u64,
            },
        );

        // 31 / 16 = 1 full piece + 15 bytes partial
        assert_eq!(meta.info.num_pieces(), 2);
        assert_eq!(meta.info.total_size(), 31);

        // Verify piece hashes are distinct (different content)
        assert_ne!(meta.info.pieces[0], meta.info.pieces[1]);

        // Round-trip
        let bytes = meta.to_bytes().unwrap();
        let parsed = from_bytes(&bytes).unwrap();
        assert_eq!(parsed, meta);
    }

    #[test]
    fn add_data_called_multiple_times() {
        let piece_length: u32 = 8;
        let mut builder = MetainfoBuilder::new(piece_length);

        // Feed data in chunks that cross piece boundaries
        builder.add_data(b"abcdefgh"); // exactly 1 piece
        builder.add_data(b"ijkl"); // 4 bytes into next piece
        builder.add_data(b"mnop"); // completes 2nd piece
        builder.add_data(b"qr"); // 2 bytes partial

        let meta = builder.finish(
            "http://example.com/announce".into(),
            Mode::Single {
                name: "chunks.dat".into(),
                length: 18,
            },
        );

        // 18 bytes ÷ 8 = 2 full + 2 partial
        assert_eq!(meta.info.num_pieces(), 3);
        assert_eq!(meta.info.total_size(), 18);

        let bytes = meta.to_bytes().unwrap();
        let parsed = from_bytes(&bytes).unwrap();
        assert_eq!(parsed, meta);
    }

    #[test]
    fn piece_hash_computes_correct_sha1() {
        // Known SHA-1 for the given data
        let data = b"The quick brown fox jumps over the lazy dog";
        let expected_sha1: [u8; 20] = Sha1::digest(data).into();

        let mut builder = MetainfoBuilder::new(data.len() as u32);
        builder.add_data(data);

        let meta = builder.finish(
            "http://example.com/announce".into(),
            Mode::Single {
                name: "fox.txt".into(),
                length: data.len() as u64,
            },
        );

        assert_eq!(meta.info.pieces[0], expected_sha1);
    }

    #[test]
    fn multi_file_mode() {
        let piece_length: u32 = 16;
        let mut builder = MetainfoBuilder::new(piece_length);

        // Simulate two files concatenated
        builder.add_data(b"AAAAAAAAAAAAAAAA"); // file1: 16 bytes, 1 piece
        builder.add_data(b"BBBBBBBBBBBBBBBBCCCC"); // file2: 20 bytes, 1 full + 1 partial

        let files = vec![
            FileInfo {
                length: 16,
                path: vec!["dir".into(), "a.txt".into()],
            },
            FileInfo {
                length: 20,
                path: vec!["dir".into(), "b.txt".into()],
            },
        ];

        let meta = builder.finish(
            "http://example.com/announce".into(),
            Mode::Multiple {
                name: "my_data".into(),
                files,
            },
        );

        // 36 bytes ÷ 16 = 2 full + 4 partial = 3 pieces
        assert_eq!(meta.info.num_pieces(), 3);
        assert_eq!(meta.info.total_size(), 36);

        let bytes = meta.to_bytes().unwrap();
        let parsed = from_bytes(&bytes).unwrap();
        assert_eq!(parsed, meta);
    }

    #[test]
    fn round_trip_via_try_from() {
        let data = vec![0x42u8; 1024];
        let mut builder = MetainfoBuilder::new(256);
        builder.add_data(&data);

        let meta = builder.finish(
            "http://tracker.example.com/announce".into(),
            Mode::Single {
                name: "roundtrip.bin".into(),
                length: data.len() as u64,
            },
        );

        // Serialize and re-parse
        let torrent_bytes = meta.to_bytes().unwrap();
        let parsed = Metainfo::try_from(&torrent_bytes[..]).unwrap();

        assert_eq!(parsed.announce, meta.announce);
        assert_eq!(parsed.info.pieces, meta.info.pieces);
        assert_eq!(parsed.info.mode, meta.info.mode);
        assert_eq!(parsed.info_hash(), meta.info_hash());
    }

    #[test]
    fn total_length_tracks_correctly() {
        let mut builder = MetainfoBuilder::new(1024);
        assert_eq!(builder.total_length(), 0);

        builder.add_data(&[0u8; 500]);
        assert_eq!(builder.total_length(), 500);

        builder.add_data(&[0u8; 600]);
        assert_eq!(builder.total_length(), 1100);
    }

    #[test]
    fn num_completed_pieces_tracks_correctly() {
        let mut builder = MetainfoBuilder::new(10);
        assert_eq!(builder.num_completed_pieces(), 0);

        builder.add_data(&[0u8; 25]); // 2 full pieces + 5 leftover
        assert_eq!(builder.num_completed_pieces(), 2);

        builder.add_data(&[0u8; 5]); // completes the partial piece
        assert_eq!(builder.num_completed_pieces(), 3);
    }
}

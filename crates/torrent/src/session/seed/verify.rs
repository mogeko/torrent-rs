//! Disk verification — hash on-disk data against [`Metainfo`] piece hashes.

use sha1::{Digest, Sha1};

use crate::error::Error;
use crate::metainfo::Info;
use crate::piece::PieceManager;
use crate::storage::Storage;

use super::InfoHash;

/// Verify existing data against the torrent's piece hashes.
///
/// For each piece, reads from `storage`, computes SHA-1, and compares
/// against `info.pieces[i]`. Matching pieces are marked complete via
/// [`PieceManager::set_piece`]. Returns the number of verified pieces.
pub(crate) async fn verify_existing(
    storage: &dyn Storage, info: &Info, piece_mgr: &mut PieceManager,
) -> Result<usize, Error> {
    let num_pieces = info.num_pieces();
    let piece_length = info.piece_length as usize;
    let mut verified = 0usize;
    let mut buf = vec![0u8; piece_length];

    for i in 0..num_pieces {
        let idx = i as u32;
        let actual_len = if i == num_pieces - 1 {
            let total = info.total_size() as usize;
            total - (i * piece_length)
        } else {
            piece_length
        };
        let read_buf = &mut buf[..actual_len];
        storage.read_piece(idx, read_buf).await?;
        let actual_hash: InfoHash = Sha1::digest(read_buf).into();
        if actual_hash == info.pieces[i] {
            piece_mgr.set_piece(idx);
            verified += 1;
        }
    }

    tracing::info!("disk verification complete: {verified}/{num_pieces} pieces verified");
    Ok(verified)
}

//! Piece hashing orchestration — reads a [`DataSource`] and feeds it
//! into [`MetainfoBuilder`] to produce a complete [`Metainfo`].

use crate::error::{Error, ErrorKind};
use crate::metainfo::MetainfoBuilder;

use super::source::DataSource;

/// Infer a sensible piece length from the total file size.
///
/// Follows common conventions: smaller files get smaller pieces to
/// keep the piece count reasonable, while larger files get larger
/// pieces to keep the .torrent file compact.
fn infer_piece_length(total_size: u64) -> u32 {
    match total_size {
        0..=67_108_864 => 32 * 1024,                 // 0 – 64 MiB → 32 KiB
        67_108_865..=536_870_912 => 64 * 1024,       // 64 MiB – 512 MiB → 64 KiB
        536_870_913..=1_073_741_824 => 128 * 1024,   // 512 MiB – 1 GiB → 128 KiB
        1_073_741_825..=8_589_934_592 => 256 * 1024, // 1 GiB – 8 GiB → 256 KiB
        _ => 512 * 1024,                             // > 8 GiB → 512 KiB
    }
}

/// Hash a data source into a [`Metainfo`].
///
/// Reads the source sequentially in `piece_length`-sized chunks,
/// feeding each to [`MetainfoBuilder::add_data`]. The returned
/// [`Metainfo`] has [`RawInfo::Bytes`](crate::metainfo::RawInfo::Bytes)
/// populated, ready for serialization.
pub(crate) async fn hash_source(
    source: &dyn DataSource, piece_length: u32,
) -> Result<MetainfoBuilder, Error> {
    let total = source.total_size().await?;
    let mut builder = MetainfoBuilder::new(piece_length);

    // Cap buffer at 16 MiB to bound memory usage for very large pieces
    let buf_size = (piece_length as usize).min(16 * 1024 * 1024);
    let mut buf = vec![0u8; buf_size];
    let mut offset = 0u64;

    while offset < total {
        let remaining = total - offset;
        let read_len = (buf_size as u64).min(remaining) as usize;
        let n = source.read_at(offset, &mut buf[..read_len]).await?;
        if n == 0 {
            return Err(Error::new(ErrorKind::InvalidInput));
        }
        builder.add_data(&buf[..n]);
        offset += n as u64;
    }

    Ok(builder)
}

/// Determine the piece length to use, either from user override or
/// inferred from the total size.
pub(crate) async fn resolve_piece_length(
    source: &dyn DataSource, user_override: Option<u32>,
) -> Result<u32, Error> {
    if let Some(pl) = user_override {
        return Ok(pl);
    }
    let total = source.total_size().await?;
    Ok(infer_piece_length(total))
}

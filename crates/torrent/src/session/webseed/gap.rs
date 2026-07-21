//! Gap-finding utilities for web seed downloads (BEP 19).
//!
//! Scans the piece bitfield to locate contiguous ranges of missing pieces
//! suitable for HTTP Range requests.

use crate::metainfo::Metainfo;

/// Find the largest contiguous gap of missing pieces in a bitfield.
///
/// Returns `(gap_start_index, gap_size_in_pieces)`, or `None` if
/// no gaps exist (all pieces present).
pub(crate) fn find_largest_gap(bitfield: &[bool]) -> Option<(u32, u32)> {
    let (mut best_start, mut best_size) = (None, 0u32);
    let (mut gap_start, mut gap_size) = (None, 0u32);

    for (i, &has) in bitfield.iter().enumerate() {
        if !has {
            if gap_start.is_none() {
                gap_start = Some(i as u32);
            }
            gap_size += 1;
        } else if let Some(start) = gap_start {
            if gap_size > best_size {
                best_start = Some(start);
                best_size = gap_size;
            }
            gap_start = None;
            gap_size = 0;
        }
    }

    if let Some(start) = gap_start {
        if gap_size > best_size {
            best_start = Some(start);
            best_size = gap_size;
        }
    }

    best_start.map(|s| (s, best_size))
}

/// Find the largest contiguous gap within a single file's piece range.
pub(crate) fn gap_within_file(
    bitfield: &[bool], metainfo: &Metainfo, piece_length: u64, min_gap_pieces: u32,
) -> Option<(u32, u32)> {
    let offsets = metainfo.info.file_offsets();
    let mut best: Option<(u32, u32)> = None;

    for fo in &offsets {
        let first_piece = (fo.offset / piece_length) as u32;
        let last_piece = ((fo.offset + fo.length).saturating_sub(1) / piece_length) as u32;
        let last_piece = last_piece.min(bitfield.len().saturating_sub(1) as u32);

        let (mut gap_start, mut gap_size) = (None, 0u32);
        for idx in first_piece..=last_piece {
            if !bitfield[idx as usize] {
                if gap_start.is_none() {
                    gap_start = Some(idx);
                }
                gap_size += 1;
            } else if let Some(start) = gap_start {
                if gap_size > best.map(|(_, s)| s).unwrap_or(0) && gap_size >= min_gap_pieces {
                    best = Some((start, gap_size));
                }
                gap_start = None;
                gap_size = 0;
            }
        }
        if let Some(start) = gap_start {
            if gap_size > best.map(|(_, s)| s).unwrap_or(0) && gap_size >= min_gap_pieces {
                best = Some((start, gap_size));
            }
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_largest_gap_empty() {
        assert_eq!(find_largest_gap(&[true; 10]), None);
    }

    #[test]
    fn find_largest_gap_full() {
        assert_eq!(find_largest_gap(&[false; 10]), Some((0, 10)));
    }

    #[test]
    fn find_largest_gap_middle() {
        let bf = [true, true, false, false, false, true, true];
        assert_eq!(find_largest_gap(&bf), Some((2, 3)));
    }

    #[test]
    fn find_largest_gap_trailing() {
        let bf = [true, false, true, true];
        assert_eq!(find_largest_gap(&bf), Some((1, 1)));
    }
}

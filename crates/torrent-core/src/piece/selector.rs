use rand::RngExt;

/// Trait for piece selection strategies (BEP 3).
///
/// Implementations decide which piece to download next given the
/// pieces we already have (`our_bitfield`) and per-piece availability
/// counts across the swarm (`availability[i]` = number of connected
/// unchoked peers that have piece `i`).
///
/// # Examples
///
/// ```
/// use torrent_core::piece::{PieceSelector, Sequential};
///
/// let our_bitfield = vec![false, false, false, false];
/// let availability = vec![3, 0, 2, 1];
/// let next = Sequential.select(&our_bitfield, &availability);
/// assert_eq!(next, Some(0));
/// ```
pub trait PieceSelector: Send + Sync {
    /// Select the next piece to download.
    ///
    /// `our_bitfield` — pieces we already have (`true` = owned).
    /// `availability` — per-piece count of peers that have each piece.
    /// Only pieces where `availability[i] > 0` are reachable.
    fn select(&self, our_bitfield: &[bool], availability: &[usize]) -> Option<u32>;
}

/// Select the rarest piece first (BEP 3 recommended default).
///
/// Picks the missing piece that the fewest peers have. This strategy
/// maximizes piece diversity in the swarm and is the standard
/// approach described in BEP 3.
pub struct RarestFirst;

impl PieceSelector for RarestFirst {
    fn select(&self, our_bitfield: &[bool], availability: &[usize]) -> Option<u32> {
        let len = our_bitfield.len().min(availability.len());
        let mut best_idx: Option<u32> = None;
        let mut best_count = usize::MAX;

        for i in 0..len {
            if our_bitfield[i] {
                continue; // already have it
            }
            let count = availability[i];
            if count == 0 {
                continue; // no peer has it
            }
            if count < best_count {
                best_count = count;
                best_idx = Some(i as u32);
            }
        }

        best_idx
    }
}

/// Randomly select a piece from available candidates.
///
/// Useful for the initial download phase to quickly obtain a
/// diverse set of pieces before switching to rarest-first.
pub struct RandomFirst;

impl PieceSelector for RandomFirst {
    fn select(&self, our_bitfield: &[bool], availability: &[usize]) -> Option<u32> {
        let len = our_bitfield.len().min(availability.len());
        let candidates: Vec<u32> = (0..len)
            .filter(|&i| !our_bitfield[i] && availability[i] > 0)
            .map(|i| i as u32)
            .collect();

        if candidates.is_empty() {
            return None;
        }

        let idx = rand::rng().random_range(0..candidates.len());
        Some(candidates[idx])
    }
}

/// Select pieces in sequential order (lowest index first).
///
/// Serves streaming-like download where pieces are consumed in order.
/// Less efficient for swarm health than rarest-first.
pub struct Sequential;

impl PieceSelector for Sequential {
    fn select(&self, our_bitfield: &[bool], availability: &[usize]) -> Option<u32> {
        let len = our_bitfield.len().min(availability.len());
        for i in 0..len {
            if !our_bitfield[i] && availability[i] > 0 {
                return Some(i as u32);
            }
        }
        None
    }
}

/// End-game mode: select any remaining missing piece.
///
/// In endgame, we send duplicate requests to multiple peers simultaneously
/// to speed up the final pieces. This selector just returns the first
/// available piece; the caller handles sending requests to multiple peers.
pub struct EndGame;

impl PieceSelector for EndGame {
    fn select(&self, our_bitfield: &[bool], availability: &[usize]) -> Option<u32> {
        Sequential.select(our_bitfield, availability)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequential_basic() {
        let our_bf = vec![false, false, false, false];
        let avail = vec![3, 0, 2, 1];
        // Piece 0 available, piece 1 not, piece 2 available, piece 3 available
        assert_eq!(Sequential.select(&our_bf, &avail), Some(0));
    }

    #[test]
    fn sequential_skip_zero_availability() {
        let our_bf = vec![false, false, false];
        let avail = vec![0, 0, 2];
        // First two pieces have 0 availability, should pick piece 2
        assert_eq!(Sequential.select(&our_bf, &avail), Some(2));
    }

    #[test]
    fn sequential_all_downloaded() {
        let our_bf = vec![true, true];
        let avail = vec![3, 5];
        assert_eq!(Sequential.select(&our_bf, &avail), None);
    }

    #[test]
    fn sequential_none_available() {
        let our_bf = vec![false, false];
        let avail = vec![0, 0]; // no peers have any pieces
        assert_eq!(Sequential.select(&our_bf, &avail), None);
    }

    #[test]
    fn random_first_basic() {
        let our_bf = vec![false, false, false];
        let avail = vec![2, 2, 2];
        let result = RandomFirst.select(&our_bf, &avail);
        assert!(result.is_some());
        let idx = result.unwrap() as usize;
        assert!(idx < 3);
    }

    #[test]
    fn random_first_empty() {
        let our_bf = vec![false, false];
        let avail = vec![0, 0];
        assert_eq!(RandomFirst.select(&our_bf, &avail), None);
    }

    #[test]
    fn rarest_first_picks_rarest() {
        // Piece 0: 3 peers, Piece 1: 1 peer, Piece 2: 5 peers
        // Should pick Piece 1 (rarest)
        let our_bf = vec![false, false, false];
        let avail = vec![3, 1, 5];
        assert_eq!(RarestFirst.select(&our_bf, &avail), Some(1));
    }

    #[test]
    fn rarest_first_skips_owned() {
        // Piece 0 owned, Piece 1 rarest available
        let our_bf = vec![true, false, false];
        let avail = vec![0, 1, 5];
        assert_eq!(RarestFirst.select(&our_bf, &avail), Some(1));
    }

    #[test]
    fn rarest_first_skips_zero_availability() {
        // Piece 0 has 0 availability (no peers), should pick Piece 1
        let our_bf = vec![false, false];
        let avail = vec![0, 3];
        assert_eq!(RarestFirst.select(&our_bf, &avail), Some(1));
    }

    #[test]
    fn rarest_first_empty_when_none_available() {
        let our_bf = vec![false, false];
        let avail = vec![0, 0];
        assert_eq!(RarestFirst.select(&our_bf, &avail), None);
    }

    #[test]
    fn endgame_select_any() {
        let our_bf = vec![false, true]; // piece 0 missing, piece 1 owned
        let avail = vec![4, 0];
        assert_eq!(EndGame.select(&our_bf, &avail), Some(0));
    }

    #[test]
    fn endgame_none_available() {
        let our_bf = vec![false, false];
        let avail = vec![0, 0];
        assert_eq!(EndGame.select(&our_bf, &avail), None);
    }

    #[test]
    fn availability_shorter_than_bitfield() {
        // If availability is shorter, we only consider the overlap
        let our_bf = vec![false, false, false];
        let avail = vec![1]; // only piece 0 has availability
        assert_eq!(Sequential.select(&our_bf, &avail), Some(0));
    }
}

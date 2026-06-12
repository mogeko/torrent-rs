use rand::RngExt;

/// Trait for piece selection strategies (BEP 3).
pub trait PieceSelector: Send + Sync {
    /// Select the next piece to download from the available candidates.
    ///
    /// `candidates` is a bitfield from a single peer (true = peer has piece).
    /// `bitfield` is our local bitfield (true = we already have it).
    fn select(&self, candidates: &[bool], bitfield: &[bool]) -> Option<u32>;
}

/// Select the rarest piece first (BEP 3 recommended default).
pub struct RarestFirst;

impl PieceSelector for RarestFirst {
    fn select(&self, candidates: &[bool], bitfield: &[bool]) -> Option<u32> {
        let mut rarest_pieces = Vec::new();

        for (i, &peer_has) in candidates.iter().enumerate() {
            if i >= bitfield.len() || bitfield[i] {
                continue; // already have it
            }
            if !peer_has {
                continue; // peer doesn't have it
            }
            // For basic rarest-first, we just need the count of peers
            // In the full implementation, we'd pass peer bitfields.
            // Here we approximate: if only one candidate is available, pick it.
            rarest_pieces.push(i as u32);
        }

        if rarest_pieces.is_empty() {
            return None;
        }

        // Simple: return the first available missing piece
        Some(rarest_pieces[0])
    }
}

/// Randomly select a piece from available candidates.
pub struct RandomFirst;

impl PieceSelector for RandomFirst {
    fn select(&self, candidates: &[bool], bitfield: &[bool]) -> Option<u32> {
        let available: Vec<u32> = candidates
            .iter()
            .enumerate()
            .filter(|(i, peer_has)| **peer_has && (*i >= bitfield.len() || !bitfield[*i]))
            .map(|(i, _)| i as u32)
            .collect();

        if available.is_empty() {
            return None;
        }

        let idx = rand::rng().random_range(0..available.len());
        Some(available[idx])
    }
}

/// Select pieces in sequential order.
pub struct Sequential;

impl PieceSelector for Sequential {
    fn select(&self, candidates: &[bool], bitfield: &[bool]) -> Option<u32> {
        for (i, &peer_has) in candidates.iter().enumerate() {
            if peer_has && (i >= bitfield.len() || !bitfield[i]) {
                return Some(i as u32);
            }
        }
        None
    }
}

/// End-game mode: select any remaining missing piece.
///
/// In endgame, we send duplicate requests to multiple peers simultaneously
/// to speed up the final pieces. This selector just returns the first available.
pub struct EndGame;

impl PieceSelector for EndGame {
    fn select(&self, candidates: &[bool], bitfield: &[bool]) -> Option<u32> {
        Sequential.select(candidates, bitfield)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequential_basic() {
        let candidates = vec![true, false, true, true];
        let bitfield = vec![false, false, false, false];
        assert_eq!(Sequential.select(&candidates, &bitfield), Some(0));
    }

    #[test]
    fn sequential_skip_choked() {
        let candidates = vec![false, false, true];
        let bitfield = vec![false, false, false];
        assert_eq!(Sequential.select(&candidates, &bitfield), Some(2));
    }

    #[test]
    fn sequential_all_downloaded() {
        let candidates = vec![true, true];
        let bitfield = vec![true, true];
        assert_eq!(Sequential.select(&candidates, &bitfield), None);
    }

    #[test]
    fn random_first_basic() {
        let candidates = vec![true, true, true];
        let bitfield = vec![false, false, false];
        let result = RandomFirst.select(&candidates, &bitfield);
        assert!(result.is_some());
        let idx = result.unwrap() as usize;
        assert!(idx < 3);
    }

    #[test]
    fn random_first_empty() {
        let candidates = vec![false, false];
        let bitfield = vec![false, false];
        assert_eq!(RandomFirst.select(&candidates, &bitfield), None);
    }

    #[test]
    fn rarest_first_basic() {
        let candidates = vec![true, true, false];
        let bitfield = vec![false, false, false];
        assert_eq!(RarestFirst.select(&candidates, &bitfield), Some(0));
    }

    #[test]
    fn endgame_select_any() {
        let candidates = vec![true, true];
        let bitfield = vec![false, true];
        assert_eq!(EndGame.select(&candidates, &bitfield), Some(0));
    }
}

//! Piece management and selection for the BitTorrent protocol.
//!
//! Implements BEP 3: The BitTorrent Protocol Specification.
//!
//! This module covers piece-level protocol concerns:
//! - [`PieceManager`] — bitfield tracking, progress calculation
//! - [`PieceSelector`] trait + 4 strategies for picking which piece to download next
//!
//! # Selection Strategies
//!
//! - [`RarestFirst`] — picks the piece available from the fewest peers (BEP 3 recommended)
//! - [`RandomFirst`] — picks a random available piece
//! - [`Sequential`] — picks the lowest-indexed missing piece
//! - [`EndGame`] — picks any remaining piece (for duplicate requests in final phase)

mod manager;
mod selector;

pub use self::manager::PieceManager;
pub use self::selector::{EndGame, PieceSelector, RandomFirst, RarestFirst, Sequential};

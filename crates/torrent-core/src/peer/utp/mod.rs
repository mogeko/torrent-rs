//! uTP (Micro Transport Protocol) — BEP 29.
//!
//! This module provides sync primitives for the uTorrent Transport Protocol,
//! a UDP-based reliable transport with delay-based congestion control.
//!
//! # Submodules
//!
//! - [`header`]: 20-byte packet header and packet type enum
//! - [`selective_ack`]: Selective ACK extension for non-sequential acknowledgements
//! - [`congestion`]: Delay-based congestion control algorithm

pub mod congestion;
pub mod header;
pub mod selective_ack;

pub use self::congestion::UtpCongestionControl;
pub use self::header::{UtpHeader, UtpType};
pub use self::selective_ack::SelectiveAck;

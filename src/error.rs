//! Error types for the torrent library.
//!
//! Uses a `kind` + `source` pattern similar to [`std::io::Error`].
//!
//! # Key Types
//!
//! - [`Error`] — the main error type
//! - [`ErrorKind`] — categorization via a `#[non_exhaustive]` enum
//!
//! # Examples
//!
//! ```
//! use torrent::error::{Error, ErrorKind};
//!
//! let err = Error::new(ErrorKind::InvalidInput);
//! assert_eq!(err.kind(), ErrorKind::InvalidInput);
//! ```

use std::fmt;

/// Top-level error type for the torrent library.
///
/// Uses a `kind` + `source` pattern similar to [`std::io::Error`].
/// The [`ErrorKind`] enum categorizes the error, while an optional
/// underlying source provides additional context.
///
/// # Examples
///
/// ```
/// use torrent::error::{Error, ErrorKind};
///
/// let err = Error::new(ErrorKind::InvalidInput);
/// assert_eq!(err.kind(), ErrorKind::InvalidInput);
/// ```
///
/// Creating an error with a source:
///
/// ```
/// use torrent::error::{Error, ErrorKind};
/// use std::io;
///
/// let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
/// let err = Error::with_source(ErrorKind::Io, io_err);
/// assert_eq!(err.kind(), ErrorKind::Io);
/// ```
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// Categorization of errors produced by the torrent library.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    // Bencode errors
    BencodeInvalidSyntax,
    BencodeInvalidInteger,
    BencodeUnexpectedEof,
    BencodeIntegerOverflow,
    // Placeholder categories for future phases
    Io,
    InvalidInput,
    Protocol,
    // Metainfo errors
    MetainfoMissingField,
    MetainfoInvalidField,
    MetainfoInvalidPieces,
    // Peer errors
    PeerInvalidHandshake,
    PeerInvalidMessage,
    PeerConnectionClosed,
    // Tracker errors
    TrackerInvalidResponse,
    TrackerRequestFailed,
    TrackerProtocolError,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            ErrorKind::BencodeInvalidSyntax => write!(f, "invalid bencode syntax"),
            ErrorKind::BencodeInvalidInteger => write!(f, "invalid bencode integer"),
            ErrorKind::BencodeUnexpectedEof => write!(f, "unexpected end of bencode data"),
            ErrorKind::BencodeIntegerOverflow => write!(f, "bencode integer overflow"),
            ErrorKind::Io => write!(f, "I/O error"),
            ErrorKind::InvalidInput => write!(f, "invalid input"),
            ErrorKind::Protocol => write!(f, "protocol error"),
            ErrorKind::MetainfoMissingField => write!(f, "missing required metainfo field"),
            ErrorKind::MetainfoInvalidField => write!(f, "invalid metainfo field"),
            ErrorKind::MetainfoInvalidPieces => write!(f, "invalid pieces length in metainfo"),
            ErrorKind::PeerInvalidHandshake => write!(f, "invalid peer handshake"),
            ErrorKind::PeerInvalidMessage => write!(f, "invalid peer message"),
            ErrorKind::PeerConnectionClosed => write!(f, "peer connection closed"),
            ErrorKind::TrackerInvalidResponse => write!(f, "invalid tracker response"),
            ErrorKind::TrackerRequestFailed => write!(f, "tracker request failed"),
            ErrorKind::TrackerProtocolError => write!(f, "tracker protocol error"),
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorKind::BencodeInvalidSyntax => write!(f, "BencodeInvalidSyntax"),
            ErrorKind::BencodeInvalidInteger => write!(f, "BencodeInvalidInteger"),
            ErrorKind::BencodeUnexpectedEof => write!(f, "BencodeUnexpectedEof"),
            ErrorKind::BencodeIntegerOverflow => write!(f, "BencodeIntegerOverflow"),
            ErrorKind::Io => write!(f, "Io"),
            ErrorKind::InvalidInput => write!(f, "InvalidInput"),
            ErrorKind::Protocol => write!(f, "Protocol"),
            ErrorKind::MetainfoMissingField => write!(f, "MetainfoMissingField"),
            ErrorKind::MetainfoInvalidField => write!(f, "MetainfoInvalidField"),
            ErrorKind::MetainfoInvalidPieces => write!(f, "MetainfoInvalidPieces"),
            ErrorKind::PeerInvalidHandshake => write!(f, "PeerInvalidHandshake"),
            ErrorKind::PeerInvalidMessage => write!(f, "PeerInvalidMessage"),
            ErrorKind::PeerConnectionClosed => write!(f, "PeerConnectionClosed"),
            ErrorKind::TrackerInvalidResponse => write!(f, "TrackerInvalidResponse"),
            ErrorKind::TrackerRequestFailed => write!(f, "TrackerRequestFailed"),
            ErrorKind::TrackerProtocolError => write!(f, "TrackerProtocolError"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|e| &**e as _)
    }
}

impl Error {
    /// Create a new error with the given kind and no source.
    pub fn new(kind: ErrorKind) -> Self {
        Self { kind, source: None }
    }

    /// Create a new error with the given kind and an underlying source.
    pub fn with_source(
        kind: ErrorKind,
        source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self {
            kind,
            source: Some(source.into()),
        }
    }

    /// Returns the `ErrorKind` of this error.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

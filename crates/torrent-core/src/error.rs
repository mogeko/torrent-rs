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
//! use torrent_core::error::{Error, ErrorKind};
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
/// use torrent_core::error::{Error, ErrorKind};
///
/// let err = Error::new(ErrorKind::InvalidInput);
/// assert_eq!(err.kind(), ErrorKind::InvalidInput);
/// ```
///
/// Creating an error with a source:
///
/// ```
/// use torrent_core::error::{Error, ErrorKind};
/// use std::io;
///
/// let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
/// let err = Error::with_source(ErrorKind::Io, io_err);
/// assert_eq!(err.kind(), ErrorKind::Io);
/// ```
#[derive(Debug)]
pub struct Error {
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
    kind: ErrorKind,
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
    // Metainfo errors
    MetainfoMissingField,
    MetainfoInvalidField,
    MetainfoInvalidPieces,
    // Peer errors
    PeerInvalidHandshake,
    PeerInvalidMessage,
    PeerConnectionClosed,
    /// An extended peer message (BEP 10) was malformed.
    PeerInvalidExtendedMessage,
    /// A PEX message (BEP 11) was malformed.
    PeerInvalidPexMessage,
    // Tracker errors
    TrackerInvalidResponse,
    TrackerRequestFailed,
    TrackerProtocolError,
    // Placeholder categories for future phases
    Io,
    InvalidInput,
    Protocol,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            ErrorKind::BencodeInvalidSyntax => write!(f, "invalid bencode syntax"),
            ErrorKind::BencodeInvalidInteger => write!(f, "invalid bencode integer"),
            ErrorKind::BencodeUnexpectedEof => write!(f, "unexpected end of bencode data"),
            ErrorKind::BencodeIntegerOverflow => write!(f, "bencode integer overflow"),
            ErrorKind::MetainfoMissingField => write!(f, "missing required metainfo field"),
            ErrorKind::MetainfoInvalidField => write!(f, "invalid metainfo field"),
            ErrorKind::MetainfoInvalidPieces => write!(f, "invalid pieces length in metainfo"),
            ErrorKind::PeerInvalidHandshake => write!(f, "invalid peer handshake"),
            ErrorKind::PeerInvalidMessage => write!(f, "invalid peer message"),
            ErrorKind::PeerConnectionClosed => write!(f, "peer connection closed"),
            ErrorKind::PeerInvalidExtendedMessage => write!(f, "invalid extended peer message"),
            ErrorKind::PeerInvalidPexMessage => write!(f, "invalid PEX message"),
            ErrorKind::TrackerInvalidResponse => write!(f, "invalid tracker response"),
            ErrorKind::TrackerRequestFailed => write!(f, "tracker request failed"),
            ErrorKind::TrackerProtocolError => write!(f, "tracker protocol error"),
            ErrorKind::Io => write!(f, "I/O error"),
            ErrorKind::InvalidInput => write!(f, "invalid input"),
            ErrorKind::Protocol => write!(f, "protocol error"),
        }?;
        if let Some(ref source) = self.source {
            write!(f, ": {source}")?;
        }
        Ok(())
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorKind::BencodeInvalidSyntax => write!(f, "BencodeInvalidSyntax"),
            ErrorKind::BencodeInvalidInteger => write!(f, "BencodeInvalidInteger"),
            ErrorKind::BencodeUnexpectedEof => write!(f, "BencodeUnexpectedEof"),
            ErrorKind::BencodeIntegerOverflow => write!(f, "BencodeIntegerOverflow"),
            ErrorKind::MetainfoMissingField => write!(f, "MetainfoMissingField"),
            ErrorKind::MetainfoInvalidField => write!(f, "MetainfoInvalidField"),
            ErrorKind::MetainfoInvalidPieces => write!(f, "MetainfoInvalidPieces"),
            ErrorKind::PeerInvalidHandshake => write!(f, "PeerInvalidHandshake"),
            ErrorKind::PeerInvalidMessage => write!(f, "PeerInvalidMessage"),
            ErrorKind::PeerConnectionClosed => write!(f, "PeerConnectionClosed"),
            ErrorKind::PeerInvalidExtendedMessage => write!(f, "PeerInvalidExtendedMessage"),
            ErrorKind::PeerInvalidPexMessage => write!(f, "PeerInvalidPexMessage"),
            ErrorKind::TrackerInvalidResponse => write!(f, "TrackerInvalidResponse"),
            ErrorKind::TrackerRequestFailed => write!(f, "TrackerRequestFailed"),
            ErrorKind::TrackerProtocolError => write!(f, "TrackerProtocolError"),
            ErrorKind::Io => write!(f, "Io"),
            ErrorKind::InvalidInput => write!(f, "InvalidInput"),
            ErrorKind::Protocol => write!(f, "Protocol"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|e| &**e as _)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self {
            source: Some(Box::new(e)),
            kind: ErrorKind::Io,
        }
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self { kind, source: None }
    }
}

impl Error {
    /// Create a new error with the given kind and no source.
    pub fn new(kind: ErrorKind) -> Self {
        Self { kind, source: None }
    }

    /// Create a new error with the given kind and an underlying source.
    pub fn with_source(
        kind: ErrorKind, source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
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

    /// Creates an I/O error with the given source.
    pub fn io(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Error {
        Error::with_source(ErrorKind::Io, source)
    }

    /// Creates a peer connection closed error.
    pub fn peer_closed(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Error::with_source(ErrorKind::PeerConnectionClosed, source)
    }

    /// Creates a tracker request failed error.
    pub fn tracker_failed(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Error::with_source(ErrorKind::TrackerRequestFailed, source)
    }

    /// Creates a protocol error.
    pub fn protocol(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Error::with_source(ErrorKind::Protocol, source)
    }

    /// Creates an invalid input error.
    pub fn invalid_input(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Error::with_source(ErrorKind::InvalidInput, source)
    }
}

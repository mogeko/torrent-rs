use std::fmt;

/// Top-level error type for the torrent library.
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

use crate::error::{Error, ErrorKind};
use crate::tracker::Url;

/// Convert a type into a tracker URL.
///
/// Inspired by `reqwest::IntoUrl`, this trait allows `HttpTracker::new()` and
/// `UdpTracker::new()` to accept `&str`, `String`, `&String`, or `Url`.
pub trait IntoUrl {
    /// Convert `self` into a `Url`.
    fn into_url(self) -> Result<Url, Error>;
}

impl IntoUrl for Url {
    fn into_url(self) -> Result<Url, Error> {
        Ok(self)
    }
}

impl IntoUrl for &str {
    fn into_url(self) -> Result<Url, Error> {
        Url::parse(self).map_err(|e| Error::with_source(ErrorKind::InvalidInput, e))
    }
}

impl IntoUrl for String {
    fn into_url(self) -> Result<Url, Error> {
        self.as_str().into_url()
    }
}

impl IntoUrl for &String {
    fn into_url(self) -> Result<Url, Error> {
        self.as_str().into_url()
    }
}

use crate::error::Error;

use super::Url;

/// Convert a type into a tracker URL.
///
/// Inspired by `reqwest::IntoUrl`, this trait allows `HttpTracker::new()`,
/// `UdpTracker::new()`, and all [`Tracker`](super::Tracker) constructors
/// to accept `&str`, `String`, `&String`, or `Url`.
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
        Url::parse(self).map_err(Error::invalid_input)
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

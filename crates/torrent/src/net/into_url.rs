use crate::error::Error;

use super::Url;

/// Convert a type into a [`Url`].
///
/// Inspired by `reqwest::IntoUrl`, this trait lets functions accept
/// multiple URL-like types (`&str`, `String`, `&String`, `Url`)
/// transparently, avoiding boilerplate `Url::parse` calls at every
/// call site.
///
/// # Provided Implementations
///
/// | From        | Behavior                          |
/// |-------------|-----------------------------------|
/// | [`Url`]     | Identity (no-op)                  |
/// | `&str`      | [`Url::parse`], maps to `Error::invalid_input` |
/// | `String`    | Delegates to `as_str()`           |
/// | `&String`   | Delegates to `as_str()`           |
///
/// # Examples
///
/// ```
/// use torrent::IntoUrl;
///
/// fn connect(url: impl IntoUrl) -> Result<(), torrent::error::Error> {
///     let url = url.into_url()?;
///     // ... use `url` ...
///     Ok(())
/// }
///
/// connect("https://example.com/announce")?;
/// connect(String::from("https://example.com/announce"))?;
/// # Ok::<(), torrent::error::Error>(())
/// ```
pub trait IntoUrl {
    /// Convert `self` into a [`Url`].
    ///
    /// # Errors
    ///
    /// Returns an error if the input is not a valid URL.
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

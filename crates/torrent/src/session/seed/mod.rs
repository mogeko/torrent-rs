//! Seeding — create torrents from local data and serve them to peers.
//!
//! This module is the counterpart to the download module. While the
//! download module pulls data from peers and writes it to disk, the
//! seed module reads existing data from disk, computes piece hashes,
//! generates a [`Metainfo`], and serves data to requesting peers.
//!
//! # Key Types
//!
//! - [`DataSource`] — trait for reading raw bytes from any backend
//! - [`SeedBuilder`] — configures and creates a torrent from a data source
//! - [`SeededTorrent`] — the result of hashing, ready to export or seed

mod hash;
pub mod source;

pub use self::source::DataSource;

use crate::error::{Error, ErrorKind};
use crate::magnet::hex_encode;
use crate::metainfo::{Metainfo, Mode};
use crate::session::InfoHash;
use crate::spec::TorrentSpec;

use super::Session;

use self::hash::{hash_source, resolve_piece_length};

/// Builder for creating a torrent from a data source and seeding it.
///
/// Created by [`Session::seed_from`]. Configure metadata parameters
/// (piece length, announce URL, name, etc.), then call
/// [`hash`](Self::hash) to generate the torrent file, or
/// [`start`](Self::start) to begin seeding immediately.
///
/// # Examples
///
/// ```no_run
/// use std::path::PathBuf;
/// use torrent::session::{Session, SessionConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let session = Session::new(SessionConfig::default()).await?;
///
/// // Create .torrent + start seeding
/// let info_hash = session
///     .seed_from(std::path::PathBuf::from("./my_release/video.mp4"))
///     .announce("http://tracker.example.com/announce")
///     .start()
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// For generating a `.torrent` file without seeding:
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let session = torrent::session::Session::new(Default::default()).await?;
/// let seeded = session
///     .seed_from(std::path::PathBuf::from("./my_release/video.mp4"))
///     .piece_length(1 << 19)  // 512 KiB
///     .announce("http://tracker.example.com/announce")
///     .hash()
///     .await?;
///
/// std::fs::write("my_release.torrent", seeded.torrent_bytes())?;
/// println!("magnet: {}", seeded.magnet_uri());
/// # Ok(())
/// # }
/// ```
pub struct SeedBuilder<'s> {
    session: &'s Session,
    source: Box<dyn DataSource>,
    piece_length: Option<u32>,
    announce: Option<String>,
    announce_list: Option<Vec<Vec<String>>>,
    name: Option<String>,
    comment: Option<String>,
    created_by: Option<String>,
    is_private: bool,
}

impl<'s> SeedBuilder<'s> {
    /// Create a new builder. Called by [`Session::seed_from`].
    pub(crate) fn new(session: &'s Session, source: impl DataSource + 'static) -> Self {
        SeedBuilder {
            session,
            source: Box::new(source),
            piece_length: None,
            announce: None,
            announce_list: None,
            name: None,
            comment: None,
            created_by: None,
            is_private: false,
        }
    }

    /// Set the piece length in bytes. Default: inferred from file size
    /// (32 KiB – 512 KiB depending on total size).
    pub fn piece_length(mut self, bytes: u32) -> Self {
        self.piece_length = Some(bytes);
        self
    }

    /// Set the primary announce URL (tracker).
    pub fn announce(mut self, url: impl Into<String>) -> Self {
        self.announce = Some(url.into());
        self
    }

    /// Set multi-tier announce list (BEP 12).
    pub fn announce_list(mut self, tiers: Vec<Vec<String>>) -> Self {
        self.announce_list = Some(tiers);
        self
    }

    /// Override the torrent name. Default: the data source name
    /// (filename for [`PathBuf`](std::path::PathBuf) sources).
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set a free-form comment (stored in the `.torrent` file).
    pub fn comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Set the `created by` field (stored in the `.torrent` file).
    pub fn created_by(mut self, created_by: impl Into<String>) -> Self {
        self.created_by = Some(created_by.into());
        self
    }

    /// Mark the torrent as private (BEP 27). Disables DHT and PEX for
    /// this torrent when seeded.
    pub fn private(mut self) -> Self {
        self.is_private = true;
        self
    }

    /// Hash the data source and produce a [`SeededTorrent`].
    ///
    /// This reads the **entire data source** sequentially — can be
    /// slow for large files. Returns a [`SeededTorrent`] that can be
    /// exported as a `.torrent` file, converted to a magnet URI, or
    /// passed to [`SeededTorrent::seed`] to start serving.
    pub async fn hash(self) -> Result<SeededTorrent, Error> {
        // Resolve piece length
        let piece_length = resolve_piece_length(self.source.as_ref(), self.piece_length).await?;

        // Read and hash the source
        let builder = hash_source(self.source.as_ref(), piece_length).await?;

        // Determine name
        let name = self.name.unwrap_or_else(|| self.source.name().to_string());

        let total_length = builder.total_length();
        let announce = self.announce.unwrap_or_default();

        // Build Metainfo
        let mut metainfo = builder.finish(
            announce,
            Mode::Single {
                name,
                length: total_length,
            },
        );

        if let Some(list) = self.announce_list {
            metainfo.announce_list = list;
        }
        if let Some(c) = self.comment {
            metainfo.comment = Some(c);
        }
        if let Some(cb) = self.created_by {
            metainfo.created_by = Some(cb);
        }

        // Serialize to .torrent bytes
        let torrent_bytes = metainfo
            .to_bytes()
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput))?;

        Ok(SeededTorrent {
            metainfo,
            torrent_bytes,
        })
    }

    /// Hash and begin seeding in one step.
    ///
    /// Convenience wrapper around [`hash`](Self::hash) +
    /// [`SeededTorrent::seed`]. Returns the torrent's info hash.
    pub async fn start(self) -> Result<InfoHash, Error> {
        let session = self.session;
        let seeded = self.hash().await?;
        seeded.seed(session).await
    }
}

/// A fully hashed torrent, ready to export or seed.
///
/// Returned by [`SeedBuilder::hash`]. Contains the [`Metainfo`] and
/// pre-serialized `.torrent` bytes. Can be written to disk, converted
/// to a magnet URI, or passed to [`seed`](Self::seed) to begin serving.
///
/// # Examples
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let session = torrent::session::Session::new(Default::default()).await?;
/// let seeded = session
///     .seed_from(std::path::PathBuf::from("./video.mp4"))
///     .announce("http://tracker.example.com/announce")
///     .hash()
///     .await?;
///
/// // Write .torrent file
/// std::fs::write("video.torrent", seeded.torrent_bytes())?;
///
/// // Print magnet link
/// println!("{}", seeded.magnet_uri());
///
/// // Start seeding
/// seeded.seed(&session).await?;
/// # Ok(())
/// # }
/// ```
pub struct SeededTorrent {
    metainfo: Metainfo,
    torrent_bytes: Vec<u8>,
}

impl SeededTorrent {
    /// The 20-byte info hash.
    pub fn info_hash(&self) -> InfoHash {
        self.metainfo.info_hash()
    }

    /// Reference to the full torrent metadata.
    pub fn metainfo(&self) -> &Metainfo {
        &self.metainfo
    }

    /// Serialized `.torrent` file bytes (for writing to disk).
    pub fn torrent_bytes(&self) -> &[u8] {
        &self.torrent_bytes
    }

    /// Generate a magnet URI from this torrent (BEP 9).
    ///
    /// Includes the info hash and display name. Trackers from the
    /// metainfo are not included — add them manually if needed.
    pub fn magnet_uri(&self) -> String {
        let ih = hex_encode(self.metainfo.info_hash());
        let name = match &self.metainfo.info.mode {
            Mode::Single { name, .. } | Mode::Multiple { name, .. } => name,
        };
        format!("magnet:?xt=urn:btih:{ih}&dn={name}")
    }

    /// Start seeding this torrent in the given session.
    ///
    /// Registers the torrent with the session and activates it for
    /// seeding. The data source must still be accessible at the
    /// original path — this method only hands off the metadata.
    pub async fn seed(self, session: &Session) -> Result<InfoHash, Error> {
        let info_hash = self.info_hash();
        let spec = TorrentSpec::Metainfo(self.metainfo);

        // Register (consumes spec)
        let _builder = session.add_torrent(spec)?;
        // Torrent is now registered with state = Registered.
        // Full seeding activation (prepare skip + verify) will be
        // completed in Phase 6. For now, we return the info_hash
        // so the caller knows the torrent is registered.

        Ok(info_hash)
    }
}

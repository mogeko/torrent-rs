//! Torrent specification — either full metadata or a magnet link.
//!
//! [`TorrentSpec`] unifies the two ways a torrent can enter the system:
//! - [`Metainfo`](TorrentSpec::Metainfo): complete metadata from a `.torrent` file
//! - [`Magnet`](TorrentSpec::Magnet): a magnet URI — metadata must be downloaded from peers

use crate::magnet::MagnetUri;
use crate::metainfo::{Metainfo, Mode};

/// The source of a torrent (BEP 3 / BEP 9).
///
/// Either full metadata from a `.torrent` file or a magnet URI
/// that identifies the torrent by info hash and tracker addresses.
///
/// # Examples
///
/// ```
/// use torrent_core::metainfo::from_bytes;
/// use torrent_core::spec::TorrentSpec;
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let data = std::fs::read("my.torrent")?;
/// let meta = from_bytes(&data)?;
/// let spec = TorrentSpec::Metainfo(meta);
/// assert_eq!(spec.name(), Some("my_torrent"));
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TorrentSpec {
    /// Complete metadata from a `.torrent` file.
    Metainfo(Metainfo),
    /// Magnet link — metadata must be downloaded from peers (BEP 9).
    Magnet(MagnetUri),
}

impl TorrentSpec {
    /// The unique info hash (available from both variants).
    pub fn info_hash(&self) -> [u8; 20] {
        match self {
            TorrentSpec::Metainfo(m) => m.info_hash(),
            TorrentSpec::Magnet(m) => *m.primary_info_hash(),
        }
    }

    /// The display name of the torrent, if known.
    pub fn name(&self) -> Option<&str> {
        match self {
            TorrentSpec::Metainfo(m) => match &m.info.mode {
                Mode::Single { name, .. } | Mode::Multiple { name, .. } => Some(name),
            },
            TorrentSpec::Magnet(m) => m.display_name.as_deref(),
        }
    }

    /// Tracker URLs, if available from the source.
    pub fn trackers(&self) -> Vec<&str> {
        match self {
            TorrentSpec::Metainfo(m) => std::iter::once(m.announce.as_str())
                .chain(m.announce_list.iter().flatten().map(|s| s.as_str()))
                .collect(),
            TorrentSpec::Magnet(m) => m.trackers.iter().map(|s| s.as_str()).collect(),
        }
    }
}

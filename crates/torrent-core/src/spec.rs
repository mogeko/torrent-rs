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
/// use torrent_core::metainfo::Metainfo;
/// use torrent_core::spec::TorrentSpec;
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let data = std::fs::read("my.torrent")?;
/// let meta = Metainfo::try_from(&data)?;
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

/// Wrap a [`Metainfo`] into a [`TorrentSpec`].
///
/// This allows passing `Metainfo` directly to `Session::add_torrent`
/// in the `torrent` crate.
impl From<Metainfo> for TorrentSpec {
    fn from(meta: Metainfo) -> Self {
        TorrentSpec::Metainfo(meta)
    }
}

/// Wrap a [`MagnetUri`] into a [`TorrentSpec`].
///
/// This allows passing `MagnetUri` directly to `Session::add_torrent`
/// in the `torrent` crate.
impl From<MagnetUri> for TorrentSpec {
    fn from(uri: MagnetUri) -> Self {
        TorrentSpec::Magnet(uri)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metainfo::{Bytes, FileInfo, Info, Metainfo, Mode, RawInfo};

    fn make_meta() -> Metainfo {
        Metainfo {
            announce: "http://tracker.example.com/announce".into(),
            announce_list: vec![vec!["http://t2.com/ann".into(), "http://t3.com/ann".into()]],
            info: Info {
                piece_length: 262144,
                pieces: vec![[0u8; 20]],
                mode: Mode::Single {
                    name: "test.txt".into(),
                    length: 1024,
                },
                raw_info: RawInfo::Bytes(Bytes::from_static(b"d4:infod...e")),
            },
            creation_date: None,
            comment: None,
            created_by: None,
            encoding: None,
        }
    }

    fn make_multi_meta() -> Metainfo {
        Metainfo {
            announce: "http://t.com/ann".into(),
            announce_list: vec![],
            info: Info {
                piece_length: 16384,
                pieces: vec![[0u8; 20], [0u8; 20]],
                mode: Mode::Multiple {
                    name: "root_dir".into(),
                    files: vec![FileInfo {
                        length: 512,
                        path: vec!["a.txt".into()],
                    }],
                },
                raw_info: RawInfo::Bytes(Bytes::from_static(b"d4:infod...e")),
            },
            creation_date: None,
            comment: None,
            created_by: None,
            encoding: None,
        }
    }

    #[test]
    fn from_metainfo_info_hash() {
        let meta = make_meta();
        let hash = meta.info_hash();
        let spec = TorrentSpec::from(meta);
        assert_eq!(spec.info_hash(), hash);
    }

    #[test]
    fn from_magnet_info_hash() {
        let uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"
            .parse::<MagnetUri>()
            .unwrap();
        let expected = *uri.primary_info_hash();
        let spec = TorrentSpec::from(uri);
        assert_eq!(spec.info_hash(), expected);
    }

    #[test]
    fn name_from_metainfo_single() {
        let spec = TorrentSpec::from(make_meta());
        assert_eq!(spec.name(), Some("test.txt"));
    }

    #[test]
    fn name_from_metainfo_multi() {
        let spec = TorrentSpec::from(make_multi_meta());
        assert_eq!(spec.name(), Some("root_dir"));
    }

    #[test]
    fn name_from_magnet_with_dn() {
        let uri = "magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&dn=Ubuntu+24.04"
            .parse::<MagnetUri>()
            .unwrap();
        let spec = TorrentSpec::from(uri);
        assert_eq!(spec.name(), Some("Ubuntu+24.04"));
    }

    #[test]
    fn name_from_magnet_without_dn() {
        let uri = "magnet:?xt=urn:btih:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            .parse::<MagnetUri>()
            .unwrap();
        let spec = TorrentSpec::from(uri);
        assert_eq!(spec.name(), None);
    }

    #[test]
    fn trackers_from_metainfo() {
        let spec = TorrentSpec::from(make_meta());
        let trackers = spec.trackers();
        assert_eq!(trackers.len(), 3);
        assert!(trackers.contains(&"http://tracker.example.com/announce"));
        assert!(trackers.contains(&"http://t2.com/ann"));
        assert!(trackers.contains(&"http://t3.com/ann"));
    }

    #[test]
    fn trackers_from_metainfo_no_list() {
        let spec = TorrentSpec::from(make_multi_meta());
        let trackers = spec.trackers();
        assert_eq!(trackers.len(), 1);
        assert_eq!(trackers[0], "http://t.com/ann");
    }

    #[test]
    fn trackers_from_magnet() {
        let uri = "magnet:?xt=urn:btih:cccccccccccccccccccccccccccccccccccccccc\
             &tr=http://t1.com/ann&tr=http://t2.com/ann"
            .parse::<MagnetUri>()
            .unwrap();
        let spec = TorrentSpec::from(uri);
        let trackers = spec.trackers();
        assert_eq!(trackers.len(), 2);
        assert_eq!(trackers[0], "http://t1.com/ann");
        assert_eq!(trackers[1], "http://t2.com/ann");
    }

    #[test]
    fn trackers_from_magnet_empty() {
        let uri = "magnet:?xt=urn:btih:dddddddddddddddddddddddddddddddddddddddd"
            .parse::<MagnetUri>()
            .unwrap();
        let spec = TorrentSpec::from(uri);
        assert!(spec.trackers().is_empty());
    }
}

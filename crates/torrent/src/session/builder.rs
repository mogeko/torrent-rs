//! Torrent builder — configures and activates a registered torrent.
//!
//! Created by [`Session::add_torrent`] (or its convenience wrappers).
//! The torrent is registered immediately; call [`start`](TorrentBuilder::start)
//! to create storage and begin downloading.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use crate::bencode::{decode as bencode_decode, encode as bencode_encode};
use crate::error::{Error, ErrorKind};
use crate::metainfo::Metainfo;
use crate::peer::metadata::{
    METADATA_PIECE_SIZE, MetadataData, MetadataRequest, UT_METADATA_EXT, UT_METADATA_ID,
};
use crate::peer::{ExtensionNegotiation, PeerConnection, PeerId, PeerMessage};
use crate::storage::{FileStorageFactory, StorageFactory};

use super::{InfoHash, Session};

/// Builder for configuring and activating a torrent.
///
/// Holds a reference to the [`Session`] — cannot outlive it.
pub struct TorrentBuilder<'s> {
    session: &'s Session,
    pub(crate) info_hash: InfoHash,
    storage_factory: Option<Arc<dyn StorageFactory>>,
    metadata_resolved: bool,
    /// Peers extracted from magnet URI x.pe (BEP 9). Injected in [`start`](Self::start).
    magnet_peers: Vec<SocketAddr>,
}

impl<'s> TorrentBuilder<'s> {
    /// Create a new builder. Called by [`Session::add_torrent`].
    pub(crate) fn new(
        session: &'s Session, info_hash: InfoHash, metadata_resolved: bool,
        magnet_peers: Vec<SocketAddr>,
    ) -> Self {
        TorrentBuilder {
            session,
            info_hash,
            storage_factory: None,
            metadata_resolved,
            magnet_peers,
        }
    }

    /// The 20-byte info hash of this torrent.
    pub fn info_hash(&self) -> InfoHash {
        self.info_hash
    }

    // ── Metadata resolution ──

    /// Ensure full metadata is available.
    ///
    /// For [`Metainfo`](crate::metainfo::Metainfo) torrents this is a no-op.
    /// For magnet links (BEP 9), downloads metadata from peers via
    /// the LTEP extension protocol (BEP 10).
    ///
    /// Idempotent: safe to call multiple times.
    pub async fn resolve_metadata(mut self) -> Result<Self, Error> {
        if self.metadata_resolved {
            return Ok(self);
        }

        let needs_resolve = {
            let torrents = self.session.torrents().read().unwrap();
            let Some(handle) = torrents.get(&self.info_hash) else {
                return Err(Error::new(ErrorKind::InvalidInput));
            };
            // Metainfo torrents have non-zero piece_length and non-empty pieces
            handle.metainfo.info.piece_length == 0
        };

        if needs_resolve {
            let addrs: Vec<SocketAddr> = std::mem::take(&mut self.magnet_peers);

            // If no peer addresses are available, skip resolution.
            // The download loop will discover peers via DHT/tracker
            // and can download metadata once connected.
            if addrs.is_empty() {
                self.metadata_resolved = true;
                return Ok(self);
            }

            // Download metadata from the first reachable peer
            let meta_bytes = download_metadata_from_peers(self.info_hash, &addrs).await?;

            // Parse and update the handle
            let new_meta = Metainfo::try_from(&meta_bytes[..])?;
            {
                let mut torrents = self.session.torrents().write().unwrap();

                match torrents.get_mut(&self.info_hash) {
                    Some(handle) => handle.metainfo = new_meta,
                    None => {
                        return Err(Error::new(ErrorKind::InvalidInput));
                    }
                }
            }
        }

        self.metadata_resolved = true;
        Ok(self)
    }

    // ── Storage configuration ──

    /// Bind a download directory. Internally creates
    /// [`FileStorageFactory::new(dir)`](FileStorageFactory::new).
    pub fn download_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.storage_factory = Some(Arc::new(FileStorageFactory::new(dir)));
        self
    }

    /// Inject a custom storage factory. Overrides any previous
    /// [`download_dir`](Self::download_dir) or [`storage`](Self::storage) call.
    pub fn storage(mut self, factory: Arc<dyn StorageFactory>) -> Self {
        self.storage_factory = Some(factory);
        self
    }

    // ── Activation ──

    /// Create storage and start the download/upload loop.
    pub async fn start(mut self) -> Result<InfoHash, Error> {
        // 0. Auto-resolve metadata if not already done
        if !self.metadata_resolved {
            self = self.resolve_metadata().await?;
        }

        // 0b. Inject magnet x.pe addresses into peer_mgr
        if !self.magnet_peers.is_empty() {
            let peer_mgr = {
                let torrents = self.session.torrents().read().unwrap();
                torrents.get(&self.info_hash).map(|h| h.peer_mgr.clone())
            };
            if let Some(peer_mgr) = peer_mgr {
                peer_mgr
                    .write()
                    .await
                    .add_peers(std::mem::take(&mut self.magnet_peers));
            }
        }

        // 1. Check active capacity (only counts torrents with running download loop)
        {
            let torrents = self.session.torrents().read().unwrap();
            let active_count = torrents.values().filter(|h| h.task.is_some()).count();
            let limit = self.session.config().max_active_torrents;
            if limit > 0 && active_count >= limit {
                return Err(Error::new(ErrorKind::InvalidInput));
            }
        }

        // 2. Resolve factory
        let factory = match &self.storage_factory {
            Some(f) => f.clone(),
            None => return Ok(self.info_hash), // Stay Registered
        };

        // 3. Get Info from registered handle
        let info = {
            let torrents = self.session.torrents().read().unwrap();

            match torrents.get(&self.info_hash) {
                Some(handle) => handle.metainfo.info.clone(),
                None => {
                    return Err(Error::new(ErrorKind::InvalidInput));
                }
            }
        };

        // 4. Create storage
        let storage = factory.create(&info).await?;

        // 5. Prepare (factory-defined resource allocation)
        storage.prepare().await?;

        // 6. Activate download loop
        {
            let mut torrents = self.session.torrents().write().unwrap();

            match torrents.get_mut(&self.info_hash) {
                Some(handle) => handle.activate(storage, self.session.config()),
                None => {
                    return Err(Error::new(ErrorKind::InvalidInput));
                }
            }
        }

        Ok(self.info_hash)
    }
}

/// Maximum number of peer connection attempts for metadata download.
const MAX_METADATA_PEERS: usize = 8;

/// Download full metainfo bytes from a magnet link peer (BEP 9/10).
///
/// Tries each peer address in order. On success, returns the raw
/// bencoded bytes of the info dictionary.
async fn download_metadata_from_peers(
    info_hash: [u8; 20], addrs: &[SocketAddr],
) -> Result<Vec<u8>, Error> {
    if addrs.is_empty() {
        return Err(Error::new(ErrorKind::InvalidInput));
    }

    let our_peer_id = PeerId::random();

    for &addr in &addrs[..addrs.len().min(MAX_METADATA_PEERS)] {
        match download_metadata_from_peer(addr, info_hash, our_peer_id).await {
            Ok(bytes) => return Ok(bytes),
            Err(e) => {
                tracing::debug!("metadata download from {} failed: {}", addr, e);
                continue;
            }
        }
    }

    Err(Error::new(ErrorKind::PeerConnectionClosed))
}

/// Connect to a single peer and download metadata via LTEP (BEP 10) + BEP 9.
async fn download_metadata_from_peer(
    addr: SocketAddr, info_hash: [u8; 20], our_peer_id: PeerId,
) -> Result<Vec<u8>, Error> {
    // 1. TCP connect + BEP 3 handshake
    let conn = PeerConnection::connect(addr, info_hash, our_peer_id).await?;

    // 2. Send LTEP handshake (ext_id 0) with ut_metadata extension
    let mut our_neg = ExtensionNegotiation::new();
    our_neg.add_extension(UT_METADATA_EXT, UT_METADATA_ID);
    let handshake_data = our_neg.to_bencode();
    let handshake_bytes = bencode_encode(&handshake_data);
    conn.send(&PeerMessage::Extended {
        ext_id: 0,
        data: handshake_bytes,
    })
    .await?;

    // 3. Receive remote LTEP handshake
    let msg = conn.recv().await?;
    let (remote_ext_id, metadata_size) = match msg {
        PeerMessage::Extended { ext_id: 0, data } => {
            let (ben, _) = bencode_decode(&data)
                .map_err(|_| Error::new(ErrorKind::PeerInvalidExtendedMessage))?;
            let neg = ExtensionNegotiation::from_bencode(&ben)
                .map_err(|_| Error::new(ErrorKind::PeerInvalidExtendedMessage))?;
            let ext_id = neg.m.get(UT_METADATA_EXT).copied();
            let size = neg.metadata_size.map(|s| s as u64);
            (ext_id, size)
        }
        _ => return Err(Error::new(ErrorKind::PeerInvalidExtendedMessage)),
    };

    let ext_id = remote_ext_id.ok_or_else(|| Error::new(ErrorKind::PeerInvalidExtendedMessage))?;
    let total_size =
        metadata_size.ok_or_else(|| Error::new(ErrorKind::PeerInvalidExtendedMessage))?;

    // 4. Calculate number of pieces
    let num_pieces = total_size.div_ceil(METADATA_PIECE_SIZE);

    // 5. Request and collect all pieces
    let mut buf = vec![0u8; total_size as usize];
    for piece_idx in 0..num_pieces as u32 {
        let req = MetadataRequest { piece: piece_idx };
        let req_ben = req.to_bencode();
        conn.send(&PeerMessage::Extended {
            ext_id,
            data: bencode_encode(&req_ben),
        })
        .await?;

        let resp = conn.recv().await?;
        match resp {
            PeerMessage::Extended {
                ext_id: resp_id,
                data,
            } if resp_id == ext_id => {
                // BEP 9: data contains bencoded dict prefix followed by raw piece data
                // Parse the bencoded dict to get piece index and total_size
                let (dict, raw_data) = split_bep9_data(&data)?;
                let (ben, _) = bencode_decode(&dict)
                    .map_err(|_| Error::new(ErrorKind::PeerInvalidExtendedMessage))?;

                if MetadataData::is_reject(&ben) {
                    return Err(Error::new(ErrorKind::PeerInvalidExtendedMessage));
                }

                let piece = MetadataData::from_bencode(&ben, raw_data)?;
                let offset = piece.piece as usize * METADATA_PIECE_SIZE as usize;
                let end = (offset + piece.data.len()).min(buf.len());
                buf[offset..end].copy_from_slice(&piece.data);
            }
            _ => return Err(Error::new(ErrorKind::PeerInvalidExtendedMessage)),
        }
    }

    Ok(buf)
}

/// Split BEP 9 extended message data into bencoded dict prefix and raw data.
///
/// BEP 9 specifies that metadata messages contain a bencoded dictionary
/// followed by the raw piece bytes (without any length prefix for the raw bytes).
/// We parse the bencoded portion, and the remainder is the raw data.
fn split_bep9_data(data: &[u8]) -> Result<(Vec<u8>, Vec<u8>), Error> {
    // Find the end of the bencoded dictionary (ends with 'e')
    // This is a simplification — a full recursive parser would be more robust
    let mut depth = 0i32;
    let mut dict_end = None;
    for (i, &b) in data.iter().enumerate() {
        match b {
            b'd' => depth += 1,
            b'e' => {
                depth -= 1;
                if depth == 0 {
                    dict_end = Some(i + 1);
                    break;
                }
            }
            b'l' => depth += 1,
            b'i' => {
                // Skip integer: find 'e' (depth unchanged for integers)
                let _end = data[i..].iter().position(|&c| c == b'e').unwrap_or(0);
            }
            _ => {}
        }
    }
    let end = dict_end.ok_or_else(|| Error::new(ErrorKind::PeerInvalidExtendedMessage))?;
    Ok((data[..end].to_vec(), data[end..].to_vec()))
}

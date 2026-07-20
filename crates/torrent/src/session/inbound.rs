//! Inbound connection handling — TCP accept and uTP SYN dispatch.
//!
//! This module manages incoming peer connections: accepts TCP connections
//! on the configured listen port, performs the BEP 3 handshake, and
//! dispatches established connections to the appropriate [`SwarmLoop`].

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

use crate::error::{Error, ErrorKind};
use crate::peer::utp::UtpStream;
use crate::peer::{Handshake, PeerConnection, PeerId};

use super::config::InfoHash;
use super::swarm::{TorrentCommand, TorrentHandle};

/// Timeout for the inbound BEP 3 handshake.
const INBOUND_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// Accept TCP connections and dispatch them to the appropriate torrent.
pub(crate) async fn accept_loop(
    listener: TcpListener, torrents: Arc<RwLock<HashMap<InfoHash, TorrentHandle>>>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                tracing::debug!("inbound TCP connection from {}", addr);
                handle_inbound_tcp(stream, addr, &torrents).await;
            }
            Err(e) => {
                tracing::warn!("TCP accept error: {}", e);
            }
        }
    }
}

/// Handle a single inbound TCP connection: handshake → dispatch.
async fn handle_inbound_tcp(
    mut stream: TcpStream, addr: SocketAddr,
    torrents: &Arc<RwLock<HashMap<InfoHash, TorrentHandle>>>,
) {
    // Phase 1: Read the remote handshake first (inbound path)
    let remote_handshake = match read_handshake(&mut stream, INBOUND_HANDSHAKE_TIMEOUT).await {
        Ok(hs) => hs,
        Err(e) => {
            tracing::debug!("inbound handshake read failed from {}: {}", addr, e);
            return;
        }
    };

    let info_hash = remote_handshake.info_hash;
    let remote_id = PeerId(remote_handshake.peer_id);
    let remote_reserved = remote_handshake.reserved;

    // Phase 2: Look up the torrent — extract control_tx, drop lock before I/O
    let control_tx = {
        let guard = torrents.read().unwrap();
        guard.get(&info_hash).and_then(|h| h.control_tx.clone())
    };

    let control_tx = match control_tx {
        Some(tx) => tx,
        None => {
            tracing::debug!("inbound connection for unknown info_hash from {}", addr);
            return;
        }
    };

    // Phase 3: Write our handshake (remote already read above)
    let conn = match PeerConnection::inbound(
        stream,
        remote_id,
        remote_reserved,
        info_hash,
        PeerId::random(),
    )
    .await
    {
        Ok(c) => Arc::new(c),
        Err(e) => {
            tracing::debug!("inbound handshake failed for {}: {}", addr, e);
            return;
        }
    };

    let _ = control_tx
        .send(TorrentCommand::InboundPeer(addr, conn))
        .await;
}

/// Read the BEP 3 handshake from a stream (inbound path: read-first).
async fn read_handshake(stream: &mut TcpStream, timeout: Duration) -> Result<Handshake, Error> {
    let mut buf = [0u8; 68];
    tokio::time::timeout(timeout, AsyncReadExt::read_exact(stream, &mut buf))
        .await
        .map_err(|_| Error::new(ErrorKind::PeerConnectionClosed))?
        .map_err(|e| Error::with_source(ErrorKind::PeerConnectionClosed, e))?;

    Handshake::from_bytes(&buf)
}

/// Handle an inbound uTP connection: handshake → dispatch.
pub(crate) async fn handle_inbound_utp(
    mut stream: UtpStream, addr: SocketAddr,
    torrents: &Arc<RwLock<HashMap<InfoHash, TorrentHandle>>>,
) {
    // Read remote handshake first
    let mut buf = [0u8; 68];
    let remote_handshake = match tokio::time::timeout(
        INBOUND_HANDSHAKE_TIMEOUT,
        AsyncReadExt::read_exact(&mut stream, &mut buf),
    )
    .await
    {
        Ok(Ok(_)) => match Handshake::from_bytes(&buf) {
            Ok(hs) => hs,
            Err(e) => {
                tracing::debug!("inbound uTP handshake parse failed from {}: {}", addr, e);
                return;
            }
        },
        _ => {
            tracing::debug!("inbound uTP handshake read failed from {}", addr);
            return;
        }
    };

    let info_hash = remote_handshake.info_hash;
    let remote_id = PeerId(remote_handshake.peer_id);
    let remote_reserved = remote_handshake.reserved;

    let control_tx = {
        let guard = torrents.read().unwrap();
        guard.get(&info_hash).and_then(|h| h.control_tx.clone())
    };

    let control_tx = match control_tx {
        Some(tx) => tx,
        None => {
            tracing::debug!("inbound uTP for unknown info_hash from {}", addr);
            return;
        }
    };

    let conn = match PeerConnection::inbound_utp(
        stream,
        remote_id,
        remote_reserved,
        info_hash,
        PeerId::random(),
    )
    .await
    {
        Ok(c) => Arc::new(c),
        Err(e) => {
            tracing::debug!("inbound uTP handshake failed for {}: {}", addr, e);
            return;
        }
    };

    let _ = control_tx
        .send(TorrentCommand::InboundPeer(addr, conn))
        .await;
}

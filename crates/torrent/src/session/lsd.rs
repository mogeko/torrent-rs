//! Local Service Discovery (LSD) — BEP 14.
//!
//! A best-effort background service that uses dual-stack UDP multicast
//! to announce our presence for active torrents and discover LAN peers.
//!
//! The service is spawned once per [`Session`](super::Session) and
//! automatically covers torrents added or removed after startup.

use std::collections::HashMap;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;

use crate::error::{Error, ErrorKind};
use crate::peer::lsd::{LSD_IPV4_MULTICAST, LSD_IPV6_MULTICAST, LSD_PORT, LsdAnnounce, LsdHost};
use crate::session::config::{InfoHash, SessionConfig};

use super::peer_mgr::PeerManager;
use super::swarm::TorrentHandle;

/// Maximum UDP datagram size we accept from LSD.
const MAX_LSD_DATAGRAM: usize = 1500;

/// Background LSD service — does not block [`Session::new`](super::Session::new).
pub(crate) struct LsdService {
    /// IPv4 multicast socket, if available.
    socket_v4: Option<UdpSocket>,
    /// IPv6 multicast socket, if available.
    socket_v6: Option<UdpSocket>,
    /// All active torrents (same Arc as DHT and session use).
    torrents: Arc<RwLock<HashMap<InfoHash, TorrentHandle>>>,
    /// Our TCP listening port (announced to LAN peers).
    listen_port: u16,
    /// Interval between announce broadcasts.
    announce_interval: Duration,
}

impl LsdService {
    /// Try to create the LSD service.
    ///
    /// Opens one UDP socket per address family that is available on
    /// this host.  If neither address family can bind, returns `Ok`
    /// with both sockets set to `None` — LSD silently degrades.
    pub(crate) fn new(
        config: &SessionConfig, torrents: Arc<RwLock<HashMap<InfoHash, TorrentHandle>>>,
    ) -> Result<Self, Error> {
        let socket_v4 = Self::bind_v4().ok();
        let socket_v6 = Self::bind_v6().ok();

        if socket_v4.is_none() && socket_v6.is_none() {
            tracing::warn!("LSD: neither IPv4 nor IPv6 multicast socket available, LSD disabled");
        } else {
            tracing::info!(
                "LSD: service started (v4={}, v6={})",
                socket_v4.is_some(),
                socket_v6.is_some()
            );
        }

        Ok(LsdService {
            socket_v4,
            socket_v6,
            torrents,
            listen_port: config.listen_port,
            announce_interval: config.lsd_interval,
        })
    }

    /// Bind an IPv4 UDP socket and join the LSD multicast group.
    fn bind_v4() -> Result<UdpSocket, Error> {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).map_err(|e| {
            tracing::warn!("LSD: failed to create IPv4 socket: {e}");
            Error::new(ErrorKind::Io)
        })?;

        socket
            .set_reuse_address(true)
            .map_err(|e| tracing::warn!("LSD: set_reuse_address(v4) failed: {e}"))
            .ok();

        let v4_addr: SocketAddr = (Ipv4Addr::UNSPECIFIED, LSD_PORT).into();
        socket.bind(&v4_addr.into()).map_err(|e| {
            tracing::warn!("LSD: failed to bind IPv4: {e}");
            Error::new(ErrorKind::Io)
        })?;

        socket
            .join_multicast_v4(&LSD_IPV4_MULTICAST, &Ipv4Addr::UNSPECIFIED)
            .map_err(|e| {
                tracing::warn!("LSD: failed to join IPv4 multicast group: {e}");
                Error::new(ErrorKind::Io)
            })?;

        socket
            .set_multicast_ttl_v4(2)
            .map_err(|e| tracing::warn!("LSD: set_multicast_ttl_v4(2) failed: {e}"))
            .ok();

        socket.set_nonblocking(true).map_err(|e| {
            tracing::warn!("LSD: set_nonblocking(v4) failed: {e}");
            Error::new(ErrorKind::Io)
        })?;

        let std_socket: std::net::UdpSocket = socket.into();
        tokio::net::UdpSocket::from_std(std_socket).map_err(|e| {
            tracing::warn!("LSD: failed to create tokio UdpSocket (v4): {e}");
            Error::new(ErrorKind::Io)
        })
    }

    /// Bind an IPv6 UDP socket and join the LSD multicast group.
    fn bind_v6() -> Result<UdpSocket, Error> {
        let socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP)).map_err(|e| {
            tracing::warn!("LSD: failed to create IPv6 socket: {e}");
            Error::new(ErrorKind::Io)
        })?;

        socket
            .set_reuse_address(true)
            .map_err(|e| tracing::warn!("LSD: set_reuse_address(v6) failed: {e}"))
            .ok();

        socket
            .set_only_v6(true)
            .map_err(|e| tracing::warn!("LSD: set_only_v6 failed: {e}"))
            .ok();

        let v6_addr: SocketAddr = (Ipv6Addr::UNSPECIFIED, LSD_PORT).into();
        socket.bind(&v6_addr.into()).map_err(|e| {
            tracing::warn!("LSD: failed to bind IPv6: {e}");
            Error::new(ErrorKind::Io)
        })?;

        socket
            .join_multicast_v6(&LSD_IPV6_MULTICAST, 0)
            .map_err(|e| {
                tracing::warn!("LSD: failed to join IPv6 multicast group: {e}");
                Error::new(ErrorKind::Io)
            })?;

        socket
            .set_multicast_hops_v6(2)
            .map_err(|e| tracing::warn!("LSD: set_multicast_hops_v6(2) failed: {e}"))
            .ok();

        socket.set_nonblocking(true).map_err(|e| {
            tracing::warn!("LSD: set_nonblocking(v6) failed: {e}");
            Error::new(ErrorKind::Io)
        })?;

        let std_socket: std::net::UdpSocket = socket.into();
        tokio::net::UdpSocket::from_std(std_socket).map_err(|e| {
            tracing::warn!("LSD: failed to create tokio UdpSocket (v6): {e}");
            Error::new(ErrorKind::Io)
        })
    }

    // ── Main loop ─────────────────────────────────────────────────────────

    /// Run the LSD service until the session is dropped.
    pub(crate) async fn run(&mut self) {
        let mut announce_tick = tokio::time::interval(self.announce_interval);

        // Fire an initial announce immediately (don't wait 5 min)
        self.broadcast_announce();

        loop {
            tokio::select! {
                // ── Incoming multicast announce ──
                result = self.recv_any() => {
                    if let Some((buf, src)) = result {
                        self.handle_incoming(&buf, src).await;
                    }
                }

                // ── Periodic announce ──
                _ = announce_tick.tick() => {
                    self.broadcast_announce();
                }
            }
        }
    }

    // ── Receive ───────────────────────────────────────────────────────────

    /// Receive from whichever socket (v4 or v6) has data first.
    async fn recv_any(&mut self) -> Option<(Vec<u8>, SocketAddr)> {
        match (&self.socket_v4, &self.socket_v6) {
            (Some(v4), Some(v6)) => {
                let mut buf_v4 = vec![0u8; MAX_LSD_DATAGRAM];
                let mut buf_v6 = vec![0u8; MAX_LSD_DATAGRAM];
                tokio::select! {
                    result = v4.recv_from(&mut buf_v4) => {
                        result.ok().map(|(n, src)| (buf_v4[..n].to_vec(), src))
                    }
                    result = v6.recv_from(&mut buf_v6) => {
                        result.ok().map(|(n, src)| (buf_v6[..n].to_vec(), src))
                    }
                }
            }
            (Some(v4), None) => {
                let mut buf = vec![0u8; MAX_LSD_DATAGRAM];
                v4.recv_from(&mut buf)
                    .await
                    .ok()
                    .map(|(n, src)| (buf[..n].to_vec(), src))
            }
            (None, Some(v6)) => {
                let mut buf = vec![0u8; MAX_LSD_DATAGRAM];
                v6.recv_from(&mut buf)
                    .await
                    .ok()
                    .map(|(n, src)| (buf[..n].to_vec(), src))
            }
            (None, None) => {
                tokio::time::sleep(Duration::from_secs(30)).await;
                None
            }
        }
    }

    /// Handle an incoming LSD announce.
    async fn handle_incoming(&mut self, data: &[u8], src: SocketAddr) {
        let announce = match LsdAnnounce::from_bytes(data) {
            Ok(a) => a,
            Err(_) => {
                tracing::trace!("LSD: malformed announce from {src}");
                return;
            }
        };

        let remote_port = announce.port;

        // Collect matching peer_mgr handles — must drop the torrents
        // read guard before awaiting on tokio RwLock below (std RwLock
        // guards are not Send, so they can't cross .await).
        let peer_mgrs: Vec<Arc<tokio::sync::RwLock<PeerManager>>> = {
            let torrents = self.torrents.read().unwrap();
            announce
                .info_hashes
                .iter()
                .filter_map(|ih| torrents.get(ih))
                .map(|h| h.peer_mgr.clone())
                .collect()
        };

        let peer_addr = SocketAddr::new(src.ip(), remote_port);
        for pm in peer_mgrs {
            pm.write().await.add_peers(vec![peer_addr]);
            tracing::trace!("LSD: discovered peer {peer_addr} from announce");
        }
    }

    // ── Announce ──────────────────────────────────────────────────────────

    /// Broadcast our presence for all active torrents on both multicast groups.
    fn broadcast_announce(&self) {
        let torrents = self.torrents.read().unwrap();
        if torrents.is_empty() {
            return;
        }
        let info_hashes: Vec<InfoHash> = torrents.keys().copied().collect();
        drop(torrents);

        // IPv4 announce
        if let Some(ref socket) = self.socket_v4 {
            let announce =
                LsdAnnounce::new(LsdHost::V4, self.listen_port).info_hashes(info_hashes.clone());
            if let Some(bytes) = announce.to_bytes() {
                let dst = SocketAddr::V4(SocketAddrV4::new(LSD_IPV4_MULTICAST, LSD_PORT));
                if let Err(e) = socket.try_send_to(&bytes, dst) {
                    tracing::warn!("LSD: failed to send IPv4 announce: {e}");
                } else {
                    tracing::trace!(
                        "LSD: sent IPv4 announce for {} torrent(s)",
                        info_hashes.len()
                    );
                }
            }
        }

        // IPv6 announce
        if let Some(ref socket) = self.socket_v6 {
            let announce = LsdAnnounce::new(LsdHost::V6, self.listen_port).info_hashes(info_hashes);
            if let Some(bytes) = announce.to_bytes() {
                let dst = SocketAddr::V6(SocketAddrV6::new(LSD_IPV6_MULTICAST, LSD_PORT, 0, 0));
                if let Err(e) = socket.try_send_to(&bytes, dst) {
                    tracing::warn!("LSD: failed to send IPv6 announce: {e}");
                } else {
                    tracing::trace!("LSD: sent IPv6 announce");
                }
            }
        }
    }
}

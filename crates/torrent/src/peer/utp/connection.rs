//! Per-connection uTP state machine (BEP 29).
//!
//! Manages the lifecycle of a single uTP connection:
//! - Connection establishment (SYN → STATE → connected)
//! - Reliable data transfer with sequence numbers and retransmission
//! - Congestion control via [`UtpCongestionControl`]
//! - Connection teardown (FIN → FIN → closed)
//!
//! Each connection is identified by a `connection_id` pair:
//! `conn_id_send` (what we put in outgoing packets) and
//! `conn_id_recv` (what we expect in incoming packets).

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::net::UdpSocket;
use tokio::sync::mpsc;

use super::{UtpCongestionControl, UtpHeader, UtpType};

/// Maximum number of retransmission attempts before giving up.
const MAX_RETRANSMIT: u32 = 10;

/// Retransmission check interval.
pub(crate) const RETRANSMIT_CHECK_INTERVAL: Duration = Duration::from_millis(100);

/// A packet that has been sent but not yet acknowledged.
#[derive(Debug)]
#[allow(dead_code)]
struct SentPacket {
    /// The raw bytes sent (header + payload).
    data: Vec<u8>,
    /// Sequence number of this packet.
    seq_nr: u16,
    /// When this packet was originally sent.
    send_time: Instant,
    /// Number of times this packet has been retransmitted.
    retransmit_count: u32,
    /// Whether RTT should be measured from this packet's ACK
    /// (only the first transmission counts, per BEP 29).
    rtt_measurable: bool,
}

/// A received uTP packet with its source address.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct UtpIncoming {
    pub header: UtpHeader,
    pub payload: Vec<u8>,
    pub src: SocketAddr,
}

/// Connection state (BEP 29 §connection setup).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnState {
    /// We initiated: sent SYN, waiting for STATE response.
    SynSent,
    /// We received SYN: sent STATE, waiting for DATA.
    SynRecv,
    /// Connection established, normal data transfer.
    Connected,
    /// We sent FIN, waiting for remote FIN.
    FinSent,
    /// Remote sent FIN, waiting for our FIN to be acked.
    FinRecv,
    /// Connection fully closed.
    Closed,
}

/// Per-connection uTP state machine.
///
/// Handles packet sending/receiving, retransmission, congestion control,
/// and the connection lifecycle for a single uTP connection.
pub struct UtpConnection {
    /// Remote peer address.
    remote_addr: SocketAddr,
    /// Connection ID we put in outgoing packets.
    conn_id_send: u16,
    /// Connection ID we expect in incoming packets.
    conn_id_recv: u16,
    /// Current connection state.
    state: ConnState,
    /// Next sequence number to use when sending.
    seq_nr: u16,
    /// Highest sequence number received (cumulative ACK).
    ack_nr: u16,
    /// Sequence number of FIN packet (when we receive FIN).
    eof_pkt: Option<u16>,
    /// Congestion control state.
    cc: UtpCongestionControl,
    /// Packets sent but not yet acknowledged (ordered by seq_nr).
    send_buffer: BTreeMap<u16, SentPacket>,
    /// Received data, ordered by sequence number for reassembly.
    recv_buffer: BTreeMap<u16, Vec<u8>>,
    /// Reassembled data ready for the application to read.
    ready_data: VecDeque<u8>,
    /// Time of last packet activity (send or receive).
    last_activity: Instant,
    /// Shared UDP socket for sending.
    socket: Arc<UdpSocket>,
    /// Sender for notifying UtpSocket of new outgoing packets
    /// (for the socket to track connection liveness).
    _outgoing_tx: mpsc::UnboundedSender<(SocketAddr, Vec<u8>)>,
}

impl UtpConnection {
    /// Create a new outbound connection (initiator side).
    ///
    /// Sends the initial ST_SYN packet and enters `SynSent` state.
    pub(crate) async fn connect(
        socket: Arc<UdpSocket>, remote_addr: SocketAddr,
        outgoing_tx: mpsc::UnboundedSender<(SocketAddr, Vec<u8>)>,
    ) -> Result<Self, String> {
        let conn_id_recv = rand::random::<u16>();
        let conn_id_send = conn_id_recv.wrapping_add(1);
        let _seq_nr = 1u16; // BEP 29: initial seq_nr = 1 (used in SYN packet)

        let mut conn = UtpConnection {
            remote_addr,
            conn_id_send,
            conn_id_recv,
            state: ConnState::SynSent,
            seq_nr: 2, // next seq_nr after SYN
            ack_nr: 0,
            eof_pkt: None,
            cc: UtpCongestionControl::new(),
            send_buffer: BTreeMap::new(),
            recv_buffer: BTreeMap::new(),
            ready_data: VecDeque::new(),
            last_activity: Instant::now(),
            socket,
            _outgoing_tx: outgoing_tx,
        };

        // Send SYN
        conn.send_packet(UtpType::StSyn, &[]).await?;
        tracing::debug!(
            "uTP: sent SYN conn_id_recv={} seq=1 to {}",
            conn_id_recv,
            remote_addr
        );

        Ok(conn)
    }

    /// Create a new inbound connection (responder side).
    ///
    /// Called when we receive a ST_SYN from a remote peer.
    /// Responds with ST_STATE and enters `SynRecv` state.
    pub(crate) fn accept(
        syn: &UtpHeader, socket: Arc<UdpSocket>, remote_addr: SocketAddr,
        outgoing_tx: mpsc::UnboundedSender<(SocketAddr, Vec<u8>)>,
    ) -> Self {
        // Responder: conn_id_send = syn.conn_id + 1, conn_id_recv = syn.conn_id
        let conn_id_send = syn.connection_id.wrapping_add(1);
        let conn_id_recv = syn.connection_id;
        let seq_nr = rand::random::<u16>();

        UtpConnection {
            remote_addr,
            conn_id_send,
            conn_id_recv,
            state: ConnState::SynRecv,
            seq_nr,             // next seq_nr for our first data packet
            ack_nr: syn.seq_nr, // ACK the SYN
            eof_pkt: None,
            cc: UtpCongestionControl::new(),
            send_buffer: BTreeMap::new(),
            recv_buffer: BTreeMap::new(),
            ready_data: VecDeque::new(),
            last_activity: Instant::now(),
            socket,
            _outgoing_tx: outgoing_tx,
        }
    }

    /// Send the ST_STATE response to accept a connection (responder side).
    #[allow(dead_code)]
    pub(crate) async fn send_state_response(&mut self) -> Result<(), String> {
        let ack = self.ack_nr;
        // ST_STATE: pure ACK, does NOT increment seq_nr
        let seq = self.seq_nr; // use current seq, don't bump
        self.send_raw(UtpType::StState, seq, ack, &[]).await?;
        tracing::debug!(
            "uTP: sent STATE seq={} ack={} to {}",
            seq,
            ack,
            self.remote_addr
        );
        Ok(())
    }

    /// Process an incoming uTP packet.
    ///
    /// Updates connection state, handles ACKs, and buffers payload data.
    pub(crate) async fn handle_packet(
        &mut self, header: &UtpHeader, payload: &[u8],
    ) -> Result<(), String> {
        self.last_activity = Instant::now();

        // Update delay measurement
        let now = Instant::now();
        // Simulate reply_micro: in a real implementation this would be
        // (now - header.timestamp) converted to microseconds.
        // For simplicity, use timestamp_difference from the header.
        self.cc
            .update_delay(header.timestamp_difference_microseconds, now);

        match self.state {
            ConnState::SynSent => {
                // Expect ST_STATE to complete handshake
                if header.utp_type == UtpType::StState {
                    self.state = ConnState::Connected;
                    self.ack_nr = header.seq_nr;
                    tracing::info!("uTP: connection established to {}", self.remote_addr);
                }
            }
            ConnState::SynRecv => {
                // Expect ST_DATA to complete handshake
                if header.utp_type == UtpType::StData {
                    self.state = ConnState::Connected;
                    self.ack_nr = header.seq_nr;
                    tracing::info!("uTP: connection established from {}", self.remote_addr);
                    // Process data from this packet
                    self.process_data_packet(header, payload);
                } else if header.utp_type == UtpType::StFin {
                    // Remote closing immediately after SYN?
                    self.handle_fin(header);
                }
            }
            ConnState::Connected => match header.utp_type {
                UtpType::StData => {
                    self.process_data_packet(header, payload);
                }
                UtpType::StState => {
                    self.handle_ack(header);
                }
                UtpType::StFin => {
                    self.process_data_packet(header, payload);
                    self.handle_fin(header);
                }
                UtpType::StReset => {
                    tracing::warn!("uTP: received RST from {}", self.remote_addr);
                    self.state = ConnState::Closed;
                }
                _ => {}
            },
            ConnState::FinSent => {
                // Waiting for remote FIN
                if header.utp_type == UtpType::StFin {
                    self.state = ConnState::Closed;
                    tracing::info!("uTP: connection closed with {}", self.remote_addr);
                }
                // Still process ACKs
                self.handle_ack(header);
            }
            ConnState::FinRecv => {
                // Our FIN was sent, waiting for remote to close
                self.handle_ack(header);
                if self.send_buffer.is_empty() {
                    // All our packets (including FIN) are acked
                    self.state = ConnState::Closed;
                }
            }
            ConnState::Closed => {
                // Ignore further packets
            }
        }

        Ok(())
    }

    /// Process a ST_DATA packet: buffer data, handle ACK, send Selective ACK if needed.
    fn process_data_packet(&mut self, header: &UtpHeader, payload: &[u8]) {
        let pkt_seq = header.seq_nr;

        // Handle cumulative ACK from this packet
        self.handle_ack(header);

        // Don't re-buffer data we already have
        if pkt_seq <= self.ack_nr {
            return;
        }

        // Check if this is the next expected packet
        if pkt_seq == self.ack_nr.wrapping_add(1) {
            // In-order: advance ack_nr and deliver
            self.ack_nr = pkt_seq;
            self.ready_data.extend(payload);

            // Deliver any subsequent in-order packets from recv_buffer
            let mut next = pkt_seq.wrapping_add(1);
            while let Some(data) = self.recv_buffer.remove(&next) {
                self.ack_nr = next;
                self.ready_data.extend(data);
                next = next.wrapping_add(1);
            }
        } else if pkt_seq > self.ack_nr.wrapping_add(1) {
            // Out-of-order: buffer it
            self.recv_buffer.insert(pkt_seq, payload.to_vec());
        }
        // If pkt_seq <= ack_nr, it's a duplicate — already handled above
    }

    /// Handle the ACK field in a received packet.
    ///
    /// Removes acknowledged packets from the send buffer and updates
    /// congestion control.
    fn handle_ack(&mut self, header: &UtpHeader) {
        let remote_ack = header.ack_nr;

        // Remove acknowledged packets from send buffer
        // All packets with seq_nr <= remote_ack are acknowledged.
        // In uTP, ack_nr is the highest received seq, so all < ack_nr are acked.
        let acked_seqs: Vec<u16> = self
            .send_buffer
            .keys()
            .filter(|&&seq| {
                // u16 wrapping comparison: is seq "less than or equal to" remote_ack?
                seq.wrapping_sub(remote_ack) == 0 || seq.wrapping_sub(remote_ack) > 0x8000
            })
            .copied()
            .collect();

        for seq in acked_seqs {
            if let Some(pkt) = self.send_buffer.remove(&seq) {
                // Measure RTT for first transmission only
                if pkt.rtt_measurable {
                    let rtt_ms = pkt.send_time.elapsed().as_millis() as u32;
                    self.cc.update_rtt(rtt_ms);
                }
                // Remove from in-flight count
                self.cc.remove_in_flight(pkt.data.len() as u32);
                // Count as an ACK for loss detection
            }
        }

        // Process Selective ACK if present
        if header.extension == 1 {
            // Selective ACK extension — would be parsed separately
            // For now, skip extension parsing here (handled at socket level)
            self.handle_selective_ack(header);
        }

        // Adjust congestion window after ACK processing
        self.cc.adjust_window();
    }

    /// Process Selective ACK bits for loss detection.
    fn handle_selective_ack(&mut self, _header: &UtpHeader) {
        // Selective ACK processing would go here.
        // Each set bit in the Selective ACK counts as a duplicate ACK.
        // 3+ duplicate ACKs for a given seq_nr triggers retransmission.
        //
        // For the initial implementation, we rely on timeout-based
        // retransmission and skip Selective ACK loss detection.
    }

    /// Handle a FIN packet: record eof_pkt.
    fn handle_fin(&mut self, header: &UtpHeader) {
        self.eof_pkt = Some(header.seq_nr);
        if self.state == ConnState::Connected {
            self.state = ConnState::FinRecv;
        }
    }

    /// Send application-level data to the remote peer.
    ///
    /// Splits data into uTP-sized packets and sends them.
    /// Each ST_DATA packet increments seq_nr.
    pub async fn send(&mut self, data: &[u8]) -> Result<usize, String> {
        if self.state != ConnState::Connected {
            return Err("uTP: not connected".into());
        }

        let packet_size = self.cc.packet_size().max(150) as usize;
        let mut total_sent = 0usize;

        for chunk in data.chunks(packet_size) {
            let seq = self.seq_nr;
            self.seq_nr = self.seq_nr.wrapping_add(1);
            self.send_raw(UtpType::StData, seq, self.ack_nr, chunk)
                .await?;
            total_sent += chunk.len();
        }

        Ok(total_sent)
    }

    /// Close the connection gracefully (send FIN).
    pub async fn close(&mut self) -> Result<(), String> {
        if self.state == ConnState::Connected {
            let seq = self.seq_nr;
            self.seq_nr = self.seq_nr.wrapping_add(1);
            self.send_raw(UtpType::StFin, seq, self.ack_nr, &[]).await?;
            self.state = ConnState::FinSent;
        }
        Ok(())
    }

    /// Read reassembled application data.
    ///
    /// Returns all available bytes from the ready buffer.
    pub fn recv(&mut self) -> Vec<u8> {
        let mut data = Vec::with_capacity(self.ready_data.len());
        while let Some(byte) = self.ready_data.pop_front() {
            data.push(byte);
        }
        data
    }

    /// Check if there is data available to read.
    pub fn has_data(&self) -> bool {
        !self.ready_data.is_empty()
    }

    /// Check if the connection is established.
    pub fn is_connected(&self) -> bool {
        self.state == ConnState::Connected
    }

    /// Check if the connection is closed.
    pub fn is_closed(&self) -> bool {
        self.state == ConnState::Closed
    }

    /// Get the connection ID we use for sending.
    pub fn conn_id_send(&self) -> u16 {
        self.conn_id_send
    }

    /// Get the connection ID we expect in incoming packets.
    pub fn conn_id_recv(&self) -> u16 {
        self.conn_id_recv
    }

    /// Get the remote peer address.
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    /// Run the retransmission timer check.
    ///
    /// Should be called periodically (every ~100ms). Checks for:
    /// - Packets that have timed out and need retransmission
    /// - Overall connection timeout (no activity for too long)
    pub async fn check_retransmit(&mut self) -> Result<(), String> {
        let now = Instant::now();
        let timeout_ms = self.cc.timeout_ms() as u64;

        // Check overall connection timeout
        if self.last_activity.elapsed().as_millis() as u64 > timeout_ms * 4 {
            // Connection is dead
            self.state = ConnState::Closed;
            return Err("uTP: connection timed out".into());
        }

        // Check individual packet timeouts
        let timeout_dur = Duration::from_millis(timeout_ms);
        let timed_out_seqs: Vec<u16> = self
            .send_buffer
            .iter()
            .filter(|(_, pkt)| pkt.send_time.elapsed() >= timeout_dur)
            .map(|(&seq, _)| seq)
            .collect();

        for seq in timed_out_seqs {
            if let Some(pkt) = self.send_buffer.get(&seq) {
                if pkt.retransmit_count >= MAX_RETRANSMIT {
                    tracing::warn!("uTP: max retransmits reached for seq={}, closing", seq);
                    self.state = ConnState::Closed;
                    return Err("uTP: max retransmits reached".into());
                }
            }

            // Retransmit
            if let Some(mut pkt) = self.send_buffer.remove(&seq) {
                pkt.retransmit_count += 1;
                pkt.rtt_measurable = false; // don't measure RTT on retransmit
                pkt.send_time = now;
                tracing::debug!(
                    "uTP: retransmitting seq={} (attempt {})",
                    seq,
                    pkt.retransmit_count
                );

                self.socket
                    .send_to(&pkt.data, self.remote_addr)
                    .await
                    .map_err(|e| format!("uTP: send error: {e}"))?;

                self.send_buffer.insert(seq, pkt);
            }

            // Apply congestion control: timeout halves window
            self.cc.on_timeout();
        }

        // If we're in SynSent state and have timed out, resend SYN
        if self.state == ConnState::SynSent
            && self.send_buffer.is_empty()
            && self.last_activity.elapsed() > timeout_dur
        {
            // Resend SYN
            self.send_packet(UtpType::StSyn, &[]).await?;
        }

        Ok(())
    }

    /// Send a packet with full header construction.
    async fn send_packet(&mut self, utp_type: UtpType, payload: &[u8]) -> Result<(), String> {
        let seq = if utp_type.increments_seq_nr() {
            let s = self.seq_nr;
            self.seq_nr = self.seq_nr.wrapping_add(1);
            s
        } else {
            self.seq_nr // ST_STATE: don't increment
        };

        self.send_raw(utp_type, seq, self.ack_nr, payload).await
    }

    /// Send raw uTP data with explicit seq/ack.
    async fn send_raw(
        &mut self, utp_type: UtpType, seq_nr: u16, ack_nr: u16, payload: &[u8],
    ) -> Result<(), String> {
        let now = Instant::now();
        let timestamp_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u32;

        let header = UtpHeader {
            utp_type,
            version: 1,
            extension: 0,
            connection_id: self.conn_id_send,
            timestamp_microseconds: timestamp_us,
            timestamp_difference_microseconds: self.cc.reply_micro(),
            wnd_size: 1_000_000, // Large receive window
            seq_nr,
            ack_nr,
        };

        let header_bytes = header.to_bytes();
        let mut packet = Vec::with_capacity(header_bytes.len() + payload.len());
        packet.extend_from_slice(&header_bytes);
        packet.extend_from_slice(payload);

        // Track in-flight bytes for congestion control
        self.cc.add_in_flight(packet.len() as u32);

        // Build SentPacket for retransmission tracking
        let sent = SentPacket {
            data: packet.clone(),
            seq_nr,
            send_time: now,
            retransmit_count: 0,
            rtt_measurable: utp_type != UtpType::StSyn,
        };
        self.send_buffer.insert(seq_nr, sent);

        self.socket
            .send_to(&packet, self.remote_addr)
            .await
            .map_err(|e| format!("uTP: send error: {e}"))?;

        self.last_activity = now;

        Ok(())
    }
}

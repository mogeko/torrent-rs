use std::net::{Ipv4Addr, SocketAddr};

use bytes::Bytes;

use crate::bencode::{self, Bencode};
use crate::error::{Error, ErrorKind};

/// Transaction ID type (2-byte random value).
pub type TransactionId = [u8; 2];

/// KRPC message types (BEP 5).
///
/// Each message is a bencoded dictionary with the following structure:
///
/// ```text
/// Query:  {"t": "<2-byte id>", "y": "q", "q": "<method>", "a": <args>}
/// Response: {"t": "<2-byte id>", "y": "r", "r": <result>}
/// Error:  {"t": "<2-byte id>", "y": "e", "e": [<code>, <msg>]}
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KrpcMessage {
    Query {
        transaction_id: TransactionId,
        method: String,
        args: Bencode,
    },
    Response {
        transaction_id: TransactionId,
        result: Bencode,
    },
    Error {
        transaction_id: TransactionId,
        code: i64,
        message: String,
    },
}

impl KrpcMessage {
    /// Encode a KRPC message to bencoded bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        tracing::trace!("encoding KRPC message: {:?}", self);
        let dict = match self {
            KrpcMessage::Query {
                transaction_id,
                method,
                args,
            } => Bencode::Dict(vec![
                (
                    t_key(),
                    Bencode::Bytes(Bytes::copy_from_slice(transaction_id)),
                ),
                (y_key(), Bencode::Bytes(Bytes::copy_from_slice(b"q"))),
                (
                    q_key(),
                    Bencode::Bytes(Bytes::copy_from_slice(method.as_bytes())),
                ),
                (a_key(), args.clone()),
            ]),
            KrpcMessage::Response {
                transaction_id,
                result,
            } => Bencode::Dict(vec![
                (
                    t_key(),
                    Bencode::Bytes(Bytes::copy_from_slice(transaction_id)),
                ),
                (y_key(), Bencode::Bytes(Bytes::copy_from_slice(b"r"))),
                (r_key(), result.clone()),
            ]),
            KrpcMessage::Error {
                transaction_id,
                code,
                message,
            } => Bencode::Dict(vec![
                (
                    t_key(),
                    Bencode::Bytes(Bytes::copy_from_slice(transaction_id)),
                ),
                (y_key(), Bencode::Bytes(Bytes::copy_from_slice(b"e"))),
                (
                    e_key(),
                    Bencode::List(vec![
                        Bencode::Integer(*code),
                        Bencode::Bytes(Bytes::copy_from_slice(message.as_bytes())),
                    ]),
                ),
            ]),
        };
        bencode::encode(&dict)
    }

    /// Decode a KRPC message from bencoded bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, Error> {
        tracing::trace!("decoding KRPC message ({} bytes)", data.len());
        let (val, _rest) = bencode::decode(data)?;
        Self::from_bencode(&val)
    }

    /// Decode a KRPC message from a bencoded value.
    pub fn from_bencode(val: &Bencode) -> Result<Self, Error> {
        let t = dict_get_bytes(val, b"t").ok_or(Error::new(ErrorKind::Protocol))?;
        let mut transaction_id = [0u8; 2];
        let len = std::cmp::min(t.len(), 2);
        transaction_id[..len].copy_from_slice(&t[..len]);

        let y = dict_get_bytes(val, b"y").ok_or(Error::new(ErrorKind::Protocol))?;
        let y_byte = if !y.is_empty() { y[0] } else { 0 };

        match y_byte {
            b'q' => {
                let method = dict_get_bytes(val, b"q")
                    .and_then(|b| String::from_utf8(b.to_vec()).ok())
                    .ok_or(Error::new(ErrorKind::Protocol))?;
                let args = dict_get(val, b"a")
                    .cloned()
                    .unwrap_or(Bencode::Dict(vec![]));
                Ok(KrpcMessage::Query {
                    transaction_id,
                    method,
                    args,
                })
            }
            b'r' => {
                let result = dict_get(val, b"r")
                    .cloned()
                    .unwrap_or(Bencode::Dict(vec![]));
                Ok(KrpcMessage::Response {
                    transaction_id,
                    result,
                })
            }
            b'e' => {
                let err_val = dict_get(val, b"e").ok_or(Error::new(ErrorKind::Protocol))?;
                match err_val {
                    Bencode::List(items) if items.len() >= 2 => {
                        let code = match &items[0] {
                            Bencode::Integer(c) => *c,
                            _ => return Err(Error::new(ErrorKind::Protocol)),
                        };
                        let message = match &items[1] {
                            Bencode::Bytes(b) => String::from_utf8(b.to_vec()).unwrap_or_default(),
                            _ => return Err(Error::new(ErrorKind::Protocol)),
                        };
                        Ok(KrpcMessage::Error {
                            transaction_id,
                            code,
                            message,
                        })
                    }
                    _ => Err(Error::new(ErrorKind::Protocol)),
                }
            }
            _ => Err(Error::new(ErrorKind::Protocol)),
        }
    }
}

// ── Build helpers ────────────────────────────────────────────────────

/// Build a ping query (BEP 5).
///
/// Creates a KRPC `ping` query message with the given transaction ID
/// and node ID. The result is bencoded bytes ready to send over UDP.
pub fn build_ping(tid: TransactionId, node_id: &[u8; 20]) -> Vec<u8> {
    KrpcMessage::Query {
        transaction_id: tid,
        method: "ping".into(),
        args: Bencode::Dict(vec![(
            id_key(),
            Bencode::Bytes(Bytes::copy_from_slice(node_id)),
        )]),
    }
    .to_bytes()
}

/// Build a find_node query (BEP 5).
///
/// Creates a KRPC `find_node` query for discovering nodes close to
/// a target ID. Used during the DHT bootstrap and recursive lookup process.
pub fn build_find_node(tid: TransactionId, node_id: &[u8; 20], target: &[u8; 20]) -> Vec<u8> {
    KrpcMessage::Query {
        transaction_id: tid,
        method: "find_node".into(),
        args: Bencode::Dict(vec![
            (id_key(), Bencode::Bytes(Bytes::copy_from_slice(node_id))),
            (target_key(), Bencode::Bytes(Bytes::copy_from_slice(target))),
        ]),
    }
    .to_bytes()
}

/// Build a get_peers query (BEP 5).
///
/// Creates a KRPC `get_peers` query to discover peers sharing a torrent
/// identified by `info_hash`. The response may contain peer addresses
/// or closer DHT nodes.
pub fn build_get_peers(tid: TransactionId, node_id: &[u8; 20], info_hash: &[u8; 20]) -> Vec<u8> {
    KrpcMessage::Query {
        transaction_id: tid,
        method: "get_peers".into(),
        args: Bencode::Dict(vec![
            (id_key(), Bencode::Bytes(Bytes::copy_from_slice(node_id))),
            (
                info_hash_key(),
                Bencode::Bytes(Bytes::copy_from_slice(info_hash)),
            ),
        ]),
    }
    .to_bytes()
}

/// Build an announce_peer query (BEP 5).
///
/// Creates a KRPC `announce_peer` query that tells a DHT node we are
/// downloading the torrent identified by `info_hash` on the given `port`.
/// Requires a `token` obtained from a previous `get_peers` response.
pub fn build_announce_peer(
    tid: TransactionId,
    node_id: &[u8; 20],
    info_hash: &[u8; 20],
    port: u16,
    token: &[u8],
) -> Vec<u8> {
    KrpcMessage::Query {
        transaction_id: tid,
        method: "announce_peer".into(),
        args: Bencode::Dict(vec![
            (id_key(), Bencode::Bytes(Bytes::copy_from_slice(node_id))),
            (
                info_hash_key(),
                Bencode::Bytes(Bytes::copy_from_slice(info_hash)),
            ),
            (Bytes::from("port"), Bencode::Integer(port as i64)),
            (token_key(), Bencode::Bytes(Bytes::copy_from_slice(token))),
        ]),
    }
    .to_bytes()
}

// ── Response parsing helpers ─────────────────────────────────────────

/// Parse a ping response.
///
/// Expects a response dict containing `{"id": <20-byte node ID>}`.
///
/// # Errors
///
/// Returns an error if the message is not a response or is missing the `id` field.
pub fn parse_ping_response(msg: &KrpcMessage) -> Result<[u8; 20], Error> {
    match msg {
        KrpcMessage::Response { result, .. } => {
            let node_id = dict_get_bytes(result, b"id").ok_or(Error::new(ErrorKind::Protocol))?;
            let mut id = [0u8; 20];
            let len = std::cmp::min(node_id.len(), 20);
            id[..len].copy_from_slice(&node_id[..len]);
            Ok(id)
        }
        _ => Err(Error::new(ErrorKind::Protocol)),
    }
}

/// Result of a get_peers DHT query (BEP 5).
///
/// Two outcomes are possible:
/// - [`Values`](GetPeersResult::Values): the node returned peer addresses
///   and a token for later `announce_peer` calls
/// - [`Nodes`](GetPeersResult::Nodes): the node returned closer DHT nodes
///   for continued recursive lookup
#[derive(Debug, Clone)]
pub enum GetPeersResult {
    /// Token + list of SocketAddr.
    Values {
        token: Vec<u8>,
        peers: Vec<SocketAddr>,
    },
    /// Closer nodes in compact format.
    Nodes(Vec<super::Node>),
}

/// Parse a get_peers response (BEP 5).
///
/// Handles both possible responses:
/// - `values` key present → returns [`GetPeersResult::Values`] with token and peers
/// - `nodes` key present → returns [`GetPeersResult::Nodes`] with closer nodes
///
/// # Errors
///
/// Returns an error if the message is not a response or contains neither
/// `values` nor `nodes`.
pub fn parse_get_peers_response(msg: &KrpcMessage) -> Result<GetPeersResult, Error> {
    match msg {
        KrpcMessage::Response { result, .. } => {
            let token = dict_get_bytes(result, b"token")
                .map(|b| b.to_vec())
                .ok_or(Error::new(ErrorKind::Protocol))?;

            // Check for "values" field (list of compact peers)
            if let Some(Bencode::List(values)) = dict_get(result, b"values") {
                let mut peers = Vec::new();
                for v in values {
                    if let Bencode::Bytes(b) = v
                        && b.len() == 6
                    {
                        let ip = Ipv4Addr::new(b[0], b[1], b[2], b[3]);
                        let port = u16::from_be_bytes([b[4], b[5]]);
                        peers.push(SocketAddr::new(std::net::IpAddr::V4(ip), port));
                    }
                }
                return Ok(GetPeersResult::Values { token, peers });
            }

            // Check for "nodes" field (compact node info)
            if let Some(nodes_bytes) = dict_get_bytes(result, b"nodes") {
                let nodes = parse_compact_nodes(nodes_bytes);
                return Ok(GetPeersResult::Nodes(nodes));
            }

            Err(Error::new(ErrorKind::Protocol))
        }
        _ => Err(Error::new(ErrorKind::Protocol)),
    }
}

/// Parse compact node info (BEP 5).
///
/// Each node is 26 bytes: 20-byte node ID + 4-byte IPv4 address + 2-byte port.
/// Incomplete trailing bytes are silently ignored.
pub fn parse_compact_nodes(data: &[u8]) -> Vec<super::Node> {
    data.chunks_exact(26)
        .map(|chunk| {
            let mut id = [0u8; 20];
            id.copy_from_slice(&chunk[..20]);
            let ip = Ipv4Addr::new(chunk[20], chunk[21], chunk[22], chunk[23]);
            let port = u16::from_be_bytes([chunk[24], chunk[25]]);
            super::Node {
                id,
                addr: SocketAddr::new(std::net::IpAddr::V4(ip), port),
            }
        })
        .collect()
}

// ── Helpers ──────────────────────────────────────────────────────────

fn t_key() -> Bytes {
    Bytes::from("t")
}
fn y_key() -> Bytes {
    Bytes::from("y")
}
fn q_key() -> Bytes {
    Bytes::from("q")
}
fn a_key() -> Bytes {
    Bytes::from("a")
}
fn r_key() -> Bytes {
    Bytes::from("r")
}
fn e_key() -> Bytes {
    Bytes::from("e")
}
fn id_key() -> Bytes {
    Bytes::from("id")
}
fn target_key() -> Bytes {
    Bytes::from("target")
}
fn info_hash_key() -> Bytes {
    Bytes::from("info_hash")
}
fn token_key() -> Bytes {
    Bytes::from("token")
}

fn dict_get<'a>(val: &'a Bencode, key: &[u8]) -> Option<&'a Bencode> {
    match val {
        Bencode::Dict(entries) => entries
            .iter()
            .find(|(k, _)| k.as_ref() == key)
            .map(|(_, v)| v),
        _ => None,
    }
}

pub fn dict_get_bytes<'a>(val: &'a Bencode, key: &[u8]) -> Option<&'a [u8]> {
    match dict_get(val, key)? {
        Bencode::Bytes(b) => Some(b),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn krpc_ping_roundtrip() {
        let tid = [0xAB, 0xCD];
        let node_id = [0x42u8; 20];
        let bytes = build_ping(tid, &node_id);

        let msg = KrpcMessage::from_bytes(&bytes).unwrap();
        match &msg {
            KrpcMessage::Query {
                transaction_id,
                method,
                ..
            } => {
                assert_eq!(*transaction_id, tid);
                assert_eq!(method, "ping");
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn krpc_find_node_roundtrip() {
        let tid = [0x12, 0x34];
        let node_id = [0x11u8; 20];
        let target = [0x22u8; 20];
        let bytes = build_find_node(tid, &node_id, &target);

        let msg = KrpcMessage::from_bytes(&bytes).unwrap();
        match &msg {
            KrpcMessage::Query { method, .. } => {
                assert_eq!(method, "find_node");
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn krpc_response_roundtrip() {
        let tid = [0xFF, 0xEE];
        let msg = KrpcMessage::Response {
            transaction_id: tid,
            result: Bencode::Dict(vec![(
                Bytes::from("id"),
                Bencode::Bytes(Bytes::copy_from_slice(&[0x55u8; 20])),
            )]),
        };
        let bytes = msg.to_bytes();
        let decoded = KrpcMessage::from_bytes(&bytes).unwrap();
        match decoded {
            KrpcMessage::Response {
                transaction_id,
                result,
            } => {
                assert_eq!(transaction_id, tid);
                let id = dict_get_bytes(&result, b"id").unwrap();
                assert_eq!(id, &[0x55u8; 20]);
            }
            _ => panic!("expected response"),
        }
    }

    #[test]
    fn krpc_error_roundtrip() {
        let msg = KrpcMessage::Error {
            transaction_id: [0x01, 0x02],
            code: 203,
            message: "Server Error".into(),
        };
        let bytes = msg.to_bytes();
        let decoded = KrpcMessage::from_bytes(&bytes).unwrap();
        match decoded {
            KrpcMessage::Error { code, message, .. } => {
                assert_eq!(code, 203);
                assert_eq!(message, "Server Error");
            }
            _ => panic!("expected error"),
        }
    }

    #[test]
    fn test_parse_compact_nodes() {
        let mut data = Vec::new();
        // Node 1: id + 127.0.0.1:6881
        data.extend_from_slice(&[0x01u8; 20]);
        data.extend_from_slice(&[127, 0, 0, 1]);
        data.extend_from_slice(&6881u16.to_be_bytes());
        // Node 2: id + 192.168.1.1:51413
        data.extend_from_slice(&[0x02u8; 20]);
        data.extend_from_slice(&[192, 168, 1, 1]);
        data.extend_from_slice(&51413u16.to_be_bytes());

        let nodes = parse_compact_nodes(&data);
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].id, [0x01u8; 20]);
        assert_eq!(nodes[0].addr.to_string(), "127.0.0.1:6881");
        assert_eq!(nodes[1].addr.to_string(), "192.168.1.1:51413");
    }
}

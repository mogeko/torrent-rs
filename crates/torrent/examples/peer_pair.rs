//! Peer-to-peer handshake and message exchange between two local peers.
//!
//! Starts a local TCP listener and connects a second peer to it, then
//! performs the full BitTorrent handshake on both sides and exchanges
//! the 11 wire protocol messages. No internet required.
//!
//! Run with: `cargo run -p torrent --example peer_pair`

use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use torrent::peer::{Handshake, PeerId, PeerMessage, decode, encode};

const INFO_HASH: [u8; 20] = [0x42u8; 20];

/// Accept a peer, receiving its handshake first, then sending ours.
async fn accept_peer(
    listener: &TcpListener,
) -> Result<(PeerId, TcpStream), Box<dyn std::error::Error>> {
    let (mut stream, addr) = listener.accept().await?;
    println!("[server] Accepted connection from {}", addr);

    // Read the client's handshake
    let mut buf = [0u8; 68];
    stream.read_exact(&mut buf).await?;
    let client_hs = Handshake::from_bytes(&buf)?;
    assert_eq!(client_hs.info_hash, INFO_HASH);
    let client_id = PeerId(client_hs.peer_id);
    println!("[server] Received handshake from {}", client_id);

    // Send our handshake
    let our_id = PeerId::random();
    let our_hs = Handshake::new(INFO_HASH, our_id.0);
    stream.write_all(&our_hs.to_bytes()).await?;
    println!("[server] Sent handshake as {}", our_id);

    Ok((client_id, stream))
}

/// Connect to the server, sending our handshake first, then receiving theirs.
async fn connect_peer(addr: SocketAddr) -> Result<(PeerId, TcpStream), Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(addr).await?;
    println!("[client] Connected to {}", addr);

    // Send our handshake
    let our_id = PeerId::random();
    let our_hs = Handshake::new(INFO_HASH, our_id.0);
    stream.write_all(&our_hs.to_bytes()).await?;
    println!("[client] Sent handshake as {}", our_id);

    // Read the server's handshake
    let mut buf = [0u8; 68];
    stream.read_exact(&mut buf).await?;
    let server_hs = Handshake::from_bytes(&buf)?;
    assert_eq!(server_hs.info_hash, INFO_HASH);
    let server_id = PeerId(server_hs.peer_id);
    println!("[client] Received handshake from {}", server_id);

    Ok((server_id, stream))
}

/// Send a message (length-prefixed wire format) over the stream.
async fn send_msg(
    stream: &mut TcpStream,
    msg: &PeerMessage,
) -> Result<(), Box<dyn std::error::Error>> {
    let wire = encode(msg);
    stream.write_all(&wire).await?;
    Ok(())
}

/// Receive a message (length-prefixed wire format) from the stream.
async fn recv_msg(stream: &mut TcpStream) -> Result<PeerMessage, Box<dyn std::error::Error>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf);
    if len == 0 {
        return Ok(PeerMessage::KeepAlive);
    }
    let mut msg_buf = vec![0u8; len as usize];
    stream.read_exact(&mut msg_buf).await?;
    let mut full = len_buf.to_vec();
    full.extend_from_slice(&msg_buf);
    Ok(decode(&full)?)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Start a local listener
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    println!("=== Local Peer Pair ===");
    println!("Listener bound to {}\n", addr);

    // 2. Run server and client concurrently with try_join!
    tokio::try_join!(server_side(listener), client_side(addr))?;

    Ok(())
}

async fn server_side(listener: TcpListener) -> Result<(), Box<dyn std::error::Error>> {
    let (_client_id, mut stream) = accept_peer(&listener).await?;

    // Send Bitfield + Unchoke
    send_msg(&mut stream, &PeerMessage::Bitfield(vec![0xFF])).await?;
    println!("[server] Sent Bitfield");
    send_msg(&mut stream, &PeerMessage::Unchoke).await?;
    println!("[server] Sent Unchoke");

    // Receive messages from client
    loop {
        match recv_msg(&mut stream).await {
            Ok(PeerMessage::KeepAlive) => continue,
            Ok(PeerMessage::Interested) => {
                println!("[server] Received Interested from client");
            }
            Ok(PeerMessage::Request {
                index,
                begin,
                length,
            }) => {
                println!(
                    "[server] Received Request(index={}, begin={}, length={})",
                    index, begin, length
                );
                let data = vec![0xAB; length as usize];
                let piece = PeerMessage::Piece { index, begin, data };
                send_msg(&mut stream, &piece).await?;
                println!("[server] Sent Piece(index={}, begin={})", index, begin);
            }
            Ok(msg) => {
                println!("[server] Received: {:?}", msg);
            }
            Err(e) => {
                println!("[server] Connection closed: {}", e);
                break;
            }
        }
    }
    Ok(())
}

async fn client_side(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let (_server_id, mut stream) = connect_peer(addr).await?;

    // Exchange messages
    let msg = recv_msg(&mut stream).await?;
    println!("[client] Received: {:?}", msg);
    let msg = recv_msg(&mut stream).await?;
    println!("[client] Received: {:?}", msg);

    send_msg(&mut stream, &PeerMessage::Interested).await?;
    println!("[client] Sent Interested");

    let req = PeerMessage::Request {
        index: 0,
        begin: 0,
        length: 128, // Small block for demo readability
    };
    send_msg(&mut stream, &req).await?;
    println!("[client] Sent Request(index=0, begin=0, length=128)");

    let msg = recv_msg(&mut stream).await?;
    if let PeerMessage::Piece {
        index,
        begin,
        ref data,
    } = msg
    {
        println!(
            "[client] Received Piece(index={}, begin={}, len={})",
            index,
            begin,
            data.len()
        );
    } else {
        println!("[client] Received: {:?}", msg);
    }

    send_msg(&mut stream, &PeerMessage::Have(0)).await?;
    println!("[client] Sent Have(0)");
    send_msg(&mut stream, &PeerMessage::KeepAlive).await?;
    println!("[client] Sent KeepAlive");

    println!("\n=== All messages exchanged successfully ===");
    Ok(())
}

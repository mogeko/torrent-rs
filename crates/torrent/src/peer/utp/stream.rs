//! uTP stream (BEP 29) — `AsyncRead + AsyncWrite` adapter.
//!
//! Wraps a [`UtpConnectionHandle`] to provide standard tokio I/O traits,
//! enabling uTP connections to be used with [`PeerConnection`].
//!
//! [`PeerConnection`]: crate::peer::stream::PeerConnection

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::socket::UtpConnectionHandle;

/// A bidirectional uTP stream implementing [`AsyncRead`] + [`AsyncWrite`].
///
/// Wraps a [`UtpConnectionHandle`] to provide standard async I/O.
/// Can be used directly or passed to [`PeerConnection`] for BEP 3
/// wire protocol communication over uTP.
///
/// [`PeerConnection`]: crate::peer::stream::PeerConnection
pub(crate) struct UtpStream {
    handle: UtpConnectionHandle,
    /// Buffered data received from the remote peer, ready for reading.
    read_buf: Vec<u8>,
    /// Position in read_buf.
    read_pos: usize,
}

impl UtpStream {
    /// Create a new uTP stream from a connection handle.
    pub(crate) fn new(handle: UtpConnectionHandle) -> Self {
        UtpStream {
            handle,
            read_buf: Vec::new(),
            read_pos: 0,
        }
    }

    /// Consume the stream and return the underlying handle.
    #[allow(dead_code)]
    pub(crate) fn into_handle(self) -> UtpConnectionHandle {
        self.handle
    }
}

impl AsyncRead for UtpStream {
    fn poll_read(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // If our internal buffer is exhausted, try to get more data
        if self.read_pos >= self.read_buf.len() {
            match self.handle.try_recv() {
                Some(data) => {
                    self.read_buf = data;
                    self.read_pos = 0;
                }
                None => {
                    // No data available right now.
                    // Register waker and return Pending.
                    // In a full implementation, we'd use a Notify or similar.
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
        }

        let remaining = &self.read_buf[self.read_pos..];
        let to_copy = remaining.len().min(buf.remaining());
        buf.put_slice(&remaining[..to_copy]);
        self.read_pos += to_copy;

        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for UtpStream {
    fn poll_write(
        mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.handle.send(buf.to_vec()) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(e) => Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionReset, e))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // uTP sends immediately — no buffering at this layer
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // uTP connection close is handled by the connection task
        Poll::Ready(Ok(()))
    }
}

//! Web seed download engine (BEP 19).
//!
//! Web seeds are standard HTTP/FTP servers that host torrent files.
//! This module downloads pieces from web seed URLs using HTTP Range
//! requests, filling gaps left by P2P peer downloads.
//!
//! # Algorithm
//!
//! 1. Read the piece bitfield to find the largest contiguous gap
//!    of missing pieces
//! 2. Compute the byte range for that gap
//! 3. Issue an HTTP GET with `Range: bytes=start-end`
//! 4. Split the response into piece-sized chunks
//! 5. SHA-1 verify each piece, write to storage, update bitfield
//! 6. Repeat until the torrent is complete

use std::sync::Arc;
use std::time::Duration;

use sha1::{Digest, Sha1};
use tokio::sync::RwLock;
use url::Url;

use crate::error::{Error, ErrorKind};
use crate::metainfo::{Metainfo, Mode};
use crate::net::http::HttpClient;
use crate::piece::PieceManager;
use crate::storage::Storage;

/// Configuration for web seed downloads (BEP 19).
#[derive(Debug, Clone)]
pub(crate) struct WebSeedConfig {
    /// Minimum contiguous gap (in pieces) to trigger an HTTP download.
    /// Prevents tiny range requests. Default: 4 pieces.
    pub min_gap_pieces: u32,
    /// Maximum bytes per Range request.
    /// BEP 19 suggests ~5% of total file size. Default: 5 MB.
    pub max_range_bytes: u64,
    /// Timeout for HTTP connect + download.
    /// Default: 30 s.
    pub timeout: Duration,
    /// Delay before retrying after a transient error (503, connection
    /// refused). Doubles on each consecutive failure up to 60 s.
    /// Default: 2 s.
    pub retry_delay: Duration,
}

impl Default for WebSeedConfig {
    fn default() -> Self {
        WebSeedConfig {
            min_gap_pieces: 4,
            max_range_bytes: 5 * 1024 * 1024, // 5 MB
            timeout: Duration::from_secs(30),
            retry_delay: Duration::from_secs(2),
        }
    }
}

/// A single web seed download task.
///
/// Runs in the background for one web seed URL. Reads the piece
/// bitfield to find gaps, downloads them via HTTP Range requests,
/// verifies SHA-1 hashes, and writes completed pieces to storage.
#[expect(dead_code, reason = "will be wired into SwarmLoop in Phase 3")]
pub(crate) struct WebSeedTask {
    /// Base URL from the torrent metadata or magnet link.
    url: Url,
    /// HTTP client for this seed.
    http: HttpClient,
    /// Shared piece manager to read progress / mark pieces.
    piece_mgr: Arc<RwLock<PieceManager>>,
    /// Storage backend for writing verified pieces.
    storage: Arc<dyn Storage>,
    /// Piece length in bytes (constant for all non-final pieces).
    piece_length: u64,
    /// Number of pieces in the torrent.
    num_pieces: u32,
    /// Torrent metadata for URL construction and SHA-1 verification.
    metainfo: Metainfo,
    /// Configuration knobs.
    config: WebSeedConfig,
    /// Notification channel — woken when a peer completes a piece
    /// (so we can re-evaluate gaps).
    notify: Arc<tokio::sync::Notify>,
}

impl WebSeedTask {
    /// Create a new web seed download task.
    ///
    /// `url` is the base web seed URL from `url-list` or `ws` parameter.
    /// If it ends with `/`, the file path is appended (multi-file).
    #[expect(dead_code, reason = "will be called by SwarmLoop in Phase 3")]
    pub fn new(
        url: Url, piece_mgr: Arc<RwLock<PieceManager>>, storage: Arc<dyn Storage>,
        metainfo: Metainfo, config: WebSeedConfig, notify: Arc<tokio::sync::Notify>,
    ) -> Self {
        let num_pieces = metainfo.info.num_pieces() as u32;
        let piece_length = metainfo.info.piece_length;
        let http = HttpClient::new(config.timeout);

        WebSeedTask {
            url,
            http,
            piece_mgr,
            storage,
            piece_length,
            num_pieces,
            metainfo,
            config,
            notify,
        }
    }

    /// Run the web seed download loop.
    ///
    /// Identifies gaps in the piece bitfield and fills them with HTTP
    /// Range requests. Exits when all pieces are complete.
    #[expect(dead_code, reason = "will be spawned by SwarmLoop in Phase 3")]
    pub async fn run(self) {
        tracing::info!("web seed task started: {}", self.url);

        let mut retry_delay = self.config.retry_delay;

        loop {
            // Read current bitfield
            let bitfield = {
                let pm = self.piece_mgr.read().await;
                pm.bitfield().to_vec()
            };

            // Exit when download is complete
            if bitfield.iter().all(|&b| b) {
                tracing::info!("web seed {}: torrent complete, exiting", self.url);
                return;
            }

            // Find the largest contiguous gap
            let Some((gap_start, gap_size)) = find_largest_gap(&bitfield) else {
                // No gaps found (shouldn't happen if not all complete)
                tracing::debug!("web seed {}: no gaps found, sleeping", self.url);
                self.notify.notified().await;
                continue;
            };

            if gap_size < self.config.min_gap_pieces {
                tracing::debug!(
                    "web seed {}: largest gap ({gap_size} pieces) below threshold, sleeping",
                    self.url,
                );
                self.notify.notified().await;
                continue;
            }

            // Calculate byte range
            let start_byte = gap_start as u64 * self.piece_length;
            let total_size = self.metainfo.info.total_size();
            let end_byte = (start_byte + self.config.max_range_bytes)
                .min(total_size)
                .saturating_sub(1);

            tracing::info!(
                "web seed {}: downloading gap [{}, {}] ({} pieces, {} bytes)",
                self.url,
                gap_start,
                gap_start + gap_size - 1,
                gap_size,
                end_byte - start_byte + 1,
            );

            // Download the range
            match self.download_range(start_byte, end_byte).await {
                Ok(downloaded_pieces) => {
                    retry_delay = self.config.retry_delay; // reset backoff

                    // Notify SwarmLoop about completed pieces
                    // (webseed_notify is for waking us up,
                    //  broadcast_have is done by the caller)
                    for index in &downloaded_pieces {
                        tracing::debug!("web seed {}: completed piece {}", self.url, index,);
                    }
                    if !downloaded_pieces.is_empty() {
                        self.notify.notify_one();
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "web seed {}: download failed ({}), retrying in {:?}",
                        self.url,
                        e,
                        retry_delay,
                    );
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(Duration::from_secs(60));
                }
            }
        }
    }

    /// Download a byte range from the web seed, split into pieces,
    /// verify SHA-1, and write to storage.
    ///
    /// Returns the list of piece indices that were successfully
    /// completed.
    async fn download_range(&self, start_byte: u64, end_byte: u64) -> Result<Vec<u32>, Error> {
        // Build the URL and path for the request
        let (request_url, path_and_query) = self.build_range_url(start_byte, end_byte)?;

        let body = self
            .http
            .get_with_range(&request_url, &path_and_query, start_byte, end_byte)
            .await?;

        // Split the response into piece-sized chunks and verify
        let mut completed = Vec::new();
        let first_piece = (start_byte / self.piece_length) as u32;
        let mut offset = 0u64;

        while offset < body.len() as u64 {
            let piece_index = first_piece + (offset / self.piece_length) as u32;
            let piece_offset = (start_byte + offset) % self.piece_length;
            let piece_len = self.piece_len(piece_index);
            let chunk_end = (offset + piece_len - piece_offset).min(body.len() as u64);
            let chunk = &body[offset as usize..chunk_end as usize];

            // Only verify if we have the full piece
            if chunk.len() as u64 == piece_len {
                let expected_hash = match self.metainfo.info.pieces.get(piece_index as usize) {
                    Some(h) => *h,
                    None => {
                        tracing::warn!("web seed {}: piece {} out of range", self.url, piece_index,);
                        break;
                    }
                };

                let actual_hash: [u8; 20] = Sha1::digest(chunk).into();
                if actual_hash != expected_hash {
                    tracing::error!(
                        "web seed {}: SHA-1 mismatch for piece {} — discarding URL",
                        self.url,
                        piece_index,
                    );
                    return Err(Error::new(ErrorKind::TrackerInvalidResponse));
                }

                // Write the verified piece
                self.storage.write_piece(piece_index, chunk).await?;

                // Mark as complete
                {
                    let mut pm = self.piece_mgr.write().await;
                    pm.set_piece(piece_index);
                }
                completed.push(piece_index);
            } else {
                // Partial piece at the end of the range —
                // don't verify yet; it'll be completed by peers
                // or the next web seed range download.
                break;
            }

            offset = chunk_end;
        }

        Ok(completed)
    }

    /// Build the URL for a byte range request.
    ///
    /// For single-file torrents, the URL is used directly.
    /// For multi-file torrents with a trailing `/` URL, the
    /// file path is appended.
    ///
    /// Returns `(request_url, path_and_query)` — for web seeds,
    /// `path_and_query` is typically just `/` since the URL
    /// already contains the full path.
    fn build_range_url(&self, _start: u64, _end: u64) -> Result<(Url, String), Error> {
        match &self.metainfo.info.mode {
            Mode::Single { .. } => {
                // Single file: URL is the direct file URL.
                // The path is already part of the URL; our "path_and_query"
                // is just the URL's own path (or "/").
                let path = self.url.path().to_string();
                // Note: for a clean URL like http://mirror.com/file.iso,
                // path() returns "/file.iso". For web seed, we need
                // to separate the "base URL" from the path.
                //
                // Simplification: treat the entire url as the request
                // target, and pass path() as path_and_query.
                Ok((self.url.clone(), path))
            }
            Mode::Multiple { name: _, files: _ } => {
                // Multi-file: URL ends with "/" (directory).
                // Full multi-file support (byte → file mapping) is a
                // follow-up. For now, treat the base URL as-is.
                let path = self.url.path().to_string();
                Ok((self.url.clone(), path))
            }
        }
    }

    /// Length of the piece at `index` (last piece may be shorter).
    fn piece_len(&self, index: u32) -> u64 {
        let total_size = self.metainfo.info.total_size();
        let full_piece_count = total_size / self.piece_length;
        let last_piece_size = total_size % self.piece_length;

        if (index as u64) < full_piece_count {
            self.piece_length
        } else if (index as u64) == full_piece_count && last_piece_size > 0 {
            last_piece_size
        } else {
            0 // beyond the last piece (or empty torrent)
        }
    }
}

/// Find the largest contiguous gap of missing pieces in a bitfield.
///
/// Returns `(gap_start_index, gap_size_in_pieces)`, or `None` if
/// no gaps exist (all pieces present).
fn find_largest_gap(bitfield: &[bool]) -> Option<(u32, u32)> {
    let mut best_start = None;
    let mut best_size = 0u32;

    let mut gap_start = None;
    let mut gap_size = 0u32;

    for (i, &has) in bitfield.iter().enumerate() {
        if !has {
            if gap_start.is_none() {
                gap_start = Some(i as u32);
            }
            gap_size += 1;
        } else {
            if let Some(start) = gap_start {
                if gap_size > best_size {
                    best_start = Some(start);
                    best_size = gap_size;
                }
                gap_start = None;
                gap_size = 0;
            }
        }
    }

    // Check trailing gap
    if let Some(start) = gap_start {
        if gap_size > best_size {
            best_start = Some(start);
            best_size = gap_size;
        }
    }

    best_start.map(|s| (s, best_size))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_largest_gap_empty() {
        // All complete → no gaps
        let bf = vec![true; 10];
        assert_eq!(find_largest_gap(&bf), None);
    }

    #[test]
    fn find_largest_gap_full() {
        // All missing → one big gap
        let bf = vec![false; 10];
        assert_eq!(find_largest_gap(&bf), Some((0, 10)));
    }

    #[test]
    fn find_largest_gap_multiple() {
        // Pattern: YYnnnnYnnY
        let bf = vec![
            true, true, // YY
            false, false, false, false, // nnnn (gap of 4)
            true,  // Y
            false, false, // nn (gap of 2)
            true,  // Y
        ];
        assert_eq!(find_largest_gap(&bf), Some((2, 4)));
    }

    #[test]
    fn find_largest_gap_trailing() {
        // Trailing gap is largest
        let bf = vec![true, false, false, true, false, false, false];
        assert_eq!(find_largest_gap(&bf), Some((4, 3)));
    }

    #[test]
    fn find_largest_gap_leading() {
        // Leading gap is largest
        let bf = vec![false, false, false, true, true, false];
        assert_eq!(find_largest_gap(&bf), Some((0, 3)));
    }

    #[test]
    fn find_largest_gap_single_piece() {
        let bf = vec![false];
        assert_eq!(find_largest_gap(&bf), Some((0, 1)));
    }

    #[test]
    fn find_largest_gap_zero_length() {
        let bf: Vec<bool> = vec![];
        assert_eq!(find_largest_gap(&bf), None);
    }
}

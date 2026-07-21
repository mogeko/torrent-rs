//! Passive HTTP download worker and shared URL/probe helpers.

use std::sync::Arc;
use std::time::Instant;

use sha1::{Digest, Sha1};
use tokio::sync::{RwLock, Semaphore, mpsc};

use crate::error::{Error, ErrorKind};
use crate::metainfo::Metainfo;
use crate::net::Url;
use crate::net::http::HttpClient;
use crate::piece::PieceManager;
use crate::storage::Storage;

use super::types::{UrlKind, WorkItem, WorkResult};

// ── FetchTask ──────────────────────────────────────────────────────

/// A passive web seed download worker.
///
/// Waits for [`WorkItem`] messages on `work_rx`, downloads the
/// requested byte range, verifies SHA-1 hashes, writes pieces to
/// storage, and reports the result back via `result_tx`.
///
/// The fetcher does NOT scan the bitfield or decide what to download
/// — that is the scheduler's job.
///
/// Timeout is a cross-cutting concern applied at the call site
/// (e.g. via [`tower::ServiceBuilder::timeout`] in the session),
/// not embedded in the download logic.
pub(crate) struct FetchTask {
    /// Human-readable URL for logging.
    url: Url,
    /// Index of this URL in the scheduler's `urls` vector.
    url_index: usize,
    /// HTTP client for this fetcher.
    http: HttpClient,
    /// Shared piece manager to skip already-completed pieces.
    piece_mgr: Arc<RwLock<PieceManager>>,
    /// Storage backend for writing verified pieces.
    storage: Arc<dyn Storage>,
    /// Piece length in bytes.
    piece_length: u64,
    /// Torrent metadata for URL construction and SHA-1 verification.
    metainfo: Metainfo,
    /// Receives work from the scheduler.
    work_rx: mpsc::Receiver<WorkItem>,
    /// Reports results back to the scheduler.
    result_tx: mpsc::Sender<WorkResult>,
    /// Concurrency limiter shared across all fetchers.
    semaphore: Arc<Semaphore>,
}

impl FetchTask {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        url: Url, url_index: usize, piece_mgr: Arc<RwLock<PieceManager>>,
        storage: Arc<dyn Storage>, metainfo: Metainfo, work_rx: mpsc::Receiver<WorkItem>,
        result_tx: mpsc::Sender<WorkResult>, semaphore: Arc<Semaphore>,
    ) -> Self {
        let piece_length = metainfo.info.piece_length;
        let http = HttpClient::new();

        FetchTask {
            url,
            url_index,
            http,
            piece_mgr,
            storage,
            piece_length,
            metainfo,
            work_rx,
            result_tx,
            semaphore,
        }
    }

    /// Run the fetcher loop — waits for work, downloads, reports.
    pub async fn run(mut self) {
        tracing::trace!("web seed {}: started", self.url);
        while let Some(work) = self.work_rx.recv().await {
            let _permit = self.semaphore.clone().acquire_owned().await;
            let result = self.download_once(work).await;
            let _ = self.result_tx.send(result).await;
        }
        tracing::trace!("web seed {}: exiting", self.url);
    }

    /// Single-attempt download — raw I/O only, no timeout.
    async fn download_once(&self, work: WorkItem) -> WorkResult {
        let started = Instant::now();
        let result = self.download_range(work.start_byte, work.end_byte).await;

        match result {
            Ok(completed) => {
                let bytes: u64 = completed
                    .iter()
                    .map(|&i| piece_len(i, &self.metainfo, self.piece_length))
                    .sum();
                WorkResult {
                    url_index: self.url_index,
                    completed,
                    bytes,
                    elapsed: started.elapsed(),
                    error: None,
                }
            }
            Err(ref e) if e.kind() == ErrorKind::WebSeedHashMismatch => WorkResult {
                url_index: self.url_index,
                completed: Vec::new(),
                bytes: 0,
                elapsed: started.elapsed(),
                error: Some(ErrorKind::WebSeedHashMismatch),
            },
            Err(e) => WorkResult {
                url_index: self.url_index,
                completed: Vec::new(),
                bytes: 0,
                elapsed: started.elapsed(),
                error: Some(e.kind()),
            },
        }
    }

    /// Download a byte range, split into pieces, verify SHA-1, write to storage.
    pub(super) async fn download_range(
        &self, start_byte: u64, end_byte: u64,
    ) -> Result<Vec<u32>, Error> {
        let url_kind = UrlKind::classify(&self.url);
        let request_url = build_request_url(&self.url, &self.metainfo, &url_kind, start_byte)?;

        let range_size = end_byte - start_byte + 1;
        tracing::debug!(
            "web seed {}: GET {} [{}-{}] ({:.1}KB)",
            self.url,
            request_url,
            start_byte,
            end_byte,
            range_size as f64 / 1024.0,
        );

        let body = self
            .http
            .get_with_range(request_url, start_byte, end_byte)
            .await?;

        let mut completed = Vec::new();
        let first_piece = (start_byte / self.piece_length) as u32;
        let mut offset = 0u64;

        while offset < body.len() as u64 {
            let piece_index = first_piece + (offset / self.piece_length) as u32;
            let piece_offset = (start_byte + offset) % self.piece_length;
            let plen = piece_len(piece_index, &self.metainfo, self.piece_length);
            let chunk_end = (offset + plen - piece_offset).min(body.len() as u64);
            let chunk = &body[offset as usize..chunk_end as usize];

            if chunk.len() as u64 == plen {
                if self.piece_mgr.read().await.has_piece(piece_index) {
                    offset = chunk_end;
                    continue;
                }

                let expected_hash = match self.metainfo.info.pieces.get(piece_index as usize) {
                    Some(h) => *h,
                    None => {
                        tracing::warn!("web seed {}: piece {} out of range", self.url, piece_index);
                        break;
                    }
                };

                let actual_hash: [u8; 20] = Sha1::digest(chunk).into();
                if actual_hash != expected_hash {
                    tracing::debug!(
                        "web seed {}: SHA-1 mismatch on piece {} (will be discarded by scheduler)",
                        self.url,
                        piece_index,
                    );
                    return Err(Error::new(ErrorKind::WebSeedHashMismatch));
                }

                self.storage.write_piece(piece_index, chunk).await?;
                {
                    let mut pm = self.piece_mgr.write().await;
                    pm.set_piece(piece_index);
                }
                completed.push(piece_index);
            } else {
                break;
            }
            offset = chunk_end;
        }
        Ok(completed)
    }
}

// ── Shared helpers ─────────────────────────────────────────────────

/// Build the full file URL for an HTTP Range request starting at `start_byte`.
pub(super) fn build_request_url(
    url: &Url, metainfo: &Metainfo, url_kind: &UrlKind, start_byte: u64,
) -> Result<Url, Error> {
    match url_kind {
        UrlKind::Directory => {
            let offsets = metainfo.info.file_offsets();
            let file = offsets
                .iter()
                .find(|fo| start_byte >= fo.offset && start_byte < fo.offset + fo.length)
                .ok_or(Error::new(ErrorKind::InvalidInput))?;
            let file_path = file.path.join("/");
            url.join(&file_path)
                .map_err(|_| Error::new(ErrorKind::InvalidInput))
        }
        UrlKind::Script => Ok(url.clone()),
    }
}

/// Find the path components for the file containing `start_byte`.
pub(super) fn file_path_at_byte(metainfo: &Metainfo, start_byte: u64) -> Option<Vec<String>> {
    metainfo
        .info
        .file_offsets()
        .iter()
        .find(|fo| start_byte >= fo.offset && start_byte < fo.offset + fo.length)
        .map(|fo| fo.path.clone())
}

/// Returns the actual length of a piece in bytes.
fn piece_len(index: u32, metainfo: &Metainfo, piece_length: u64) -> u64 {
    let total = metainfo.info.total_size();
    let start = index as u64 * piece_length;
    if start + piece_length > total {
        total - start
    } else {
        piece_length
    }
}

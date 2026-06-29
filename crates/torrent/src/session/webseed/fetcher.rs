//! Passive HTTP download worker and shared URL/probe helpers.

use std::sync::Arc;
use std::time::{Duration, Instant};

use sha1::{Digest, Sha1};
use tokio::sync::{RwLock, Semaphore, mpsc};
use url::Url;

use crate::error::{Error, ErrorKind};
use crate::metainfo::Metainfo;
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
/// Unlike Phase 1's `WebSeedTask`, the fetcher does NOT scan the
/// bitfield or decide what to download — that is the scheduler's job.
pub(crate) struct FetchTask {
    /// Human-readable URL for logging.
    url: Url,
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
    /// Create a new fetcher.  `work_rx`/`result_tx` are the channels
    /// that connect this fetcher to the [`WebSeedScheduler`].
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        url: Url, piece_mgr: Arc<RwLock<PieceManager>>, storage: Arc<dyn Storage>,
        metainfo: Metainfo, work_rx: mpsc::Receiver<WorkItem>, result_tx: mpsc::Sender<WorkResult>,
        semaphore: Arc<Semaphore>, timeout: Duration,
    ) -> Self {
        let piece_length = metainfo.info.piece_length;
        let max_response = timeout.as_secs() * 1024 * 1024;
        let http = HttpClient::with_max_response(timeout, max_response);

        FetchTask {
            url,
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

    /// Run the fetcher loop — waits for work, downloads with retry, reports.
    pub async fn run(mut self) {
        tracing::debug!("web seed {}: fetcher started", self.url);
        while let Some(work) = self.work_rx.recv().await {
            let _permit = self.semaphore.clone().acquire_owned().await;
            let result = self.download_with_retry(work).await;
            let _ = self.result_tx.send(result).await;
        }
        tracing::debug!("web seed {}: fetcher exiting (channel closed)", self.url);
    }

    /// Download with up to 3 retries on transient errors (exponential backoff).
    async fn download_with_retry(&self, work: WorkItem) -> WorkResult {
        const MAX_RETRIES: u32 = 3;
        let mut retry_delay = Duration::from_secs(2);
        let mut last_error = None;

        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                tracing::debug!(
                    "web seed {}: retry {}/{} after {:?}",
                    self.url,
                    attempt,
                    MAX_RETRIES,
                    retry_delay,
                );
                tokio::time::sleep(retry_delay).await;
                retry_delay = (retry_delay * 2).min(Duration::from_secs(60));
            }

            let started = Instant::now();
            match self.download_range(work.start_byte, work.end_byte).await {
                Ok(completed) => {
                    let bytes: u64 = completed
                        .iter()
                        .map(|&i| piece_len(i, &self.metainfo, self.piece_length))
                        .sum();
                    return WorkResult {
                        completed,
                        bytes,
                        elapsed: started.elapsed(),
                        error: None,
                    };
                }
                Err(ref e) if e.kind() == ErrorKind::WebSeedHashMismatch => {
                    return WorkResult {
                        completed: Vec::new(),
                        bytes: 0,
                        elapsed: started.elapsed(),
                        error: Some(ErrorKind::WebSeedHashMismatch),
                    };
                }
                Err(e) => {
                    last_error = Some(e.kind());
                }
            }
        }

        WorkResult {
            completed: Vec::new(),
            bytes: 0,
            elapsed: Instant::now().duration_since(Instant::now()),
            error: last_error,
        }
    }

    /// Download a byte range, split into pieces, verify SHA-1, write to storage.
    pub(super) async fn download_range(
        &self, start_byte: u64, end_byte: u64,
    ) -> Result<Vec<u32>, Error> {
        let url_kind = UrlKind::classify(&self.url);
        let request_url = build_request_url(&self.url, &self.metainfo, &url_kind, start_byte)?;
        let path_and_query = request_url.path().to_string();

        tracing::debug!(
            "web seed {}: GET {} (bytes {}-{})",
            self.url,
            request_url,
            start_byte,
            end_byte,
        );

        let body = self
            .http
            .get_with_range(&request_url, &path_and_query, start_byte, end_byte)
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
                        tracing::warn!("web seed {}: piece {} out of range", self.url, piece_index,);
                        break;
                    }
                };

                let actual_hash: [u8; 20] = Sha1::digest(chunk).into();
                if actual_hash != expected_hash {
                    tracing::warn!(
                        "web seed {}: SHA-1 mismatch for piece {} — discarding URL",
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
///
/// For directory URLs (BEP 19 §2): finds the file containing `start_byte`
/// via [`Info::file_offsets`] and appends its path.
/// For script URLs: returns the URL as-is.
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

/// Length of the piece at `index` (last piece may be shorter).
pub(super) fn piece_len(index: u32, metainfo: &Metainfo, piece_length: u64) -> u64 {
    let total_size = metainfo.info.total_size();
    let full_piece_count = total_size / piece_length;
    let last_piece_size = total_size % piece_length;

    if (index as u64) < full_piece_count {
        piece_length
    } else if (index as u64) == full_piece_count && last_piece_size > 0 {
        last_piece_size
    } else {
        0
    }
}

/// Probe a web seed URL with a three-level fallback.
pub(super) async fn probe_url(
    url: &Url, url_kind: &UrlKind, metainfo: &Metainfo, probe_timeout: Duration,
) -> bool {
    let request_url = match build_request_url(url, metainfo, url_kind, 0) {
        Ok(u) => u,
        Err(_) => return false,
    };
    let path = request_url.path().to_string();

    if try_head(&request_url, &path, probe_timeout).await {
        tracing::debug!("web seed {url}: HEAD probe OK");
        return true;
    }
    if try_tiny_range(&request_url, &path, probe_timeout).await {
        tracing::debug!("web seed {url}: tiny Range probe OK");
        return true;
    }
    if try_short_get(
        &request_url,
        &path,
        metainfo.info.total_size(),
        probe_timeout,
    )
    .await
    {
        tracing::debug!("web seed {url}: short GET probe OK");
        return true;
    }
    tracing::debug!("web seed {url}: all probes failed");
    false
}

async fn try_head(url: &Url, path: &str, timeout: Duration) -> bool {
    HttpClient::new(timeout).head(url, path).await.is_ok()
}

async fn try_tiny_range(url: &Url, path: &str, timeout: Duration) -> bool {
    HttpClient::new(timeout)
        .get_with_range(url, path, 0, 0)
        .await
        .is_ok()
}

async fn try_short_get(url: &Url, path: &str, total_size: u64, timeout: Duration) -> bool {
    let end = 4095u64.min(total_size.saturating_sub(1));
    HttpClient::new(timeout)
        .get_with_range(url, path, 0, end)
        .await
        .is_ok()
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use crate::metainfo::{MetainfoBuilder, Mode};
    use crate::storage::{FileStorageFactory, StorageFactory};

    use super::*;

    async fn mock_http_server(body: Vec<u8>) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = stream.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..n]);

            let range = if request.contains("Range: bytes=") {
                let line = request
                    .lines()
                    .find(|l| l.starts_with("Range: bytes="))
                    .unwrap();
                let range_str = line.strip_prefix("Range: bytes=").unwrap();
                let parts: Vec<&str> = range_str.split('-').collect();
                let start: usize = parts[0].parse().unwrap();
                let end: usize = parts[1].parse().unwrap();
                Some((start, end))
            } else {
                None
            };

            let (response_body, status) = if let Some((start, end)) = range {
                let slice = &body[start..=end.min(body.len().saturating_sub(1))];
                (slice.to_vec(), "206 Partial Content")
            } else {
                (body.clone(), "200 OK")
            };

            let response = format!(
                "HTTP/1.1 {}\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
                status,
                response_body.len(),
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(&response_body).await.unwrap();
            stream.shutdown().await.unwrap();
        });
        (addr, handle)
    }

    fn build_test_metainfo(data: &[u8], piece_length: u32) -> crate::metainfo::Metainfo {
        let mut builder = MetainfoBuilder::new(piece_length);
        builder.add_data(data);
        builder.finish(
            "http://tracker.example.com/announce".into(),
            Mode::Single {
                name: "test.bin".into(),
                length: data.len() as u64,
            },
            Vec::new(),
            Vec::new(),
        )
    }

    #[tokio::test]
    async fn downloads_full_file_single_piece() {
        let piece_length = 256u32;
        let data = vec![0xABu8; piece_length as usize];
        let metainfo = build_test_metainfo(&data, piece_length);

        let (server_url, _server) = mock_http_server(data.clone()).await;
        let url = Url::parse(&server_url).unwrap();
        let client = HttpClient::new(Duration::from_secs(5));
        let body = client.get_with_range(&url, "/", 0, 255).await.unwrap();
        assert_eq!(body.len(), 256);

        let (server_url2, _server2) = mock_http_server(data.clone()).await;
        let url2 = Url::parse(&server_url2).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let factory = FileStorageFactory::new(tmp.path().to_path_buf());
        let storage = factory.create(&metainfo.info).await.unwrap();
        storage.prepare().await.unwrap();
        let piece_mgr = Arc::new(RwLock::new(PieceManager::new(metainfo.info.num_pieces())));

        let (_work_tx, work_rx) = mpsc::channel::<WorkItem>(1);
        let (result_tx, _result_rx) = mpsc::channel::<WorkResult>(1);
        let timeout = Duration::from_secs(5);

        let task = FetchTask::new(
            url2,
            piece_mgr.clone(),
            storage.clone(),
            metainfo.clone(),
            work_rx,
            result_tx,
            Arc::new(Semaphore::new(1)),
            timeout,
        );

        let completed = task.download_range(0, 255).await.unwrap();
        assert_eq!(completed, vec![0]);
        assert!(piece_mgr.read().await.has_piece(0));
    }

    #[tokio::test]
    async fn downloads_multiple_pieces() {
        let piece_length = 128u32;
        let data: Vec<u8> = (0u32..(piece_length * 3) as u32).map(|v| v as u8).collect();
        let metainfo = build_test_metainfo(&data, piece_length);

        let (server_url, _server) = mock_http_server(data.clone()).await;
        let url = Url::parse(&server_url).unwrap();
        let client = HttpClient::new(Duration::from_secs(5));
        let body = client.get_with_range(&url, "/", 0, 383).await.unwrap();
        assert_eq!(body.len(), 384);
        assert_eq!(&body[..128], &data[0..128]);

        let (server_url2, _server2) = mock_http_server(data.clone()).await;
        let url2 = Url::parse(&server_url2).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let factory = FileStorageFactory::new(tmp.path().to_path_buf());
        let storage = factory.create(&metainfo.info).await.unwrap();
        storage.prepare().await.unwrap();
        let piece_mgr = Arc::new(RwLock::new(PieceManager::new(metainfo.info.num_pieces())));

        let (_work_tx, work_rx) = mpsc::channel::<WorkItem>(1);
        let (result_tx, _result_rx) = mpsc::channel::<WorkResult>(1);
        let task = FetchTask::new(
            url2,
            piece_mgr.clone(),
            storage.clone(),
            metainfo.clone(),
            work_rx,
            result_tx,
            Arc::new(Semaphore::new(1)),
            Duration::from_secs(5),
        );

        let completed = task.download_range(0, 383).await.unwrap();
        assert_eq!(completed, vec![0, 1, 2]);
        assert!(piece_mgr.read().await.has_piece(0));
    }

    #[tokio::test]
    async fn sha1_mismatch_does_not_mark_complete() {
        let piece_length = 256u32;
        let correct_data = vec![0xABu8; piece_length as usize];
        let wrong_data = vec![0xCDu8; piece_length as usize];
        let metainfo = build_test_metainfo(&correct_data, piece_length);

        let (server_url, _server) = mock_http_server(wrong_data).await;
        let tmp = tempfile::tempdir().unwrap();
        let factory = FileStorageFactory::new(tmp.path().to_path_buf());
        let storage = factory.create(&metainfo.info).await.unwrap();
        storage.prepare().await.unwrap();
        let piece_mgr = Arc::new(RwLock::new(PieceManager::new(metainfo.info.num_pieces())));

        let url = Url::parse(&server_url).unwrap();
        let (work_tx, work_rx) = mpsc::channel::<WorkItem>(1);
        let (result_tx, mut result_rx) = mpsc::channel::<WorkResult>(1);
        let semaphore = Arc::new(Semaphore::new(1));

        let fetcher = FetchTask::new(
            url,
            piece_mgr.clone(),
            storage.clone(),
            metainfo.clone(),
            work_rx,
            result_tx,
            semaphore.clone(),
            Duration::from_secs(5),
        );

        let handle = tokio::spawn(async move { fetcher.run().await });
        let _ = work_tx
            .send(WorkItem {
                start_byte: 0,
                end_byte: 255,
            })
            .await;

        let result = tokio::time::timeout(Duration::from_secs(3), result_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.error, Some(ErrorKind::WebSeedHashMismatch));
        assert!(result.completed.is_empty());
        assert!(!piece_mgr.read().await.has_piece(0));

        handle.abort();
    }
}

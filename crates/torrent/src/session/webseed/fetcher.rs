//! Passive HTTP download worker and shared URL/probe helpers.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use http::Request;
use sha1::{Digest, Sha1};
use tokio::sync::{RwLock, Semaphore, mpsc};
use tower::Service;

use crate::error::{Error, ErrorKind};
use crate::metainfo::Metainfo;
use crate::net::Url;
use crate::net::http::HttpClient;
use crate::piece::PieceManager;
use crate::storage::Storage;

use super::types::{UrlKind, WorkItem, WorkResult};

// ── FetchTask ──────────────────────────────────────────────────────

pub(crate) struct FetchTask {
    url: Url,
    url_index: usize,
    http: HttpClient,
    piece_mgr: Arc<RwLock<PieceManager>>,
    storage: Arc<dyn Storage>,
    piece_length: u64,
    metainfo: Metainfo,
    work_rx: mpsc::Receiver<WorkItem>,
    result_tx: mpsc::Sender<WorkResult>,
    semaphore: Arc<Semaphore>,
}

impl FetchTask {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        url: Url, url_index: usize, piece_mgr: Arc<RwLock<PieceManager>>,
        storage: Arc<dyn Storage>, metainfo: Metainfo, work_rx: mpsc::Receiver<WorkItem>,
        result_tx: mpsc::Sender<WorkResult>, semaphore: Arc<Semaphore>,
    ) -> Self {
        FetchTask {
            url,
            url_index,
            http: HttpClient::new(),
            piece_mgr,
            storage,
            piece_length: metainfo.info.piece_length,
            metainfo,
            work_rx,
            result_tx,
            semaphore,
        }
    }

    pub async fn run(mut self) {
        tracing::trace!("web seed {}: started", self.url);
        while let Some(work) = self.work_rx.recv().await {
            let _permit = self.semaphore.clone().acquire_owned().await;
            let result = self.download_once(work).await;
            let _ = self.result_tx.send(result).await;
        }
        tracing::trace!("web seed {}: exiting", self.url);
    }

    async fn download_once(&self, work: WorkItem) -> WorkResult {
        let started = Instant::now();
        let result = download_range_raw(
            &self.url,
            &self.http,
            &*self.piece_mgr,
            &*self.storage,
            self.piece_length,
            &self.metainfo,
            work.start_byte,
            work.end_byte,
        )
        .await;

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
}

impl Service<WorkItem> for FetchTask {
    type Response = WorkResult;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, work: WorkItem) -> Self::Future {
        let url = self.url.clone();
        let url_index = self.url_index;
        let http = self.http.clone();
        let piece_mgr = self.piece_mgr.clone();
        let storage = self.storage.clone();
        let piece_length = self.piece_length;
        let metainfo = self.metainfo.clone();

        Box::pin(async move {
            let started = Instant::now();
            let result = download_range_raw(
                &url,
                &http,
                &*piece_mgr,
                &*storage,
                piece_length,
                &metainfo,
                work.start_byte,
                work.end_byte,
            )
            .await;
            match result {
                Ok(completed) => Ok(WorkResult {
                    url_index,
                    completed,
                    bytes: 0,
                    elapsed: started.elapsed(),
                    error: None,
                }),
                Err(e) => Ok(WorkResult {
                    url_index,
                    completed: Vec::new(),
                    bytes: 0,
                    elapsed: started.elapsed(),
                    error: Some(if e.kind() == ErrorKind::WebSeedHashMismatch {
                        ErrorKind::WebSeedHashMismatch
                    } else {
                        e.kind()
                    }),
                }),
            }
        })
    }
}

async fn download_range_raw(
    url: &Url, http: &HttpClient, piece_mgr: &RwLock<PieceManager>, storage: &dyn Storage,
    piece_length: u64, metainfo: &Metainfo, start_byte: u64, end_byte: u64,
) -> Result<Vec<u32>, Error> {
    let url_kind = UrlKind::classify(url);
    let request_url = build_request_url(url, metainfo, &url_kind, start_byte)?;

    let range_size = end_byte - start_byte + 1;
    tracing::debug!(
        "web seed {}: GET {} [{}-{}] ({:.1}KB)",
        url,
        request_url,
        start_byte,
        end_byte,
        range_size as f64 / 1024.0
    );

    let http_req = Request::get(request_url.as_str())
        .header("Range", format!("bytes={start_byte}-{end_byte}"))
        .body(vec![])
        .map_err(|_| Error::new(ErrorKind::InvalidInput))?;

    let mut http = http.clone();
    let resp = Service::call(&mut http, http_req).await?;
    let body = resp.into_body();

    let mut completed = Vec::new();
    let first_piece = (start_byte / piece_length) as u32;
    let mut offset = 0u64;

    while offset < body.len() as u64 {
        let piece_index = first_piece + (offset / piece_length) as u32;
        let piece_offset = (start_byte + offset) % piece_length;
        let plen = piece_len(piece_index, metainfo, piece_length);
        let chunk_end = (offset + plen - piece_offset).min(body.len() as u64);
        let chunk = &body[offset as usize..chunk_end as usize];

        if chunk.len() as u64 == plen {
            if piece_mgr.read().await.has_piece(piece_index) {
                offset = chunk_end;
                continue;
            }
            let expected_hash = match metainfo.info.pieces.get(piece_index as usize) {
                Some(h) => *h,
                None => break,
            };
            let actual_hash: [u8; 20] = Sha1::digest(chunk).into();
            if actual_hash != expected_hash {
                return Err(Error::new(ErrorKind::WebSeedHashMismatch));
            }
            storage.write_piece(piece_index, chunk).await?;
            {
                let mut pm = piece_mgr.write().await;
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

// ── Shared helpers ─────────────────────────────────────────────────

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
            url.join(&file.path.join("/"))
                .map_err(|_| Error::new(ErrorKind::InvalidInput))
        }
        UrlKind::Script => Ok(url.clone()),
    }
}

pub(super) fn file_path_at_byte(metainfo: &Metainfo, start_byte: u64) -> Option<Vec<String>> {
    metainfo
        .info
        .file_offsets()
        .iter()
        .find(|fo| start_byte >= fo.offset && start_byte < fo.offset + fo.length)
        .map(|fo| fo.path.clone())
}

fn piece_len(index: u32, metainfo: &Metainfo, piece_length: u64) -> u64 {
    let total = metainfo.info.total_size();
    let start = index as u64 * piece_length;
    if start + piece_length > total {
        total - start
    } else {
        piece_length
    }
}

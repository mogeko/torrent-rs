use std::future::Future;
use std::pin::Pin;

use tokio::task::JoinSet;

use crate::error::Error;
use crate::session::swarm::{SwarmContext, SwarmExtension};

use super::gap::{find_largest_gap, gap_within_file};
use super::service::WebSeedService;
use super::types::{PieceRange, WebSeedConfig};

/// Web seed download extension (BEP 19).
///
/// Downloads missing piece gaps via HTTP Range requests.  Each
/// [`on_tick`] finds the largest gap in the bitfield, submits a
/// Range download task (up to the concurrency limit), and harvests
/// completed tasks via [`JoinSet::try_join_next`].
pub(crate) struct WebSeedExtension {
    urls: Vec<String>,
    config: WebSeedConfig,
    concurrency: usize,
    service: Option<WebSeedService>,
    tasks: JoinSet<Result<Vec<u32>, Error>>,
}

impl WebSeedExtension {
    pub fn new(urls: Vec<String>, config: WebSeedConfig, concurrency: usize) -> Self {
        WebSeedExtension {
            urls,
            config,
            concurrency,
            service: None,
            tasks: JoinSet::new(),
        }
    }
}

impl SwarmExtension for WebSeedExtension {
    fn on_start<'a>(
        &'a mut self, ctx: &'a mut SwarmContext<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            if !self.urls.is_empty() {
                self.service = Some(WebSeedService::new(
                    self.urls.clone(),
                    ctx.piece_mgr.clone(),
                    ctx.storage.clone(),
                    ctx.metainfo.clone(),
                    self.config.clone(),
                ));
            }
            Ok(())
        })
    }

    fn on_tick<'a>(
        &'a mut self, ctx: &'a mut SwarmContext<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            // Harvest completed download tasks (non-blocking).
            while let Some(result) = self.tasks.try_join_next() {
                match result {
                    Ok(Ok(pieces)) if !pieces.is_empty() => {
                        tracing::debug!("web seed: downloaded {} pieces", pieces.len());
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        tracing::debug!("web seed: download failed: {}", e);
                    }
                    Err(join_err) => {
                        tracing::warn!("web seed: task panicked: {}", join_err);
                    }
                }
            }

            // Submit new gap downloads up to the concurrency limit.
            // If no URLs are available (all parked and not ready for retry),
            // skip spawning to avoid wasting work on tasks that will fail.
            let Some(ws) = self.service.as_ref() else {
                return Ok(());
            };
            if !ws.has_available_urls() {
                return Ok(());
            }
            while self.tasks.len() < self.concurrency {
                let ws = ws.clone();

                let (bitfield, piece_length) = {
                    let pm = ctx.piece_mgr.read().await;
                    (pm.bitfield().to_vec(), ctx.metainfo.info.piece_length)
                };

                let gap = find_largest_gap(&bitfield).or_else(|| {
                    gap_within_file(
                        &bitfield,
                        ctx.metainfo,
                        piece_length,
                        self.config.min_gap_pieces,
                    )
                });

                let Some((gap_start, gap_size)) = gap else {
                    break; // no more gaps
                };

                let start_byte = gap_start as u64 * piece_length;
                let end_byte = (start_byte + self.config.max_range_bytes)
                    .min(ctx.metainfo.info.total_size())
                    .saturating_sub(1);

                tracing::debug!(
                    "web seed: found gap of {} pieces at index {}, requesting bytes [{}-{}] ({:.1}KB) [slot {}/{}]",
                    gap_size,
                    gap_start,
                    start_byte,
                    end_byte,
                    (end_byte - start_byte + 1) as f64 / 1024.0,
                    self.tasks.len(),
                    self.concurrency,
                );

                let range = PieceRange {
                    start_byte,
                    end_byte,
                };
                self.tasks.spawn(async move {
                    let mut ws = ws;
                    ws.download(range).await
                });
            }

            Ok(())
        })
    }
}

use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{Identity, Sink, Source, Transform};

pub struct PipelineConfig {
    pub poll_interval: Duration,
    pub stats_interval: Duration,
    pub reconnect_max_retries: u32,
    pub reconnect_initial_backoff: Duration,
    pub reconnect_max_backoff: Duration,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(100),
            stats_interval: Duration::from_secs(60),
            reconnect_max_retries: 10,
            reconnect_initial_backoff: Duration::from_millis(500),
            reconnect_max_backoff: Duration::from_secs(30),
        }
    }
}

pub struct CdcPipeline<S, K, T> {
    source: S,
    sink: K,
    transform: T,
    config: PipelineConfig,
}

impl<E, S, K> CdcPipeline<S, K, Identity<E>>
where
    E: Send + Sync + 'static,
    S: Source<Event = E>,
    K: Sink<Event = E>,
{
    pub fn new(source: S, sink: K, config: PipelineConfig) -> Self {
        Self {
            source,
            sink,
            transform: Identity::new(),
            config,
        }
    }
}

impl<S, K, T> CdcPipeline<S, K, T> {
    pub fn with_transform<T2>(self, transform: T2) -> CdcPipeline<S, K, T2> {
        CdcPipeline {
            source: self.source,
            sink: self.sink,
            transform,
            config: self.config,
        }
    }
}

impl<S, K, T> CdcPipeline<S, K, T>
where
    S: Source,
    T: Transform<Input = S::Event>,
    K: Sink<Event = T::Output>,
    S::Event: 'static,
    T::Output: 'static,
{
    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        let result = self.run_loop(cancel).await;
        self.sink.shutdown().await;
        info!("Pipeline stopped");
        result
    }

    async fn run_loop(&mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        let mut poll_interval = tokio::time::interval(self.config.poll_interval);
        let mut stats_interval = tokio::time::interval_at(
            tokio::time::Instant::now() + self.config.stats_interval,
            self.config.stats_interval,
        );

        let started_at = std::time::Instant::now();
        let mut total_processed: u64 = 0;
        let mut interval_processed: u64 = 0;

        info!("Pipeline started");

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    break;
                }
                _ = stats_interval.tick() => {
                    let uptime = started_at.elapsed();
                    let avg_rate = if uptime.as_secs() > 0 {
                        total_processed as f64 / uptime.as_secs_f64()
                    } else {
                        0.0
                    };
                    info!(
                        interval_processed,
                        total_processed,
                        avg_rate,
                        uptime_secs = uptime.as_secs(),
                        "Pipeline stats"
                    );
                    interval_processed = 0;
                }
                _ = poll_interval.tick() => {
                    match self.process_batch().await {
                        Ok(count) => {
                            total_processed += count;
                            interval_processed += count;
                        }
                        Err(e) if self.source.is_retriable_error(&e) => {
                            warn!(error = %e, "Source connection lost, reconnecting...");
                            self.reconnect_source().await?;
                        }
                        Err(e) => {
                            error!(error = %e, "Error processing batch, retrying...");
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    }
                }
            }
        }

        let uptime = started_at.elapsed();
        info!(total_processed, uptime_secs = uptime.as_secs(), "Pipeline shutting down");
        Ok(())
    }

    async fn process_batch(&mut self) -> anyhow::Result<u64> {
        let events = self.source.peek().await?;
        if events.is_empty() {
            return Ok(0);
        }

        let transformed = self.transform.transform(events).await?;
        let count = transformed.len() as u64;
        self.sink.publish(&transformed).await?;
        self.source.advance().await?;

        Ok(count)
    }

    async fn reconnect_source(&mut self) -> anyhow::Result<()> {
        let mut backoff = self.config.reconnect_initial_backoff;

        for attempt in 1..=self.config.reconnect_max_retries {
            info!(attempt, max_retries = self.config.reconnect_max_retries, "Attempting reconnection");
            tokio::time::sleep(backoff).await;

            match self.source.reconnect().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let next_backoff = (backoff * 2).min(self.config.reconnect_max_backoff);
                    warn!(
                        error = %e,
                        attempt,
                        next_backoff_ms = next_backoff.as_millis() as u64,
                        "Reconnection failed"
                    );
                    backoff = next_backoff;
                }
            }
        }

        Err(anyhow::anyhow!(
            "Failed to reconnect after {} attempts",
            self.config.reconnect_max_retries
        ))
    }
}

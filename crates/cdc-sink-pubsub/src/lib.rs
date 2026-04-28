use std::time::Duration;

use async_trait::async_trait;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::publisher::Publisher;
use tracing::{info, warn};

use cdc_core::Sink;

pub use google_cloud_googleapis::pubsub::v1::PubsubMessage;

const MAX_RETRIES: u32 = 5;
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(10);

#[derive(Debug, thiserror::Error)]
pub enum PubSubError {
    #[error("failed to create Pub/Sub auth config: {0}")]
    Auth(String),
    #[error("failed to create Pub/Sub client: {0}")]
    Client(String),
    #[error("publisher already shut down")]
    PublisherShutDown,
    #[error("publish failed: {0}")]
    Publish(String),
}

pub struct PubSubSink<E> {
    publisher: Option<Publisher>,
    topic_name: String,
    to_message: Box<dyn Fn(&E) -> PubsubMessage + Send + Sync>,
}

impl<E: Send + Sync + 'static> PubSubSink<E> {
    pub async fn new(
        topic: &str,
        to_message: impl Fn(&E) -> PubsubMessage + Send + Sync + 'static,
    ) -> Result<Self, PubSubError> {
        let config = if std::env::var("PUBSUB_EMULATOR_HOST").is_ok() {
            ClientConfig::default()
        } else {
            ClientConfig::default()
                .with_auth()
                .await
                .map_err(|e| PubSubError::Auth(e.to_string()))?
        };

        let client = Client::new(config)
            .await
            .map_err(|e| PubSubError::Client(e.to_string()))?;

        let topic = client.topic(topic);
        let publisher = topic.new_publisher(None);

        info!(topic = %topic.id(), "Pub/Sub publisher initialized");

        Ok(Self {
            publisher: Some(publisher),
            topic_name: topic.id().to_string(),
            to_message: Box::new(to_message),
        })
    }

    async fn try_publish(&self, messages: &[PubsubMessage]) -> Result<(), PubSubError> {
        let publisher = self
            .publisher
            .as_ref()
            .ok_or(PubSubError::PublisherShutDown)?;

        let mut awaiters = Vec::with_capacity(messages.len());

        for msg in messages {
            let awaiter = publisher.publish_blocking(msg.clone());
            awaiters.push(awaiter);
        }

        for awaiter in awaiters {
            awaiter
                .get()
                .await
                .map_err(|e| PubSubError::Publish(format!("{e:?}")))?;
        }

        Ok(())
    }
}

#[async_trait]
impl<E: Send + Sync + 'static> Sink for PubSubSink<E> {
    type Event = E;
    type Error = PubSubError;

    async fn publish(&self, events: &[E]) -> Result<(), PubSubError> {
        if events.is_empty() {
            return Ok(());
        }

        let messages: Vec<PubsubMessage> = events.iter().map(|e| (self.to_message)(e)).collect();
        let count = messages.len();

        let mut last_err = None;
        for attempt in 0..=MAX_RETRIES {
            match self.try_publish(&messages).await {
                Ok(()) => {
                    info!(count, topic = %self.topic_name, "Messages published to Pub/Sub");
                    return Ok(());
                }
                Err(e) => {
                    if attempt < MAX_RETRIES {
                        let backoff = INITIAL_BACKOFF * 2u32.pow(attempt);
                        let backoff = backoff.min(MAX_BACKOFF);
                        warn!(
                            error = %e,
                            attempt = attempt + 1,
                            max_retries = MAX_RETRIES,
                            backoff_ms = backoff.as_millis() as u64,
                            "Pub/Sub publish failed, retrying"
                        );
                        tokio::time::sleep(backoff).await;
                    }
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap())
    }

    async fn shutdown(&mut self) {
        if let Some(mut publisher) = self.publisher.take() {
            publisher.shutdown().await;
        }
    }
}

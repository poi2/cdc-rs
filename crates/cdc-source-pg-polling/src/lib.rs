pub mod decoder;
mod wal_reader;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use tracing::{info, warn};

use cdc_core::Source;
use cdc_source_pg_common as pg_common;
use decoder::WalPlugin;
use wal_reader::WalReader;

pub use pg_common::connect;

pub struct PgSourceConfig {
    pub database_url: String,
    pub outbox_table: String,
    pub slot_name: String,
    pub publication_name: String,
    pub max_changes_per_poll: u32,
    pub plugin: WalPlugin,
}

pub struct PgSource<E> {
    client: pg_common::Client,
    wal_reader: WalReader,
    config: PgSourceConfig,
    _marker: std::marker::PhantomData<E>,
}

impl<E: DeserializeOwned + Send + Sync + 'static> PgSource<E> {
    pub async fn new(config: PgSourceConfig) -> anyhow::Result<Self> {
        let client = pg_common::connect(&config.database_url).await?;
        pg_common::setup_replication(
            &client,
            &config.slot_name,
            &config.publication_name,
            &config.outbox_table,
            config.plugin.pg_output_plugin(),
        )
        .await?;
        let wal_reader = WalReader::new(
            &config.slot_name,
            config.max_changes_per_poll,
            &config.plugin.slot_options(),
        );
        Ok(Self {
            client,
            wal_reader,
            config,
            _marker: std::marker::PhantomData,
        })
    }

    pub async fn get_slot_lag_bytes(&self) -> anyhow::Result<i64> {
        pg_common::get_slot_lag_bytes(&self.client, &self.config.slot_name).await
    }
}

#[async_trait]
impl<E: DeserializeOwned + Send + Sync + 'static> Source for PgSource<E> {
    type Event = E;

    async fn peek(&mut self) -> anyhow::Result<Vec<E>> {
        let raw_entries = self.wal_reader.peek_changes(&self.client).await?;
        let mut events = Vec::new();

        for raw in &raw_entries {
            if let Some(json) = self.config.plugin.decode(raw, &self.config.outbox_table) {
                match serde_json::from_value::<E>(json) {
                    Ok(event) => events.push(event),
                    Err(e) => {
                        warn!(error = %e, data = raw, "Failed to deserialize WAL event");
                    }
                }
            }
        }

        if !events.is_empty() {
            info!(count = events.len(), "INSERT events decoded from WAL");
        }

        Ok(events)
    }

    async fn advance(&mut self) -> anyhow::Result<()> {
        self.wal_reader.advance_slot(&self.client).await
    }

    async fn reconnect(&mut self) -> anyhow::Result<()> {
        let client = pg_common::connect(&self.config.database_url).await?;
        self.client = client;
        info!("PostgreSQL reconnected");
        Ok(())
    }

    fn is_retriable_error(&self, err: &anyhow::Error) -> bool {
        pg_common::is_connection_error(err)
    }
}

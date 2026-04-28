mod config;
mod event;
mod shutdown;

use std::collections::HashMap;
use std::time::Duration;

use clap::Parser;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tracing::{error, info};

use cdc_core::{CdcPipeline, PipelineConfig};
use cdc_sink_pubsub::{PubSubSink, PubsubMessage};
use cdc_source_pg_polling::{PgSource, PgSourceConfig};

use config::Config;
use event::OutboxEvent;

fn to_pubsub_message(event: &OutboxEvent) -> PubsubMessage {
    let mut attributes = HashMap::with_capacity(4);
    attributes.insert("event_id".to_string(), event.event_id.to_string());
    attributes.insert("entity_id".to_string(), event.entity_id.clone());
    attributes.insert("event_name".to_string(), event.event_name.clone());
    attributes.insert("occurred_at".to_string(), event.occurred_at.clone());

    PubsubMessage {
        data: event.payload_binary.clone().into(),
        ordering_key: event.entity_id.clone(),
        attributes,
        ..Default::default()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .json()
        .init();

    let config = Config::parse();

    info!(
        version = env!("CARGO_PKG_VERSION"),
        database_url = %mask_database_url(&config.database_url),
        outbox_table = %config.outbox_table,
        slot_name = %config.slot_name,
        publication_name = %config.publication_name,
        pubsub_topic = %config.pubsub_topic,
        poll_interval_ms = config.poll_interval_ms,
        max_changes_per_poll = config.max_changes_per_poll,
        health_check_port = config.health_check_port,
        "cdc-rs starting"
    );

    let cancellation_token = shutdown::setup_shutdown_handler();

    let health_token = cancellation_token.clone();
    let health_port = config.health_check_port;
    tokio::spawn(async move {
        if let Err(e) = run_health_server(health_port, health_token).await {
            error!(error = %e, "Health check server failed");
        }
    });

    let source_config = PgSourceConfig {
        database_url: config.database_url.clone(),
        outbox_table: config.outbox_table.clone(),
        slot_name: config.slot_name.clone(),
        publication_name: config.publication_name.clone(),
        max_changes_per_poll: config.max_changes_per_poll,
        plugin: Default::default(),
    };

    let source = PgSource::<OutboxEvent>::new(source_config).await?;
    let sink = PubSubSink::new(&config.pubsub_topic, to_pubsub_message).await?;

    let pipeline_config = PipelineConfig {
        poll_interval: Duration::from_millis(config.poll_interval_ms),
        ..Default::default()
    };

    CdcPipeline::new(source, sink, pipeline_config)
        .run(cancellation_token)
        .await?;

    Ok(())
}

fn mask_database_url(url: &str) -> String {
    let after_scheme = url.find("://").map(|p| p + 3).unwrap_or(0);
    if let Some(at_pos) = url[after_scheme..].find('@') {
        let at_pos = after_scheme + at_pos;
        if let Some(colon_pos) = url[after_scheme..at_pos].rfind(':') {
            let colon_pos = after_scheme + colon_pos;
            let prefix = &url[..colon_pos + 1];
            let suffix = &url[at_pos..];
            return format!("{prefix}***{suffix}");
        }
    }
    url.to_string()
}

async fn run_health_server(
    port: u16,
    cancel: tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    info!(port, "Health check server started");

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            result = listener.accept() => {
                if let Ok((mut socket, _)) = result {
                    let _ = socket.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK"
                    ).await;
                }
            }
        }
    }

    Ok(())
}

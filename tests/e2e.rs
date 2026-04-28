//! E2E integration tests for the CDC pipeline.
//!
//! Requirements:
//!   docker compose up -d
//!
//! Run:
//!   cargo test --test e2e -- --ignored --test-threads=1

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use cdc_core::{LsnEvent, Sink, Source};
use cdc_sink_pubsub::{PubSubSink, PubsubMessage};
use cdc_source_pg_polling::{PgSource, PgSourceConfig};
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscription::SubscriptionConfig;

const DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/cdc_rs";
const EMULATOR_HOST: &str = "localhost:8085";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OutboxEvent {
    event_id: Uuid,
    entity_id: String,
    occurred_at: String,
    event_name: String,
    payload: serde_json::Value,
    payload_binary: Vec<u8>,
}

fn setup_env() {
    // SAFETY: These tests are run with --test-threads=1
    unsafe {
        std::env::set_var("PUBSUB_EMULATOR_HOST", EMULATOR_HOST);
    }
}

fn to_pubsub_message(event: &LsnEvent<OutboxEvent>) -> PubsubMessage {
    let mut attributes = HashMap::with_capacity(5);
    attributes.insert("event_id".to_string(), event.event.event_id.to_string());
    attributes.insert("entity_id".to_string(), event.event.entity_id.clone());
    attributes.insert("event_name".to_string(), event.event.event_name.clone());
    attributes.insert("occurred_at".to_string(), event.event.occurred_at.clone());
    attributes.insert("__lsn".to_string(), event.lsn.to_string());

    PubsubMessage {
        data: event.event.payload_binary.clone().into(),
        ordering_key: event.event.entity_id.clone(),
        attributes,
        ..Default::default()
    }
}

fn test_source_config(slot_name: &str, pub_name: &str) -> PgSourceConfig {
    PgSourceConfig {
        database_url: DATABASE_URL.to_string(),
        outbox_table: "transactional_box.outbox".to_string(),
        slot_name: slot_name.to_string(),
        publication_name: pub_name.to_string(),
        max_changes_per_poll: 1000,
        plugin: Default::default(),
    }
}

async fn setup_pg_client() -> tokio_postgres::Client {
    cdc_source_pg_polling::connect(DATABASE_URL).await.unwrap()
}

async fn setup_outbox_table(client: &tokio_postgres::Client) {
    client
        .simple_query("CREATE SCHEMA IF NOT EXISTS transactional_box")
        .await
        .unwrap();
    client
        .simple_query("DROP TABLE IF EXISTS transactional_box.outbox CASCADE")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE TABLE transactional_box.outbox (
                event_id UUID PRIMARY KEY,
                entity_id TEXT NOT NULL,
                occurred_at TIMESTAMP NOT NULL,
                event_name TEXT NOT NULL,
                payload JSONB NOT NULL,
                payload_binary BYTEA NOT NULL DEFAULT ''::bytea
            )",
        )
        .await
        .unwrap();
}

async fn cleanup_slot(client: &tokio_postgres::Client, slot_name: &str) {
    let _ = client
        .simple_query(&format!(
            "SELECT pg_drop_replication_slot('{}')",
            slot_name
        ))
        .await;
}

async fn cleanup_publication(client: &tokio_postgres::Client, pub_name: &str) {
    let _ = client
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {}", pub_name))
        .await;
}

async fn create_pubsub_client() -> Client {
    let config = ClientConfig::default();
    Client::new(config).await.unwrap()
}

async fn insert_outbox_row(
    client: &tokio_postgres::Client,
    event_id: &Uuid,
    entity_id: &str,
    event_name: &str,
    payload_json: &str,
    payload_binary_hex: &str,
) {
    client
        .simple_query(&format!(
            "INSERT INTO transactional_box.outbox \
             (event_id, entity_id, occurred_at, event_name, payload, payload_binary) \
             VALUES ('{event_id}', '{entity_id}', '2024-01-15 10:30:00', \
             '{event_name}', '{payload_json}', '\\x{payload_binary_hex}')"
        ))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore]
async fn test_pipeline_publishes_insert_events() {
    setup_env();

    let slot_name = "e2e_insert_slot";
    let pub_name = "e2e_insert_pub";
    let topic_name = "e2e-insert-topic";
    let sub_name = "e2e-insert-sub";

    // Setup PostgreSQL
    let pg_client = setup_pg_client().await;
    setup_outbox_table(&pg_client).await;
    cleanup_slot(&pg_client, slot_name).await;
    cleanup_publication(&pg_client, pub_name).await;

    // Create source (sets up replication slot and publication)
    let source_config = test_source_config(slot_name, pub_name);
    let mut source = PgSource::<OutboxEvent>::new(source_config).await.unwrap();

    // Setup Pub/Sub emulator
    let pubsub_client = create_pubsub_client().await;
    let topic = pubsub_client.topic(topic_name);
    let _ = topic.create(None, None).await;

    let sub = pubsub_client.subscription(sub_name);
    let sub_config = SubscriptionConfig {
        enable_message_ordering: true,
        ..Default::default()
    };
    let _ = sub.create(topic.fully_qualified_name(), sub_config, None).await;

    // Insert a row into the outbox table
    let event_id = Uuid::new_v4();
    let binary_data = b"Hello, gRPC!";
    let binary_hex = hex::encode(binary_data);
    insert_outbox_row(
        &pg_client,
        &event_id,
        "entity-1",
        "OrderCreated",
        r#"{"key": "value"}"#,
        &binary_hex,
    )
    .await;

    // Run pipeline: peek -> publish -> advance
    let mut sink = PubSubSink::new(topic_name, to_pubsub_message).await.unwrap();

    let events = source.peek().await.unwrap();
    assert_eq!(events.len(), 1, "Should decode exactly one INSERT event");

    sink.publish(&events).await.unwrap();
    source.advance().await.unwrap();

    // Pull messages from subscription and verify
    let received = sub.pull(10, None).await.unwrap();
    assert_eq!(received.len(), 1, "Should receive exactly one message");

    let msg = &received[0].message;

    assert_eq!(msg.data.as_slice(), binary_data);
    assert_eq!(msg.ordering_key, "entity-1");

    let attrs = &msg.attributes;
    assert_eq!(attrs.get("event_id").unwrap(), &event_id.to_string());
    assert_eq!(attrs.get("entity_id").unwrap(), "entity-1");
    assert_eq!(attrs.get("event_name").unwrap(), "OrderCreated");
    assert_eq!(attrs.get("occurred_at").unwrap(), "2024-01-15 10:30:00");

    // Cleanup
    received[0].ack().await.unwrap();
    sink.shutdown().await;
    cleanup_slot(&pg_client, slot_name).await;
    cleanup_publication(&pg_client, pub_name).await;
}

#[tokio::test]
#[ignore]
async fn test_pipeline_filters_update_events() {
    setup_env();

    let slot_name = "e2e_update_slot";
    let pub_name = "e2e_update_pub";

    // Setup PostgreSQL
    let pg_client = setup_pg_client().await;
    setup_outbox_table(&pg_client).await;
    cleanup_slot(&pg_client, slot_name).await;
    cleanup_publication(&pg_client, pub_name).await;

    let source_config = test_source_config(slot_name, pub_name);
    let mut source = PgSource::<OutboxEvent>::new(source_config).await.unwrap();

    // Insert a row
    let event_id = Uuid::new_v4();
    insert_outbox_row(
        &pg_client,
        &event_id,
        "entity-1",
        "OrderCreated",
        r#"{"key": "value"}"#,
        "",
    )
    .await;

    // Consume the INSERT event
    let events = source.peek().await.unwrap();
    assert_eq!(events.len(), 1, "Should have one INSERT event");
    source.advance().await.unwrap();

    // UPDATE the row
    pg_client
        .simple_query(&format!(
            "UPDATE transactional_box.outbox SET event_name = 'Updated' WHERE event_id = '{}'",
            event_id
        ))
        .await
        .unwrap();

    // Peek again - UPDATE should be filtered by decoder
    let events = source.peek().await.unwrap();
    assert_eq!(
        events.len(),
        0,
        "UPDATE events should be filtered out by decoder"
    );

    // Cleanup
    source.advance().await.unwrap();
    cleanup_slot(&pg_client, slot_name).await;
    cleanup_publication(&pg_client, pub_name).await;
}

#[tokio::test]
#[ignore]
async fn test_multiple_inserts_same_entity() {
    setup_env();

    let slot_name = "e2e_multi_slot";
    let pub_name = "e2e_multi_pub";
    let topic_name = "e2e-multi-topic";
    let sub_name = "e2e-multi-sub";

    // Setup PostgreSQL
    let pg_client = setup_pg_client().await;
    setup_outbox_table(&pg_client).await;
    cleanup_slot(&pg_client, slot_name).await;
    cleanup_publication(&pg_client, pub_name).await;

    let source_config = test_source_config(slot_name, pub_name);
    let mut source = PgSource::<OutboxEvent>::new(source_config).await.unwrap();

    // Setup Pub/Sub emulator
    let pubsub_client = create_pubsub_client().await;
    let topic = pubsub_client.topic(topic_name);
    let _ = topic.create(None, None).await;

    let sub = pubsub_client.subscription(sub_name);
    let sub_config = SubscriptionConfig {
        enable_message_ordering: true,
        ..Default::default()
    };
    let _ = sub.create(topic.fully_qualified_name(), sub_config, None).await;

    // Insert 3 rows with the same entity_id
    let entity_id = "shared-entity";
    let event_ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();

    for (i, event_id) in event_ids.iter().enumerate() {
        insert_outbox_row(
            &pg_client,
            event_id,
            entity_id,
            &format!("Event{}", i),
            &format!("{{\"index\": {}}}", i),
            &hex::encode(format!("msg-{}", i)),
        )
        .await;
    }

    // Run pipeline
    let mut sink = PubSubSink::new(topic_name, to_pubsub_message).await.unwrap();

    let events = source.peek().await.unwrap();
    assert_eq!(events.len(), 3, "Should decode 3 INSERT events");

    sink.publish(&events).await.unwrap();
    source.advance().await.unwrap();

    // Pull and verify all messages share the same ordering key
    let received = sub.pull(10, None).await.unwrap();
    assert_eq!(received.len(), 3, "Should receive 3 messages");

    for msg in &received {
        assert_eq!(
            msg.message.ordering_key,
            entity_id,
            "All messages should have the same ordering key (entity_id)"
        );
        msg.ack().await.unwrap();
    }

    let received_event_ids: Vec<String> = received
        .iter()
        .map(|m| m.message.attributes.get("event_id").unwrap().clone())
        .collect();
    for event_id in &event_ids {
        assert!(
            received_event_ids.contains(&event_id.to_string()),
            "Event {} should be in received messages",
            event_id
        );
    }

    // Cleanup
    sink.shutdown().await;
    cleanup_slot(&pg_client, slot_name).await;
    cleanup_publication(&pg_client, pub_name).await;
}

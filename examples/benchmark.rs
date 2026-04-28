//! CDC-RS Benchmark: WAL read + decode + publish throughput measurement.
//!
//! Compares:
//!   1. Polling via pg_logical_slot_get_changes (batch=1000, 10000)
//!   2. Streaming via pg_recvlogical (PostgreSQL built-in streaming replication)
//!   3. Full pipeline: Polling + Pub/Sub publish (via emulator)
//!
//! Requirements:
//!   docker compose up -d
//!
//! Run:
//!   cargo run --example benchmark --release [NUM_ROWS] [PAYLOAD_BYTES]
//!
//! Default: 100,000 rows, 300 bytes payload
//!
//! To compare payload sizes:
//!   cargo run --example benchmark --release 100000 100
//!   cargo run --example benchmark --release 100000 300
//!   cargo run --example benchmark --release 100000 1000

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;
use uuid::Uuid;

use cdc_core::Sink;
use cdc_sink_pubsub::{PubSubSink, PubsubMessage};
use cdc_source_pg_polling::decoder::WalPlugin;

const DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/cdc_rs";
const TABLE: &str = "transactional_box.outbox";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OutboxEvent {
    event_id: Uuid,
    entity_id: String,
    occurred_at: String,
    event_name: String,
    payload: serde_json::Value,
    payload_binary: Vec<u8>,
}

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

struct BenchResult {
    label: String,
    events: usize,
    elapsed: Duration,
}

impl BenchResult {
    fn tps(&self) -> f64 {
        self.events as f64 / self.elapsed.as_secs_f64()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let num_rows: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);

    let payload_bytes: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(300);

    println!("CDC-RS Benchmark");
    println!("================");
    println!("Rows: {num_rows}");
    println!("Payload size: {payload_bytes} bytes");
    println!();

    let (client, conn) = tokio_postgres::connect(DATABASE_URL, NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    setup_table(&client).await?;

    // Create slots (all capture the same WAL inserts)
    let slots = [
        "bench_poll_1k",
        "bench_poll_10k",
        "bench_stream",
        "bench_pipeline",
    ];
    for s in &slots {
        drop_slot(&client, s).await;
        create_slot(&client, s).await?;
    }

    // Insert test data
    print!("Inserting {num_rows} rows ({payload_bytes}B payload)... ");
    let t = Instant::now();
    insert_rows(&client, num_rows, payload_bytes).await?;
    let insert_time = t.elapsed();
    let insert_tps = num_rows as f64 / insert_time.as_secs_f64();
    println!("{insert_time:.2?} ({insert_tps:.0} rows/sec)");

    // Get current WAL position (needed for pg_recvlogical --endpos)
    let end_lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?
        .get(0);
    println!("WAL end position: {end_lsn}");
    println!();

    let mut results = Vec::new();

    // --- Polling benchmarks ---
    for (slot, batch) in [("bench_poll_1k", 1_000), ("bench_poll_10k", 10_000)] {
        let r = run_poll_bench(&client, slot, batch, num_rows).await?;
        results.push(r);
    }

    // --- Streaming benchmark (pg_recvlogical via docker exec) ---
    if let Some(r) = run_stream_bench("bench_stream", num_rows, &end_lsn).await {
        results.push(r);
    }

    // --- Full pipeline benchmark (WAL read + decode + Pub/Sub emulator publish) ---
    if let Some(r) = run_pipeline_bench(&client, "bench_pipeline", num_rows).await {
        results.push(r);
    }

    // --- Summary ---
    println!("=== Summary ===");
    println!("{:<35} {:>12} {:>14}", "Approach", "msgs/sec", "Time");
    println!("{}", "-".repeat(63));
    for r in &results {
        println!(
            "{:<35} {:>12.0} {:>14.2?}",
            r.label,
            r.tps(),
            r.elapsed
        );
    }

    if let Some(best) = results.iter().max_by(|a, b| a.tps().partial_cmp(&b.tps()).unwrap()) {
        println!();
        println!("Best: {} ({:.0} msgs/sec)", best.label, best.tps());
        println!();
        println!("Estimated time for 1B messages:");
        for r in &results {
            let hours = 1_000_000_000.0 / r.tps() / 3600.0;
            println!("  {:<33} {hours:.1} hours", r.label);
        }
    }

    // Cleanup
    for s in &slots {
        drop_slot(&client, s).await;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Setup helpers
// ---------------------------------------------------------------------------

async fn setup_table(client: &tokio_postgres::Client) -> anyhow::Result<()> {
    client
        .simple_query("CREATE SCHEMA IF NOT EXISTS transactional_box")
        .await?;
    client
        .simple_query(&format!("DROP TABLE IF EXISTS {TABLE} CASCADE"))
        .await?;
    client
        .simple_query(&format!(
            "CREATE TABLE {TABLE} (
                event_id UUID PRIMARY KEY,
                entity_id TEXT NOT NULL,
                occurred_at TIMESTAMP NOT NULL,
                event_name TEXT NOT NULL,
                payload JSONB NOT NULL,
                payload_binary BYTEA NOT NULL DEFAULT ''::bytea
            )"
        ))
        .await?;

    // Create publication for pg_recvlogical streaming
    let _ = client
        .simple_query("DROP PUBLICATION IF EXISTS bench_pub")
        .await;
    client
        .simple_query(&format!(
            "CREATE PUBLICATION bench_pub FOR TABLE {TABLE}"
        ))
        .await?;

    Ok(())
}

async fn create_slot(client: &tokio_postgres::Client, name: &str) -> anyhow::Result<()> {
    client
        .query(
            &format!("SELECT pg_create_logical_replication_slot('{name}', 'test_decoding')"),
            &[],
        )
        .await?;
    Ok(())
}

async fn drop_slot(client: &tokio_postgres::Client, name: &str) {
    let _ = client
        .simple_query(&format!("SELECT pg_drop_replication_slot('{name}')"))
        .await;
}

async fn insert_rows(client: &tokio_postgres::Client, n: usize, payload_bytes: usize) -> anyhow::Result<()> {
    let chunk = 100_000usize;
    let mut inserted = 0usize;
    while inserted < n {
        let batch = chunk.min(n - inserted);
        client
            .simple_query(&format!(
                "INSERT INTO {TABLE}
                    (event_id, entity_id, occurred_at,
                     event_name, payload, payload_binary)
                SELECT
                    gen_random_uuid(),
                    'entity-' || i::text,
                    '2024-01-15 10:30:00'::timestamp,
                    'BenchmarkEvent',
                    '{{}}'::jsonb,
                    decode(repeat('00', {payload_bytes}), 'hex')
                FROM generate_series(1, {batch}) AS i"
            ))
            .await?;
        inserted += batch;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Polling benchmark
// ---------------------------------------------------------------------------

async fn run_poll_bench(
    client: &tokio_postgres::Client,
    slot_name: &str,
    batch_size: usize,
    expected: usize,
) -> anyhow::Result<BenchResult> {
    let label = format!("Polling (batch={batch_size})");
    print!("{label}... ");
    let plugin = WalPlugin::TestDecoding;

    let t = Instant::now();
    let breakdown = bench_poll(client, slot_name, batch_size, &plugin).await?;
    let elapsed = t.elapsed();
    let count = breakdown.total;
    let tps = count as f64 / elapsed.as_secs_f64();

    println!("{count} events in {elapsed:.2?} ({tps:.0} msgs/sec)");
    let mb = breakdown.total_bytes as f64 / 1_048_576.0;
    let sql_pct = breakdown.sql_time.as_secs_f64() / elapsed.as_secs_f64() * 100.0;
    let decode_pct = breakdown.decode_time.as_secs_f64() / elapsed.as_secs_f64() * 100.0;
    println!(
        "  Breakdown: SQL {:.2?} ({sql_pct:.0}%) | Decode {:.2?} ({decode_pct:.0}%) | Data {mb:.1} MB",
        breakdown.sql_time, breakdown.decode_time,
    );
    if count != expected {
        println!("  WARNING: expected {expected}, got {count}");
    }

    Ok(BenchResult {
        label,
        events: count,
        elapsed,
    })
}

struct PollBreakdown {
    total: usize,
    sql_time: Duration,
    decode_time: Duration,
    total_bytes: usize,
}

async fn bench_poll(
    client: &tokio_postgres::Client,
    slot_name: &str,
    batch_size: usize,
    plugin: &WalPlugin,
) -> anyhow::Result<PollBreakdown> {
    let mut total = 0;
    let mut sql_time = Duration::ZERO;
    let mut decode_time = Duration::ZERO;
    let mut total_bytes = 0usize;
    loop {
        let query = format!(
            "SELECT data FROM pg_logical_slot_get_changes('{slot_name}', NULL, {batch_size})"
        );
        let t = Instant::now();
        let rows = client.query(&query, &[]).await?;
        sql_time += t.elapsed();

        if rows.is_empty() {
            break;
        }

        let t = Instant::now();
        for row in &rows {
            let data: String = row.get(0);
            total_bytes += data.len();
            if let Some(json) = plugin.decode(&data, TABLE) {
                if serde_json::from_value::<OutboxEvent>(json).is_ok() {
                    total += 1;
                }
            }
        }
        decode_time += t.elapsed();
    }
    Ok(PollBreakdown { total, sql_time, decode_time, total_bytes })
}

// ---------------------------------------------------------------------------
// Streaming benchmark (pg_recvlogical via docker exec)
// ---------------------------------------------------------------------------

async fn run_stream_bench(slot_name: &str, expected: usize, end_lsn: &str) -> Option<BenchResult> {
    let label = "Streaming (pg_recvlogical)".to_string();
    print!("{label}... ");

    let container = find_postgres_container().await;
    let container = match container {
        Some(c) => c,
        None => {
            println!("SKIP (Docker container not found)");
            return None;
        }
    };

    let t = Instant::now();

    let output = tokio::process::Command::new("docker")
        .args([
            "exec",
            &container,
            "pg_recvlogical",
            "-d",
            "cdc_rs",
            "-U",
            "postgres",
            "-S",
            slot_name,
            "--start",
            "-f",
            "-",
            "--no-loop",
            &format!("--endpos={end_lsn}"),
        ])
        .output()
        .await;

    let elapsed = t.elapsed();
    let plugin = WalPlugin::TestDecoding;

    match output {
        Ok(out) if out.status.success() => {
            let text = String::from_utf8_lossy(&out.stdout);
            let count = text
                .lines()
                .filter(|line| {
                    plugin.decode(line, TABLE)
                        .and_then(|json| serde_json::from_value::<OutboxEvent>(json).ok())
                        .is_some()
                })
                .count();
            let tps = count as f64 / elapsed.as_secs_f64();

            println!("{count} events in {elapsed:.2?} ({tps:.0} msgs/sec)");
            if count != expected {
                println!("  WARNING: expected {expected}, got {count}");
            }

            println!("  Note: runs inside Docker container (localhost connection)");

            Some(BenchResult {
                label,
                events: count,
                elapsed,
            })
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            println!("FAILED: {stderr}");
            None
        }
        Err(e) => {
            println!("SKIP ({e})");
            None
        }
    }
}

async fn find_postgres_container() -> Option<String> {
    let output = tokio::process::Command::new("docker")
        .args(["ps", "--filter", "ancestor=postgres:15", "--format", "{{.Names}}"])
        .output()
        .await
        .ok()?;

    let name = String::from_utf8_lossy(&output.stdout);
    let name = name.trim();
    if name.is_empty() {
        None
    } else {
        Some(name.lines().next().unwrap().to_string())
    }
}

// ---------------------------------------------------------------------------
// Full pipeline benchmark (WAL read + decode + Pub/Sub emulator publish)
// ---------------------------------------------------------------------------

const EMULATOR_HOST: &str = "localhost:8085";
const TOPIC_NAME: &str = "bench-pipeline-topic";

async fn run_pipeline_bench(
    pg_client: &tokio_postgres::Client,
    slot_name: &str,
    expected: usize,
) -> Option<BenchResult> {
    let label = "Pipeline + Pub/Sub emulator".to_string();
    print!("{label}... ");

    unsafe {
        std::env::set_var("PUBSUB_EMULATOR_HOST", EMULATOR_HOST);
    }

    let pubsub_client = match google_cloud_pubsub::client::Client::new(
        google_cloud_pubsub::client::ClientConfig::default(),
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            println!("SKIP (Pub/Sub emulator not available: {e})");
            return None;
        }
    };

    let topic = pubsub_client.topic(TOPIC_NAME);
    let _ = topic.create(None, None).await;

    let mut sink = match PubSubSink::new(TOPIC_NAME, to_pubsub_message).await {
        Ok(s) => s,
        Err(e) => {
            println!("SKIP (sink init failed: {e})");
            return None;
        }
    };

    let plugin = WalPlugin::TestDecoding;
    let batch_size = 1_000usize;
    let mut total = 0usize;

    let t = Instant::now();

    loop {
        let query = format!(
            "SELECT data FROM pg_logical_slot_get_changes('{slot_name}', NULL, {batch_size})"
        );
        let rows = pg_client.query(&query, &[]).await.ok()?;
        if rows.is_empty() {
            break;
        }

        let mut events = Vec::new();
        for row in &rows {
            let data: String = row.get(0);
            if let Some(json) = plugin.decode(&data, TABLE) {
                if let Ok(event) = serde_json::from_value::<OutboxEvent>(json) {
                    events.push(event);
                }
            }
        }

        if !events.is_empty() {
            if let Err(e) = sink.publish(&events).await {
                println!("FAILED (publish error: {e})");
                return None;
            }
            total += events.len();
        }
    }

    let elapsed = t.elapsed();
    let tps = total as f64 / elapsed.as_secs_f64();

    sink.shutdown().await;

    println!("{total} events in {elapsed:.2?} ({tps:.0} msgs/sec)");
    if total != expected {
        println!("  WARNING: expected {expected}, got {total}");
    }

    Some(BenchResult {
        label,
        events: total,
        elapsed,
    })
}

//! Example: PostgreSQL -> CDC -> Iceberg
//!
//! Demonstrates the full CDC pipeline that captures WAL changes from a
//! PostgreSQL table and writes them to an Apache Iceberg table (Parquet).
//!
//! Requirements:
//!   docker compose up -d
//!
//! Run:
//!   cargo run --example pg_to_iceberg
//!
//! Verify (after run):
//!   find /tmp/cdc-iceberg-example -name '*.parquet'
//!   # or with DuckDB:
//!   duckdb -c "SELECT * FROM parquet_scan('/tmp/cdc-iceberg-example/**/*.parquet')"

use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::RecordBatch;
use arrow_schema::DataType;
use futures::TryStreamExt;
use iceberg::spec::PrimitiveType;
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;

use cdc_core::{LsnEvent, Sink, Source};
use cdc_sink_iceberg::{IcebergFieldDef, IcebergSink, IcebergSinkConfig};
use cdc_source_pg_polling::decoder::WalPlugin;
use cdc_source_pg_polling::{PgSource, PgSourceConfig};

const DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/cdc_rs";
const TABLE: &str = "public.sensor_readings";
const SLOT_NAME: &str = "iceberg_demo_slot";
const PUB_NAME: &str = "iceberg_demo_pub";
const WAREHOUSE_PATH: &str = "/tmp/cdc-iceberg-example";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SensorReading {
    sensor_id: String,
    temperature: i64,
    humidity: i64,
}

fn event_fields() -> Vec<IcebergFieldDef> {
    vec![
        IcebergFieldDef {
            name: "sensor_id".to_string(),
            iceberg_type: PrimitiveType::String,
            arrow_type: DataType::Utf8,
            required: true,
        },
        IcebergFieldDef {
            name: "temperature".to_string(),
            iceberg_type: PrimitiveType::Long,
            arrow_type: DataType::Int64,
            required: true,
        },
        IcebergFieldDef {
            name: "humidity".to_string(),
            iceberg_type: PrimitiveType::Long,
            arrow_type: DataType::Int64,
            required: true,
        },
    ]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .compact()
        .init();

    println!("=== PostgreSQL -> CDC -> Iceberg Demo ===");
    println!();

    // -- 1. Setup PostgreSQL table --
    println!("[1/5] Setting up PostgreSQL table...");
    let (pg_client, pg_conn) = tokio_postgres::connect(DATABASE_URL, NoTls).await?;
    tokio::spawn(async move {
        let _ = pg_conn.await;
    });

    // Clean up from previous runs
    let _ = pg_client
        .simple_query(&format!("SELECT pg_drop_replication_slot('{SLOT_NAME}')"))
        .await;
    let _ = pg_client
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {PUB_NAME}"))
        .await;
    pg_client
        .simple_query(&format!("DROP TABLE IF EXISTS {TABLE}"))
        .await?;
    pg_client
        .simple_query(&format!(
            "CREATE TABLE {TABLE} (
                sensor_id TEXT NOT NULL,
                temperature INTEGER NOT NULL,
                humidity INTEGER NOT NULL
            )"
        ))
        .await?;
    println!("  Created table: {TABLE}");

    // Clean warehouse directory
    let _ = std::fs::remove_dir_all(WAREHOUSE_PATH);
    std::fs::create_dir_all(WAREHOUSE_PATH)?;
    println!("  Warehouse: {WAREHOUSE_PATH}");

    // -- 2. Create CDC source (creates replication slot + publication) --
    println!();
    println!("[2/5] Creating CDC source (replication slot)...");
    let source_config = PgSourceConfig {
        database_url: DATABASE_URL.to_string(),
        outbox_table: TABLE.to_string(),
        slot_name: SLOT_NAME.to_string(),
        publication_name: PUB_NAME.to_string(),
        max_changes_per_poll: 1000,
        plugin: WalPlugin::TestDecoding,
    };
    let mut source = PgSource::<SensorReading>::new(source_config).await?;
    println!("  Slot: {SLOT_NAME}, Publication: {PUB_NAME}");

    // -- 3. Insert test data (after slot creation so WAL captures it) --
    println!();
    println!("[3/5] Inserting test data...");
    let test_data = [
        ("sensor-A", 22, 45),
        ("sensor-B", 25, 60),
        ("sensor-C", 18, 72),
        ("sensor-A", 23, 44),
        ("sensor-B", 26, 58),
    ];
    for (sensor_id, temp, hum) in &test_data {
        pg_client
            .execute(
                &format!(
                    "INSERT INTO {TABLE} (sensor_id, temperature, humidity) VALUES ($1, $2, $3)"
                ),
                &[sensor_id, temp, hum],
            )
            .await?;
    }
    println!("  Inserted {} rows", test_data.len());

    // -- 4. Create Iceberg sink --
    println!();
    println!("[4/5] Creating Iceberg sink...");
    let sink = IcebergSink::<SensorReading, _>::new(
        IcebergSinkConfig {
            warehouse_path: WAREHOUSE_PATH.to_string(),
            namespace: "cdc_demo".to_string(),
            table_name: "sensor_readings".to_string(),
        },
        event_fields(),
    )
    .await?;
    println!("  Table: cdc_demo.sensor_readings");

    // -- 5. Run CDC: poll -> publish -> advance --
    println!();
    println!("[5/5] Running CDC pipeline...");
    let mut total_events = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }

        let events: Vec<LsnEvent<SensorReading>> = source.peek().await?;
        if events.is_empty() {
            if total_events > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }

        println!(
            "  Captured {} events from WAL (LSN range: {:#X}..{:#X})",
            events.len(),
            events.first().unwrap().lsn,
            events.last().unwrap().lsn,
        );

        sink.publish(&events).await?;
        source.advance().await?;
        total_events += events.len();
    }

    println!("  Total events written to Iceberg: {total_events}");

    // -- Verify: read back from Iceberg --
    println!();
    println!("=== Iceberg Table Contents ===");
    println!();

    let table = sink.table().await;
    let scan = table.scan().build()?;
    let batches: Vec<RecordBatch> = scan.to_arrow().await?.try_collect().await?;

    println!(
        "  {:<12} {:<12} {:<14} {:<10}",
        "__lsn", "sensor_id", "temperature", "humidity"
    );
    println!("  {}", "-".repeat(50));

    for batch in &batches {
        let lsn_col = batch.column(0).as_primitive::<Int64Type>();
        let sensor_col = batch.column(1).as_string::<i32>();
        let temp_col = batch.column(2).as_primitive::<Int64Type>();
        let hum_col = batch.column(3).as_primitive::<Int64Type>();

        for i in 0..batch.num_rows() {
            println!(
                "  {:<12} {:<12} {:<14} {:<10}",
                format!("{:#X}", lsn_col.value(i)),
                sensor_col.value(i),
                temp_col.value(i),
                hum_col.value(i),
            );
        }
    }

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!();
    println!("  Total rows: {total_rows}");

    drop(table);

    // -- Manual verification instructions --
    println!();
    println!("=== Manual Verification ===");
    println!();
    println!("  Parquet files:");

    for entry in walkdir(WAREHOUSE_PATH) {
        if entry.ends_with(".parquet") {
            println!("    {entry}");
        }
    }

    println!();
    println!("  DuckDB:");
    println!("    duckdb -c \"SELECT * FROM parquet_scan('{WAREHOUSE_PATH}/**/*.parquet')\"");

    // -- Cleanup replication slot --
    println!();
    let _ = pg_client
        .simple_query(&format!("SELECT pg_drop_replication_slot('{SLOT_NAME}')"))
        .await;
    let _ = pg_client
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {PUB_NAME}"))
        .await;
    println!("Cleaned up replication slot and publication.");

    Ok(())
}

fn walkdir(path: &str) -> Vec<String> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                files.extend(walkdir(p.to_str().unwrap_or_default()));
            } else {
                files.push(p.display().to_string());
            }
        }
    }
    files
}

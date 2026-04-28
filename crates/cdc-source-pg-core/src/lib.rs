use tokio_postgres::NoTls;
use tracing::info;

pub use tokio_postgres::Client;

pub fn is_connection_error(err: &anyhow::Error) -> bool {
    if let Some(pg_err) = err.downcast_ref::<tokio_postgres::Error>() {
        return pg_err.is_closed();
    }
    false
}

pub async fn connect(database_url: &str) -> anyhow::Result<Client> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("Database connection error: {}", e);
        }
    });

    info!("Connected to PostgreSQL");
    Ok(client)
}

pub async fn get_slot_lag_bytes(client: &Client, slot_name: &str) -> anyhow::Result<i64> {
    let row = client
        .query_one(
            "SELECT coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await?;
    Ok(row.get(0))
}

pub async fn setup_replication(
    client: &Client,
    slot_name: &str,
    publication_name: &str,
    outbox_table: &str,
    output_plugin: &str,
) -> anyhow::Result<()> {
    let existing_slots = client
        .query(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await?;

    if existing_slots.is_empty() {
        let slot_query = format!(
            "SELECT pg_create_logical_replication_slot('{}', '{}')",
            slot_name, output_plugin
        );
        client.query(&slot_query, &[]).await?;
        info!(slot = %slot_name, "Replication slot created");
    } else {
        info!(slot = %slot_name, "Replication slot already exists");
    }

    let pub_exists = client
        .query(
            "SELECT pubname FROM pg_publication WHERE pubname = $1",
            &[&publication_name],
        )
        .await?;

    if pub_exists.is_empty() {
        let pub_query = format!(
            "CREATE PUBLICATION {} FOR TABLE {}",
            publication_name, outbox_table
        );
        client.simple_query(&pub_query).await?;
        info!(
            publication = %publication_name,
            table = %outbox_table,
            "Publication created"
        );
    } else {
        info!(publication = %publication_name, "Publication already exists");
    }

    Ok(())
}

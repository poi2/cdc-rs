use tokio_postgres::Client;
use tracing::{debug, info};

pub struct WalReader {
    slot_name: String,
    max_changes: u32,
    slot_options: String,
}

impl WalReader {
    pub fn new(slot_name: &str, max_changes: u32, options: &[(&str, &str)]) -> Self {
        let slot_options = options
            .iter()
            .map(|(k, v)| format!(", '{k}', '{v}'"))
            .collect();
        Self {
            slot_name: slot_name.to_string(),
            max_changes,
            slot_options,
        }
    }

    pub async fn peek_changes(&self, client: &Client) -> Result<Vec<(String, String)>, tokio_postgres::Error> {
        let query = format!(
            "SELECT lsn::text, data FROM pg_logical_slot_peek_changes('{}', NULL, {}{})",
            self.slot_name, self.max_changes, self.slot_options
        );

        let rows = client.query(&query, &[]).await?;

        if rows.is_empty() {
            return Ok(vec![]);
        }

        debug!(count = rows.len(), slot = %self.slot_name, "WAL entries found");

        Ok(rows.iter().map(|row| (row.get(0), row.get(1))).collect())
    }

    pub async fn advance_slot(&self, client: &Client) -> Result<(), tokio_postgres::Error> {
        let query = format!(
            "SELECT pg_logical_slot_get_changes('{}', NULL, {}{})",
            self.slot_name, self.max_changes, self.slot_options
        );

        let consumed = client.query(&query, &[]).await?;

        if !consumed.is_empty() {
            info!(count = consumed.len(), slot = %self.slot_name, "Slot advanced");
        }

        Ok(())
    }
}

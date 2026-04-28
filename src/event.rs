use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxEvent {
    pub event_id: Uuid,
    pub entity_id: String,
    pub occurred_at: String,
    pub event_name: String,
    pub payload: serde_json::Value,
    pub payload_binary: Vec<u8>,
}

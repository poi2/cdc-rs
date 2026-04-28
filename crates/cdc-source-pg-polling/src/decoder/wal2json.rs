use tracing::warn;

/// Decodes wal2json format-version 2 output to flat JSON.
///
/// Each WAL record is a separate JSON object:
///   INSERT: {"action":"I","schema":"s","table":"t","columns":[...]}
///   BEGIN:  {"action":"B"}
///   COMMIT: {"action":"C"}
pub fn decode_to_json(raw: &str, table: &str) -> Option<serde_json::Value> {
    let parsed: serde_json::Value = serde_json::from_str(raw).ok()?;

    let action = parsed.get("action")?.as_str()?;
    if action != "I" {
        return None;
    }

    let schema = parsed.get("schema")?.as_str()?;
    let tbl = parsed.get("table")?.as_str()?;
    let (expected_schema, expected_table) = table.split_once('.')?;
    if schema != expected_schema || tbl != expected_table {
        return None;
    }

    let columns = parsed.get("columns")?.as_array()?;
    if columns.is_empty() {
        return None;
    }

    let mut map = serde_json::Map::new();
    for col in columns {
        let name = col.get("name").and_then(|n| n.as_str());
        let col_type = col.get("type").and_then(|t| t.as_str()).unwrap_or("");
        let value = col.get("value");

        let (Some(name), Some(value)) = (name, value) else {
            warn!(column = ?col, "Malformed column entry in wal2json output");
            return None;
        };

        let converted = if col_type == "bytea" {
            match value.as_str() {
                Some(hex_str) => {
                    let bytes = parse_bytea(hex_str);
                    serde_json::Value::Array(
                        bytes
                            .into_iter()
                            .map(|b| serde_json::Value::Number(b.into()))
                            .collect(),
                    )
                }
                None => value.clone(),
            }
        } else {
            value.clone()
        };

        map.insert(name.to_string(), converted);
    }

    Some(serde_json::Value::Object(map))
}

fn parse_bytea(s: &str) -> Vec<u8> {
    if let Some(hex) = s.strip_prefix("\\x") {
        hex::decode(hex).unwrap_or_default()
    } else {
        s.as_bytes().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE: &str = "transactional_box.outbox";

    #[test]
    fn test_decode_insert() {
        let input = r#"{
            "action": "I",
            "schema": "transactional_box",
            "table": "outbox",
            "columns": [
                {"name": "event_id", "type": "uuid", "value": "550e8400-e29b-41d4-a716-446655440000"},
                {"name": "entity_id", "type": "text", "value": "order-123"},
                {"name": "occurred_at", "type": "timestamp without time zone", "value": "2024-01-15 10:30:00"},
                {"name": "event_name", "type": "text", "value": "OrderCreated"},
                {"name": "payload", "type": "jsonb", "value": {"key": "value"}},
                {"name": "payload_binary", "type": "bytea", "value": "\\x48656c6c6f"}
            ]
        }"#;

        let json = decode_to_json(input, TABLE).expect("should parse");

        assert_eq!(json["event_id"], "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(json["entity_id"], "order-123");
        assert_eq!(json["occurred_at"], "2024-01-15 10:30:00");
        assert_eq!(json["event_name"], "OrderCreated");
        assert_eq!(json["payload"], serde_json::json!({"key": "value"}));
        assert_eq!(json["payload_binary"], serde_json::json!([72, 101, 108, 108, 111]));
    }

    #[test]
    fn test_decode_deserialize_roundtrip() {
        let input = r#"{
            "action": "I",
            "schema": "transactional_box",
            "table": "outbox",
            "columns": [
                {"name": "event_id", "type": "uuid", "value": "550e8400-e29b-41d4-a716-446655440000"},
                {"name": "entity_id", "type": "text", "value": "order-123"},
                {"name": "occurred_at", "type": "timestamp without time zone", "value": "2024-01-15 10:30:00"},
                {"name": "event_name", "type": "text", "value": "OrderCreated"},
                {"name": "payload", "type": "jsonb", "value": {"key": "value"}},
                {"name": "payload_binary", "type": "bytea", "value": "\\x48656c6c6f"}
            ]
        }"#;

        #[derive(Debug, serde::Deserialize, PartialEq)]
        struct TestEvent {
            event_id: uuid::Uuid,
            entity_id: String,
            occurred_at: String,
            event_name: String,
            payload: serde_json::Value,
            payload_binary: Vec<u8>,
        }

        let json = decode_to_json(input, TABLE).unwrap();
        let event: TestEvent = serde_json::from_value(json).unwrap();

        assert_eq!(
            event.event_id,
            uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
        );
        assert_eq!(event.entity_id, "order-123");
        assert_eq!(event.payload_binary, b"Hello");
    }

    #[test]
    fn test_typed_values() {
        let input = r#"{
            "action": "I",
            "schema": "transactional_box",
            "table": "outbox",
            "columns": [
                {"name": "count", "type": "integer", "value": 42},
                {"name": "active", "type": "boolean", "value": true},
                {"name": "score", "type": "numeric", "value": 3.14}
            ]
        }"#;

        let json = decode_to_json(input, TABLE).unwrap();
        assert_eq!(json["count"], 42);
        assert_eq!(json["active"], true);
        assert_eq!(json["score"], 3.14);
    }

    #[test]
    fn test_null_values() {
        let input = r#"{
            "action": "I",
            "schema": "transactional_box",
            "table": "outbox",
            "columns": [
                {"name": "id", "type": "integer", "value": 1},
                {"name": "name", "type": "text", "value": null}
            ]
        }"#;

        let json = decode_to_json(input, TABLE).unwrap();
        assert_eq!(json["id"], 1);
        assert!(json["name"].is_null());
    }

    #[test]
    fn test_ignore_begin_commit() {
        assert!(decode_to_json(r#"{"action":"B"}"#, TABLE).is_none());
        assert!(decode_to_json(r#"{"action":"C"}"#, TABLE).is_none());
    }

    #[test]
    fn test_ignore_update_delete() {
        let update = r#"{
            "action": "U",
            "schema": "transactional_box",
            "table": "outbox",
            "columns": [{"name": "id", "type": "integer", "value": 1}]
        }"#;
        let delete = r#"{
            "action": "D",
            "schema": "transactional_box",
            "table": "outbox",
            "identity": [{"name": "id", "type": "integer", "value": 1}]
        }"#;
        assert!(decode_to_json(update, TABLE).is_none());
        assert!(decode_to_json(delete, TABLE).is_none());
    }

    #[test]
    fn test_ignore_other_table() {
        let input = r#"{
            "action": "I",
            "schema": "public",
            "table": "users",
            "columns": [{"name": "id", "type": "integer", "value": 1}]
        }"#;
        assert!(decode_to_json(input, TABLE).is_none());
    }

    #[test]
    fn test_invalid_json() {
        assert!(decode_to_json("not json", TABLE).is_none());
        assert!(decode_to_json("", TABLE).is_none());
    }
}

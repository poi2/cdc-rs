use tracing::warn;

pub fn decode_to_json(raw: &str, table: &str) -> Option<serde_json::Value> {
    let prefix = format!("table {}: INSERT: ", table);
    let columns_str = raw.strip_prefix(&prefix)?;
    let columns = tokenize_columns(columns_str);
    let json = columns_to_json(columns);
    if json.is_none() {
        warn!(data = raw, "Failed to parse INSERT columns");
    }
    json
}

fn columns_to_json(columns: Vec<(String, String, Option<String>)>) -> Option<serde_json::Value> {
    if columns.is_empty() {
        return None;
    }
    let mut map = serde_json::Map::new();
    for (name, col_type, value) in columns {
        let json_val = match value {
            Some(v) => wal_value_to_json(&col_type, v),
            None => serde_json::Value::Null,
        };
        map.insert(name, json_val);
    }
    Some(serde_json::Value::Object(map))
}

fn wal_value_to_json(col_type: &str, value: String) -> serde_json::Value {
    let t = col_type.to_lowercase();

    if t == "jsonb" || t == "json" {
        return serde_json::from_str(&value).unwrap_or(serde_json::Value::String(value));
    }

    if t == "bytea" {
        let bytes = parse_bytea(&value);
        return serde_json::Value::Array(
            bytes
                .into_iter()
                .map(|b| serde_json::Value::Number(b.into()))
                .collect(),
        );
    }

    if t == "boolean" || t == "bool" {
        return serde_json::Value::Bool(value == "t" || value == "true");
    }

    if matches!(
        t.as_str(),
        "integer" | "int" | "int4" | "smallint" | "int2" | "bigint" | "int8"
    ) {
        if let Ok(n) = value.parse::<i64>() {
            return serde_json::Value::Number(n.into());
        }
    }

    if matches!(
        t.as_str(),
        "real" | "float4" | "double precision" | "float8" | "numeric" | "decimal"
    ) {
        if let Ok(n) = value.parse::<f64>() {
            if let Some(n) = serde_json::Number::from_f64(n) {
                return serde_json::Value::Number(n);
            }
        }
    }

    serde_json::Value::String(value)
}

fn tokenize_columns(input: &str) -> Vec<(String, String, Option<String>)> {
    let mut columns = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let len = chars.len();
    let mut pos = 0;

    while pos < len {
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        if pos >= len {
            break;
        }

        let name_start = pos;
        while pos < len && chars[pos] != '[' {
            pos += 1;
        }
        if pos >= len {
            break;
        }
        let name = chars[name_start..pos].iter().collect::<String>();
        pos += 1;

        let type_start = pos;
        while pos < len && chars[pos] != ']' {
            pos += 1;
        }
        if pos >= len {
            break;
        }
        let col_type = chars[type_start..pos].iter().collect::<String>();
        pos += 1;

        if pos >= len || chars[pos] != ':' {
            break;
        }
        pos += 1;

        let value = if pos < len && chars[pos] == '\'' {
            pos += 1;
            let mut val = String::new();
            while pos < len {
                if chars[pos] == '\'' {
                    if pos + 1 < len && chars[pos + 1] == '\'' {
                        val.push('\'');
                        pos += 2;
                    } else {
                        pos += 1;
                        break;
                    }
                } else {
                    val.push(chars[pos]);
                    pos += 1;
                }
            }
            Some(val)
        } else {
            let val_start = pos;
            while pos < len && !chars[pos].is_whitespace() {
                pos += 1;
            }
            let val: String = chars[val_start..pos].iter().collect();
            if val == "null" {
                None
            } else {
                Some(val)
            }
        };

        columns.push((name, col_type, value));
    }

    columns
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
    fn test_decode_basic() {
        let input = "table transactional_box.outbox: INSERT: \
            event_id[uuid]:'550e8400-e29b-41d4-a716-446655440000' \
            entity_id[text]:'order-123' \
            occurred_at[timestamp without time zone]:'2024-01-15 10:30:00' \
            event_name[text]:'OrderCreated' \
            payload[jsonb]:'{\"key\": \"value\"}' \
            payload_binary[bytea]:'\\x48656c6c6f'";

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
        let input = "table transactional_box.outbox: INSERT: \
            event_id[uuid]:'550e8400-e29b-41d4-a716-446655440000' \
            entity_id[text]:'order-123' \
            occurred_at[timestamp without time zone]:'2024-01-15 10:30:00' \
            event_name[text]:'OrderCreated' \
            payload[jsonb]:'{\"key\": \"value\"}' \
            payload_binary[bytea]:'\\x48656c6c6f'";

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
    fn test_decode_with_spaces_in_payload() {
        let input = "table transactional_box.outbox: INSERT: \
            event_id[uuid]:'550e8400-e29b-41d4-a716-446655440000' \
            entity_id[text]:'order 123 with spaces' \
            occurred_at[timestamp without time zone]:'2024-01-15 10:30:00' \
            event_name[text]:'Order Created Event' \
            payload[jsonb]:'{\"message\": \"hello world\"}' \
            payload_binary[bytea]:'\\x'";

        let json = decode_to_json(input, TABLE).expect("should parse");
        assert_eq!(json["entity_id"], "order 123 with spaces");
        assert_eq!(json["event_name"], "Order Created Event");
    }

    #[test]
    fn test_decode_with_escaped_quotes() {
        let input = "table transactional_box.outbox: INSERT: \
            event_id[uuid]:'550e8400-e29b-41d4-a716-446655440000' \
            entity_id[text]:'it''s a test' \
            occurred_at[timestamp without time zone]:'2024-01-15 10:30:00' \
            event_name[text]:'Test' \
            payload[jsonb]:'{\"key\": \"val\"}' \
            payload_binary[bytea]:'\\x'";

        let json = decode_to_json(input, TABLE).unwrap();
        assert_eq!(json["entity_id"], "it's a test");
    }

    #[test]
    fn test_ignore_update() {
        let input = "table transactional_box.outbox: UPDATE: old-tuple: ... new-tuple: ...";
        assert!(decode_to_json(input, TABLE).is_none());
    }

    #[test]
    fn test_ignore_begin_commit() {
        assert!(decode_to_json("BEGIN 12345", TABLE).is_none());
        assert!(decode_to_json("COMMIT 12345", TABLE).is_none());
    }

    #[test]
    fn test_ignore_other_table() {
        let input = "table public.users: INSERT: id[integer]:1 name[text]:'test'";
        assert!(decode_to_json(input, TABLE).is_none());
    }

    #[test]
    fn test_integer_and_boolean_conversion() {
        let input = "table transactional_box.outbox: INSERT: \
            count[integer]:42 \
            active[boolean]:true \
            score[numeric]:3.14";

        let json = decode_to_json(input, TABLE).unwrap();
        assert_eq!(json["count"], 42);
        assert_eq!(json["active"], true);
        assert_eq!(json["score"], 3.14);
    }

    #[test]
    fn test_null_values() {
        let input = "table transactional_box.outbox: INSERT: \
            id[integer]:1 \
            name[text]:null \
            score[numeric]:null";

        let json = decode_to_json(input, TABLE).unwrap();
        assert_eq!(json["id"], 1);
        assert!(json["name"].is_null());
        assert!(json["score"].is_null());
    }

    #[test]
    fn test_null_vs_string_null() {
        let input = "table transactional_box.outbox: INSERT: \
            a[text]:null \
            b[text]:'null'";

        let json = decode_to_json(input, TABLE).unwrap();
        assert!(json["a"].is_null());
        assert_eq!(json["b"], "null");
    }
}

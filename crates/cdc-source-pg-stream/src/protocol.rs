use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::time::{SystemTime, UNIX_EPOCH};

const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("{0}")]
    InvalidData(String),
    #[error("invalid UTF-8 in C string")]
    Utf8(#[from] std::string::FromUtf8Error),
}

macro_rules! ensure {
    ($cond:expr, $($arg:tt)*) => {
        if !$cond {
            return Err(ProtocolError::InvalidData(format!($($arg)*)));
        }
    };
}

pub enum ReplicationMessage {
    XLogData(XLogData),
    PrimaryKeepalive(PrimaryKeepalive),
}

pub struct XLogData {
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub timestamp: i64,
    pub data: Vec<u8>,
}

pub struct PrimaryKeepalive {
    pub end_lsn: u64,
    pub timestamp: i64,
    pub reply_required: bool,
}

pub fn parse_replication_message(data: &[u8]) -> Result<ReplicationMessage, ProtocolError> {
    ensure!(!data.is_empty(), "Empty replication message");
    let mut buf = data;
    match buf.get_u8() {
        b'w' => {
            ensure!(buf.remaining() >= 24, "XLogData too short");
            let start_lsn = buf.get_u64();
            let end_lsn = buf.get_u64();
            let timestamp = buf.get_i64();
            let data = buf.to_vec();
            Ok(ReplicationMessage::XLogData(XLogData {
                start_lsn,
                end_lsn,
                timestamp,
                data,
            }))
        }
        b'k' => {
            ensure!(buf.remaining() >= 17, "PrimaryKeepalive too short");
            let end_lsn = buf.get_u64();
            let timestamp = buf.get_i64();
            let reply_required = buf.get_u8() != 0;
            Ok(ReplicationMessage::PrimaryKeepalive(PrimaryKeepalive {
                end_lsn,
                timestamp,
                reply_required,
            }))
        }
        tag => Err(ProtocolError::InvalidData(format!(
            "Unknown replication message tag: {tag:#x}"
        ))),
    }
}

pub enum PgOutputMessage {
    Begin(Begin),
    Commit(Commit),
    Relation(Relation),
    Insert(Insert),
}

pub struct Begin {
    pub final_lsn: u64,
    pub timestamp: i64,
    pub xid: u32,
}

pub struct Commit {
    pub flags: u8,
    pub commit_lsn: u64,
    pub end_lsn: u64,
    pub timestamp: i64,
}

pub struct Relation {
    pub id: u32,
    pub namespace: String,
    pub name: String,
    pub replica_identity: u8,
    pub columns: Vec<Column>,
}

pub struct Column {
    pub flags: u8,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

pub struct Insert {
    pub relation_id: u32,
    pub tuple: TupleData,
}

pub struct TupleData {
    pub values: Vec<TupleValue>,
}

pub enum TupleValue {
    Null,
    Unchanged,
    Text(Vec<u8>),
}

pub fn parse_pgoutput_message(data: &[u8]) -> Result<Option<PgOutputMessage>, ProtocolError> {
    ensure!(!data.is_empty(), "Empty pgoutput message");
    let mut buf = data;
    match buf.get_u8() {
        b'B' => {
            ensure!(buf.remaining() >= 20, "Begin too short");
            let final_lsn = buf.get_u64();
            let timestamp = buf.get_i64();
            let xid = buf.get_u32();
            Ok(Some(PgOutputMessage::Begin(Begin {
                final_lsn,
                timestamp,
                xid,
            })))
        }
        b'C' => {
            ensure!(buf.remaining() >= 25, "Commit too short");
            let flags = buf.get_u8();
            let commit_lsn = buf.get_u64();
            let end_lsn = buf.get_u64();
            let timestamp = buf.get_i64();
            Ok(Some(PgOutputMessage::Commit(Commit {
                flags,
                commit_lsn,
                end_lsn,
                timestamp,
            })))
        }
        b'R' => {
            ensure!(buf.remaining() >= 4, "Relation too short");
            let id = buf.get_u32();
            let namespace = read_cstring(&mut buf)?;
            let name = read_cstring(&mut buf)?;
            ensure!(buf.remaining() >= 3, "Relation columns too short");
            let replica_identity = buf.get_u8();
            let num_columns = buf.get_i16() as usize;
            let mut columns = Vec::with_capacity(num_columns);
            for _ in 0..num_columns {
                ensure!(buf.remaining() >= 1, "Column too short");
                let flags = buf.get_u8();
                let col_name = read_cstring(&mut buf)?;
                ensure!(buf.remaining() >= 8, "Column type too short");
                let type_oid = buf.get_u32();
                let type_modifier = buf.get_i32();
                columns.push(Column {
                    flags,
                    name: col_name,
                    type_oid,
                    type_modifier,
                });
            }
            Ok(Some(PgOutputMessage::Relation(Relation {
                id,
                namespace,
                name,
                replica_identity,
                columns,
            })))
        }
        b'I' => {
            ensure!(buf.remaining() >= 5, "Insert too short");
            let relation_id = buf.get_u32();
            let tag = buf.get_u8();
            ensure!(tag == b'N', "Expected 'N' tag in Insert, got {tag:#x}");
            let tuple = parse_tuple_data(&mut buf)?;
            Ok(Some(PgOutputMessage::Insert(Insert {
                relation_id,
                tuple,
            })))
        }
        _ => Ok(None),
    }
}

fn read_cstring(buf: &mut &[u8]) -> Result<String, ProtocolError> {
    let pos = buf
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| ProtocolError::InvalidData("Missing null terminator in C string".into()))?;
    let s = String::from_utf8(buf[..pos].to_vec())?;
    buf.advance(pos + 1);
    Ok(s)
}

fn parse_tuple_data(buf: &mut &[u8]) -> Result<TupleData, ProtocolError> {
    ensure!(buf.remaining() >= 2, "TupleData too short");
    let num_columns = buf.get_i16() as usize;
    let mut values = Vec::with_capacity(num_columns);
    for _ in 0..num_columns {
        ensure!(buf.remaining() >= 1, "TupleValue too short");
        match buf.get_u8() {
            b'n' => values.push(TupleValue::Null),
            b'u' => values.push(TupleValue::Unchanged),
            b't' => {
                ensure!(buf.remaining() >= 4, "Text value length too short");
                let len = buf.get_i32() as usize;
                ensure!(buf.remaining() >= len, "Text value data too short");
                let data = buf[..len].to_vec();
                buf.advance(len);
                values.push(TupleValue::Text(data));
            }
            tag => {
                return Err(ProtocolError::InvalidData(format!(
                    "Unknown tuple value tag: {tag:#x}"
                )));
            }
        }
    }
    Ok(TupleData { values })
}

pub fn tuple_to_json(relation: &Relation, tuple: &TupleData) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (col, val) in relation.columns.iter().zip(tuple.values.iter()) {
        let json_val = match val {
            TupleValue::Null => serde_json::Value::Null,
            TupleValue::Unchanged => continue,
            TupleValue::Text(data) => {
                let text = String::from_utf8_lossy(data);
                oid_to_json(col.type_oid, &text)
            }
        };
        map.insert(col.name.clone(), json_val);
    }
    serde_json::Value::Object(map)
}

fn oid_to_json(oid: u32, text: &str) -> serde_json::Value {
    match oid {
        16 => serde_json::Value::Bool(text == "t"),
        17 => {
            let hex_str = text.strip_prefix("\\x").unwrap_or(text);
            match hex::decode(hex_str) {
                Ok(bytes) => serde_json::Value::Array(
                    bytes
                        .into_iter()
                        .map(|b| serde_json::Value::Number(b.into()))
                        .collect(),
                ),
                Err(_) => serde_json::Value::String(text.to_string()),
            }
        }
        20 | 21 | 23 => text
            .parse::<i64>()
            .map(|n| serde_json::Value::Number(n.into()))
            .unwrap_or_else(|_| serde_json::Value::String(text.to_string())),
        700 | 701 => text
            .parse::<f64>()
            .ok()
            .and_then(|f| serde_json::Number::from_f64(f))
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::String(text.to_string())),
        1700 => {
            if let Ok(n) = text.parse::<i64>() {
                serde_json::Value::Number(n.into())
            } else {
                text.parse::<f64>()
                    .ok()
                    .and_then(|f| serde_json::Number::from_f64(f))
                    .map(serde_json::Value::Number)
                    .unwrap_or_else(|| serde_json::Value::String(text.to_string()))
            }
        }
        114 | 3802 => {
            serde_json::from_str(text)
                .unwrap_or_else(|_| serde_json::Value::String(text.to_string()))
        }
        _ => serde_json::Value::String(text.to_string()),
    }
}

pub fn build_standby_status_update(received_lsn: u64, flushed_lsn: u64) -> Bytes {
    let mut buf = BytesMut::with_capacity(34);
    buf.put_u8(b'r');
    buf.put_u64(received_lsn);
    buf.put_u64(flushed_lsn);
    buf.put_u64(flushed_lsn);
    buf.put_i64(pg_timestamp_now());
    buf.put_u8(0);
    buf.freeze()
}

fn pg_timestamp_now() -> i64 {
    let unix_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;
    unix_us - PG_EPOCH_OFFSET_US
}

pub fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFFFFFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_xlog_data(start_lsn: u64, end_lsn: u64, timestamp: i64, data: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(b'w');
        buf.extend_from_slice(&start_lsn.to_be_bytes());
        buf.extend_from_slice(&end_lsn.to_be_bytes());
        buf.extend_from_slice(&timestamp.to_be_bytes());
        buf.extend_from_slice(data);
        buf
    }

    fn build_keepalive(end_lsn: u64, timestamp: i64, reply: bool) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(b'k');
        buf.extend_from_slice(&end_lsn.to_be_bytes());
        buf.extend_from_slice(&timestamp.to_be_bytes());
        buf.push(if reply { 1 } else { 0 });
        buf
    }

    fn build_begin(final_lsn: u64, timestamp: i64, xid: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(b'B');
        buf.extend_from_slice(&final_lsn.to_be_bytes());
        buf.extend_from_slice(&timestamp.to_be_bytes());
        buf.extend_from_slice(&xid.to_be_bytes());
        buf
    }

    fn build_commit(flags: u8, commit_lsn: u64, end_lsn: u64, timestamp: i64) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(b'C');
        buf.push(flags);
        buf.extend_from_slice(&commit_lsn.to_be_bytes());
        buf.extend_from_slice(&end_lsn.to_be_bytes());
        buf.extend_from_slice(&timestamp.to_be_bytes());
        buf
    }

    fn build_relation(
        id: u32,
        namespace: &str,
        name: &str,
        replica_identity: u8,
        columns: &[(u8, &str, u32, i32)],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(b'R');
        buf.extend_from_slice(&id.to_be_bytes());
        buf.extend_from_slice(namespace.as_bytes());
        buf.push(0);
        buf.extend_from_slice(name.as_bytes());
        buf.push(0);
        buf.push(replica_identity);
        buf.extend_from_slice(&(columns.len() as i16).to_be_bytes());
        for (flags, col_name, type_oid, type_mod) in columns {
            buf.push(*flags);
            buf.extend_from_slice(col_name.as_bytes());
            buf.push(0);
            buf.extend_from_slice(&type_oid.to_be_bytes());
            buf.extend_from_slice(&type_mod.to_be_bytes());
        }
        buf
    }

    fn build_insert(relation_id: u32, values: &[TupleValue]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(b'I');
        buf.extend_from_slice(&relation_id.to_be_bytes());
        buf.push(b'N');
        buf.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for val in values {
            match val {
                TupleValue::Null => buf.push(b'n'),
                TupleValue::Unchanged => buf.push(b'u'),
                TupleValue::Text(data) => {
                    buf.push(b't');
                    buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
                    buf.extend_from_slice(data);
                }
            }
        }
        buf
    }

    #[test]
    fn test_parse_xlog_data() {
        let inner_data = b"hello";
        let msg = build_xlog_data(0x100, 0x200, 12345, inner_data);
        let result = parse_replication_message(&msg).unwrap();
        match result {
            ReplicationMessage::XLogData(xlog) => {
                assert_eq!(xlog.start_lsn, 0x100);
                assert_eq!(xlog.end_lsn, 0x200);
                assert_eq!(xlog.timestamp, 12345);
                assert_eq!(xlog.data, b"hello");
            }
            _ => panic!("Expected XLogData"),
        }
    }

    #[test]
    fn test_parse_keepalive_reply_required() {
        let msg = build_keepalive(0x300, 67890, true);
        let result = parse_replication_message(&msg).unwrap();
        match result {
            ReplicationMessage::PrimaryKeepalive(ka) => {
                assert_eq!(ka.end_lsn, 0x300);
                assert_eq!(ka.timestamp, 67890);
                assert!(ka.reply_required);
            }
            _ => panic!("Expected PrimaryKeepalive"),
        }
    }

    #[test]
    fn test_parse_keepalive_no_reply() {
        let msg = build_keepalive(0x300, 67890, false);
        let result = parse_replication_message(&msg).unwrap();
        match result {
            ReplicationMessage::PrimaryKeepalive(ka) => {
                assert!(!ka.reply_required);
            }
            _ => panic!("Expected PrimaryKeepalive"),
        }
    }

    #[test]
    fn test_parse_begin() {
        let data = build_begin(0x400, 11111, 42);
        let result = parse_pgoutput_message(&data).unwrap().unwrap();
        match result {
            PgOutputMessage::Begin(b) => {
                assert_eq!(b.final_lsn, 0x400);
                assert_eq!(b.timestamp, 11111);
                assert_eq!(b.xid, 42);
            }
            _ => panic!("Expected Begin"),
        }
    }

    #[test]
    fn test_parse_commit() {
        let data = build_commit(0, 0x500, 0x600, 22222);
        let result = parse_pgoutput_message(&data).unwrap().unwrap();
        match result {
            PgOutputMessage::Commit(c) => {
                assert_eq!(c.flags, 0);
                assert_eq!(c.commit_lsn, 0x500);
                assert_eq!(c.end_lsn, 0x600);
                assert_eq!(c.timestamp, 22222);
            }
            _ => panic!("Expected Commit"),
        }
    }

    #[test]
    fn test_parse_relation() {
        let columns = vec![(0u8, "id", 23u32, -1i32), (0, "name", 25, -1)];
        let data = build_relation(16384, "public", "outbox_events", b'd', &columns);
        let result = parse_pgoutput_message(&data).unwrap().unwrap();
        match result {
            PgOutputMessage::Relation(r) => {
                assert_eq!(r.id, 16384);
                assert_eq!(r.namespace, "public");
                assert_eq!(r.name, "outbox_events");
                assert_eq!(r.replica_identity, b'd');
                assert_eq!(r.columns.len(), 2);
                assert_eq!(r.columns[0].name, "id");
                assert_eq!(r.columns[0].type_oid, 23);
                assert_eq!(r.columns[1].name, "name");
                assert_eq!(r.columns[1].type_oid, 25);
            }
            _ => panic!("Expected Relation"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let values = vec![
            TupleValue::Text(b"42".to_vec()),
            TupleValue::Text(b"hello".to_vec()),
            TupleValue::Null,
        ];
        let data = build_insert(16384, &values);
        let result = parse_pgoutput_message(&data).unwrap().unwrap();
        match result {
            PgOutputMessage::Insert(ins) => {
                assert_eq!(ins.relation_id, 16384);
                assert_eq!(ins.tuple.values.len(), 3);
                assert!(matches!(&ins.tuple.values[0], TupleValue::Text(d) if d == b"42"));
                assert!(matches!(&ins.tuple.values[1], TupleValue::Text(d) if d == b"hello"));
                assert!(matches!(ins.tuple.values[2], TupleValue::Null));
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_parse_unknown_pgoutput_message() {
        let data = vec![b'T', 0, 0, 0, 0];
        let result = parse_pgoutput_message(&data).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_tuple_to_json() {
        let relation = Relation {
            id: 1,
            namespace: "public".to_string(),
            name: "test".to_string(),
            replica_identity: b'd',
            columns: vec![
                Column {
                    flags: 0,
                    name: "id".to_string(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                Column {
                    flags: 0,
                    name: "name".to_string(),
                    type_oid: 25,
                    type_modifier: -1,
                },
                Column {
                    flags: 0,
                    name: "active".to_string(),
                    type_oid: 16,
                    type_modifier: -1,
                },
            ],
        };
        let tuple = TupleData {
            values: vec![
                TupleValue::Text(b"42".to_vec()),
                TupleValue::Text(b"hello".to_vec()),
                TupleValue::Text(b"t".to_vec()),
            ],
        };
        let json = tuple_to_json(&relation, &tuple);
        assert_eq!(json["id"], 42);
        assert_eq!(json["name"], "hello");
        assert_eq!(json["active"], true);
    }

    #[test]
    fn test_tuple_to_json_with_null() {
        let relation = Relation {
            id: 1,
            namespace: "public".to_string(),
            name: "test".to_string(),
            replica_identity: b'd',
            columns: vec![
                Column {
                    flags: 0,
                    name: "id".to_string(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                Column {
                    flags: 0,
                    name: "optional".to_string(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        };
        let tuple = TupleData {
            values: vec![TupleValue::Text(b"1".to_vec()), TupleValue::Null],
        };
        let json = tuple_to_json(&relation, &tuple);
        assert_eq!(json["id"], 1);
        assert!(json["optional"].is_null());
    }

    #[test]
    fn test_tuple_to_json_with_unchanged() {
        let relation = Relation {
            id: 1,
            namespace: "public".to_string(),
            name: "test".to_string(),
            replica_identity: b'd',
            columns: vec![
                Column {
                    flags: 0,
                    name: "id".to_string(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                Column {
                    flags: 0,
                    name: "toast_col".to_string(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        };
        let tuple = TupleData {
            values: vec![TupleValue::Text(b"1".to_vec()), TupleValue::Unchanged],
        };
        let json = tuple_to_json(&relation, &tuple);
        assert_eq!(json["id"], 1);
        assert!(json.get("toast_col").is_none());
    }

    #[test]
    fn test_oid_to_json_bool() {
        assert_eq!(oid_to_json(16, "t"), serde_json::Value::Bool(true));
        assert_eq!(oid_to_json(16, "f"), serde_json::Value::Bool(false));
    }

    #[test]
    fn test_oid_to_json_integers() {
        assert_eq!(oid_to_json(21, "42"), serde_json::json!(42));
        assert_eq!(oid_to_json(23, "-100"), serde_json::json!(-100));
        assert_eq!(
            oid_to_json(20, "9999999999"),
            serde_json::json!(9999999999_i64)
        );
    }

    #[test]
    fn test_oid_to_json_floats() {
        assert_eq!(oid_to_json(700, "3.14"), serde_json::json!(3.14));
        assert_eq!(oid_to_json(701, "2.718"), serde_json::json!(2.718));
    }

    #[test]
    fn test_oid_to_json_numeric() {
        assert_eq!(oid_to_json(1700, "42"), serde_json::json!(42));
        assert_eq!(oid_to_json(1700, "3.14"), serde_json::json!(3.14));
    }

    #[test]
    fn test_oid_to_json_text() {
        assert_eq!(oid_to_json(25, "hello"), serde_json::json!("hello"));
        assert_eq!(oid_to_json(1043, "world"), serde_json::json!("world"));
    }

    #[test]
    fn test_oid_to_json_jsonb() {
        let result = oid_to_json(3802, r#"{"key":"value"}"#);
        assert_eq!(result, serde_json::json!({"key": "value"}));
    }

    #[test]
    fn test_oid_to_json_json() {
        let result = oid_to_json(114, r#"[1,2,3]"#);
        assert_eq!(result, serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn test_oid_to_json_bytea() {
        let result = oid_to_json(17, "\\x48656c6c6f");
        assert_eq!(result, serde_json::json!([72, 101, 108, 108, 111]));
    }

    #[test]
    fn test_oid_to_json_uuid() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(oid_to_json(2950, uuid), serde_json::json!(uuid));
    }

    #[test]
    fn test_oid_to_json_unknown_type() {
        assert_eq!(oid_to_json(99999, "whatever"), serde_json::json!("whatever"));
    }

    #[test]
    fn test_format_lsn() {
        assert_eq!(format_lsn(0), "0/0");
        assert_eq!(format_lsn(0x0000000100000000), "1/0");
        assert_eq!(format_lsn(0x00000001000000FF), "1/FF");
        assert_eq!(format_lsn(0x00000000016B3A80), "0/16B3A80");
    }

    #[test]
    fn test_build_standby_status_update() {
        let msg = build_standby_status_update(0x100, 0x80);
        assert_eq!(msg[0], b'r');
        assert_eq!(msg.len(), 34);
        assert_eq!(&msg[1..9], &0x100u64.to_be_bytes());
        assert_eq!(&msg[9..17], &0x80u64.to_be_bytes());
        assert_eq!(&msg[17..25], &0x80u64.to_be_bytes());
        assert_eq!(msg[33], 0);
    }

    #[test]
    fn test_parse_empty_message() {
        assert!(parse_replication_message(&[]).is_err());
        assert!(parse_pgoutput_message(&[]).is_err());
    }

    #[test]
    fn test_parse_unknown_replication_message() {
        assert!(parse_replication_message(&[b'x', 0, 0]).is_err());
    }

    #[test]
    fn test_roundtrip_xlog_with_insert() {
        let rel_columns = vec![
            (0u8, "event_id", 2950u32, -1i32),
            (0, "entity_id", 25, -1),
            (0, "payload", 3802, -1),
        ];
        let rel_data = build_relation(16384, "public", "outbox_events", b'd', &rel_columns);

        let values = vec![
            TupleValue::Text(b"550e8400-e29b-41d4-a716-446655440000".to_vec()),
            TupleValue::Text(b"order-1".to_vec()),
            TupleValue::Text(br#"{"key":"value"}"#.to_vec()),
        ];
        let ins_data = build_insert(16384, &values);

        let xlog_rel = build_xlog_data(0x100, 0x200, 1000, &rel_data);
        let xlog_ins = build_xlog_data(0x200, 0x300, 2000, &ins_data);

        let relation = match parse_replication_message(&xlog_rel).unwrap() {
            ReplicationMessage::XLogData(xlog) => {
                match parse_pgoutput_message(&xlog.data).unwrap().unwrap() {
                    PgOutputMessage::Relation(r) => r,
                    _ => panic!("Expected Relation"),
                }
            }
            _ => panic!("Expected XLogData"),
        };

        let insert = match parse_replication_message(&xlog_ins).unwrap() {
            ReplicationMessage::XLogData(xlog) => {
                match parse_pgoutput_message(&xlog.data).unwrap().unwrap() {
                    PgOutputMessage::Insert(i) => i,
                    _ => panic!("Expected Insert"),
                }
            }
            _ => panic!("Expected XLogData"),
        };

        let json = tuple_to_json(&relation, &insert.tuple);
        assert_eq!(json["event_id"], "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(json["entity_id"], "order-1");
        assert_eq!(json["payload"], serde_json::json!({"key": "value"}));
    }
}

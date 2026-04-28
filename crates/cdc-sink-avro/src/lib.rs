use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use apache_avro::types::Value;
use apache_avro::{AvroSchema, Codec, Schema, Writer, to_value};
use async_trait::async_trait;
use serde::Serialize;
use tracing::info;

use cdc_core::{LsnEvent, Sink};

#[derive(Debug, thiserror::Error)]
pub enum AvroSinkError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("avro error: {0}")]
    Avro(#[from] apache_avro::Error),
    #[error("lock poisoned")]
    LockPoisoned,
    #[error("invalid inner schema: expected Record")]
    InvalidSchema,
}

pub struct AvroSink<E> {
    writer: Mutex<Writer<'static, BufWriter<File>>>,
    path: PathBuf,
    _marker: PhantomData<E>,
}

impl<E: AvroSchema> AvroSink<E> {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, AvroSinkError> {
        let path = path.as_ref().to_path_buf();
        let schema = build_lsn_event_schema::<E>()?;
        let schema: &'static Schema = Box::leak(Box::new(schema));

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        let buf_writer = BufWriter::new(file);
        let writer = Writer::with_codec(schema, buf_writer, Codec::Snappy);

        info!(path = %path.display(), "Avro sink initialized");

        Ok(Self {
            writer: Mutex::new(writer),
            path,
            _marker: PhantomData,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub fn build_lsn_event_schema<E: AvroSchema>() -> Result<Schema, AvroSinkError> {
    let inner_schema = E::get_schema();
    let Schema::Record(record_schema) = &inner_schema else {
        return Err(AvroSinkError::InvalidSchema);
    };

    let mut fields_json = vec![serde_json::json!({
        "name": "__lsn",
        "type": "long"
    })];

    for field in &record_schema.fields {
        fields_json.push(serde_json::json!({
            "name": field.name,
            "type": field.schema,
        }));
    }

    let schema_json = serde_json::json!({
        "type": "record",
        "name": record_schema.name.fullname(None),
        "fields": fields_json
    });

    let schema = Schema::parse_str(&schema_json.to_string())?;
    Ok(schema)
}

fn to_lsn_event_value<E: Serialize>(event: &LsnEvent<E>) -> Result<Value, AvroSinkError> {
    let inner_value = to_value(&event.event)?;
    let Value::Record(mut fields) = inner_value else {
        return Err(AvroSinkError::InvalidSchema);
    };
    fields.insert(0, ("__lsn".to_string(), Value::Long(event.lsn as i64)));
    Ok(Value::Record(fields))
}

#[async_trait]
impl<E: Serialize + Send + Sync + 'static> Sink for AvroSink<E> {
    type Event = LsnEvent<E>;
    type Error = AvroSinkError;

    async fn publish(&self, events: &[LsnEvent<E>]) -> Result<(), AvroSinkError> {
        if events.is_empty() {
            return Ok(());
        }

        let mut writer = self.writer.lock().map_err(|_| AvroSinkError::LockPoisoned)?;

        for event in events {
            let value = to_lsn_event_value(event)?;
            writer.append(value)?;
        }
        writer.flush()?;

        info!(count = events.len(), path = %self.path.display(), "Events written to Avro file");
        Ok(())
    }

    async fn shutdown(&mut self) {
        if let Ok(mut writer) = self.writer.lock() {
            let _ = writer.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::{Reader, from_value};
    use serde::Deserialize;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, AvroSchema)]
    struct TestEvent {
        id: i32,
        name: String,
    }

    #[test]
    fn test_build_lsn_event_schema() {
        let schema = build_lsn_event_schema::<TestEvent>().unwrap();
        let Schema::Record(record) = &schema else {
            panic!("Expected Record schema");
        };
        assert_eq!(record.fields[0].name, "__lsn");
        assert_eq!(record.fields[1].name, "id");
        assert_eq!(record.fields[2].name, "name");
    }

    #[tokio::test]
    async fn test_avro_sink_write_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("output.avro");

        let mut sink = AvroSink::<TestEvent>::new(&path).unwrap();

        let events = vec![
            LsnEvent { lsn: 100, event: TestEvent { id: 1, name: "first".into() } },
            LsnEvent { lsn: 200, event: TestEvent { id: 2, name: "second".into() } },
        ];

        sink.publish(&events).await.unwrap();
        sink.shutdown().await;

        let file = std::fs::File::open(&path).unwrap();
        let reader = Reader::new(file).unwrap();
        let records: Vec<_> = reader.map(|r| r.unwrap()).collect();

        assert_eq!(records.len(), 2);

        let first: LsnEvent<TestEvent> = from_value(&records[0]).unwrap();
        assert_eq!(first.lsn, 100);
        assert_eq!(first.event.id, 1);
        assert_eq!(first.event.name, "first");

        let second: LsnEvent<TestEvent> = from_value(&records[1]).unwrap();
        assert_eq!(second.lsn, 200);
        assert_eq!(second.event.id, 2);
        assert_eq!(second.event.name, "second");
    }

    #[tokio::test]
    async fn test_avro_sink_empty_publish() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.avro");

        let sink = AvroSink::<TestEvent>::new(&path).unwrap();
        sink.publish(&[]).await.unwrap();
    }

    #[tokio::test]
    async fn test_avro_sink_multiple_batches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("batches.avro");

        let mut sink = AvroSink::<TestEvent>::new(&path).unwrap();

        sink.publish(&[
            LsnEvent { lsn: 100, event: TestEvent { id: 1, name: "a".into() } },
        ]).await.unwrap();

        sink.publish(&[
            LsnEvent { lsn: 200, event: TestEvent { id: 2, name: "b".into() } },
            LsnEvent { lsn: 300, event: TestEvent { id: 3, name: "c".into() } },
        ]).await.unwrap();

        sink.shutdown().await;

        let file = std::fs::File::open(&path).unwrap();
        let reader = Reader::new(file).unwrap();
        let records: Vec<_> = reader.map(|r| r.unwrap()).collect();

        assert_eq!(records.len(), 3);

        let third: LsnEvent<TestEvent> = from_value(&records[2]).unwrap();
        assert_eq!(third.lsn, 300);
        assert_eq!(third.event.name, "c");
    }
}

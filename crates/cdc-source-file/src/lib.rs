use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use tracing::info;

use cdc_core::Source;

pub struct FileSourceConfig {
    pub path: PathBuf,
    pub batch_size: usize,
}

pub struct FileSource<E> {
    events: Vec<E>,
    offset: usize,
    batch_size: usize,
}

impl<E: DeserializeOwned> FileSource<E> {
    pub fn new(config: FileSourceConfig) -> anyhow::Result<Self> {
        let file = File::open(&config.path)?;
        let reader = BufReader::new(file);
        let mut events = Vec::new();

        for (i, line) in reader.lines().enumerate() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let event: E = serde_json::from_str(&line)
                .map_err(|e| anyhow::anyhow!("Failed to parse line {}: {e}", i + 1))?;
            events.push(event);
        }

        info!(path = %config.path.display(), count = events.len(), "Loaded events from file");

        Ok(Self {
            events,
            offset: 0,
            batch_size: config.batch_size,
        })
    }
}

#[async_trait]
impl<E: Send + Sync + Clone + 'static> Source for FileSource<E> {
    type Event = E;

    async fn peek(&mut self) -> anyhow::Result<Vec<E>> {
        let end = (self.offset + self.batch_size).min(self.events.len());
        Ok(self.events[self.offset..end].to_vec())
    }

    async fn advance(&mut self) -> anyhow::Result<()> {
        self.offset = (self.offset + self.batch_size).min(self.events.len());
        Ok(())
    }

    async fn reconnect(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn is_retriable_error(&self, _err: &anyhow::Error) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn sample_events_jsonl() -> String {
        let events = vec![
            r#"{"event_id":"550e8400-e29b-41d4-a716-446655440000","entity_id":"order-1","occurred_at":"2024-01-15 10:30:00","event_name":"OrderCreated","payload":{"key":"value"},"payload_binary":[72,101,108,108,111]}"#,
            r#"{"event_id":"550e8400-e29b-41d4-a716-446655440001","entity_id":"order-2","occurred_at":"2024-01-15 10:31:00","event_name":"OrderUpdated","payload":{"key":"value2"},"payload_binary":[]}"#,
        ];
        events.join("\n") + "\n"
    }

    #[derive(Debug, Clone, serde::Deserialize, PartialEq)]
    struct TestEvent {
        event_id: String,
        entity_id: String,
        occurred_at: String,
        event_name: String,
        payload: serde_json::Value,
        payload_binary: Vec<u8>,
    }

    fn write_temp_file(content: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[tokio::test]
    async fn test_file_source_reads_all_events() {
        let file = write_temp_file(&sample_events_jsonl());
        let config = FileSourceConfig {
            path: file.path().to_path_buf(),
            batch_size: 100,
        };
        let mut source = FileSource::<TestEvent>::new(config).unwrap();

        let events = source.peek().await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].entity_id, "order-1");
        assert_eq!(events[1].entity_id, "order-2");
    }

    #[tokio::test]
    async fn test_file_source_batching() {
        let file = write_temp_file(&sample_events_jsonl());
        let config = FileSourceConfig {
            path: file.path().to_path_buf(),
            batch_size: 1,
        };
        let mut source = FileSource::<TestEvent>::new(config).unwrap();

        let batch1 = source.peek().await.unwrap();
        assert_eq!(batch1.len(), 1);
        assert_eq!(batch1[0].entity_id, "order-1");

        source.advance().await.unwrap();

        let batch2 = source.peek().await.unwrap();
        assert_eq!(batch2.len(), 1);
        assert_eq!(batch2[0].entity_id, "order-2");

        source.advance().await.unwrap();

        let batch3 = source.peek().await.unwrap();
        assert!(batch3.is_empty());
    }

    #[tokio::test]
    async fn test_file_source_skips_blank_lines() {
        let content = format!(
            "{}\n\n{}\n",
            r#"{"event_id":"a","entity_id":"a","occurred_at":"2024-01-15","event_name":"E","payload":{},"payload_binary":[]}"#,
            r#"{"event_id":"b","entity_id":"b","occurred_at":"2024-01-15","event_name":"E","payload":{},"payload_binary":[]}"#,
        );
        let file = write_temp_file(&content);
        let config = FileSourceConfig {
            path: file.path().to_path_buf(),
            batch_size: 100,
        };
        let mut source = FileSource::<TestEvent>::new(config).unwrap();

        let events = source.peek().await.unwrap();
        assert_eq!(events.len(), 2);
    }
}

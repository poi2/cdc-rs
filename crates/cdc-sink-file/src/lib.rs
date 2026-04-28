use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;
use serde::Serialize;
use tracing::info;

use cdc_core::Sink;

#[derive(Debug, thiserror::Error)]
pub enum FileSinkError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("serialization error")]
    Serialization(#[source] serde_json::Error),
    #[error("lock poisoned")]
    LockPoisoned,
}

pub struct FileSink<E> {
    writer: Mutex<BufWriter<File>>,
    path: PathBuf,
    _marker: std::marker::PhantomData<E>,
}

impl<E> FileSink<E> {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, FileSinkError> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        info!(path = %path.display(), "File sink initialized");

        Ok(Self {
            writer: Mutex::new(BufWriter::new(file)),
            path,
            _marker: std::marker::PhantomData,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[async_trait]
impl<E: Serialize + Send + Sync + 'static> Sink for FileSink<E> {
    type Event = E;
    type Error = FileSinkError;

    async fn publish(&self, events: &[E]) -> Result<(), FileSinkError> {
        if events.is_empty() {
            return Ok(());
        }

        let mut writer = self.writer.lock().map_err(|_| FileSinkError::LockPoisoned)?;

        for event in events {
            serde_json::to_writer(&mut *writer, event)
                .map_err(FileSinkError::Serialization)?;
            writeln!(&mut *writer)?;
        }
        writer.flush()?;

        info!(count = events.len(), path = %self.path.display(), "Events written to file");
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
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestEvent {
        id: String,
        name: String,
    }

    #[tokio::test]
    async fn test_file_sink_writes_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("output.jsonl");

        let sink = FileSink::<TestEvent>::new(&path).unwrap();
        let events = vec![
            TestEvent { id: "1".into(), name: "order-1".into() },
            TestEvent { id: "2".into(), name: "order-2".into() },
        ];

        sink.publish(&events).await.unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);

        let parsed: TestEvent = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed.name, "order-1");

        let parsed: TestEvent = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(parsed.name, "order-2");
    }

    #[tokio::test]
    async fn test_file_sink_appends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("output.jsonl");

        let sink = FileSink::<TestEvent>::new(&path).unwrap();
        sink.publish(&[TestEvent { id: "1".into(), name: "a".into() }]).await.unwrap();
        sink.publish(&[TestEvent { id: "2".into(), name: "b".into() }]).await.unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
    }

    #[tokio::test]
    async fn test_file_sink_empty_publish() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("output.jsonl");

        let sink = FileSink::<TestEvent>::new(&path).unwrap();
        sink.publish(&[]).await.unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.is_empty());
    }
}

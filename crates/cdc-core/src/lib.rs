mod pipeline;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use pipeline::{CdcPipeline, PipelineConfig, PipelineError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LsnEvent<E> {
    #[serde(rename = "__lsn")]
    pub lsn: u64,
    #[serde(flatten)]
    pub event: E,
}

#[async_trait]
pub trait Source: Send {
    type Event: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;
    async fn peek(&mut self) -> Result<Vec<Self::Event>, Self::Error>;
    async fn advance(&mut self) -> Result<(), Self::Error>;
    async fn reconnect(&mut self) -> Result<(), Self::Error>;
    fn is_retriable_error(&self, err: &Self::Error) -> bool;
}

#[async_trait]
pub trait Sink: Send + Sync {
    type Event: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;
    async fn publish(&self, events: &[Self::Event]) -> Result<(), Self::Error>;
    async fn shutdown(&mut self);
}

#[async_trait]
pub trait Transform: Send + Sync {
    type Input: Send + Sync;
    type Output: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;
    async fn transform(&self, events: Vec<Self::Input>) -> Result<Vec<Self::Output>, Self::Error>;
}

pub struct Identity<E>(std::marker::PhantomData<E>);

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestEvent {
        id: u32,
        name: String,
    }

    #[test]
    fn test_lsn_event_serialize_flattens() {
        let event = LsnEvent {
            lsn: 0x16B3698,
            event: TestEvent {
                id: 1,
                name: "test".to_string(),
            },
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["__lsn"], 0x16B3698u64);
        assert_eq!(json["id"], 1);
        assert_eq!(json["name"], "test");
        assert!(json.get("lsn").is_none());
        assert!(json.get("event").is_none());
    }

    #[test]
    fn test_lsn_event_deserialize_from_flat() {
        let json = serde_json::json!({
            "__lsn": 0x16B3698u64,
            "id": 42,
            "name": "hello"
        });
        let event: LsnEvent<TestEvent> = serde_json::from_value(json).unwrap();
        assert_eq!(event.lsn, 0x16B3698);
        assert_eq!(event.event.id, 42);
        assert_eq!(event.event.name, "hello");
    }

    #[test]
    fn test_lsn_event_roundtrip() {
        let original = LsnEvent {
            lsn: u64::MAX,
            event: TestEvent {
                id: 999,
                name: "roundtrip".to_string(),
            },
        };
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: LsnEvent<TestEvent> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.lsn, original.lsn);
        assert_eq!(deserialized.event, original.event);
    }
}

impl<E> Identity<E> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<E> Default for Identity<E> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<E: Send + Sync + 'static> Transform for Identity<E> {
    type Input = E;
    type Output = E;
    type Error = std::convert::Infallible;
    async fn transform(&self, events: Vec<E>) -> Result<Vec<E>, Self::Error> {
        Ok(events)
    }
}

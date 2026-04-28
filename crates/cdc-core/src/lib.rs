mod pipeline;

use async_trait::async_trait;

pub use pipeline::{CdcPipeline, PipelineConfig};

#[async_trait]
pub trait Source: Send + Sync {
    type Event: Send + Sync;
    async fn peek(&mut self) -> anyhow::Result<Vec<Self::Event>>;
    async fn advance(&mut self) -> anyhow::Result<()>;
    async fn reconnect(&mut self) -> anyhow::Result<()>;
    fn is_retriable_error(&self, err: &anyhow::Error) -> bool;
}

#[async_trait]
pub trait Sink: Send + Sync {
    type Event: Send + Sync;
    async fn publish(&self, events: &[Self::Event]) -> anyhow::Result<()>;
    async fn shutdown(&mut self);
}

#[async_trait]
pub trait Transform: Send + Sync {
    type Input: Send + Sync;
    type Output: Send + Sync;
    async fn transform(&self, events: Vec<Self::Input>) -> anyhow::Result<Vec<Self::Output>>;
}

pub struct Identity<E>(std::marker::PhantomData<E>);

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
    async fn transform(&self, events: Vec<E>) -> anyhow::Result<Vec<E>> {
        Ok(events)
    }
}

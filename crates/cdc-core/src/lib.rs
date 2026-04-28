mod pipeline;

use async_trait::async_trait;

pub use pipeline::{CdcPipeline, PipelineConfig, PipelineError};

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

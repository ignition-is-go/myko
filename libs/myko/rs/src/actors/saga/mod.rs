//! Saga actor module for event-driven command generation.
//!
//! This module contains:
//! - `SagaManager` - Coordinates all saga runners
//! - `SagaRunner` - Runs individual saga pipelines

mod saga_manager;
mod saga_runner;

pub use saga_manager::{SagaManager, SagaManagerArgs, SagaManagerMsg};
pub use saga_runner::{SagaRunner, SagaRunnerArgs, SagaRunnerMsg};

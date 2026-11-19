//! PostgreSQL Query execution using native SQLx arguments and row decoding.
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{PgPool, postgres::{PgArguments, PgQueryResult, PgRow}};

use super::{Collector, HandlerError, HandlerResult, Operation};
use crate::runtime::{common::{MessageContext, Payload}};

/// SQL and values remain separate. Build arguments with sqlx::Arguments::add.
/// The SQL may come from include_str! or application query construction.
pub struct Query {
    pub sql: String,
    pub arguments: PgArguments,
}

#[async_trait]
pub trait QueryBuilder<HandlerState, T>: Send + Sync
where HandlerState: Send + 'static, T: Send + Sync + 'static {
    async fn build_query(&self, context: MessageContext, state: &mut HandlerState,
        value: Payload<T>) -> Result<Query, HandlerError>;
}

#[async_trait]
pub trait RowsHandler<HandlerState, T, R>: QueryBuilder<HandlerState, T>
where HandlerState: Send + 'static, T: Send + Sync + 'static, R: Send + Sync + 'static {
    async fn handle_row(&self, context: MessageContext, state: &mut HandlerState,
        row: PgRow, out: &Collector<R>) -> HandlerResult;
}

#[async_trait]
pub trait ExecuteHandler<HandlerState, T, R>: QueryBuilder<HandlerState, T>
where HandlerState: Send + 'static, T: Send + Sync + 'static, R: Send + Sync + 'static {
    async fn handle_result(&self, context: MessageContext, state: &mut HandlerState,
        result: PgQueryResult, out: &Collector<R>) -> HandlerResult;
}

fn operation_context(context: MessageContext, timeout: std::time::Duration) -> MessageContext {
    if timeout.is_zero() { context } else { context.with_timeout_limit(timeout) }
}

#[cfg(test)]
mod timeout_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn operation_deadline_preserves_shorter_caller_deadline_and_zero_disables_cap() {
        let parent = MessageContext::new().with_timeout_limit(Duration::from_secs(1));
        assert_eq!(operation_context(parent.clone(), Duration::ZERO).deadline(), parent.deadline());
        assert_eq!(operation_context(parent.clone(), Duration::from_secs(10)).deadline(), parent.deadline());
        assert!(operation_context(parent.clone(), Duration::from_millis(10)).deadline() < parent.deadline());
        assert!(operation_context(MessageContext::new(), Duration::ZERO).deadline().is_none());
    }

    #[tokio::test]
    async fn operation_deadline_cancels_wait_without_cancelling_parent() {
        let parent = MessageContext::new();
        let request = operation_context(parent.clone(), Duration::from_millis(1));
        tokio::time::timeout(Duration::from_secs(1), request.cancelled()).await.unwrap();
        assert!(request.is_cancelled());
        assert!(!parent.is_cancelled());
    }
}

fn cancelled() -> HandlerError {
    std::io::Error::new(std::io::ErrorKind::Interrupted, "PostgreSQL Query cancelled").into()
}

/// Pool ownership belongs to the connector; clones share that native pool.
pub struct RowsOperation {
    pool: PgPool,
    operation_timeout: std::time::Duration,
}

impl RowsOperation {
    pub fn new(pool: PgPool) -> Self { Self { pool, operation_timeout: std::time::Duration::ZERO } }
}

#[async_trait]
impl<HandlerState, T, R, H> Operation<HandlerState, T, R, H> for RowsOperation
where HandlerState: Send + 'static, T: Send + Sync + 'static, R: Send + Sync + 'static,
    H: RowsHandler<HandlerState, T, R> {
    async fn execute(&self, context: MessageContext, state: &mut HandlerState,
        value: Payload<T>, handler: &H, out: &Collector<R>) -> HandlerResult {
        let context = operation_context(context, self.operation_timeout);
        let query = handler.build_query(context.clone(), state, value).await?;
        let mut connection = tokio::select! {
            biased;
            _ = context.cancelled() => return Err(cancelled()),
            result = self.pool.acquire() => result?,
        };
        let result: HandlerResult = async {
            let mut rows = sqlx::query_with(&query.sql, query.arguments).fetch(&mut *connection);
            loop {
                let row = tokio::select! {
                    biased;
                    _ = context.cancelled() => return Err(cancelled()),
                    result = rows.try_next() => result?,
                };
                let Some(row) = row else { break; };
                // Do not drop an application mapper halfway through its work.
                handler.handle_row(context.clone(), state, row, out).await?;
            }
            Ok(())
        }.await;
        // Dropping an unfinished SQLx stream must never return a busy protocol
        // connection to the pool. Detach and drop closes its transport instead.
        if result.is_err() {
            drop(connection.detach());
        } else {
            drop(connection);
        }
        result
    }
}

pub struct ExecuteOperation {
    pool: PgPool,
    operation_timeout: std::time::Duration,
}

impl ExecuteOperation {
    pub fn new(pool: PgPool) -> Self { Self { pool, operation_timeout: std::time::Duration::ZERO } }
}

#[async_trait]
impl<HandlerState, T, R, H> Operation<HandlerState, T, R, H> for ExecuteOperation
where HandlerState: Send + 'static, T: Send + Sync + 'static, R: Send + Sync + 'static,
    H: ExecuteHandler<HandlerState, T, R> {
    async fn execute(&self, context: MessageContext, state: &mut HandlerState,
        value: Payload<T>, handler: &H, out: &Collector<R>) -> HandlerResult {
        let context = operation_context(context, self.operation_timeout);
        let query = handler.build_query(context.clone(), state, value).await?;
        let mut connection = tokio::select! {
            biased;
            _ = context.cancelled() => return Err(cancelled()),
            result = self.pool.acquire() => result?,
        };
        let result: Result<PgQueryResult, HandlerError> = tokio::select! {
            biased;
            _ = context.cancelled() => Err(cancelled()),
            result = sqlx::query_with(&query.sql, query.arguments).execute(&mut *connection) => result.map_err(Into::into),
        };
        match result {
            Ok(result) => {
                drop(connection);
                handler.handle_result(context.clone(), state, result, out).await?;
                // Mapping is cooperative: finish it before finalization, but do
                // not report success if its invocation deadline has elapsed.
                if context.is_cancelled() { return Err(cancelled()); }
                Ok(())
            }
            Err(error) => {
                // Unknown write outcomes are reported, never automatically retried.
                drop(connection.detach());
                Err(error)
            }
        }
    }
}

/// Register once per connector ID with ServiceApp::add_component.
/// It owns one native pool shared by all its Query endpoints.
pub struct Connector {
    pool: PgPool,
    operation_timeout: std::time::Duration,
}

impl Connector {
    /// Transfer lifecycle ownership of a maker-configured SQLx pool. Clones
    /// retained by endpoints share it; managed stop closes the entire pool.
    pub fn from_pool(pool: PgPool) -> Self { Self { pool, operation_timeout: std::time::Duration::ZERO } }

    /// Set the per-operation deadline cap for a maker-configured pool.
    /// Zero preserves the caller deadline without adding a connector deadline.
    pub fn with_operation_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.operation_timeout = timeout;
        self
    }

    pub fn new(config: &crate::runtime::config::PostgreSQLSQLxDataConnectorConfig)
        -> crate::runtime::environment::RuntimeResult<Self> {
        use std::{str::FromStr, time::Duration};
        use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
        use crate::runtime::environment::RuntimeError;
        config.validate()?;
        // Native DSN parse errors can contain credential-bearing input.
        let options = PgConnectOptions::from_str(&config.connection_string)
            .map_err(|_| RuntimeError::InvalidConfiguration("invalid PostgreSQL connection string".into()))?;
        let mut pool_options = PgPoolOptions::new().max_connections(config.connections_count as u32);
        // Zero leaves the native pool policy intact; it must not create an
        // immediately expired acquisition deadline.
        if config.connect_timeout_ms > 0 {
            pool_options = pool_options.acquire_timeout(Duration::from_millis(config.connect_timeout_ms));
        }
        let pool = pool_options.connect_lazy_with(options);
        Ok(Self::from_pool(pool).with_operation_timeout(Duration::from_millis(config.operation_timeout_ms)))
    }

    pub fn rows(&self) -> RowsOperation { RowsOperation { pool: self.pool.clone(), operation_timeout: self.operation_timeout } }
    pub fn execute(&self) -> ExecuteOperation { ExecuteOperation { pool: self.pool.clone(), operation_timeout: self.operation_timeout } }
}

#[async_trait]
impl crate::runtime::environment::Lifecycle for Connector {
    async fn start(&self, context: MessageContext) -> crate::runtime::environment::RuntimeResult<()> {
        use crate::runtime::environment::RuntimeError;
        tokio::select! {
            biased;
            _ = context.cancelled() => Err(RuntimeError::Transport("PostgreSQL startup cancelled".into())),
            result = self.pool.acquire() => result.map(drop)
                .map_err(|_| RuntimeError::Transport("PostgreSQL startup connection failed".into())),
        }
    }

    async fn stop(&self, context: MessageContext) -> crate::runtime::environment::RuntimeResult<()> {
        use crate::runtime::environment::RuntimeError;
        tokio::select! {
            // Poll close first so new acquisitions stop even with an expired context.
            biased;
            _ = self.pool.close() => Ok(()),
            _ = context.cancelled() => Err(RuntimeError::Transport("PostgreSQL pool close deadline exceeded".into())),
        }
    }
}

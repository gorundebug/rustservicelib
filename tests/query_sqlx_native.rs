//! Native Query acceptance. Requires a disposable PostgreSQL from Compose.
use std::{sync::{Arc, atomic::{AtomicUsize, Ordering}}, time::Duration};
use async_trait::async_trait;
use sqlx::{Arguments, Row, PgPool, postgres::{PgPoolOptions, PgArguments, PgRow, PgQueryResult}};
use servicelib::{MessageContext, Payload, dataquery::{self, BeginRequest, Collector,
    EndpointHandler, HandlerError, HandlerResult, postgresqlsqlx::{self, QueryBuilder, RowsHandler, ExecuteHandler}},
    operators::QueryStream, runtime::{common::{Consumer, RuntimeStream},
    config::{CallSemantics, QueryStreamConfig, StreamConfig}, environment::{RuntimeEnvironment, metrics::Metrics}}};

#[derive(Default)]
struct Probe { rows: AtomicUsize, ends: AtomicUsize, errors: AtomicUsize }
struct Handler { probe: Arc<Probe>, mapper_error: bool, wait_deadline: bool, typed: bool }
#[async_trait]
impl QueryBuilder<(), String> for Handler {
    async fn build_query(&self, _: MessageContext, _: &mut (), value: Payload<String>) -> Result<postgresqlsqlx::Query, HandlerError> {
        if self.typed {
            let mut arguments = PgArguments::default();
            arguments.add((*value).clone())?;
            return Ok(postgresqlsqlx::Query { sql: "SELECT $1::text AS key, NULL::text AS optional, 9223372036854775807::bigint AS large, 1234567890.123456789012345678::numeric AS amount, 12345678901234567890.123456789012345678::numeric AS wide, '2026-11-19 12:34:56.123456+00'::timestamptz AS instant, '12345678-1234-5678-1234-567812345678'::uuid AS uuid, decode('6100ff','hex') AS body, jsonb_build_object('value', jsonb_build_array(1,NULL,'quoted')) AS document".into(), arguments });
        }
        Ok(postgresqlsqlx::Query { sql: (*value).clone(), arguments: PgArguments::default() })
    }
}
#[async_trait]
impl RowsHandler<(), String, ()> for Handler {
    async fn handle_row(&self, _: MessageContext, _: &mut (), row: PgRow, _: &Collector<()>) -> HandlerResult {
        if self.typed {
            assert_eq!(row.try_get::<String, _>("key")?, "bound-'key");
            assert_eq!(row.try_get::<Option<String>, _>("optional")?, None);
            assert_eq!(row.try_get::<i64, _>("large")?, i64::MAX);
            assert_eq!(row.try_get::<sqlx::types::Decimal, _>("amount")?.to_string(), "1234567890.123456789012345678");
            // Use arbitrary precision for PostgreSQL numeric beyond rust_decimal's range.
            assert_eq!(row.try_get::<sqlx::types::BigDecimal, _>("wide")?, "12345678901234567890.123456789012345678".parse::<sqlx::types::BigDecimal>().unwrap());
            assert_eq!(row.try_get::<chrono::DateTime<chrono::Utc>, _>("instant")?.to_rfc3339(), "2026-11-19T12:34:56.123456+00:00");
            assert_eq!(row.try_get::<sqlx::types::Uuid, _>("uuid")?.to_string(), "12345678-1234-5678-1234-567812345678");
            assert_eq!(row.try_get::<Vec<u8>, _>("body")?, vec![97, 0, 255]);
            assert_eq!(row.try_get::<serde_json::Value, _>("document")?, serde_json::json!({"value": [1, null, "quoted"]}));
        }
        self.probe.rows.fetch_add(1, Ordering::SeqCst);
        if self.mapper_error { return Err(std::io::Error::other("fixture mapper failed").into()); }
        Ok(())
    }
}
#[async_trait]
impl ExecuteHandler<(), String, ()> for Handler {
    async fn handle_result(&self, context: MessageContext, _: &mut (), _: PgQueryResult, _: &Collector<()>) -> HandlerResult {
        self.probe.rows.fetch_add(1, Ordering::SeqCst);
        if self.wait_deadline { context.cancelled().await; }
        Ok(())
    }
}
#[async_trait]
impl EndpointHandler<(), (), ()> for Handler {
    async fn begin_request(&self, context: MessageContext, _: &dyn RuntimeStream) -> Result<BeginRequest<()>, HandlerError> {
        Ok(BeginRequest { context, state: (), error: None })
    }
    async fn end_request(&self, _: MessageContext, _: &dyn RuntimeStream, state: Option<()>, result: &HandlerResult, _: &Collector<()>, _: &Collector<()>) {
        assert_eq!(state, Some(()));
        self.probe.ends.fetch_add(1, Ordering::SeqCst);
        self.probe.errors.fetch_add(usize::from(result.is_err()), Ordering::SeqCst);
    }
}
fn stream() -> Arc<QueryStream<String, (), ()>> {
    QueryStream::new(&QueryStreamConfig { stream: StreamConfig::new(1, "native query"), endpoint_id: 1 },
        RuntimeEnvironment::with_metrics(CallSemantics::FunctionCall, Metrics::noop())).unwrap()
}
async fn assert_pool_available(pool: &PgPool) {
    assert_eq!(tokio::time::timeout(Duration::from_secs(2), sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(pool)).await.unwrap().unwrap(), 1);
}
#[tokio::test]
#[ignore = "requires disposable Compose PostgreSQL and SA_QUERY_DISPOSABLE=1"]
async fn native_rows_errors_deadlines_and_execute_finalization() {
    assert_eq!(std::env::var("SA_QUERY_DISPOSABLE").as_deref(), Ok("1"));
    let pool = PgPoolOptions::new().max_connections(1).connect(&std::env::var("SA_QUERY_POSTGRES_DSN").unwrap()).await.unwrap();
    sqlx::query("CREATE TABLE query_returning (value integer)").execute(&pool).await.unwrap();
    for (sql, mapper_error, deadline, rows, errors) in [
        ("SELECT 1 WHERE false", false, false, 0, 0),
        ("INSERT INTO query_returning SELECT generate_series(1,128) RETURNING value", false, false, 128, 0),
        ("SELECT 100/(3-i) FROM generate_series(1,4) AS i", false, false, 2, 1),
        ("SELECT generate_series(1,10)", true, false, 1, 1),
        ("SELECT pg_sleep(5)", false, true, 0, 1),
    ] {
        let probe = Arc::new(Probe::default());
        let stream = stream();
        let consumer = dataquery::make_endpoint_consumer(&stream,
            Handler { probe: probe.clone(), mapper_error, wait_deadline: false, typed: false }, postgresqlsqlx::RowsOperation::new(pool.clone())).unwrap();
        let context = MessageContext::new().with_timeout_limit(if deadline { Duration::from_millis(100) } else { Duration::from_secs(2) });
        tokio::time::timeout(Duration::from_secs(3), consumer.consume(context, Payload::new(sql.to_owned()))).await.unwrap();
        assert_eq!(probe.rows.load(Ordering::SeqCst), rows, "{sql}");
        assert_eq!(probe.ends.load(Ordering::SeqCst), 1, "{sql}");
        assert_eq!(probe.errors.load(Ordering::SeqCst), errors, "{sql}");
        assert_pool_available(&pool).await;
    }
    // A completed write whose mapper outlives the deadline is not reported as
    // success and is never replayed. The write itself already committed.
    let probe = Arc::new(Probe::default());
    let stream = stream();
    let consumer = dataquery::make_endpoint_consumer(&stream,
        Handler { probe: probe.clone(), mapper_error: false, wait_deadline: true, typed: false }, postgresqlsqlx::ExecuteOperation::new(pool.clone())).unwrap();
    consumer.consume(MessageContext::new().with_timeout_limit(Duration::from_millis(100)), Payload::new("INSERT INTO query_returning VALUES (999)".to_owned())).await;
    assert_eq!(probe.rows.load(Ordering::SeqCst), 1);
    assert_eq!(probe.ends.load(Ordering::SeqCst), 1);
    assert_eq!(probe.errors.load(Ordering::SeqCst), 1);
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM query_returning WHERE value=999").fetch_one(&pool).await.unwrap(), 1);
    assert_pool_available(&pool).await;
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires disposable Compose PostgreSQL and SA_QUERY_DISPOSABLE=1"]
async fn native_values_preserve_precision_and_bindings() {
    assert_eq!(std::env::var("SA_QUERY_DISPOSABLE").as_deref(), Ok("1"));
    let pool = PgPoolOptions::new().max_connections(1).connect(&std::env::var("SA_QUERY_POSTGRES_DSN").unwrap()).await.unwrap();
    let probe = Arc::new(Probe::default());
    let stream = stream();
    let consumer = dataquery::make_endpoint_consumer(&stream,
        Handler { probe: probe.clone(), mapper_error: false, wait_deadline: false, typed: true },
        postgresqlsqlx::RowsOperation::new(pool.clone())).unwrap();
    consumer.consume(MessageContext::new().with_timeout_limit(Duration::from_secs(2)), Payload::new("bound-'key".to_owned())).await;
    assert_eq!(probe.rows.load(Ordering::SeqCst), 1);
    assert_eq!(probe.ends.load(Ordering::SeqCst), 1);
    assert_eq!(probe.errors.load(Ordering::SeqCst), 0);
    assert_pool_available(&pool).await;
    pool.close().await;
}

struct GatedRows {
    probe: Arc<Probe>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    pool: PgPool,
}
#[async_trait]
impl QueryBuilder<(), String> for GatedRows {
    async fn build_query(&self, _: MessageContext, _: &mut (), value: Payload<String>) -> Result<postgresqlsqlx::Query, HandlerError> {
        Ok(postgresqlsqlx::Query { sql: (*value).clone(), arguments: PgArguments::default() })
    }
}
#[async_trait]
impl RowsHandler<(), String, ()> for GatedRows {
    async fn handle_row(&self, context: MessageContext, _: &mut (), row: PgRow, out: &Collector<()>) -> HandlerResult {
        let index = self.probe.rows.fetch_add(1, Ordering::SeqCst) + 1;
        assert_eq!(row.try_get::<i32, _>(0)?, index as i32);
        if index == 1 {
            self.entered.notify_one();
            tokio::select! {
                _ = self.release.notified() => {},
                _ = context.cancelled() => return Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "fixture deadline").into()),
            }
        }
        out.collect(context, ()).await;
        Ok(())
    }
}
#[async_trait]
impl EndpointHandler<(), (), ()> for GatedRows {
    async fn begin_request(&self, context: MessageContext, _: &dyn RuntimeStream) -> Result<BeginRequest<()>, HandlerError> {
        Ok(BeginRequest { context, state: (), error: None })
    }
    async fn end_request(&self, _: MessageContext, _: &dyn RuntimeStream, state: Option<()>, result: &HandlerResult, _: &Collector<()>, _: &Collector<()>) {
        assert_eq!(state, Some(()));
        assert!(result.is_ok(), "{result:?}");
        // With max_connections=1 this succeeds only after releasing the lease.
        assert_pool_available(&self.pool).await;
        self.probe.ends.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
#[ignore = "requires disposable Compose PostgreSQL and SA_QUERY_DISPOSABLE=1"]
async fn native_large_rows_wait_for_mapper() {
    assert_eq!(std::env::var("SA_QUERY_DISPOSABLE").as_deref(), Ok("1"));
    let pool = PgPoolOptions::new().max_connections(1).connect(&std::env::var("SA_QUERY_POSTGRES_DSN").unwrap()).await.unwrap();
    let probe = Arc::new(Probe::default());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let stream = stream();
    let consumer = dataquery::make_endpoint_consumer(&stream, GatedRows {
        probe: probe.clone(), entered: entered.clone(), release: release.clone(), pool: pool.clone(),
    }, postgresqlsqlx::RowsOperation::new(pool.clone())).unwrap();
    let context = MessageContext::new().with_timeout_limit(Duration::from_secs(10));
    let task = tokio::spawn(async move {
        consumer.consume(context, Payload::new("SELECT generate_series(1,10000)".to_owned())).await;
    });
    tokio::time::timeout(Duration::from_secs(3), entered.notified()).await.unwrap();
    for _ in 0..5 { tokio::task::yield_now().await; }
    assert_eq!(probe.rows.load(Ordering::SeqCst), 1);
    assert_eq!(probe.ends.load(Ordering::SeqCst), 0);
    assert!(!task.is_finished());
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(10), task).await.unwrap().unwrap();
    assert_eq!(probe.rows.load(Ordering::SeqCst), 10000);
    assert_eq!(probe.ends.load(Ordering::SeqCst), 1);
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires disposable Compose PostgreSQL TLS"]
async fn native_connector_tls_verification() {
    use servicelib::runtime::{config::PostgreSQLSQLxDataConnectorConfig, environment::Lifecycle};
    let dsn = std::env::var("SA_QUERY_TLS_POSTGRES_DSN").unwrap();
    assert!(dsn.contains("sslmode=verify-full"));
    assert!(dsn.contains("sslrootcert=/fixtures/ca.pem"));
    for trusted in [false, true] {
        let address = if trusted { dsn.clone() } else { dsn.replace("sslrootcert=/fixtures/ca.pem", "sslrootcert=/fixtures/untrusted-ca.pem") };
        let connector = postgresqlsqlx::Connector::new(&PostgreSQLSQLxDataConnectorConfig {
            id: 1, name: "tls".into(), connection_string: address, connections_count: 1,
            connect_timeout_ms: 2000, operation_timeout_ms: 3000,
        }).unwrap();
        let context = MessageContext::new().with_timeout_limit(Duration::from_secs(5));
        let started = connector.start(context.clone()).await;
        if trusted {
            started.unwrap();
            let probe = Arc::new(Probe::default());
            let graph = stream();
            let consumer = dataquery::make_endpoint_consumer(&graph,
                Handler { probe: probe.clone(), mapper_error: false, wait_deadline: false, typed: false }, connector.rows()).unwrap();
            // Plaintext would raise division-by-zero; assert one row and successful finalization.
            consumer.consume(context.clone(), Payload::new("SELECT 1 / ssl::int AS encrypted FROM pg_stat_ssl WHERE pid=pg_backend_pid()".into())).await;
            assert_eq!(probe.rows.load(Ordering::SeqCst), 1);
            assert_eq!(probe.ends.load(Ordering::SeqCst), 1);
            assert_eq!(probe.errors.load(Ordering::SeqCst), 0);
        } else { assert!(started.is_err(), "unrelated CA was trusted"); }
        connector.stop(context).await.unwrap();
    }
}

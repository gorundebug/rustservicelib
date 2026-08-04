use std::sync::Arc;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::runtime::{
    common::{Consumer, MessageContext, Payload},
    config::CallSemantics,
    environment::{
        CallStatistics, RuntimeEnvironment, RuntimeResult,
        metrics::{Int64Counter, Labels},
    },
    pool::{PriorityTaskPool, TaskPool},
};

pub struct Collector<T>
where
    T: Send + Sync + 'static,
{
    consumer: Arc<dyn Consumer<T>>,
    caller: Caller,
    from: String,
    to: String,
    messages_total: Int64Counter,
    call_statistics: CallStatistics,
}

impl<T> Clone for Collector<T>
where
    T: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            consumer: Arc::clone(&self.consumer),
            caller: self.caller.clone(),
            from: self.from.clone(),
            to: self.to.clone(),
            messages_total: self.messages_total.clone(),
            call_statistics: self.call_statistics.clone(),
        }
    }
}

#[derive(Clone)]
enum Caller {
    FunctionCall,
    ParallelCall,
    TaskPool(Arc<TaskPool>),
    PriorityTaskPool {
        pool: Arc<PriorityTaskPool>,
        priority: i32,
    },
}

pub trait Collect<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    fn out(
        &self,
        context: MessageContext,
        value: T,
    ) -> impl std::future::Future<Output = ()> + Send;

    fn out_payload(
        &self,
        context: MessageContext,
        payload: Payload<T>,
    ) -> impl std::future::Future<Output = ()> + Send;
}

impl<T> Collector<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(
        consumer: Arc<dyn Consumer<T>>,
        call_semantics: CallSemantics,
        environment: &RuntimeEnvironment,
        source_id: i32,
        target_id: i32,
        source_name: String,
    ) -> RuntimeResult<Self> {
        let caller = match call_semantics.clone() {
            CallSemantics::FunctionCall => Caller::FunctionCall,
            CallSemantics::ParallelCall => Caller::ParallelCall,
            CallSemantics::TaskPool { pool_name } => {
                Caller::TaskPool(environment.task_pool(&pool_name)?)
            }
            CallSemantics::PriorityTaskPool {
                pool_name,
                priority,
            } => Caller::PriorityTaskPool {
                pool: environment.priority_task_pool(&pool_name)?,
                priority,
            },
        };
        let from = source_name;
        let to = environment.stream_name(target_id);
        let messages_total = environment
            .metrics()
            .scope(
                "stream",
                [
                    ("service".to_owned(), environment.service_name()),
                    ("from".to_owned(), from.clone()),
                    ("to".to_owned(), to.clone()),
                ]
                .into_iter()
                .collect(),
            )
            .counter(
                "messages_total",
                "Total number of messages processed by stream link",
                Labels::new(),
            )?;
        let call_statistics = CallStatistics::default();
        environment.register_graph_link(
            source_id,
            target_id,
            call_semantics,
            short_type_name::<T>(),
            call_statistics.clone(),
        );
        Ok(Self {
            consumer,
            caller,
            from,
            to,
            messages_total,
            call_statistics,
        })
    }

    pub fn is_async(&self) -> bool {
        !matches!(self.caller, Caller::FunctionCall)
    }

    fn start_span(
        &self,
        context: MessageContext,
        call_type: Option<&'static str>,
        pool: Option<&str>,
    ) -> (MessageContext, tracing::Span) {
        if !context.sampling_enabled() {
            return (context, tracing::Span::none());
        }
        let span = match (call_type, pool) {
            (None, _) => tracing::info_span!(
                "stream.call",
                from = %self.from,
                to = %self.to,
                error = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
                otel.status_message = tracing::field::Empty,
            ),
            (Some(call_type), None) => tracing::info_span!(
                "stream.call",
                from = %self.from,
                to = %self.to,
                r#type = call_type,
                error = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
                otel.status_message = tracing::field::Empty,
            ),
            (Some(call_type), Some(pool)) => tracing::info_span!(
                "stream.call",
                from = %self.from,
                to = %self.to,
                r#type = call_type,
                taskpoolname = pool,
                error = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
                otel.status_message = tracing::field::Empty,
            ),
        };
        let _ = span.set_parent(context.open_telemetry_context().clone());
        let child = span.context();
        (context.with_open_telemetry_context(child), span)
    }
}

fn short_type_name<T>() -> String {
    std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .unwrap_or("unknown")
        .to_owned()
}

impl<T> Collect<T> for Collector<T>
where
    T: Send + Sync + 'static,
{
    async fn out(&self, context: MessageContext, value: T) {
        self.out_payload(context, Payload::new(value)).await;
    }

    async fn out_payload(&self, context: MessageContext, payload: Payload<T>) {
        self.call_statistics.inc();
        self.messages_total.inc();
        match &self.caller {
            Caller::FunctionCall => {
                let (context, span) = self.start_span(context, None, None);
                self.consumer
                    .consume(context, payload)
                    .instrument(span)
                    .await;
            }
            Caller::ParallelCall => {
                let (context, span) = self.start_span(context, Some("parallel"), None);
                let consumer = Arc::clone(&self.consumer);
                tokio::spawn(async move {
                    consumer.consume(context, payload).instrument(span).await;
                });
            }
            Caller::TaskPool(pool) => {
                let (context, span) = self.start_span(context, Some("taskpool"), Some(pool.name()));
                let rejection_span = span.clone();
                let consumer = Arc::clone(&self.consumer);
                let task_context = context.clone();
                if let Err(error) = pool
                    .add_task(
                        context,
                        Box::pin(async move {
                            consumer
                                .consume(task_context, payload)
                                .instrument(span)
                                .await;
                        }),
                    )
                    .await
                {
                    crate::runtime::telemetry::record_span_error(&rejection_span, &error);
                    rejection_span.in_scope(|| {
                        tracing::warn!(
                            pool = pool.name(),
                            error = %error,
                            "task pool rejected task"
                        )
                    });
                }
            }
            Caller::PriorityTaskPool { pool, priority } => {
                let (context, span) =
                    self.start_span(context, Some("prioritytaskpool"), Some(pool.name()));
                let rejection_span = span.clone();
                let priority = context.priority().unwrap_or(*priority);
                let consumer = Arc::clone(&self.consumer);
                let task_context = context.clone();
                if let Err(error) = pool
                    .add_task(
                        context,
                        priority,
                        Box::pin(async move {
                            consumer
                                .consume(task_context, payload)
                                .instrument(span)
                                .await;
                        }),
                    )
                    .await
                {
                    crate::runtime::telemetry::record_span_error(&rejection_span, &error);
                    rejection_span.in_scope(|| {
                        tracing::warn!(
                            pool = pool.name(),
                            error = %error,
                            "priority task pool rejected task"
                        )
                    });
                }
            }
        }
    }
}

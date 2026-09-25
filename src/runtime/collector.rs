use std::{marker::PhantomData, sync::Arc};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::runtime::{
    common::{Consumer, MessageContext, Payload},
    config::CallSemantics,
    environment::{
        CallStatistics, RuntimeEnvironment, RuntimeResult,
        metrics::{Int64Counter, Labels},
    },
    pool::{PriorityTaskPool, TaskPool},
    stream::Stream,
};

pub(crate) struct LinkCollector<T, N: ?Sized = dyn crate::runtime::common::ErasedConsumer<T>>
where
    T: Send + Sync + 'static,
    N: Consumer<T> + 'static,
{
    consumer: Arc<N>,
    value_type: PhantomData<fn(T)>,
    caller: Caller,
    from: String,
    to: String,
    pipeline: String,
    component: String,
    messages_total: Option<Int64Counter>,
    call_statistics: CallStatistics,
    environment: RuntimeEnvironment,
}

#[derive(Clone)]
enum Caller {
    FunctionCall(bool),
    ParallelCall,
    TaskPool(Arc<TaskPool>),
    PriorityTaskPool {
        pool: Arc<PriorityTaskPool>,
        priority: i32,
    },
}

impl Caller {
    fn is_async(&self) -> bool {
        match self {
            Self::FunctionCall(r#async) => *r#async,
            _ => true,
        }
    }
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

    fn collect(
        &self,
        context: MessageContext,
        value: T,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.out(context, value)
    }

    fn emit(
        &self,
        context: MessageContext,
        payload: Payload<T>,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.out_payload(context, payload)
    }
}

pub struct Collector<T, C = Stream<T>>
where
    T: Send + Sync + 'static,
{
    stream: Stream<T>,
    output: C,
}

impl<T> Collector<T>
where
    T: Send + Sync + 'static,
{
    pub(crate) fn from_stream(stream: Stream<T>) -> Self {
        Self {
            output: stream.clone(),
            stream,
        }
    }
}

impl<T, C> Collector<T, C>
where
    T: Send + Sync + 'static,
    C: Collect<T>,
{
    pub(crate) fn from_output(stream: Stream<T>, output: C) -> Self {
        Self { stream, output }
    }

    pub fn stream(&self) -> &Stream<T> {
        &self.stream
    }

    pub fn is_async(&self) -> bool {
        self.stream
            .link_collector()
            .is_some_and(|link| link.is_async())
    }

    pub fn collect(
        &self,
        context: MessageContext,
        value: T,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.output.out(context, value)
    }

    pub fn emit(
        &self,
        context: MessageContext,
        payload: Payload<T>,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.output.out_payload(context, payload)
    }
}

impl<T, C: Clone> Clone for Collector<T, C>
where
    T: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            stream: self.stream.clone(),
            output: self.output.clone(),
        }
    }
}

impl<T, C> Collect<T> for Collector<T, C>
where
    T: Send + Sync + 'static,
    C: Collect<T>,
{
    fn out(
        &self,
        context: MessageContext,
        value: T,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.collect(context, value)
    }

    fn out_payload(
        &self,
        context: MessageContext,
        payload: Payload<T>,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.emit(context, payload)
    }
}

impl<T, C> Collect<T> for Arc<C>
where
    T: Send + Sync + 'static,
    C: Collect<T> + ?Sized,
{
    fn out(
        &self,
        context: MessageContext,
        value: T,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.as_ref().out(context, value)
    }
    fn out_payload(
        &self,
        context: MessageContext,
        payload: Payload<T>,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.as_ref().out_payload(context, payload)
    }
}

impl<T, N: ?Sized> LinkCollector<T, N>
where
    T: Send + Sync + 'static,
    N: Consumer<T> + 'static,
{
    pub fn new(
        consumer: Arc<N>,
        call_semantics: CallSemantics,
        environment: &RuntimeEnvironment,
        source_id: i32,
        target_id: i32,
        source_name: String,
        function_call_async: bool,
    ) -> RuntimeResult<Self> {
        let caller = match call_semantics.clone() {
            CallSemantics::FunctionCall => Caller::FunctionCall(function_call_async),
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
        let (pipeline, component) = environment.stream_grouping(target_id);
        let messages_total = if environment.metrics().is_noop() {
            None
        } else {
            Some(
                environment
                    .metrics()
                    .scope(
                        "stream",
                        [
                            ("service".to_owned(), environment.service_name()),
                            ("from".to_owned(), from.clone()),
                            ("to".to_owned(), to.clone()),
                            ("pipeline".to_owned(), pipeline.clone()),
                            ("component".to_owned(), component.clone()),
                        ]
                        .into_iter()
                        .collect(),
                    )
                    .counter(
                        "messages_total",
                        "Total number of messages processed by stream link",
                        Labels::new(),
                    )?,
            )
        };
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
            value_type: PhantomData,
            caller,
            from,
            to,
            pipeline,
            component,
            messages_total,
            call_statistics,
            environment: environment.clone(),
        })
    }

    pub fn is_async(&self) -> bool {
        self.caller.is_async()
    }

    fn start_span(
        &self,
        context: MessageContext,
        call_type: Option<&'static str>,
        pool: Option<&str>,
    ) -> (MessageContext, Option<tracing::Span>) {
        if !self.environment.tracing_enabled() || !context.sampling_enabled() {
            return (context, None);
        }
        let span = match (call_type, pool) {
            (None, _) => tracing::info_span!(
                "stream.call",
                from = %self.from,
                to = %self.to,
                pipeline = %self.pipeline,
                component = %self.component,
                error = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
                otel.status_message = tracing::field::Empty,
            ),
            (Some(call_type), None) => tracing::info_span!(
                "stream.call",
                from = %self.from,
                to = %self.to,
                pipeline = %self.pipeline,
                component = %self.component,
                r#type = call_type,
                error = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
                otel.status_message = tracing::field::Empty,
            ),
            (Some(call_type), Some(pool)) => tracing::info_span!(
                "stream.call",
                from = %self.from,
                to = %self.to,
                pipeline = %self.pipeline,
                component = %self.component,
                r#type = call_type,
                taskpoolname = pool,
                error = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
                otel.status_message = tracing::field::Empty,
            ),
        };
        if span.is_disabled() {
            return (context, None);
        }
        let _ = span.set_parent(context.open_telemetry_context().clone());
        let child = span.context();
        (context.with_open_telemetry_context(child), Some(span))
    }
}

pub(crate) fn short_type_name<T>() -> String {
    std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .unwrap_or("unknown")
        .to_owned()
}

impl<T, N: ?Sized> Collect<T> for LinkCollector<T, N>
where
    T: Send + Sync + 'static,
    N: Consumer<T> + 'static,
{
    fn out(
        &self,
        context: MessageContext,
        value: T,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.out_payload(context, Payload::new(value))
    }

    async fn out_payload(&self, context: MessageContext, payload: Payload<T>) {
        self.call_statistics.inc();
        if let Some(counter) = &self.messages_total {
            counter.inc();
        }
        match &self.caller {
            Caller::FunctionCall(_) => {
                let (context, span) = self.start_span(context, None, None);
                crate::runtime::common::instrument_if_present!(
                    self.consumer.consume(context, payload),
                    span,
                );
            }
            Caller::ParallelCall => {
                let (context, span) = self.start_span(context, Some("parallel"), None);
                let consumer = Arc::clone(&self.consumer);
                self.environment.spawn_parallel(async move {
                    crate::runtime::common::instrument_if_present!(
                        consumer.consume(context, payload),
                        span,
                    );
                });
            }
            Caller::TaskPool(pool) => {
                let (context, span) = self.start_span(context, Some("taskpool"), Some(pool.name()));
                let rejection_span = span.as_ref().filter(|span| !span.is_disabled()).cloned();
                let consumer = Arc::clone(&self.consumer);
                let task_context = context.clone();
                if let Err(error) = pool
                    .add_task(
                        context,
                        Box::pin(async move {
                            crate::runtime::common::instrument_if_present!(
                                consumer.consume(task_context, payload),
                                span,
                            );
                        }),
                    )
                    .await
                {
                    let report = || {
                        tracing::warn!(
                            pool = pool.name(),
                            error = %error,
                            "task pool rejected task"
                        )
                    };
                    if let Some(rejection_span) = rejection_span {
                        crate::runtime::telemetry::record_span_error(&rejection_span, &error);
                        rejection_span.in_scope(report);
                    } else {
                        report();
                    }
                }
            }
            Caller::PriorityTaskPool { pool, priority } => {
                let (context, span) =
                    self.start_span(context, Some("prioritytaskpool"), Some(pool.name()));
                let rejection_span = span.as_ref().filter(|span| !span.is_disabled()).cloned();
                let priority = context.priority().unwrap_or(*priority);
                let consumer = Arc::clone(&self.consumer);
                let task_context = context.clone();
                if let Err(error) = pool
                    .add_task(
                        context,
                        priority,
                        Box::pin(async move {
                            crate::runtime::common::instrument_if_present!(
                                consumer.consume(task_context, payload),
                                span,
                            );
                        }),
                    )
                    .await
                {
                    let report = || {
                        tracing::warn!(
                            pool = pool.name(),
                            error = %error,
                            "priority task pool rejected task"
                        )
                    };
                    if let Some(rejection_span) = rejection_span {
                        crate::runtime::telemetry::record_span_error(&rejection_span, &error);
                        rejection_span.in_scope(report);
                    } else {
                        report();
                    }
                }
            }
        }
    }
}

// Erasure is performed only when entering through a dynamic Stream handle.
// Both views share this collector, including its counters and scheduling.
impl<T, N> Consumer<T> for LinkCollector<T, N>
where
    T: Send + Sync + 'static,
    N: Consumer<T> + 'static,
{
    fn consume(
        &self,
        context: MessageContext,
        payload: Payload<T>,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.out_payload(context, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::Caller;

    #[test]
    fn function_call_async_flag_only_changes_caller_metadata() {
        assert!(!Caller::FunctionCall(false).is_async());
        assert!(Caller::FunctionCall(true).is_async());
        assert!(Caller::ParallelCall.is_async());
    }
}

//! Request-driven Query lifecycle, independent from DataSink.
//!
//! Native operations own and release their cursor/body before returning. They
//! observe the supplied context and never replay an operation after partial output.
use std::{error::Error, marker::PhantomData, sync::{Arc, Weak}};

use async_trait::async_trait;
use crate::{operators::QueryStream, runtime::{
    common::{CompleteOnDrop, Consumer, MessageContext, Payload, RuntimeStream},
    environment::RuntimeResult,
}};

mod collector;
mod metrics;
pub use collector::Collector;

pub mod postgresqlsqlx;
pub mod s3aws;

pub type HandlerError = Box<dyn Error + Send + Sync>;
pub type HandlerResult = Result<(), HandlerError>;

pub struct BeginRequest<HandlerState> {
    pub context: MessageContext,
    pub state: HandlerState,
    pub error: Option<HandlerError>,
}

#[async_trait]
pub trait EndpointHandler<HandlerState, R, E>: Send + Sync
where HandlerState: Send + 'static, R: Send + Sync + 'static, E: Send + Sync + 'static {
    async fn begin_request(&self, context: MessageContext, stream: &dyn RuntimeStream)
        -> Result<BeginRequest<HandlerState>, HandlerError>;

    /// Called once, including begin failure. None means begin returned no state.
    /// The user may emit completion through out; the SDK emits no completion event.
    async fn end_request(&self, context: MessageContext, stream: &dyn RuntimeStream,
        state: Option<HandlerState>, result: &HandlerResult,
        out: &Collector<R>, errors: &Collector<E>);
}

#[async_trait]
pub trait Operation<HandlerState, T, R, H>: Send + Sync
where HandlerState: Send + 'static, T: Send + Sync + 'static,
    R: Send + Sync + 'static, H: Send + Sync {
    /// Execute on the caller's task. Return only after native resources are released.
    /// Cancellation must be handled by the native adapter, including cleanup.
    async fn execute(&self, context: MessageContext, state: &mut HandlerState,
        value: Payload<T>, handler: &H, out: &Collector<R>) -> HandlerResult;
}

pub struct EndpointConsumer<HandlerState, T, R, E, H, O>
where T: Send + Sync + 'static, R: Send + Sync + 'static, E: Send + Sync + 'static {
    stream: Weak<QueryStream<T, R, E>>,
    handler: Arc<H>,
    operation: Arc<O>,
    requests: Arc<QueryRequests>,
    metrics: Option<Arc<metrics::EndpointMetrics>>,
    state: PhantomData<fn(HandlerState)>,
}

pub fn make_endpoint_consumer<HandlerState, T, R, E, H, O>(
    stream: &Arc<QueryStream<T, R, E>>, handler: H, operation: O,
) -> RuntimeResult<Arc<EndpointConsumer<HandlerState, T, R, E, H, O>>>
where HandlerState: Send + 'static, T: Send + Sync + 'static,
    R: Send + Sync + 'static, E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, R, E> + 'static,
    O: Operation<HandlerState, T, R, H> + 'static {
    let consumer = Arc::new(EndpointConsumer {
        stream: Arc::downgrade(stream), handler: Arc::new(handler), operation: Arc::new(operation),
        requests: Arc::new(QueryRequests::default()), state: PhantomData,
        metrics: metrics::EndpointMetrics::bind(stream.environment(), stream.endpoint_id())?,
    });
    stream.set_query_consumer(consumer.clone())?;
    Ok(consumer)
}

impl<HandlerState, T, R, E, H, O> Consumer<T> for EndpointConsumer<HandlerState, T, R, E, H, O>
where HandlerState: Send + 'static, T: Send + Sync + 'static,
    R: Send + Sync + 'static, E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, R, E> + 'static,
    O: Operation<HandlerState, T, R, H> + 'static {
    async fn consume(&self, context: MessageContext, value: Payload<T>) {
        let Some(stream) = self.stream.upgrade() else { return; };
        if self.requests.tasks.is_closed() { return; }
        // Cancellation belongs to this invocation, never to sibling graph work.
        let context = context.child();
        let cancellation = context.clone();
        let handler = Arc::clone(&self.handler);
        let operation = Arc::clone(&self.operation);
        let metrics = self.metrics.clone();
        let request = self.requests.tasks.track_future(async move {
            let _observation = metrics::Request::begin(metrics.clone());
            let out = Collector::new(stream.stream().clone(), metrics.as_ref().map(|m| m.results.clone()));
            let errors = Collector::new(stream.error_stream().clone(), None);
            let (context, state, result) = match handler.begin_request(context.clone(), stream.as_ref()).await {
                Err(error) => (context, None, Err(error)),
                Ok(mut begun) => {
                    let result = if let Some(error) = begun.error {
                        Err(error)
                    } else if begun.context.is_cancelled() {
                        Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "Query cancelled").into())
                    } else {
                        operation.execute(begun.context.clone(), &mut begun.state, value, handler.as_ref(), &out).await
                    };
                    (begun.context, Some(begun.state), result)
                }
            };
            if let Some(metrics) = &metrics { metrics.outcome(&result, context.is_cancelled()); }
            handler.end_request(context, stream.as_ref(), state, &result, &out, &errors).await;
        });
        CompleteOnDrop::new(cancellation, request).await;
    }
}

/// Tracks Query finalizers as a service lifecycle component.
#[derive(Default)]
pub struct QueryRequests {
    tasks: tokio_util::task::TaskTracker,
}

impl<HandlerState, T, R, E, H, O> EndpointConsumer<HandlerState, T, R, E, H, O>
where T: Send + Sync + 'static, R: Send + Sync + 'static, E: Send + Sync + 'static {
    pub fn requests(&self) -> Arc<QueryRequests> { Arc::clone(&self.requests) }
}

#[async_trait]
impl crate::runtime::environment::Lifecycle for QueryRequests {
    async fn start(&self, _context: MessageContext) -> RuntimeResult<()> {
        self.tasks.reopen();
        Ok(())
    }

    async fn stop(&self, context: MessageContext) -> RuntimeResult<()> {
        self.tasks.close();
        tokio::select! {
            biased;
            _ = self.tasks.wait() => Ok(()),
            _ = context.cancelled() => Err(crate::runtime::environment::RuntimeError::Transport(
                "Query finalization deadline exceeded".into())),
        }
    }
}

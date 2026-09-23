use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

use crate::runtime::{
    common::{
        CallableSubStream, ConstructionCell, Consumer, ContextKey, MessageContext, Payload,
        SubStreamCollector,
    },
    config::SubStreamConfig,
    environment::{RuntimeBuildable, RuntimeEnvironment, RuntimeError, RuntimeResult},
    stream::Stream,
};

struct Callback<R: Send + Sync + 'static> {
    context: MessageContext,
    collector: Arc<dyn SubStreamCollector<R>>,
}

struct Call<R: Send + Sync + 'static> {
    callback: Mutex<Option<Callback<R>>>,
    gate: AsyncMutex<()>,
    done: CancellationToken,
}

impl<R: Send + Sync + 'static> Call<R> {
    fn close(&self) {
        self.done.cancel();
        self.callback
            .lock()
            .expect("SubStream callback lock poisoned")
            .take();
    }

    async fn deliver(&self, payload: Payload<R>) {
        let _gate = self.gate.lock().await;
        if self.done.is_cancelled() {
            return;
        }
        let callback = self
            .callback
            .lock()
            .expect("SubStream callback lock poisoned")
            .as_ref()
            .map(|callback| (callback.context.clone(), Arc::clone(&callback.collector)));
        let Some((context, collector)) = callback else {
            return;
        };
        if context.is_cancelled() {
            self.close();
            return;
        }
        // Restore the caller's outer invocation for nested calls from a collector.
        if collector.out(context, payload).await {
            self.close();
        }
    }
}

struct CallGuard<R: Send + Sync + 'static>(Arc<Call<R>>);

impl<R: Send + Sync + 'static> Drop for CallGuard<R> {
    fn drop(&mut self) {
        // Also close when the caller drops/aborts its Consume future.
        self.0.close();
    }
}

pub struct SubStream<T: Send + Sync + 'static, R: Send + Sync + 'static> {
    inner: Arc<SubStreamInner<T, R>>,
}

struct SubStreamInner<T: Send + Sync + 'static, R: Send + Sync + 'static> {
    stream: Stream<T>,
    source: ConstructionCell<Stream<R>>,
    key: ContextKey<Call<R>>,
    service_id: i32,
}

impl<T: Send + Sync + 'static, R: Send + Sync + 'static> Clone for SubStream<T, R> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T, R> SubStream<T, R>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
{
    pub fn new(config: &SubStreamConfig, environment: RuntimeEnvironment) -> Self {
        let inner = Arc::new(SubStreamInner {
            stream: Stream::new(&config.stream, environment.clone()),
            source: ConstructionCell::empty(),
            key: ContextKey::new(),
            service_id: config.stream.id_service,
        });
        let buildable: Arc<dyn RuntimeBuildable> = inner.clone();
        environment.register_runtime_buildable(Arc::downgrade(&buildable));
        Self { inner }
    }
}

impl<T: Send + Sync + 'static, R: Send + Sync + 'static> SubStream<T, R> {
    pub fn stream(&self) -> &Stream<T> {
        &self.inner.stream
    }

    pub fn set_source(&self, source: &Stream<R>) -> RuntimeResult<()> {
        if self.inner.source.get().is_some() {
            return Err(RuntimeError::SourceAlreadySet {
                stream: self.stream().name(),
            });
        }
        if source.id() == self.stream().id()
            || source.config().stream().id_service != self.inner.service_id
        {
            return Err(RuntimeError::InvalidConfiguration(
                "SubStream result source must be a different stream in the same service".to_owned(),
            ));
        }
        source.try_set_consumer(
            Arc::new(ResultLink {
                key: self.inner.key.clone(),
            }),
            self.stream().id(),
        )?;
        self.inner
            .source
            .set(source.clone())
            .map_err(|_| RuntimeError::SourceAlreadySet {
                stream: self.stream().name(),
            })
    }

    pub async fn consume(
        &self,
        context: MessageContext,
        value: T,
        collector: Arc<dyn SubStreamCollector<R>>,
    ) -> RuntimeResult<()> {
        self.inner.build()?;
        if context.is_cancelled() {
            return Err(RuntimeError::ContextCancelled);
        }
        let call = Arc::new(Call {
            callback: Mutex::new(Some(Callback {
                context: context.clone(),
                collector,
            })),
            gate: AsyncMutex::new(()),
            done: CancellationToken::new(),
        });
        let _guard = CallGuard(Arc::clone(&call));
        let dispatch_context = context
            .clone()
            .with_local_value(&self.inner.key, Arc::clone(&call));
        if !self.stream().environment().tracing_enabled() || !context.sampling_enabled() {
            return self.dispatch(context, dispatch_context, value, &call).await;
        }
        let (dispatch_context, span) = self
            .stream()
            .start_span(dispatch_context, "stream.substream");
        crate::runtime::common::instrument_if_present!(
            self.dispatch(context, dispatch_context, value, &call),
            span,
        )
    }

    async fn dispatch(
        &self,
        context: MessageContext,
        dispatch_context: MessageContext,
        value: T,
        call: &Call<R>,
    ) -> RuntimeResult<()> {
        let dispatch = self.stream().emit(dispatch_context, Payload::new(value));
        tokio::pin!(dispatch);
        tokio::select! {
            biased;
            _ = context.cancelled() => {
                call.close();
                // A direct callback may be part of this same future. Keep polling
                // it while draining rather than dropping it or deadlocking on its gate.
                let drain = call.gate.lock();
                tokio::pin!(drain);
                tokio::select! {
                    _ = &mut drain => {},
                    _ = &mut dispatch => { let _gate = drain.await; },
                }
                return Err(RuntimeError::ContextCancelled);
            },
            _ = &mut dispatch => {},
        }
        let result = tokio::select! {
            biased;
            _ = context.cancelled() => Err(RuntimeError::ContextCancelled),
            _ = call.done.cancelled() => Ok(()),
        };
        call.close();
        let _gate = call.gate.lock().await;
        result
    }
}

impl<T: Send + Sync + 'static, R: Send + Sync + 'static> RuntimeBuildable for SubStreamInner<T, R> {
    fn build(&self) -> RuntimeResult<()> {
        if self.stream.link_collector().is_none() {
            return Err(RuntimeError::ConsumerNotSet {
                stream: self.stream.name(),
            });
        }
        if self.source.get().is_none() {
            return Err(RuntimeError::InvalidConfiguration(
                "SubStream result source is missing".to_owned(),
            ));
        }
        if self
            .stream
            .environment()
            .service_id()
            .is_some_and(|id| id != self.service_id)
        {
            return Err(RuntimeError::InvalidConfiguration(
                "SubStream belongs to a different service".to_owned(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl<T: Send + Sync + 'static, R: Send + Sync + 'static> CallableSubStream<T, R>
    for SubStream<T, R>
{
    async fn consume(
        &self,
        context: MessageContext,
        value: T,
        collector: Arc<dyn SubStreamCollector<R>>,
    ) -> RuntimeResult<()> {
        SubStream::consume(self, context, value, collector).await
    }
}

struct ResultLink<R: Send + Sync + 'static> {
    key: ContextKey<Call<R>>,
}

#[async_trait]
impl<R: Send + Sync + 'static> Consumer<R> for ResultLink<R> {
    async fn consume(&self, context: MessageContext, payload: Payload<R>) {
        if let Some(call) = context.local_value(&self.key) {
            call.deliver(payload).await;
        }
    }
}

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use servicelib::{
    MessageContext, Payload,
    datasource::localsource::{
        DataProducer, EndpointHandler, HandlerError, HandlerResult, ResultCallback, ResultContext,
        StreamContext, make_custom_endpoint_consumer,
    },
    operators::{InputStream, MapFunction},
    runtime::{
        collector::Collector,
        common::{Consumer, RuntimeStream},
        config::{InputStreamConfig, StreamConfig},
        environment::{Lifecycle, RuntimeEnvironment},
    },
};
use tokio::sync::oneshot;

struct OneValueProducer;

#[async_trait]
impl DataProducer<i32> for OneValueProducer {
    async fn start(
        &self,
        context: MessageContext,
        consumer: Arc<dyn Consumer<i32>>,
    ) -> HandlerResult {
        consumer.consume(context, Payload::new(21)).await;
        Ok(())
    }

    async fn stop(&self, _context: MessageContext) {}
}

struct Double;

#[async_trait]
impl MapFunction<i32, i32> for Double {
    async fn map(
        &self,
        context: MessageContext,
        _stream: &dyn RuntimeStream,
        value: &i32,
        out: &Collector<i32>,
    ) {
        out.emit(context, Payload::new(*value * 2)).await;
    }
}

struct Handler {
    finished: Mutex<Option<oneshot::Sender<i32>>>,
}

#[async_trait]
impl EndpointHandler<(), i32, i32, String> for Handler {
    fn concurrency(&self, _stream: &StreamContext<i32, i32, String>) -> usize {
        1
    }

    async fn begin_request(
        &self,
        context: MessageContext,
        _stream: StreamContext<i32, i32, String>,
    ) -> Result<(MessageContext, ()), HandlerError> {
        Ok((context, ()))
    }

    async fn consume_message(
        &self,
        context: MessageContext,
        stream: StreamContext<i32, i32, String>,
        _handler_state: Arc<()>,
        value: Payload<i32>,
        result_context: Arc<ResultContext<(), i32, i32, String>>,
    ) -> HandlerResult {
        let done = Arc::clone(&result_context);
        let callback: ResultCallback<(), i32, i32, String> =
            Arc::new(move |_context, _stream, _state, value| {
                let done = Arc::clone(&done);
                Box::pin(async move {
                    assert_eq!(*value, 42);
                    done.done();
                    true
                })
            });
        result_context.set_result_callback("42", callback);
        stream.collect(context, *value).await;
        Ok(())
    }

    fn get_message_id(
        &self,
        _context: &MessageContext,
        _stream: &StreamContext<i32, i32, String>,
        _handler_state: &(),
        value: &i32,
    ) -> String {
        value.to_string()
    }

    async fn end_request(
        &self,
        _context: MessageContext,
        _stream: StreamContext<i32, i32, String>,
        result: &HandlerResult,
        _handler_state: Arc<()>,
    ) {
        assert!(result.is_ok());
        if let Some(finished) = self.finished.lock().unwrap().take() {
            let _ = finished.send(42);
        }
    }
}

#[tokio::test]
async fn custom_source_correlates_result_and_waits_for_done() {
    let environment = RuntimeEnvironment::default();
    let input = InputStream::<i32, i32, String>::new(
        &InputStreamConfig {
            stream: StreamConfig::new(1, "Input"),
            endpoint_id: 10,
        },
        environment,
    );
    let result = input
        .stream()
        .map(&(StreamConfig::new(2, "Double").into()), Double)
        .unwrap();
    input.set_source(&result).unwrap();
    let (finished, wait_finished) = oneshot::channel();
    let data_source = make_custom_endpoint_consumer(
        input,
        OneValueProducer,
        Handler {
            finished: Mutex::new(Some(finished)),
        },
    )
    .unwrap();

    data_source.start(MessageContext::new()).await.unwrap();
    assert_eq!(wait_finished.await.unwrap(), 42);
    data_source.stop(MessageContext::new()).await.unwrap();
}

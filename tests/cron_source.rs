use std::sync::Arc;

use async_trait::async_trait;
use servicelib::{
    Collector, MessageContext, Payload, ScheduleEndpointFunction, ScheduleTrigger,
    api::{ScheduleMissedRunPolicy, ScheduleOverlapPolicy},
    datasource::cron::{CronDataSource, make_croner_endpoint_consumer},
    operators::InputStream,
    runtime::{
        common::Consumer,
        config::{
            CallSemantics, CronDataConnectorConfig, CronEndpointConfig, InputStreamConfig,
            RuntimeConfig, StreamConfig,
        },
        environment::{Lifecycle, RuntimeEnvironment},
    },
};
use tokio::{sync::mpsc, time::Duration};

fn runtime(enabled: bool, schedule: &str) -> (RuntimeEnvironment, InputStream<String, (), String>) {
    let environment = RuntimeEnvironment::default();
    let input_config = InputStreamConfig {
        stream: StreamConfig::new(1, "scheduled input"),
        endpoint_id: 2,
    };
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [input_config.clone().into()],
            [],
            [CronDataConnectorConfig {
                id: 4,
                name: "local scheduler".to_owned(),
            }
            .into()],
            [CronEndpointConfig {
                id: 2,
                name: "every second".to_owned(),
                id_data_connector: 4,
                tracing_enabled: false,
                enabled,
                schedule: schedule.to_owned(),
                timezone: "UTC".to_owned(),
                overlap_policy: ScheduleOverlapPolicy::Skip,
                missed_run_policy: ScheduleMissedRunPolicy::Skip,
            }
            .into()],
            [],
        )
        .expect("valid cron fixture"),
    ));
    let input = InputStream::new(&input_config, environment.clone());
    (environment, input)
}

struct Capture(mpsc::UnboundedSender<(String, String)>);

#[async_trait]
impl Consumer<String> for Capture {
    async fn consume(&self, context: MessageContext, payload: Payload<String>) {
        self.0
            .send((
                context
                    .stream_id()
                    .expect("cron assigns stream id")
                    .to_owned(),
                payload.into_value(),
            ))
            .expect("test receiver remains alive");
    }
}

struct BuildScheduledValue;

#[async_trait]
impl ScheduleEndpointFunction<String> for BuildScheduledValue {
    async fn on_trigger(
        &self,
        context: MessageContext,
        trigger: ScheduleTrigger,
        out: &Collector<String>,
    ) {
        out.collect(
            context,
            format!("{}:{:?}", trigger.schedule_id, trigger.backend),
        )
        .await;
    }
}

#[tokio::test]
async fn cron_source_activates_the_existing_input_with_a_fresh_stream_id() {
    let (environment, input) = runtime(true, "* * * * * *");
    let data_source = CronDataSource::new(4, environment).expect("cron datasource");
    make_croner_endpoint_consumer(&data_source, &input, BuildScheduledValue)
        .expect("cron endpoint");
    let (sender, mut receiver) = mpsc::unbounded_channel();
    input.stream().set_consumer(Arc::new(Capture(sender)), 3);

    data_source
        .start(MessageContext::new())
        .await
        .expect("start cron datasource");
    let (stream_id, value) = tokio::time::timeout(Duration::from_secs(3), receiver.recv())
        .await
        .expect("cron fired before timeout")
        .expect("capture channel remains open");
    data_source
        .stop(MessageContext::new())
        .await
        .expect("stop cron datasource");

    assert!(!stream_id.is_empty());
    assert_eq!(value, "every second:Local");
}

#[tokio::test]
async fn disabled_cron_endpoint_exists_without_parsing_or_starting_its_schedule() {
    let (environment, input) = runtime(false, "not a cron expression");
    let data_source = CronDataSource::new(4, environment.clone()).expect("cron datasource");
    make_croner_endpoint_consumer(&data_source, &input, BuildScheduledValue)
        .expect("disabled endpoint still exists");

    assert!(environment.endpoint_consumer(2).is_some());
    data_source
        .start(MessageContext::new())
        .await
        .expect("disabled endpoint starts no transport");
    data_source
        .stop(MessageContext::new())
        .await
        .expect("stop empty scheduler");
}

use std::{
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use chrono::{DateTime, LocalResult, TimeZone, Utc};
use chrono_tz::Tz;
use croner::Cron;
use tokio::{sync::Mutex as AsyncMutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    api::{DataConnectorType, ScheduleMissedRunPolicy, ScheduleOverlapPolicy},
    operators::InputStream,
    runtime::{
        common::{Consumer, MessageContext, Payload, RuntimeEndpointConsumer, new_stream_id},
        config::{CronEndpointConfig, RuntimeEndpointConfig},
        datasource::{DataSource, DataSourceEndpointMetrics},
        environment::{Lifecycle, RuntimeEnvironment, RuntimeError, RuntimeResult},
        schedule::{ScheduleBackend, ScheduleEndpointFunction, ScheduleTrigger},
    },
};

struct CronJob {
    endpoint_id: i32,
    endpoint_name: String,
    schedule: Cron,
    timezone: Tz,
    overlap_policy: ScheduleOverlapPolicy,
    missed_run_policy: ScheduleMissedRunPolicy,
    consumer: Arc<dyn Consumer<ScheduleTrigger>>,
    running: Arc<AtomicBool>,
}

impl CronJob {
    fn next(&self, after: DateTime<Utc>) -> RuntimeResult<DateTime<Utc>> {
        let mut cursor = after.with_timezone(&self.timezone);
        loop {
            let candidate = self
                .schedule
                .find_next_occurrence(&cursor, false)
                .map_err(|error| RuntimeError::InvalidConfiguration(error.to_string()))?;
            let matches = self
                .schedule
                .is_time_matching(&candidate)
                .map_err(|error| RuntimeError::InvalidConfiguration(error.to_string()))?;
            let first_fold = match self.timezone.from_local_datetime(&candidate.naive_local()) {
                LocalResult::Ambiguous(first, _) => {
                    first.with_timezone(&Utc) == candidate.with_timezone(&Utc)
                }
                LocalResult::Single(_) => true,
                LocalResult::None => false,
            };
            if matches && first_fold {
                return Ok(candidate.with_timezone(&Utc));
            }
            // Croner remains the sole parser and next-occurrence evaluator.
            // The adapter only filters its shifted spring-gap candidate and
            // the second instant of an ambiguous fall wall time.
            cursor = candidate;
        }
    }

    fn dispatch(&self, scheduled_at: DateTime<Utc>, tasks: &mut tokio::task::JoinSet<()>) {
        let overlap_guard = if self.overlap_policy == ScheduleOverlapPolicy::Skip {
            if self
                .running
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return;
            }
            Some(RunningGuard(Arc::clone(&self.running)))
        } else {
            None
        };
        let consumer = Arc::clone(&self.consumer);
        let endpoint_id = self.endpoint_id;
        let endpoint_name = self.endpoint_name.clone();
        tasks.spawn(async move {
            let _overlap_guard = overlap_guard;
            let fired_at = Utc::now();
            let context = MessageContext::new().with_stream_id(new_stream_id());
            consumer
                .consume(
                    context,
                    Payload::new(ScheduleTrigger::new(
                        endpoint_id,
                        endpoint_name,
                        scheduled_at,
                        fired_at,
                        ScheduleBackend::Local,
                    )),
                )
                .await;
        });
    }
}

struct RunningGuard(Arc<AtomicBool>);

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

struct ScheduledJob {
    job: Arc<CronJob>,
    next: DateTime<Utc>,
}

pub struct CronDataSource {
    id: i32,
    name: String,
    environment: RuntimeEnvironment,
    jobs: Mutex<Vec<Arc<CronJob>>>,
    cancellation: Mutex<CancellationToken>,
    scheduler: AsyncMutex<Option<JoinHandle<RuntimeResult<()>>>>,
}

impl CronDataSource {
    pub fn new(connector_id: i32, environment: RuntimeEnvironment) -> RuntimeResult<Arc<Self>> {
        let config = environment
            .runtime_config()
            .data_connector_by_id(connector_id)
            .ok_or_else(|| {
                RuntimeError::InvalidConfiguration(format!(
                    "cron data connector {connector_id} not found"
                ))
            })?;
        if config.connector_type() != DataConnectorType::Cron {
            return Err(RuntimeError::InvalidConfiguration(format!(
                "data connector {:?} is not cron",
                config.name()
            )));
        }
        Ok(Arc::new(Self {
            id: connector_id,
            name: config.name().to_owned(),
            environment,
            jobs: Mutex::new(Vec::new()),
            cancellation: Mutex::new(CancellationToken::new()),
            scheduler: AsyncMutex::new(None),
        }))
    }

    fn add_job(&self, job: Arc<CronJob>) -> RuntimeResult<()> {
        let mut jobs = self.jobs.lock().expect("cron job registry lock poisoned");
        if jobs
            .iter()
            .any(|existing| existing.endpoint_id == job.endpoint_id)
        {
            return Err(RuntimeError::DuplicateResource(job.endpoint_name.clone()));
        }
        jobs.push(job);
        Ok(())
    }

    fn endpoint_config(&self, endpoint_id: i32) -> RuntimeResult<CronEndpointConfig> {
        let endpoint = self
            .environment
            .runtime_config()
            .endpoint_by_id(endpoint_id)
            .ok_or_else(|| {
                RuntimeError::InvalidConfiguration(format!("cron endpoint {endpoint_id} not found"))
            })?;
        let RuntimeEndpointConfig::Cron(config) = endpoint.as_ref() else {
            return Err(RuntimeError::InvalidConfiguration(format!(
                "endpoint {endpoint_id} is not cron"
            )));
        };
        if config.id_data_connector != self.id {
            return Err(RuntimeError::InvalidConfiguration(format!(
                "cron endpoint {:?} belongs to connector {}, not {}",
                config.name, config.id_data_connector, self.id
            )));
        }
        Ok(config.clone())
    }
}

impl DataSource for CronDataSource {
    fn id(&self) -> i32 {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl Lifecycle for CronDataSource {
    async fn start(&self, _context: MessageContext) -> RuntimeResult<()> {
        let mut scheduler = self.scheduler.lock().await;
        if scheduler.is_some() {
            return Err(RuntimeError::ResourceAlreadyStarted(self.name.clone()));
        }
        let cancellation = CancellationToken::new();
        *self
            .cancellation
            .lock()
            .expect("cron cancellation lock poisoned") = cancellation.clone();
        let jobs = self
            .jobs
            .lock()
            .expect("cron job registry lock poisoned")
            .clone();
        let mut scheduled = Vec::with_capacity(jobs.len());
        let now = Utc::now();
        for job in jobs {
            scheduled.push(ScheduledJob {
                next: job.next(now)?,
                job,
            });
        }
        *scheduler = Some(tokio::spawn(run_scheduler(scheduled, cancellation)));
        Ok(())
    }

    async fn stop(&self, _context: MessageContext) -> RuntimeResult<()> {
        self.cancellation
            .lock()
            .expect("cron cancellation lock poisoned")
            .cancel();
        if let Some(scheduler) = self.scheduler.lock().await.take() {
            scheduler
                .await
                .map_err(|error| RuntimeError::Transport(error.to_string()))??;
        }
        Ok(())
    }
}

async fn run_scheduler(
    mut scheduled: Vec<ScheduledJob>,
    cancellation: CancellationToken,
) -> RuntimeResult<()> {
    let mut tasks = tokio::task::JoinSet::new();
    loop {
        let Some(next) = scheduled.iter().map(|job| job.next).min() else {
            cancellation.cancelled().await;
            return Ok(());
        };
        let delay = (next - Utc::now()).to_std().unwrap_or_default();
        tokio::select! {
            () = cancellation.cancelled() => {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                return Ok(());
            }
            () = tokio::time::sleep(delay) => {}
            result = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    tracing::error!(error = %error, "cron endpoint task failed");
                }
                continue;
            }
        }

        let now = Utc::now();
        for scheduled_job in &mut scheduled {
            if scheduled_job.next > now {
                continue;
            }
            let mut last_due = scheduled_job.next;
            let mut due_count = 1usize;
            let mut next = scheduled_job.job.next(scheduled_job.next)?;
            while next <= now {
                last_due = next;
                due_count += 1;
                next = scheduled_job.job.next(next)?;
            }
            scheduled_job.next = next;
            if due_count == 1
                || scheduled_job.job.missed_run_policy == ScheduleMissedRunPolicy::FireOnce
            {
                scheduled_job.job.dispatch(last_due, &mut tasks);
            }
        }
    }
}

pub struct CronEndpointConsumer<T, R, E, F>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    F: ScheduleEndpointFunction<T>,
{
    input: InputStream<T, R, E>,
    function: F,
    metrics: DataSourceEndpointMetrics,
}

#[async_trait]
impl<T, R, E, F> Consumer<ScheduleTrigger> for CronEndpointConsumer<T, R, E, F>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    F: ScheduleEndpointFunction<T>,
{
    async fn consume(&self, context: MessageContext, payload: Payload<ScheduleTrigger>) {
        let started = self.metrics.request_start();
        self.function
            .on_trigger(
                context,
                payload.into_value(),
                &self.input.stream().collector(),
            )
            .await;
        self.metrics.request_end(started, true);
    }
}

impl<T, R, E, F> RuntimeEndpointConsumer for CronEndpointConsumer<T, R, E, F>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    F: ScheduleEndpointFunction<T> + 'static,
{
    fn id(&self) -> i32 {
        self.input.endpoint_id()
    }

    fn function_implementation(&self) -> &'static str {
        std::any::type_name::<F>()
    }
}

pub fn make_croner_endpoint_consumer<T, R, E, F>(
    data_source: &Arc<CronDataSource>,
    input: &InputStream<T, R, E>,
    function: F,
) -> RuntimeResult<Arc<CronEndpointConsumer<T, R, E, F>>>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    F: ScheduleEndpointFunction<T> + 'static,
{
    let config = data_source.endpoint_config(input.endpoint_id())?;
    let consumer = Arc::new(CronEndpointConsumer {
        input: input.clone(),
        function,
        metrics: DataSourceEndpointMetrics::from_input(input)?,
    });
    input
        .stream()
        .environment()
        .register_endpoint_consumer(consumer.clone())?;
    if config.enabled {
        let schedule = Cron::from_str(&config.schedule)
            .map_err(|error| RuntimeError::InvalidConfiguration(error.to_string()))?;
        let timezone = Tz::from_str(&config.timezone)
            .map_err(|error| RuntimeError::InvalidConfiguration(error.to_string()))?;
        data_source.add_job(Arc::new(CronJob {
            endpoint_id: config.id,
            endpoint_name: config.name,
            schedule,
            timezone,
            overlap_policy: config.overlap_policy,
            missed_run_policy: config.missed_run_policy,
            consumer: consumer.clone(),
            running: Arc::new(AtomicBool::new(false)),
        }))?;
    }
    Ok(consumer)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn croner_calculates_the_next_utc_occurrence() {
        let job = CronJob {
            endpoint_id: 1,
            endpoint_name: "hourly".to_owned(),
            schedule: Cron::from_str("0 * * * *").expect("valid cron fixture"),
            timezone: Tz::from_str("UTC").expect("valid timezone fixture"),
            overlap_policy: ScheduleOverlapPolicy::Skip,
            missed_run_policy: ScheduleMissedRunPolicy::Skip,
            consumer: Arc::new(NoopConsumer),
            running: Arc::new(AtomicBool::new(false)),
        };
        let after = Utc
            .with_ymd_and_hms(2026, 8, 24, 12, 30, 0)
            .single()
            .expect("valid fixture");
        assert_eq!(
            job.next(after).expect("next occurrence"),
            Utc.with_ymd_and_hms(2026, 8, 24, 13, 0, 0)
                .single()
                .expect("valid fixture")
        );
    }

    struct NoopConsumer;

    #[async_trait]
    impl Consumer<ScheduleTrigger> for NoopConsumer {
        async fn consume(&self, _context: MessageContext, _payload: Payload<ScheduleTrigger>) {}
    }
}

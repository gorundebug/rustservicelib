use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::runtime::{collector::Collector, common::MessageContext};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScheduleBackend {
    Local,
    Temporal,
}

/// Portable input payload emitted by local Cron and Temporal schedules.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleTrigger {
    pub trigger_id: String,
    pub schedule_id: String,
    pub scheduled_at: DateTime<Utc>,
    pub fired_at: DateTime<Utc>,
    pub backend: ScheduleBackend,
}

/// User-defined boundary between a scheduler trigger and the graph input type.
///
/// Cron and Temporal schedule transports create only [`ScheduleTrigger`]. The
/// generated endpoint function decides which values enter the declared input
/// stream and emits them through the typed collector.
#[async_trait]
pub trait ScheduleEndpointFunction<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    async fn on_trigger(
        &self,
        context: MessageContext,
        trigger: ScheduleTrigger,
        out: &Collector<T>,
    );
}

impl ScheduleTrigger {
    pub fn new(
        endpoint_id: i32,
        schedule_id: impl Into<String>,
        scheduled_at: DateTime<Utc>,
        fired_at: DateTime<Utc>,
        backend: ScheduleBackend,
    ) -> Self {
        let schedule_id = schedule_id.into();
        let scheduled = scheduled_at.to_rfc3339_opts(SecondsFormat::AutoSi, true);
        let identity =
            format!("servicegen:schedule-trigger:v1\n{endpoint_id}\n{schedule_id}\n{scheduled}");
        let trigger_id = format!("{:x}", Sha256::digest(identity.as_bytes()));
        Self {
            trigger_id,
            schedule_id,
            scheduled_at,
            fired_at,
            backend,
        }
    }
}

pub fn normalize_temporal_priority(priority: i32) -> u8 {
    match priority {
        ..=-2 => 1,
        -1 => 2,
        0 => 3,
        1 => 4,
        2.. => 5,
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn identity_is_stable_across_retries() {
        let scheduled = Utc
            .with_ymd_and_hms(2026, 8, 24, 12, 30, 0)
            .single()
            .expect("valid fixture")
            + chrono::Duration::microseconds(123_456);
        let first = ScheduleTrigger::new(
            17,
            "hourly",
            scheduled,
            scheduled + chrono::Duration::milliseconds(1),
            ScheduleBackend::Temporal,
        );
        let retry = ScheduleTrigger::new(
            17,
            "hourly",
            scheduled,
            scheduled + chrono::Duration::seconds(1),
            ScheduleBackend::Temporal,
        );
        assert_eq!(first.trigger_id, retry.trigger_id);
        assert_eq!(
            first.trigger_id,
            "29b272e3eeee0c67fe5b5a121f8f39d4b5d9625d656e8a0ec7f2b0f1615e2914"
        );
    }

    #[test]
    fn temporal_priority_is_bounded() {
        assert_eq!(
            [-100, -2, -1, 0, 1, 2, 100].map(normalize_temporal_priority),
            [1, 1, 2, 3, 4, 5, 5]
        );
    }
}

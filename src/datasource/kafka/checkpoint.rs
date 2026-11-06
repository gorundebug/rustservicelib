//! Transport-side checkpoint assignment; no application state or snapshot I/O.
use std::{collections::HashMap, sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}}, time::Duration};
use rdkafka::{ClientContext, Offset, TopicPartitionList,
    consumer::{BaseConsumer, Consumer, ConsumerContext, RebalanceProtocol},
    statistics::Statistics, types::RDKafkaRespErr};
use tokio_util::sync::CancellationToken;
use crate::runtime::{common::MessageContext, telemetry::librdkafka_statistics::LibrdkafkaStatisticsContext};
use super::rdkafka::{CheckpointOffsets, HandlerError, HandlerResult};

pub(super) type Restore = Arc<dyn Fn() -> HandlerResult<Option<CheckpointOffsets>> + Send + Sync>;

pub(super) fn validate(offsets: &Option<CheckpointOffsets>) -> HandlerResult {
    if offsets.as_ref().is_some_and(|values| values.iter().any(|(p, n)| *p < 0 || *n < 0)) {
        return Err("Kafka checkpoint has a negative partition or next offset".into());
    }
    Ok(())
}

pub(super) struct Assignments {
    partitions: Mutex<HashMap<i32, (u64, MessageContext)>>,
    revision: AtomicU64,
    error: Mutex<Option<HandlerError>>,
    pub failed: CancellationToken,
}
impl Assignments {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { partitions: Mutex::new(HashMap::new()), revision: AtomicU64::new(0),
            error: Mutex::new(None), failed: CancellationToken::new() })
    }
    pub fn revision(&self) -> u64 { self.revision.load(Ordering::Acquire) }
    pub fn get(&self, partition: i32) -> Option<(u64, MessageContext)> {
        self.partitions.lock().expect("Kafka assignment lock poisoned").get(&partition).cloned()
    }
    pub fn current(&self, partition: i32, generation: u64) -> bool {
        self.get(partition).is_some_and(|(g, ctx)| g == generation && !ctx.is_cancelled())
    }
    // Serialize ACK with revoke/reassign. A retained old message must not store
    // its offset into the new native assignment between a check and the call.
    pub fn acknowledge<T>(&self, partition: i32, generation: u64, apply: impl FnOnce() -> HandlerResult<T>) -> HandlerResult<T> {
        let states = self.partitions.lock().expect("Kafka assignment lock poisoned");
        if self.failed.is_cancelled() || !states.get(&partition).is_some_and(|(g, ctx)| *g == generation && !ctx.is_cancelled()) {
            return Err("Kafka message belongs to a revoked assignment".into());
        }
        apply()
    }
    pub fn take_error(&self) -> Option<HandlerError> { self.error.lock().expect("Kafka checkpoint error lock poisoned").take() }
    fn fail(&self, error: HandlerError) {
        let mut slot = self.error.lock().expect("Kafka checkpoint error lock poisoned");
        if slot.is_none() { *slot = Some(error); }
        for (_, context) in self.partitions.lock().expect("Kafka assignment lock poisoned").values() { context.cancel(); }
        self.failed.cancel();
    }
}

pub(super) struct CheckpointContext {
    statistics: LibrdkafkaStatisticsContext,
    restore: Restore,
    restoring: bool,
    topic: String,
    owner: MessageContext,
    assignments: Arc<Assignments>,
}
impl CheckpointContext {
    pub fn new(statistics: LibrdkafkaStatisticsContext, restore: Restore, restoring: bool,
        topic: String, owner: MessageContext, assignments: Arc<Assignments>) -> Self {
        Self { statistics, restore, restoring, topic, owner, assignments }
    }
    fn apply(&self, consumer: &BaseConsumer<Self>, event: RDKafkaRespErr, list: &mut TopicPartitionList) -> HandlerResult {
        let cooperative = matches!(consumer.rebalance_protocol(), RebalanceProtocol::Cooperative);
        match event {
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
                if self.owner.is_cancelled() || self.assignments.failed.is_cancelled() { return Err("Kafka checkpoint owner cancelled".into()); }
                let offsets = (self.restore)()?;
                validate(&offsets)?;
                if offsets.is_some() != self.restoring { return Err("Kafka recovery mode changed during lifecycle".into()); }
                // Validate all partitions before any native assignment/offset store.
                for mut item in list.elements() {
                    if item.topic() != self.topic { return Err("Kafka assigned an unexpected topic".into()); }
                    if let Some(offsets) = &offsets {
                        let next = *offsets.get(&item.partition()).ok_or("Kafka checkpoint does not cover assigned partition")?;
                        let (low, high) = consumer.fetch_watermarks(item.topic(), item.partition(), Duration::from_secs(5))?;
                        if low < 0 || next < low || next > high { return Err("Kafka checkpoint is outside retained log bounds".into()); }
                        item.set_offset(Offset::Offset(next))?;
                    }
                }
                if self.owner.is_cancelled() { return Err("Kafka checkpoint restoration cancelled".into()); }
                let mut states = self.assignments.partitions.lock().expect("Kafka assignment lock poisoned");
                if !cooperative {
                    for (_, ctx) in states.values() { ctx.cancel(); }
                    states.clear();
                }
                for item in list.elements() {
                    if let Some((_, ctx)) = states.remove(&item.partition()) { ctx.cancel(); }
                }
                if cooperative { consumer.incremental_assign(list)?; } else { consumer.assign(list)?; }
                let generation = self.assignments.revision.fetch_add(1, Ordering::AcqRel) + 1;
                for item in list.elements() { states.insert(item.partition(), (generation, self.owner.child())); }
            }
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => {
                let mut states = self.assignments.partitions.lock().expect("Kafka assignment lock poisoned");
                if cooperative {
                    for item in list.elements() { if let Some((_, ctx)) = states.remove(&item.partition()) { ctx.cancel(); } }
                    consumer.incremental_unassign(list)?;
                } else {
                    for (_, ctx) in states.values() { ctx.cancel(); }
                    states.clear();
                    consumer.unassign()?;
                }
                self.assignments.revision.fetch_add(1, Ordering::AcqRel);
            }
            _ => return Err(format!("Kafka rebalance failed: {event:?}").into()),
        }
        Ok(())
    }
}
impl ClientContext for CheckpointContext {
    fn stats(&self, statistics: Statistics) { self.statistics.stats(statistics); }
}
impl ConsumerContext for CheckpointContext {
    fn rebalance(&self, consumer: &BaseConsumer<Self>, event: RDKafkaRespErr, list: &mut TopicPartitionList) {
        if let Err(error) = self.apply(consumer, event, list) {
            self.assignments.fail(error);
            let _ = consumer.unassign();
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn checkpoint_is_a_next_offset_and_rejects_kafka_sentinel_values() {
        assert!(validate(&None).is_ok());
        assert!(validate(&Some(CheckpointOffsets::new())).is_ok());
        assert!(validate(&Some([(0, 0), (1, i64::MAX)].into())).is_ok());
        for values in [[(-1, 0)], [(0, -1)], [(0, -2)], [(0, -1001)]] {
            assert!(validate(&Some(values.into())).is_err());
        }
    }

    #[test]
    fn retained_ack_cannot_mutate_a_new_assignment() {
        let state = Assignments::new();
        let owner = MessageContext::new();
        let old = owner.child();
        state.partitions.lock().unwrap().insert(2, (1, old.clone()));
        let applied = AtomicUsize::new(0);
        let apply = || { applied.fetch_add(1, Ordering::SeqCst); Ok(()) };
        state.acknowledge(2, 1, apply).unwrap();
        // Revoke the old owner and give that same partition a new generation.
        {
            let mut partitions = state.partitions.lock().unwrap();
            partitions.remove(&2).unwrap().1.cancel();
            partitions.insert(2, (2, owner.child()));
        }
        assert!(old.is_cancelled());
        assert!(state.acknowledge(2, 1, apply).is_err());
        assert!(!state.current(2, 1));
        state.acknowledge(2, 2, apply).unwrap();
        assert_eq!(applied.load(Ordering::SeqCst), 2);
        owner.cancel();
        assert!(state.acknowledge(2, 2, apply).is_err());
        assert_eq!(applied.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn failed_restore_cancels_admission_and_preserves_original_error() {
        let state = Assignments::new();
        let context = MessageContext::new();
        state.partitions.lock().unwrap().insert(0, (7, context.clone()));
        state.fail("retained history lost".into());
        state.fail("later native cleanup error".into());
        assert!(context.is_cancelled());
        assert!(state.failed.is_cancelled());
        assert!(state.acknowledge(0, 7, || -> HandlerResult { panic!("stale ACK was executed") }).is_err());
        assert_eq!(state.take_error().unwrap().to_string(), "retained history lost");
    }
}

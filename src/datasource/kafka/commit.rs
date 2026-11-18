//! Per-request official librdkafka commit events. No global callback matching,
//! helper reactor, retry queue, or assignment lock held across a broker wait.
use std::{ptr::NonNull, sync::Arc, time::{Duration, Instant}};
use rdkafka::{bindings, consumer::{BaseConsumer, Consumer}, Offset, TopicPartitionList};
use tokio_util::sync::CancellationToken;
use crate::MessageContext;
use super::{checkpoint::{Assignments, CheckpointContext}, rdkafka::HandlerResult};

// Generic endpoint resource bound: concurrent SDK receipt waiters, not records,
// partitions, business concurrency, or a limit on native requests already sent.
pub(super) const MAX_COMMITS: u32 = 64;

fn completion_boundary(context: &MessageContext, cancelled: &CancellationToken, deadline: Instant) -> HandlerResult {
    if context.is_cancelled() || cancelled.is_cancelled() || Instant::now() >= deadline {
        return Err("Kafka commit completed after cancellation or expiry; outcome unknown".into());
    }
    Ok(())
}

async fn await_receipt(
    context: &MessageContext, cancelled: &CancellationToken, deadline: Instant,
    receipt: impl std::future::Future<Output = HandlerResult>,
) -> HandlerResult {
    tokio::select! {
        biased;
        _ = context.cancelled() => Err("Kafka commit wait cancelled; outcome unknown if admitted".into()),
        result = receipt => {
            // A ready receipt can cancel/expire the context in the same poll in
            // which the earlier select branch observed Pending.
            result?;
            completion_boundary(context, cancelled, deadline)
        }
    }
}
struct Queue(NonNull<bindings::rd_kafka_queue_t>);
impl Drop for Queue {
    fn drop(&mut self) { unsafe { bindings::rd_kafka_queue_destroy(self.0.as_ptr()) } }
}
struct Event(NonNull<bindings::rd_kafka_event_t>);
impl Drop for Event {
    fn drop(&mut self) { unsafe { bindings::rd_kafka_event_destroy(self.0.as_ptr()) } }
}
struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop { fn drop(&mut self) { self.0.cancel(); } }

pub(super) async fn commit(
    consumer: Arc<BaseConsumer<CheckpointContext>>, assignments: Arc<Assignments>,
    topic: String, partition: i32, generation: u64, offset: i64,
    context: MessageContext, timeout: Duration,
) -> HandlerResult {
    if !assignments.current(partition, generation) {
        return Err("Kafka message belongs to a revoked assignment".into());
    }
    let permit = assignments.commits.clone().try_acquire_owned()
        .map_err(|_| "Kafka commit capacity exhausted before admission")?;
    let cancelled = CancellationToken::new();
    let _cancel_on_drop = CancelOnDrop(cancelled.clone());
    let deadline = Instant::now() + timeout;
    let work_context = context.clone();
    let completion_cancelled = cancelled.clone();
    let completion_assignments = assignments.clone();
    let task = tokio::task::spawn_blocking(move || -> HandlerResult {
        let _permit = permit;
        let context = work_context;
        if cancelled.is_cancelled() || context.is_cancelled() || Instant::now() >= deadline {
            return Err("Kafka commit cancelled before admission".into());
        }
        // All raw objects are created/consumed/destroyed in this closure. The
        // Arc keeps the client alive; librdkafka retains its own queue reference
        // for an admitted request. There is no callback or Rust opaque pointer.
        let queue = Queue(NonNull::new(unsafe {
            bindings::rd_kafka_queue_new(consumer.client().native_ptr())
        }).ok_or("Kafka commit event queue allocation failed")?);
        let mut offsets = TopicPartitionList::new();
        offsets.add_partition_offset(&topic, partition, Offset::Offset(offset.checked_add(1).ok_or("Kafka offset overflow")?))?;
        assignments.acknowledge(partition, generation, || {
            if cancelled.is_cancelled() || context.is_cancelled() { return Err("Kafka commit cancelled before admission".into()); }
            let status = unsafe { bindings::rd_kafka_commit_queue(
                consumer.client().native_ptr(), offsets.ptr(), queue.0.as_ptr(), None, std::ptr::null_mut(),
            ) };
            if status != bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                return Err(format!("Kafka commit admission failed: {status:?}").into());
            }
            Ok(())
        })?;
        loop {
            if cancelled.is_cancelled() || context.is_cancelled() || Instant::now() >= deadline {
                return Err("Kafka commit wait cancelled or expired after admission; outcome unknown".into());
            }
            // Official event polling, isolated from the consumer's auto-commit
            // callback queue. Never block a Tokio worker or the assignment lock.
            let event = unsafe { bindings::rd_kafka_queue_poll(queue.0.as_ptr(), 25) };
            let Some(event) = NonNull::new(event).map(Event) else { continue };
            if unsafe { bindings::rd_kafka_event_type(event.0.as_ptr()) } != bindings::RD_KAFKA_EVENT_OFFSET_COMMIT as i32 {
                return Err("Kafka unexpected commit event; outcome unknown".into());
            }
            let status = unsafe { bindings::rd_kafka_event_error(event.0.as_ptr()) };
            if status != bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                return Err(format!("Kafka commit failed after admission: {status:?}").into());
            }
            let partitions = unsafe { bindings::rd_kafka_event_topic_partition_list(event.0.as_ptr()) };
            if partitions.is_null() { return Err("Kafka commit result missing offsets; outcome unknown".into()); }
            // The queue is exclusive to this one exact commit. Check partition
            // errors as well as the event-level result before claiming ACK.
            let list = unsafe { &*partitions };
            if list.cnt != 1 { return Err("Kafka commit result offset count mismatch".into()); }
            for index in 0..list.cnt {
                let item = unsafe { &*list.elems.add(index as usize) };
                if item.topic.is_null() || unsafe { std::ffi::CStr::from_ptr(item.topic) }.to_bytes() != topic.as_bytes()
                    || item.partition != partition || item.offset != offset + 1 {
                    return Err("Kafka commit result correlation mismatch".into());
                }
                if item.err != bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                    return Err(format!("Kafka partition commit failed: {:?}", item.err).into());
                }
            }
            completion_boundary(&context, &cancelled, deadline)?;
            if !assignments.current(partition, generation) {
                return Err("Kafka commit completed after assignment revoked; outcome unknown".into());
            }
            return Ok(());
        }
    });
    // Dropping this future requests cancellation; the bounded native waiter
    // keeps its own client/queue/permit until it actually retires.
    await_receipt(&context, &completion_cancelled, deadline, async move {
        task.await.map_err(|e| -> super::rdkafka::HandlerError { Box::new(e) })?
    }).await?;
    if !completion_assignments.current(partition, generation) {
        return Err("Kafka commit completed after assignment revoked; outcome unknown".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn receipt_cancelling_context_in_same_poll_cannot_ack() {
        let context = MessageContext::new();
        let cancelled = CancellationToken::new();
        let result = await_receipt(&context, &cancelled, Instant::now() + Duration::from_secs(1), async {
            context.cancel();
            Ok(())
        }).await;
        assert!(result.unwrap_err().to_string().contains("unknown"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn receipt_expiring_deadline_in_same_poll_cannot_ack() {
        let context = MessageContext::new();
        let cancelled = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_millis(10);
        let result = await_receipt(&context, &cancelled, deadline, async {
            // Deliberately consume this poll's budget, without yielding select
            // another opportunity to observe cancellation first.
            std::thread::sleep(Duration::from_millis(20));
            Ok(())
        }).await;
        assert!(result.unwrap_err().to_string().contains("unknown"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn live_receipt_succeeds_and_native_failure_is_preserved() {
        let context = MessageContext::new();
        let cancelled = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(1);
        await_receipt(&context, &cancelled, deadline, async { Ok(()) }).await.unwrap();
        let error = await_receipt(&context, &cancelled, deadline, async { Err("native failure".into()) }).await.unwrap_err();
        assert_eq!(error.to_string(), "native failure");
        context.cancel();
        assert!(completion_boundary(&context, &cancelled, deadline).is_err());
    }
}

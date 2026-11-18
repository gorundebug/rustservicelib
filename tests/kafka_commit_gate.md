# Rust Kafka commit and checkpoint gate

The source uses the official rdkafka 0.39.0 / librdkafka 2.12.1 APIs without patches. Public DataSource, ConsumerMessage, handler, and checkpoint interfaces are unchanged.

Failed source requests now return an internal outcome to their partition lane.
Begin/consume failure or unfinished cancellation parks only that generation and
fences its retained mark/commit handles. The native pause call is local and occurs
under the assignment lock; no broker wait is added there. Failed pause still
fences admission. Other partitions continue. Recovery requires a fresh assignment
or restart; this does not introduce an automatic application retry policy. Marks
or broker commits already issued before failure cannot be undone. Public
`endpoint_request` retains its existing signature; EndRequest still runs once
after successful BeginRequest, and Done completion wins the established result
cancellation race.

The 2026-11-18 failure gate passes 15 local Kafka lifecycle/assignment/receipt
tests and seven native tests, including synchronous/asynchronous failure in both
ordinary and restored modes. The native case verifies partition 0 stops at offset
2 while partition 1 reaches committed offset 5, and retained failed-generation
commits are rejected. Evidence: `/private/tmp/sa-rust-kafka-failure-block.log` and
`/private/tmp/sa-rust-kafka-failure-native/acceptance.log`. The new gate does not
establish automatic reopening of a parked partition without reassignment.

`ConsumerMessage::commit()` submits the explicit next offset (record offset + 1) through librdkafka's public `rd_kafka_commit_queue` API. Each request owns a native event queue: auto-commit callbacks cannot be mistaken for the explicit commit receipt. Success requires the matching topic, partition, next offset, successful event and partition status, and a still-current assignment generation. `mark_message()` only stores a local offset for existing automatic commit behavior; Done does not ACK. Retained messages from revoked generations cannot admit commits or store offsets into a new assignment.

The assignment mutex covers generation checks and local native admission only. Revocation fences old handles before calling native unassign, outside the mutex; assignment publishes new handles only after native assignment succeeds. Native consumer polling, synchronous checkpoint callbacks, watermark requests, commit receipt polling, and native close run on Tokio's stock blocking pool. There is no custom executor or upstream bridge. Per-partition delivery lanes remain bounded. At most 64 explicit commit waiters are admitted per endpoint; excess calls fail before admission. Polling uses 100 ms consumer waits and 25 ms commit-event waits. The commit wait also uses the existing configured socket/dial timeout (60 seconds by default), with earlier owner cancellation.

Dropping a commit future cancels its waiter, not its already-admitted broker request. After admission, cancellation or timeout is an unknown outcome: an offset may already be committed. There is no adapter replay or automatic retry. The native queue and consumer remain owned until the waiter retires; no Rust callback pointer is handed to librdkafka. Successful Stop joins polling, callback tasks, and commit waiters and closes the native consumer. A Stop deadline returns an error and preserves unfinished lifecycle work for a subsequent Stop; it does not kill a native call. Arbitrary synchronous application checkpoint code must cooperate with its supplied context. Native watermark work can finish at its configured timeout rather than instantly on cancellation.

**Native limitation observed:** librdkafka can defer group reassignment while an OffsetCommit response is withheld. SDK wait cancellation does not force native rebalance progress. The gate demonstrates worker responsiveness and cancellation during receipt loss, then releases the receipt before requiring group convergence and stale-ACK rejection. Immediate rebalance despite lost commit receipts is not supported or claimed.

Run from the workspace root:

```sh
python3 rustservicelib/tests/kafka_commit_gate.py --artifacts /private/tmp/rust-kafka-commit-gate
```

The host script creates and removes only its own Apache Kafka 4.1.2 container, with a protocol-aware proxy in the test container's shared network namespace. It forwards real OffsetCommit requests and selectively withholds their responses. It does not restart a shared broker. Cargo runs offline with two jobs using the existing toolchain/cache volumes. Evidence includes `acceptance.log` and `broker.log`.

The six native cases cover:

- Abandoning a commit wait under receipt loss; another synchronous mark remains responsive and a later explicit commit succeeds.
- A slow real rebalance checkpoint callback while a current-thread Tokio heartbeat progresses.
- Checkpoint next offsets overriding committed group offsets, real group rebalance, and stale handle rejection.
- Lost explicit commit receipts without false success, worker progress, receipt recovery, real rebalance, and correlated ACKs verified against broker offsets.
- Invalid checkpoints producing no delivery or commit.
- Cooperative checkpoint restore cancellation during Stop, worker progress, and unchanged broker offsets.

Three focused checkpoint unit tests additionally validate next-offset values, generation fencing, and preservation of the original restore failure.

Primary API references: [BaseConsumer](https://docs.rs/rdkafka/0.39.0/rdkafka/consumer/struct.BaseConsumer.html), [ConsumerContext](https://docs.rs/rdkafka/0.39.0/rdkafka/consumer/trait.ConsumerContext.html), and [librdkafka public API](https://docs.confluent.io/platform/current/clients/librdkafka/html/rdkafka_8h.html) (`rd_kafka_commit_queue`, native queue/event ownership).

## Completion boundary and ordering audit

Both the blocking receipt path and the async return path recheck cancellation and deadline after a successful native receipt. The async path also rechecks assignment generation before returning success. Biased `select!` alone is insufficient: a receipt can become ready and cancel the owner in the same poll after the cancellation branch returned Pending. Focused unit regressions exercise that interleaving, expiry during the receipt poll, live success, and preservation of native errors. These checks define a completion observation point, not an atomic transaction with a future cancellation or rebalance.

Explicit commits are exact offset operations, **not monotonic high-water marks**. The assignment mutex serializes native admission but does not sort concurrently scheduled calls by record offset. A later admission for an older retained record can move the broker checkpoint backward. This was also possible with concurrent calls to the previous Sync implementation; `spawn_blocking` adds no ordering guarantee. Awaiting handlers sequentially per partition preserves ordinary source delivery order, but applications retaining handles or spawning commit tasks must order their checkpoint commits themselves. Even sequentially committing a newer record and then an older record deliberately requests that regression. Mixing explicit commits with locally marked offsets/automatic commits likewise does not establish monotonicity. A successful receipt confirms that exact request, not that no other request subsequently changes the group offset. Generation fencing prevents stale *assignments*, not older offsets within the same assignment. No offset coalescing, high-water policy, cumulative processing guarantee, or implicit retry is introduced.

`MAX_COMMITS = 64` is a private generic per-endpoint resource bound on concurrently owned SDK receipt waiters (native event queues and blocking-pool jobs). It is not an application record limit, partition count, or processing policy. Capacity exhaustion fails immediately before native admission; callers are not put in an unbounded SDK wait queue. It is intentionally documented rather than represented as a broker guarantee. Cancellation retires the SDK waiter but cannot cancel already-admitted native requests, so this bound does **not** cap all outstanding librdkafka/broker requests across repeated canceled calls. Applications must not interpret cancellation as permission to replay an ambiguous commit. Changing or exposing this tuning value would be a separate configuration/API decision; this fix does not add a hidden application hook.

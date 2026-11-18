# KAFKA-17263 reproductions

[KAFKA-17263](https://issues.apache.org/jira/browse/KAFKA-17263): "Sometimes static KafkaConsumer.close will block the
thread" (classic group protocol, static membership).

When a static member's heartbeat gets `FENCED_INSTANCE_ID`, its heartbeat thread disables itself and parks in an
untimed `wait()` on the coordinator. `AbstractCoordinator.pollHeartbeat` then throws `FencedInstanceIdException` and
drops the thread (`heartbeatThread = null`) without closing it. An application that catches the exception and keeps
polling gets a new heartbeat thread, and the old one stays parked: one leaked thread per fence. All of them wait on
the same monitor, and `close()` wakes the current thread with `notify()`, then `join()`s it with no timeout. If the
current thread is parked too (during a rebalance) and the wake-up goes to a leaked thread, `close()` never returns.

This branch holds only the reproductions, not Kafka's source. The fix and its unit test are on
[`KAFKA-17263-close-fenced-heartbeat-thread`](https://github.com/richdougherty/kafka/tree/KAFKA-17263-close-fenced-heartbeat-thread) (against trunk).

| Folder | |
|---|---|
| [`duplicate-instance-id/`](duplicate-instance-id/) | a real Kafka 4.4.0 broker (one KRaft node on localhost). No fault injection: a second consumer with the same `group.instance.id` gets the first one fenced, and a new member joining starts a rebalance. About 10–15 minutes |
| [`fake-broker/`](fake-broker/) | a real `KafkaConsumer` against a small fake broker in the same file; about 20 s a round, a few minutes for a run |
| [`patches/`](patches/) | `fix.diff` (4.1 and later, trunk), `fix-3.9.1.diff` (the same change on 2.8–4.0), `test.diff` (the unit test, `AbstractCoordinatorTest`) |

Each run builds kafka-clients' published `AbstractCoordinator` source with the fix applied and runs the same case
with and without it. Nothing else in the client is changed, hooked or slowed down. (The harnesses read one private
field, `AbstractCoordinator.heartbeatThread`, to label threads in their output; nothing depends on it.)

## Results

| kafka-clients | Real broker, duplicate `group.instance.id` (10 rounds) | Fake broker (5 rounds) |
|---|---|---|
| 3.9.1 (the newest version in the issue) | `close()` blocks, 10 of 10 | blocks, 5 of 5 |
| 3.9.1 + `fix-3.9.1.diff` | returns, 10 of 10 | returns, 5 of 5 |
| 4.4.0 | blocks, 10 of 10 | blocks, 5 of 5 |
| 4.4.0 + `fix.diff` | returns, 10 of 10 | returns, 5 of 5 |
| trunk (`cabfe19`, built locally) | blocks, 10 of 10 | blocks, 5 of 5 |
| trunk + `fix.diff` | returns, 10 of 10 | returns, 5 of 5 |

"Blocks": still blocked 30 s (real broker) or 15 s (fake broker) after `close(Duration.ofSeconds(5))` was called, in
`Thread.join` ← `AbstractCoordinator.closeHeartbeatThread`. Without the fix, every round also showed the fenced
heartbeat thread still parked before `close()`; with it, none did, and no heartbeat threads were left afterwards.
With the fix, `close()` returns at its 5 s timeout because the rebalance is still in progress (its JoinGroup is
pending); that's the same as without a fence.

Full output: each folder's `example-output-*.txt`.

## Running

```
cd duplicate-instance-id && ./run.sh && CLIENT_VER=4.4.0 ./run.sh
cd fake-broker && ./run.sh && ./run.sh 3.9.1
```

Against trunk (or any local build of kafka-clients):

```
cd /path/to/kafka && ./gradlew clients:jar clients:srcJar
export CLIENTS_JAR=$PWD/clients/build/libs/kafka-clients-4.5.0-SNAPSHOT.jar
export CLIENTS_SOURCES_JAR=$PWD/clients/build/libs/kafka-clients-4.5.0-SNAPSHOT-sources.jar
cd duplicate-instance-id && CLIENT_VER=trunk ./run.sh
cd fake-broker && ./run.sh
```

To run the unit test in a Kafka checkout (trunk, or a 4.1+ tag):

```
git apply /path/to/patches/test.diff
./gradlew clients:test --tests 'org.apache.kafka.clients.consumer.internals.AbstractCoordinatorTest.testPollHeartbeatClosesFencedHeartbeatThread'
git apply /path/to/patches/fix.diff        # then again: it passes
```

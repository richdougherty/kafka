# KAFKA-17734 on a real Kafka cluster with a network partition

`KafkaConsumer.close(Duration.ZERO)` blocking against **three real Apache Kafka brokers** (4.4.0, KRaft, localhost).
The only fault is a network partition between one consumer and every broker.

The partition is the kind Jepsen's nemesis injects, which is how the issue was found: silent, with connections left
open and no resets, so the client just stops hearing back. Jepsen does it with `iptables` rules that drop packets;
here a small TCP proxy in front of each broker stops forwarding. There's no Jepsen code, no random fault schedule and
no history checker: one targeted scenario and a watchdog on `close`.

| File | |
|---|---|
| `run.sh` | starts the cluster, builds the client variants, runs every case, stops the cluster |
| `PartitionProxy.java` | a TCP proxy per broker with a partition switch (the nemesis); reads Kafka frame headers as they pass |
| `CloseDuringPartition.java` | the workload: a consumer group, a producer, a rebalance, a partition, `close(Duration.ZERO)` |
| `../patches/fix.diff` | the proposed fix, for 4.x (`ConsumerCoordinator`) |
| `../patches/fix-3.8.0.diff` | the same change for 3.8.0, whose `close(Timer)` has no `membershipOperation` parameter |
| `example-output-3.8.0.txt`, `example-output-4.4.0.txt`, `example-output-trunk.txt` | `./run.sh`, `CLIENT_VER=4.4.0 ./run.sh`, and trunk `cabfe19` via `CLIENTS_JAR` |

```
./run.sh                       # kafka-clients 3.8.0: default assignors (eager) and cooperative; as published, then with the fix
CLIENT_VER=4.4.0 ./run.sh      # the same with 4.4.0
./run.sh 3                     # 3 rounds per case instead of 10
FIX_DIFF=other.diff ./run.sh   # another change to ConsumerCoordinator in place of ../patches/fix*.diff
CLIENTS_JAR=… CLIENTS_SOURCES_JAR=… CLIENT_VER=trunk ./run.sh   # a local kafka-clients build, e.g. trunk
```

Needs a JDK (17 or later), curl, unzip and patch, and about 300 MB under `WORK` (default `./work`). Downloads the
Kafka 4.4.0 release for the brokers unless `KAFKA_HOME` points at an unpacked one. A case takes 1–2 minutes when
close returns and about 6 when every round hangs (a 30 s watchdog per round), so a full run is about 15–20 minutes.

## How it works

- **Cluster:** three combined broker/controller nodes. Each has an extra listener that it advertises at a proxy
  port, so consumer L reaches every broker only through `PartitionProxy`. Everyone else (consumers S and M, the
  producer, the admin client) connects directly and is never partitioned.
- **Nemesis:** while partitioned, the proxies forward nothing in either direction; bytes wait in socket buffers and
  flow again on heal. The proxy reads only Kafka frame headers (size, API key, correlation id), as a packet capture
  would, to time the partition. Nothing in the client is hooked, patched or slowed down; the "published" runs use
  the released jar.
- **Each round, a new group:**
  1. L joins, then S, while a producer feeds a 6-partition topic.
  2. S's application goes busy (its heartbeat thread keeps it in the group).
  3. M joins, also subscribed to a topic L has never fetched metadata for. L's next poll sends its JoinGroup; the
     coordinator holds it until S rejoins.
  4. L's application goes busy. S rejoins, and the coordinator answers every JoinGroup. When L's response has been
     written to L's socket, the nemesis partitions L from all three brokers.
  5. L's application shuts down with `close(Duration.ZERO)`. If close hasn't returned after 30 s, the watchdog
     records a hang, prints L's stack, heals the partition and times how long close then takes.
- **What's arranged:** the timing is targeted, not random: L's application is busy when its JoinGroup response
  arrives, and the partition comes right after it, i.e. a shutdown during a rebalance while the network is failing.
  Everything after that point is the real client against real brokers.

## Results (2026-11-18; brokers 4.4.0; JDK 21; the example outputs are the latest run of each)

`close(Duration.ZERO)` still blocked after 30 s of partition, out of 10 rounds each:

| kafka-clients | default assignors (eager) | cooperative (`CooperativeStickyAssignor`, auto-commit on) |
|---|---|---|
| 3.8.0 | **10 of 10** | **10 of 10** |
| 3.8.0 + `fix-3.8.0.diff` | 0 of 10 (returns in 0–10 ms) | 0 of 10 (0–10 ms) |
| 4.4.0 | **10 of 10** | **10 of 10** |
| 4.4.0 + `fix.diff` | 0 of 10 (0–13 ms) | 0 of 10 (0–13 ms) |
| trunk (`cabfe19`, built locally) | **10 of 10** | **10 of 10** |
| trunk + `fix.diff` | 0 of 10 (1–5 ms) | 0 of 10 (1–5 ms) |

Ranges combine several runs of each. Every hung close returned within about 140 ms of the partition healing: it blocks for as long as the partition lasts,
and indefinitely if the brokers don't come back.

### Stacks

**Default assignors** (3.8.0 shown; 4.4.0 is the same path): the JoinGroup response is read by the poll in
`maybeLeaveGroup`, and the elected leader waits for metadata with no deadline. The top half is the stack in the
issue, line for line:

```
      at org.apache.kafka.common.network.Selector.select(Selector.java:878)
      at org.apache.kafka.common.network.Selector.poll(Selector.java:469)
      at org.apache.kafka.clients.NetworkClient.poll(NetworkClient.java:595)
      at org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient.poll(ConsumerNetworkClient.java:281)
      at org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient.poll(ConsumerNetworkClient.java:252)
      at org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient.poll(ConsumerNetworkClient.java:243)
      at org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient.awaitMetadataUpdate(ConsumerNetworkClient.java:165)
      at org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient.ensureFreshMetadata(ConsumerNetworkClient.java:176)
      at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.updateGroupSubscription(ConsumerCoordinator.java:557)
      at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.onLeaderElected(ConsumerCoordinator.java:636)
      at org.apache.kafka.clients.consumer.internals.AbstractCoordinator.onLeaderElected(AbstractCoordinator.java:766)
      at org.apache.kafka.clients.consumer.internals.AbstractCoordinator.access$1100(AbstractCoordinator.java:117)
      at org.apache.kafka.clients.consumer.internals.AbstractCoordinator$JoinGroupResponseHandler.handle(AbstractCoordinator.java:670)
      at org.apache.kafka.clients.consumer.internals.AbstractCoordinator$JoinGroupResponseHandler.handle(AbstractCoordinator.java:631)
      at org.apache.kafka.clients.consumer.internals.AbstractCoordinator$CoordinatorResponseHandler.onSuccess(AbstractCoordinator.java:1310)
      at org.apache.kafka.clients.consumer.internals.AbstractCoordinator$CoordinatorResponseHandler.onSuccess(AbstractCoordinator.java:1285)
      at org.apache.kafka.clients.consumer.internals.RequestFuture$1.onSuccess(RequestFuture.java:206)
      at org.apache.kafka.clients.consumer.internals.RequestFuture.fireSuccess(RequestFuture.java:169)
      at org.apache.kafka.clients.consumer.internals.RequestFuture.complete(RequestFuture.java:129)
      at org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient$RequestFutureCompletionHandler.fireCompletion(ConsumerNetworkClient.java:616)
      at org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient.firePendingCompletedRequests(ConsumerNetworkClient.java:428)
      at org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient.poll(ConsumerNetworkClient.java:313)
      at org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient.pollNoWakeup(ConsumerNetworkClient.java:322)
      at org.apache.kafka.clients.consumer.internals.AbstractCoordinator.maybeLeaveGroup(AbstractCoordinator.java:1181)
      at org.apache.kafka.clients.consumer.internals.AbstractCoordinator.close(AbstractCoordinator.java:1131)
      at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.close(ConsumerCoordinator.java:986)
      at org.apache.kafka.clients.consumer.internals.LegacyKafkaConsumer.lambda$close$3(LegacyKafkaConsumer.java:1135)
      at org.apache.kafka.common.utils.Utils.swallow(Utils.java:1042)
      at org.apache.kafka.clients.consumer.internals.LegacyKafkaConsumer.close(LegacyKafkaConsumer.java:1135)
      at org.apache.kafka.clients.consumer.internals.LegacyKafkaConsumer.close(LegacyKafkaConsumer.java:1104)
      at org.apache.kafka.clients.consumer.KafkaConsumer.close(KafkaConsumer.java:1777)
```

**Cooperative with auto-commit** (4.4.0 shown): the same wait, reached one poll earlier, from the auto-commit in
`ConsumerCoordinator.close`. A cooperative member still owns partitions during the rebalance, so it has offsets to
commit:

```
      at org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient.ensureFreshMetadata(ConsumerNetworkClient.java:193)
      at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.updateGroupSubscription(ConsumerCoordinator.java:601)
      at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.onLeaderElected(ConsumerCoordinator.java:680)
      at org.apache.kafka.clients.consumer.internals.AbstractCoordinator.onLeaderElected(AbstractCoordinator.java:775)
      at org.apache.kafka.clients.consumer.internals.AbstractCoordinator$JoinGroupResponseHandler.handle(AbstractCoordinator.java:679)
      ...
      at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.commitOffsetsSync(ConsumerCoordinator.java:1162)
      at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.maybeAutoCommitOffsetsSync(ConsumerCoordinator.java:1189)
      at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.close(ConsumerCoordinator.java:1023)
      at org.apache.kafka.clients.consumer.KafkaConsumer.close(KafkaConsumer.java:1877)
```

Full output, with a stack for each case: the `example-output-*.txt` files.

Not covered: random (untargeted) partition timing, process pauses, broker versions other than 4.4.0.

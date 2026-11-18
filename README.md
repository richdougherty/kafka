# KAFKA-17734 reproductions

[KAFKA-17734](https://issues.apache.org/jira/browse/KAFKA-17734): `KafkaConsumer.close(Duration.ZERO)` can block
indefinitely (classic group protocol). If a poll inside `close()` reads a JoinGroup response that elects this member
leader, `onLeaderElected` → `updateGroupSubscription` waits for fresh metadata with `time.timer(Long.MAX_VALUE)`. When
no broker can answer, close blocks until one can, whatever its timeout.

This branch holds only the reproductions, not Kafka's source. The fix and its unit test are on
[`KAFKA-17734-close-bounded-metadata`](https://github.com/richdougherty/kafka/tree/KAFKA-17734-close-bounded-metadata) (against trunk).

| Folder | |
|---|---|
| [`network-partition/`](network-partition/) | three real Kafka 4.4.0 brokers on localhost, and a silent network partition between one consumer and all of them as the only fault (the fault a Jepsen nemesis injects, done with a TCP proxy rather than `iptables`; no Jepsen code). About 15–20 minutes |
| [`fake-broker/`](fake-broker/) | a real `KafkaConsumer` against a small fake broker in the same file; seconds; 4.x clients |
| [`patches/`](patches/) | `fix.diff` (4.x and trunk), `fix-3.8.0.diff` (the same change on 3.8.0), `test.diff` (the unit test, `ConsumerCoordinatorTest`) |

Each run builds kafka-clients' published `ConsumerCoordinator` source with the fix applied and runs the same case
with and without it. Nothing else in the client is changed, hooked or slowed down.

## Results

`close(Duration.ZERO)` with the default assignors (eager) and with `CooperativeStickyAssignor` + auto-commit:

| kafka-clients | Real cluster, partitioned (10 rounds each) | Fake broker |
|---|---|---|
| 3.8.0 (the issue's version) | blocks 10 of 10, both | (doesn't compile on 3.x) |
| 3.8.0 + `fix-3.8.0.diff` | returns 10 of 10 (0–10 ms), both | |
| 4.4.0 | blocks 10 of 10, both | blocks, both |
| 4.4.0 + `fix.diff` | returns 10 of 10 (0–13 ms), both | returns (~20–35 ms), both |
| trunk (`cabfe19`, built locally) | blocks 10 of 10, both | blocks, both |
| trunk + `fix.diff` | returns 10 of 10 (1–5 ms), both | returns (~30 ms), both |

Each blocked close returned within about 140 ms of the partition healing. Full stacks: each folder's
`example-output*.txt`; the 3.8.0 default-assignor stack matches the one in the issue line for line.

## Running

```
cd network-partition && ./run.sh && CLIENT_VER=4.4.0 ./run.sh
cd fake-broker && ./run.sh
```

Against trunk (or any local build of kafka-clients):

```
cd /path/to/kafka && ./gradlew clients:jar clients:srcJar
export CLIENTS_JAR=$PWD/clients/build/libs/kafka-clients-4.5.0-SNAPSHOT.jar
export CLIENTS_SOURCES_JAR=$PWD/clients/build/libs/kafka-clients-4.5.0-SNAPSHOT-sources.jar
cd network-partition && CLIENT_VER=trunk ./run.sh
cd fake-broker && ./run.sh
```

To run the unit test in a Kafka checkout (trunk, or the `4.4.0` tag):

```
git apply /path/to/patches/test.diff
./gradlew clients:test --tests '*ConsumerCoordinatorTest.testCloseDoesNotBlockOnMetadataWhenElectedLeaderWithAutoCommit'
git apply /path/to/patches/fix.diff        # then again: it passes
```

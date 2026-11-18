# KAFKA-17263 against a real broker: a duplicate group.instance.id

A real Kafka 4.4.0 broker (one KRaft node on localhost) and real kafka-clients, with no fault injection: the
`FENCED_INSTANCE_ID` comes from a second consumer started with the same `group.instance.id` (as in a deploy that
briefly runs two copies of one instance), and the rebalance from a new member joining.

Four consumers in one group, each with its own application thread that polls and, on
`FencedInstanceIdException`, carries on polling:

| | |
|---|---|
| A | static member, `group.instance.id=a`; the one checked |
| A2 | a second consumer with `group.instance.id=a` |
| S | dynamic member; stops polling at step 4, which keeps the rebalance in progress |
| N | dynamic member that joins at step 4 |

One round (`DuplicateInstanceClose`, a fresh JVM and group each time), with what the harness **sets** and what it
**waits for**:

1. **Set:** A and S join; **waited for:** both have an assignment.
2. **Set:** A2 joins. The broker replaces A's member id with A2's, so A's next heartbeat gets `FENCED_INSTANCE_ID`.
   **Waited for:** A's `poll()` throws `FencedInstanceIdException` and A's application catches it, then A rejoins
   (fencing A2 in turn) and has an assignment again. A2 is closed.
3. **Set:** A runs normally for 2 s.
4. **Set:** S stops polling (its heartbeat thread keeps it in the group) and N joins, which starts a rebalance.
   **Waited for:** A's `onPartitionsRevoked` (A is rejoining; its JoinGroup waits at the broker for S).
5. **Set:** A's application polls 500 ms more, then calls `close(Duration.ofSeconds(5))`.

Checked: does A's `close()` return (30 s watchdog), and how many heartbeat threads are left after A, A2, S and N are
closed (a hung A keeps its own). Default consumer settings otherwise (`heartbeat.interval.ms` 3 s); S has
`max.poll.interval.ms` 60 s so the rebalance stays open long enough.

| File | |
|---|---|
| `run.sh` | starts the broker, runs 10 rounds as published and 10 with the fix, stops the broker |
| `DuplicateInstanceClose.java` | one round |
| `example-output-*.txt` | `./run.sh` on 3.9.1, 4.4.0 and trunk (`cabfe19`, via `CLIENTS_JAR`) |

```
./run.sh                          # kafka-clients 3.9.1, the newest version in the issue (fix-3.9.1.diff)
CLIENT_VER=4.4.0 ./run.sh         # another release (fix.diff for 4.1 and later)
CLIENTS_JAR=… CLIENTS_SOURCES_JAR=… CLIENT_VER=trunk ./run.sh   # a local build, e.g. trunk
KAFKA_HOME=/path/to/kafka_2.13-4.4.0 ./run.sh                    # skip the broker download
```

Needs a JDK (17 or later), curl, unzip and patch. Downloads Apache Kafka 4.4.0 for the broker (about 130 MB) unless
`KAFKA_HOME` is set.

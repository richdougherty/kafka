# KAFKA-17263 against a fake broker (quick)

A real `KafkaConsumer` (classic group protocol, `group.instance.id` set, defaults otherwise) against a minimal
one-node broker in the same file, written with kafka-clients' own message classes. The broker answers the first
heartbeat with `FENCED_INSTANCE_ID` (what a real broker sends once another consumer has joined with the same
`group.instance.id`), and later answers one with `REBALANCE_IN_PROGRESS` and holds the JoinGroup response (what a real
broker does while it waits for the group's other members).

The application, in `FencedClose.main`:

1. polls; on `FencedInstanceIdException` it carries on polling (the member rejoins);
2. 2 s after rejoining, the broker starts a rebalance; the application polls until its JoinGroup is sent, then 500 ms
   more;
3. calls `close(Duration.ofSeconds(5))`.

| File | |
|---|---|
| `run.sh` | fetches kafka-clients with Maven, runs `FencedClose` 5 times as published and 5 times with the fix |
| `FencedClose.java` | the fake broker and the application |
| `example-output-*.txt` | `./run.sh 4.4.0`, `./run.sh 3.9.1`, and trunk (`cabfe19`) via `CLIENTS_JAR` |

```
./run.sh                   # kafka-clients 4.4.0
./run.sh 3.9.1             # another release (3.9 and later compile; fix-3.9.1.diff for 2.8–4.0)
ARGS="fences=3" ./run.sh   # 3 fences before the rebalance; fences=0 is the control (no fence: returns, no leak)
FIX_DIFF=other.diff ./run.sh
CLIENTS_JAR=… CLIENTS_SOURCES_JAR=… ./run.sh   # a local kafka-clients build, e.g. trunk (./gradlew clients:jar clients:srcJar)
```

Needs a JDK (17 or later), Maven, unzip and patch. A `close()` still running 15 s after it was called is reported
with its stack. The fixed `close()` returns after its 5 s timeout, because the held JoinGroup is still pending (the
no-fence control does the same).

The output lists the heartbeat threads before `close()`: the consumer's current one, and the ones `pollHeartbeat`
dropped. `RESULT: BUG` means `close()` hung or heartbeat threads were left after it returned.

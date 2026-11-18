# KAFKA-17734 against a fake broker (quick)

The same hang as `../network-partition/`, in seconds: a real `KafkaConsumer` (classic group protocol) against a minimal
one-node broker in the same file, written with kafka-clients' own message classes. The broker holds the rejoin's
JoinGroup response until just before `close(Duration.ZERO)` and then stops answering Metadata, as if partitioned.

| File | |
|---|---|
| `run.sh` | fetches kafka-clients with Maven, runs `CloseZeroBlocks` eager (the default assignors) and cooperative (`CooperativeStickyAssignor`, auto-commit on), as published and with the fix |
| `CloseZeroBlocks.java` | the fake broker and the consumer |
| `example-output.txt` | `./run.sh` (4.4.0) |

```
./run.sh                   # kafka-clients 4.4.0
./run.sh 4.3.2             # another 4.x release (the fake broker uses 4.x message APIs; 3.x doesn't compile)
FIX_DIFF=other.diff ./run.sh
CLIENTS_JAR=… CLIENTS_SOURCES_JAR=… ./run.sh   # a local kafka-clients build, e.g. trunk (./gradlew clients:jar clients:srcJar)
```

Needs a JDK (17 or later), Maven, unzip and patch. A case that hangs is reported after 15 s with the consumer's
stack; the fixed runs return in about 20–35 ms.

Unlike `../network-partition/`, the timing here is exact rather than arranged around real brokers: it shows the code path,
and the cluster reproduction shows it happens with real brokers.

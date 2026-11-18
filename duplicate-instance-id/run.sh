#!/usr/bin/env bash
# KAFKA-17263 against a real Kafka broker (one KRaft node on localhost). No fault injection: two consumers share a
# group.instance.id, so the broker fences one, and a new member joining starts a rebalance. Each round is a fresh JVM
# and a fresh group (DuplicateInstanceClose); kafka-clients as published, then with the fix applied.
#
#   ./run.sh [rounds]                  kafka-clients 3.9.1 (the newest version in the issue), 10 rounds per variant
#   CLIENT_VER=4.4.0 ./run.sh          another kafka-clients release
#   CLIENTS_JAR=... CLIENTS_SOURCES_JAR=... CLIENT_VER=trunk ./run.sh   a local kafka-clients build (e.g. trunk:
#                                      ./gradlew clients:jar clients:srcJar) instead of a release
#   FIX_DIFF=other.diff ./run.sh       another patch to AbstractCoordinator (default ../patches/fix.diff for 4.x and
#                                      trunk, ../patches/fix-3.9.1.diff for 2.8–4.0)
#   JAVA_OPTS for the client JVM
#
# Needs a JDK (17 or later), curl, unzip and patch. Downloads Apache Kafka 4.4.0 (about 130 MB, for the broker)
# unless KAFKA_HOME points at an unpacked release. WORK (default ./work) holds broker data, logs and builds.
# A round takes about 15 s when close returns, about 40 s when it hangs (30 s watchdog).
set -euo pipefail
cd "$(dirname "$0")"
HERE=$PWD
ROUNDS=${1:-10}
VER=4.4.0
WORK=${WORK:-$HERE/work}
CVER=${CLIENT_VER:-3.9.1}
case $CVER in 2.*|3.*|4.0.*) FIX=${FIX_DIFF:-../patches/fix-3.9.1.diff} ;; *) FIX=${FIX_DIFF:-../patches/fix.diff} ;; esac
[ -f "$FIX" ] || { echo "FAIL: $FIX not found" >&2; exit 1; }
FIX=$(cd "$(dirname "$FIX")" && pwd)/$(basename "$FIX")
mkdir -p "$WORK"
if [ -z "${KAFKA_HOME:-}" ]; then
  KAFKA_HOME=$WORK/kafka_2.13-$VER
  if [ ! -d "$KAFKA_HOME" ]; then
    for u in https://downloads.apache.org/kafka https://archive.apache.org/dist/kafka; do
      curl -sSfL --retry 5 --retry-all-errors --retry-delay 10 "$u/$VER/kafka_2.13-$VER.tgz" | tar xz -C "$WORK" && break
    done
  fi
fi
export KAFKA_HEAP_OPTS=${KAFKA_HEAP_OPTS:--Xmx512m -Xms256m}

# ---- the broker: one KRaft node, broker on localhost:19111, controller on 19211
BOOT=localhost:19111
stop_broker() {
  if [ -f "$WORK/node.pid" ]; then
    pid=$(cat "$WORK/node.pid"); kill "$pid" 2>/dev/null || true
    for _ in $(seq 20); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
    kill -9 "$pid" 2>/dev/null || true; rm -f "$WORK/node.pid"
  fi
}
trap stop_broker EXIT
stop_broker
rm -rf "$WORK/node"; mkdir -p "$WORK/node"
cat > "$WORK/node.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:19211
listeners=PLAINTEXT://localhost:19111,CONTROLLER://localhost:19211
advertised.listeners=PLAINTEXT://localhost:19111
controller.listener.names=CONTROLLER
log.dirs=$WORK/node/data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
group.initial.rebalance.delay.ms=0
EOF
"$KAFKA_HOME/bin/kafka-storage.sh" format -t "$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)" -c "$WORK/node.properties" > /dev/null
LOG_DIR="$WORK/node/logs" nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$WORK/node.properties" > "$WORK/node.out" 2>&1 &
echo $! > "$WORK/node.pid"
for _ in $(seq 60); do
  "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server $BOOT > /dev/null 2>&1 && break
  sleep 2
done
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server $BOOT --create --topic t --partitions 4 --replication-factor 1 > /dev/null
echo "broker up (Kafka $VER, $BOOT)"

# ---- the clients: published kafka-clients, and with AbstractCoordinator rebuilt from the published source + the fix
B=$WORK/build; rm -rf "$B"; mkdir -p "$B/repro" "$B/fixsrc" "$B/fix"
M=https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/$CVER
if [ -n "${CLIENTS_JAR:-}" ]; then
  [ -f "$CLIENTS_JAR" ] && [ -f "${CLIENTS_SOURCES_JAR:-}" ] \
    || { echo "FAIL: CLIENTS_JAR and CLIENTS_SOURCES_JAR must both name files" >&2; exit 1; }
  cp "$CLIENTS_SOURCES_JAR" "$B/clients-sources.jar"
else
  curl -sSf --retry 5 --retry-all-errors --retry-delay 10 -o "$B/clients-sources.jar" "$M/kafka-clients-$CVER-sources.jar"
  if [ "$CVER" = "$VER" ]; then
    CLIENTS_JAR=$KAFKA_HOME/libs/kafka-clients-$VER.jar
  else
    CLIENTS_JAR=$B/kafka-clients-$CVER.jar
    curl -sSf --retry 5 --retry-all-errors --retry-delay 10 -o "$CLIENTS_JAR" "$M/kafka-clients-$CVER.jar"
  fi
fi
CP=$CLIENTS_JAR:$(ls "$KAFKA_HOME"/libs/{slf4j-api-,lz4-java-,snappy-java-,zstd-jni-}*.jar | tr '\n' ':')
javac -nowarn -Xlint:none -d "$B/repro" -cp "$CP" DuplicateInstanceClose.java 2>&1 | grep -v '^Note:' || true
[ -f "$B/repro/DuplicateInstanceClose.class" ] || { echo "FAIL: DuplicateInstanceClose.java did not compile" >&2; exit 1; }
SRC=org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java
(cd "$B/fixsrc" && unzip -q "$B/clients-sources.jar" "$SRC")
patch -s -p5 -d "$B/fixsrc" < "$FIX"
javac -nowarn -Xlint:none -d "$B/fix" -cp "$CP" "$B/fixsrc/$SRC" 2>&1 | grep -v '^Note:' || true
[ -f "$B/fix/${SRC%.java}.class" ] || { echo "FAIL: the fixed AbstractCoordinator did not compile" >&2; exit 1; }

for v in stock fix; do
  if [ $v = stock ]; then vcp=$B/repro:$CP; label="stock $CVER"; else vcp=$B/fix:$B/repro:$CP; label="$CVER+fix"; fi
  echo; echo "######## $label, $ROUNDS rounds"
  hung=0; returned=0; leaked=0; failed=0; times=""
  for i in $(seq 1 "$ROUNDS"); do
    echo; echo "## $label, round $i"
    out=$(java ${JAVA_OPTS:-} -cp "$vcp" DuplicateInstanceClose $BOOT "g-$v-$i-$$" 2>&1 | grep -v "^SLF4J:" || true)
    echo "$out"
    r=$(grep '^ROUND ' <<<"$out" || true)
    case $r in
      "ROUND hung"*) hung=$((hung+1)) ;;
      "ROUND returned"*) returned=$((returned+1)); times="$times $(awk '{print $3}' <<<"$r")" ;;
      *) failed=$((failed+1)) ;;
    esac
    if [ -n "$r" ] && [ "$(awk '{print $5}' <<<"$r")" != 0 ]; then leaked=$((leaked+1)); fi
  done
  echo; echo "SUMMARY $label: $ROUNDS rounds, close() hung $hung, returned $returned (ms:$times), heartbeat threads left $leaked, round errors $failed"
done

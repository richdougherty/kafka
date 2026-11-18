#!/usr/bin/env bash
# KAFKA-17734 on a real three-broker Kafka cluster (KRaft, localhost), with a network partition as the only fault.
# The partition is silent, as a Jepsen nemesis's iptables drops are, but done with a TCP proxy; no Jepsen code.
# Starts the cluster, runs CloseDuringPartition with kafka-clients as published and with the fix applied, each with
# the default assignors (eager) and with a cooperative group, then stops the cluster.
#
#   ./run.sh [rounds]                         kafka-clients 3.8.0 (the issue's version), 10 rounds per case
#   CLIENT_VER=4.4.0 ./run.sh                 another kafka-clients release (../patches/fix.diff for 4.x, fix-3.8.0.diff for 3.x)
#   MODES="eager cooperative" ./run.sh        eager = RangeAssignor only; default = the client's default list
#   VARIANTS="stock fix other=some.diff:AbstractCoordinator" ./run.sh   also a client with another patch applied
#   JAVA_OPTS for the client JVM
#   CLIENTS_JAR=... CLIENTS_SOURCES_JAR=... CLIENT_VER=trunk ./run.sh   a local kafka-clients build (e.g. trunk:
#                                             ./gradlew clients:jar clients:srcJar) instead of a release
#
# Needs a JDK (17 or later), curl, unzip and patch. Downloads Apache Kafka 4.4.0 (about 130 MB, for the brokers)
# unless KAFKA_HOME points at an unpacked release. WORK (default ./work) holds broker data, logs and builds.
# A case takes 1-2 minutes when close returns, about 6 when every round hangs (30 s watchdog per round).
set -euo pipefail
cd "$(dirname "$0")"
HERE=$PWD
ROUNDS=${1:-10}
VER=4.4.0
WORK=${WORK:-$HERE/work}
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

# ---- the cluster: node i listens on 1910i (brokers, admin, other clients), 1920i (KRaft controller) and 1930i
# (consumer L), and advertises the last one as localhost:1940i, where L's proxies listen.
stop_cluster() {
  for pid in $(cat "$WORK"/node-*.pid 2>/dev/null); do kill "$pid" 2>/dev/null || true; done
  sleep 3
  for pid in $(cat "$WORK"/node-*.pid 2>/dev/null); do kill -9 "$pid" 2>/dev/null || true; done
  rm -f "$WORK"/node-*.pid
}
trap stop_cluster EXIT
stop_cluster
CID=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
for i in 1 2 3; do
  rm -rf "$WORK/node-$i"; mkdir -p "$WORK/node-$i"
  cat > "$WORK/node-$i.properties" <<EOF
process.roles=broker,controller
node.id=$i
controller.quorum.voters=1@localhost:19201,2@localhost:19202,3@localhost:19203
listeners=INTERNAL://localhost:1910$i,CONTROLLER://localhost:1920$i,PROXIED://localhost:1930$i
advertised.listeners=INTERNAL://localhost:1910$i,PROXIED://localhost:1940$i
listener.security.protocol.map=INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,PROXIED:PLAINTEXT
inter.broker.listener.name=INTERNAL
controller.listener.names=CONTROLLER
log.dirs=$WORK/node-$i/data
num.partitions=6
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
group.initial.rebalance.delay.ms=0
EOF
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CID" -c "$WORK/node-$i.properties" > /dev/null
  LOG_DIR="$WORK/node-$i/logs" nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$WORK/node-$i.properties" \
    > "$WORK/node-$i.out" 2>&1 &
  echo $! > "$WORK/node-$i.pid"
done
for _ in $(seq 60); do
  if "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:19101 > /dev/null 2>&1 \
     && [ "$("$KAFKA_HOME/bin/kafka-metadata-quorum.sh" --bootstrap-server localhost:19101 describe --status 2>/dev/null \
            | grep -c CurrentVoters)" = 1 ]; then break; fi
  sleep 2
done
echo "cluster up: $("$KAFKA_HOME/bin/kafka-metadata-quorum.sh" --bootstrap-server localhost:19101 describe --status | grep CurrentVoters)"

# ---- the client variants: published kafka-clients, and with one class rebuilt from the published source.
CVER=${CLIENT_VER:-3.8.0}
case $CVER in 3.*) FIX=${FIX_DIFF:-../patches/fix-3.8.0.diff} ;; *) FIX=${FIX_DIFF:-../patches/fix.diff} ;; esac
B=$WORK/build; rm -rf "$B"; mkdir -p "$B/stock" "$B/src"
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
CP=$CLIENTS_JAR:$(ls "$KAFKA_HOME"/libs/{slf4j-api-,lz4-java-,snappy-java-,zstd-jni-}*.jar | tr '\n' ':')${EXTRA_CP:-}
javac -nowarn -d "$B/stock" -cp "$CP" PartitionProxy.java CloseDuringPartition.java
PKG=org/apache/kafka/clients/consumer/internals
variant() {  # variant NAME DIFF CLASS
  mkdir -p "$B/$1/src" "$B/$1/cls"
  (cd "$B/$1/src" && unzip -q "$B/clients-sources.jar" "$PKG/$3.java")
  patch -s -p5 -d "$B/$1/src" < "$2"
  javac -nowarn -Xlint:none -d "$B/$1/cls" -cp "$CP" "$B/$1/src/$PKG/$3.java" 2>&1 | grep -v '^Note:' || true
  [ -f "$B/$1/cls/$PKG/$3.class" ] || { echo "FAIL: $1 did not compile" >&2; exit 1; }
}
VARIANTS=${VARIANTS:-stock fix}
for v in $VARIANTS; do
  case $v in
    stock) ;;
    fix) variant fix "$FIX" ConsumerCoordinator ;;
    *=*:*) n=${v%%=*}; rest=${v#*=}; variant "$n" "${rest%%:*}" "${rest##*:}" ;;
    *) echo "unknown variant $v (stock, fix or NAME=DIFF:CLASS)" >&2; exit 1 ;;
  esac
done

for mode in ${MODES:-default cooperative}; do
  for v in $VARIANTS; do
    n=${v%%=*}
    case $n in stock) vcp=$B/stock:$CP; label="stock $CVER" ;; *) vcp=$B/$n/cls:$B/stock:$CP; label="$CVER+$n" ;; esac
    echo; echo "######## $label, $mode group, $ROUNDS rounds"
    t0=$SECONDS
    java ${JAVA_OPTS:-} -cp "$vcp" CloseDuringPartition "$mode" "$ROUNDS" "$label" 2>&1 | grep -v "^SLF4J:"
    echo "($((SECONDS - t0)) s)"
  done
done

#!/usr/bin/env bash
# KAFKA-17263: a static member's KafkaConsumer.close() blocks forever (classic group protocol) after a
# FENCED_INSTANCE_ID heartbeat left an orphaned heartbeat thread behind.
#
# Runs FencedClose (a one-node fake broker and a real KafkaConsumer, both on 127.0.0.1) ROUNDS times on the published
# kafka-clients jar, then again with the fix applied to the published AbstractCoordinator source. Needs a JDK (17 or
# later), Maven, unzip and patch. FIX_DIFF: another patch (default ../patches/fix.diff, or fix-3.9.1.diff for 2.8–4.0).
# ROUNDS: default 5.
#
#   ./run.sh [kafka-clients-version]      default 4.4.0 (3.9.x and later compile)
#   CLIENTS_JAR=... CLIENTS_SOURCES_JAR=... ./run.sh   a local kafka-clients build (e.g. trunk:
#       ./gradlew clients:jar clients:srcJar) in place of the release; its dependencies still come from the release
#   ARGS="fences=3" ./run.sh              arguments for FencedClose
set -euo pipefail
cd "$(dirname "$0")"
VER=${1:-4.4.0}
case $VER in 2.*|3.*|4.0.*) FIX=${FIX_DIFF:-../patches/fix-3.9.1.diff} ;; *) FIX=${FIX_DIFF:-../patches/fix.diff} ;; esac
[ -f "$FIX" ] || { echo "FAIL: $FIX not found" >&2; exit 1; }
ROUNDS=${ROUNDS:-5}
W=$PWD/work/$VER
SRC=org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java

rm -rf "$W"; mkdir -p "$W/libs" "$W/src" "$W/repro" "$W/fixsrc" "$W/fix"
cat > "$W/pom.xml" <<POM
<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>
  <groupId>repro</groupId><artifactId>fenced-close</artifactId><version>1</version>
  <dependencies>
    <dependency><groupId>org.apache.kafka</groupId><artifactId>kafka-clients</artifactId><version>$VER</version></dependency>
    <dependency><groupId>org.slf4j</groupId><artifactId>slf4j-nop</artifactId><version>1.7.36</version></dependency>
  </dependencies></project>
POM
mvn -q -f "$W/pom.xml" dependency:copy-dependencies -DoutputDirectory="$W/libs"
mvn -q -f "$W/pom.xml" dependency:copy -Dartifact="org.apache.kafka:kafka-clients:$VER:jar:sources" -DoutputDirectory="$W/src"
if [ -n "${CLIENTS_JAR:-}" ]; then
  [ -f "$CLIENTS_JAR" ] && [ -f "${CLIENTS_SOURCES_JAR:-}" ] \
    || { echo "FAIL: CLIENTS_JAR and CLIENTS_SOURCES_JAR must both name files" >&2; exit 1; }
  rm -f "$W"/libs/kafka-clients-*.jar "$W"/src/*
  cp "$CLIENTS_JAR" "$W/libs/"
  cp "$CLIENTS_SOURCES_JAR" "$W/src/kafka-clients-local-sources.jar"
  VER="$(basename "$CLIENTS_JAR" .jar | sed s/^kafka-clients-//) (local build)"
fi
CP="$W/libs/*"
javac -nowarn -Xlint:none -d "$W/repro" -cp "$CP" FencedClose.java 2>&1 | grep -v "^Note:" || true
[ -f "$W/repro/FencedClose.class" ] || { echo "FAIL: FencedClose.java did not compile" >&2; exit 1; }

# the fixed AbstractCoordinator, compiled from the published source, goes first on the class path
(cd "$W/fixsrc" && unzip -q "$W"/src/kafka-clients-*-sources.jar "$SRC")
patch -s -p5 -d "$W/fixsrc" < "$FIX"
javac -nowarn -Xlint:none -d "$W/fix" -cp "$CP" "$W/fixsrc/$SRC" 2>&1 | grep -v "^Note:" || true
[ -f "$W/fix/${SRC%.java}.class" ] || { echo "FAIL: the fixed AbstractCoordinator did not compile" >&2; exit 1; }

for build in stock fixed; do
  hung=0; leaked=0; ok=0
  for i in $(seq 1 "$ROUNDS"); do
    echo; echo "######## $build kafka-clients $VER, round $i of $ROUNDS ${ARGS:-}"
    if [ $build = stock ]; then cp="$CP:$W/repro"; else cp="$W/fix:$CP:$W/repro"; fi
    out=$(java -cp "$cp" FencedClose ${ARGS:-} 2>&1 || true)
    echo "$out"
    if grep -q HUNG <<<"$out"; then hung=$((hung+1)); fi
    if grep -q "RESULT: OK" <<<"$out"; then ok=$((ok+1)); fi
    if grep -Eq "alive after close\(\): [1-9]" <<<"$out"; then leaked=$((leaked+1)); fi
  done
  echo; echo "SUMMARY $build kafka-clients $VER: $ROUNDS rounds, close() hung $hung, heartbeat threads left after close() $leaked, OK $ok"
done

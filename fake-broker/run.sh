#!/usr/bin/env bash
# KAFKA-17734: KafkaConsumer.close(Duration.ZERO) blocks forever (classic group protocol) when the rejoin's JoinGroup
# response is read during close() and metadata is stale and unavailable.
#
# Runs CloseZeroBlocks (a one-node fake broker and a real KafkaConsumer, both on 127.0.0.1) on the published
# kafka-clients jar, eager and cooperative, then again with the fix applied to the published ConsumerCoordinator
# source. Needs a JDK (17 or later), Maven, unzip and patch. FIX_DIFF: another patch (default ../patches/fix.diff).
#
#   ./run.sh [kafka-clients-version]      default 4.4.0
#   CLIENTS_JAR=... CLIENTS_SOURCES_JAR=... ./run.sh   a local kafka-clients build (e.g. trunk:
#       ./gradlew clients:jar clients:srcJar) in place of the release; its dependencies still come from the release
set -euo pipefail
cd "$(dirname "$0")"
FIX=${FIX_DIFF:-../patches/fix.diff}
[ -f "$FIX" ] || { echo "FAIL: $FIX not found" >&2; exit 1; }
VER=${1:-4.4.0}
W=$PWD/work/$VER
SRC=org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java

rm -rf "$W"; mkdir -p "$W/libs" "$W/src" "$W/repro" "$W/fixsrc" "$W/fix"
cat > "$W/pom.xml" <<POM
<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>
  <groupId>repro</groupId><artifactId>close-zero</artifactId><version>1</version>
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
javac -nowarn -Xlint:none -d "$W/repro" -cp "$CP" CloseZeroBlocks.java 2>&1 | grep -v "^Note:" || true
[ -f "$W/repro/CloseZeroBlocks.class" ] || { echo "FAIL: CloseZeroBlocks.java did not compile" >&2; exit 1; }

# the fixed ConsumerCoordinator, compiled from the published source, goes first on the class path
(cd "$W/fixsrc" && unzip -q "$W"/src/kafka-clients-*-sources.jar "$SRC")
patch -s -p5 -d "$W/fixsrc" < "$FIX"
javac -nowarn -Xlint:none -d "$W/fix" -cp "$CP" "$W/fixsrc/$SRC" 2>&1 | grep -v "^Note:" || true
[ -f "$W/fix/${SRC%.java}.class" ] || { echo "FAIL: the fixed ConsumerCoordinator did not compile" >&2; exit 1; }

for build in stock fixed; do
  for mode in eager cooperative; do
    echo; echo "######## $build kafka-clients $VER, $mode, close(Duration.ZERO)"
    if [ $build = stock ]; then cp="$CP:$W/repro"; else cp="$W/fix:$CP:$W/repro"; fi
    java -cp "$cp" CloseZeroBlocks "$mode" || true
  done
done

#!/usr/bin/env bash
# Idempotent topic bootstrap (ARCHITECTURE §9.2): existing topics are left untouched, so it is safe to re-run.
# Replication factor 3 everywhere; min.insync.replicas=2 comes from the broker default.
set -euo pipefail

BIN=/opt/kafka/bin
BOOTSTRAP="${BOOTSTRAP:-kafka-1:19092}"
HOUR=3600000
DAY=$((24 * HOUR))

# The brokers' ports open before the controller quorum has elected a leader; wait until metadata requests work.
for attempt in $(seq 1 30); do
  if "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list > /dev/null 2>&1; then break; fi
  echo "waiting for the cluster (attempt $attempt)..."
  sleep 2
done

create() { # create <topic> <partitions> [--config key=value ...]
  local topic=$1 partitions=$2
  shift 2
  "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 3 "$@"
}

# Names use '.' between segments and '-' inside them, never '_': Kafka metric names treat '.' and '_' alike.
# Station status records carry the broker's append time rather than the producer's clock: the streaming job
# measures feed -> snapshot latency against it, and the Windows host's clock can drift from the Docker VM's.
create velograph.gbfs.station-status.v1 6 --config retention.ms=$DAY --config message.timestamp.type=LogAppendTime
create velograph.weather.current.v1     1 --config cleanup.policy=compact --config segment.ms=$HOUR
create velograph.trips.replay.v1        6 --config retention.ms=$((6 * HOUR))
create velograph.trip-metrics.v1        3 --config retention.ms=$((6 * HOUR))
create velograph.predictions.v1         6 --config retention.ms=$DAY
create velograph.alerts.v1              3 --config retention.ms=$((7 * DAY))
create velograph.dlq.v1                 1 --config retention.ms=$((7 * DAY))

echo "topics:"
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list

# After a restart the broker that came back first leads every partition. Hand leadership back to each
# partition's preferred replica, so reads and writes spread over all three brokers again.
"$BIN/kafka-leader-election.sh" --bootstrap-server "$BOOTSTRAP" --election-type preferred \
  --all-topic-partitions > /dev/null 2>&1 || true
echo "preferred leaders elected"

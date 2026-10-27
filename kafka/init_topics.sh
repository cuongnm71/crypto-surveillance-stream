#!/usr/bin/env bash
# ==============================================================================
# Kafka KRaft Topic Initialization Script
# Creates core topics with retention bounds according to implementation_plan.md
# ==============================================================================
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"

echo "[INFO] Waiting for Kafka broker at ${BOOTSTRAP_SERVER}..."
until /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" --list > /dev/null 2>&1; do
    echo "[INFO] Kafka is not ready yet, waiting 2 seconds..."
    sleep 2
done

echo "[INFO] Kafka broker is ready. Initializing core topics..."

# Format: TOPIC_NAME:PARTITIONS:RETENTION_MS:RETENTION_BYTES
TOPIC_SPECS=(
    "md.trades:6:86400000:1288490188"
    "md.tickers:3:21600000:157286400"
    "md.liquidations:3:604800000:104857600"
    "candles.1m:3:604800000:104857600"
    "alerts.events:3:1209600000:52428800"
    "dlq:1:1209600000:104857600"
)

for SPEC in "${TOPIC_SPECS[@]}"; do
    IFS=":" read -r TOPIC PARTITIONS RETENTION_MS RETENTION_BYTES <<< "${SPEC}"
    echo "[INFO] Initializing topic ${TOPIC} (partitions=${PARTITIONS}, retention.ms=${RETENTION_MS}, retention.bytes=${RETENTION_BYTES})..."
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --create --if-not-exists \
        --topic "${TOPIC}" \
        --partitions "${PARTITIONS}" \
        --replication-factor 1 \
        --config retention.ms="${RETENTION_MS}" \
        --config retention.bytes="${RETENTION_BYTES}" \
        --config cleanup.policy=delete
done

echo "[INFO] All core Kafka topics successfully created. Topic summary:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" --list

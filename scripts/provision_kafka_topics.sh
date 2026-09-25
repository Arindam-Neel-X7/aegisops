#!/usr/bin/env bash
set -euo pipefail

CANONICAL_TOPICS=(
    "aegis.telemetry.metrics"
    "aegis.telemetry.logs"
    "aegis.system.events"
    "aegis.ml.anomalies"
    "aegis.incidents"
    "aegis.agent.events"
)

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
CONTAINER_NAME="aegisops-kafka"

echo "[AegisOps Kafka] Starting explicit topic provisioning..."

if ! docker ps --filter "name=${CONTAINER_NAME}" --filter "status=running" -q | grep -q .; then
    echo "[AegisOps Kafka] [ERROR] Container ${CONTAINER_NAME} is not running. Start with: docker compose --profile messaging up -d" >&2
    exit 1
fi

for topic in "${CANONICAL_TOPICS[@]}"; do
    echo "[AegisOps Kafka] Provisioning topic: ${topic}"
    docker exec "${CONTAINER_NAME}" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --create --if-not-exists \
        --topic "${topic}" \
        --partitions 1 \
        --replication-factor 1 \
        --config message.timestamp.type=CreateTime
done

echo "[AegisOps Kafka] Validating topic inventory..."
EXISTING_TOPICS=$(docker exec "${CONTAINER_NAME}" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" --list)

for topic in "${CANONICAL_TOPICS[@]}"; do
    if ! echo "${EXISTING_TOPICS}" | grep -qx "${topic}"; then
        echo "[AegisOps Kafka] [ERROR] Validation failed: topic ${topic} not found" >&2
        exit 1
    fi
done

echo "[AegisOps Kafka] [OK] All 6 canonical topics provisioned and verified."

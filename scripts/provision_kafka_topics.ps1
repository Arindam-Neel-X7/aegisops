Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$CanonicalTopics = @(
    "aegis.telemetry.metrics",
    "aegis.telemetry.logs",
    "aegis.system.events",
    "aegis.ml.anomalies",
    "aegis.incidents",
    "aegis.agent.events"
)

Write-Host "[AegisOps Kafka] Starting explicit topic provisioning..."

$BootstrapServer = if ($env:KAFKA_BOOTSTRAP_SERVERS) { $env:KAFKA_BOOTSTRAP_SERVERS } else { "localhost:9092" }
$ContainerName = "aegisops-kafka"

# Verify Kafka container is running
$ContainerRunning = docker ps --filter "name=$ContainerName" --filter "status=running" -q
if (-not $ContainerRunning) {
    Write-Error "[AegisOps Kafka] Container $ContainerName is not running. Start it with: docker compose --profile messaging up -d"
    exit 1
}

foreach ($topic in $CanonicalTopics) {
    Write-Host "[AegisOps Kafka] Provisioning topic: $topic"
    docker exec $ContainerName /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BootstrapServer --create --if-not-exists --topic $topic --partitions 1 --replication-factor 1 --config message.timestamp.type=CreateTime
    if ($LASTEXITCODE -ne 0) {
        Write-Error "[AegisOps Kafka] Failed to provision topic $topic"
        exit 1
    }
}

Write-Host "[AegisOps Kafka] Validating topic inventory..."
$ExistingTopics = docker exec $ContainerName /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BootstrapServer --list
$TopicLines = $ExistingTopics -split "`r?`n"

foreach ($topic in $CanonicalTopics) {
    if ($TopicLines -notcontains $topic) {
        Write-Error "[AegisOps Kafka] Validation failed: topic $topic was not found in Kafka"
        exit 1
    }
}

Write-Host "[AegisOps Kafka] [OK] All 6 canonical topics provisioned and verified."

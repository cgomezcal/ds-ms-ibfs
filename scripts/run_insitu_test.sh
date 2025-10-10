#!/usr/bin/env bash
set -euo pipefail

# run_insitu_test.sh
# Orquesta el entorno completo con Docker Compose, envía una transacción por Kafka
# y muestra la respuesta obtenida en el topic de respuesta.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

DATA_PAYLOAD=${DATA_PAYLOAD:-"hola-insitu"}
REQUEST_KEY=${REQUEST_KEY:-"insitu-$(date +%s)"}
KAFKA_BROKERS_HOST="localhost:9092"
REQUEST_TOPIC=${KAFKA_REQUEST_TOPIC:-"execute_transaction"}
RESPONSE_TOPIC=${KAFKA_RESPONSE_TOPIC:-"execute_transaction_response"}

log() {
  printf '[INSITU] %s\n' "$*"
}

fail() {
  printf '[INSITU][ERROR] %s\n' "$*" >&2
  exit 1
}

ensure_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    fail "Dependencia requerida no encontrada: $1"
  fi
}

ensure_cmd docker
ensure_cmd jq
ensure_cmd go
ensure_cmd curl

if docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(docker-compose)
else
  fail "Se requiere docker compose (plugin v2 o binario docker-compose)"
fi

compose() {
  "${COMPOSE_CMD[@]}" "$@"
}

wait_for_service() {
  local service=$1
  local attempts=${2:-60}
  for ((i=1; i<=attempts; i++)); do
    local state
    state=$(compose ps --format json "$service" 2>/dev/null | jq -r '.State // empty' 2>/dev/null) || state=""
    if [[ "$state" == "running" ]]; then
      return 0
    fi
    sleep 2
  done
  fail "El servicio $service no alcanzó el estado 'running' a tiempo"
}

wait_for_kafka() {
  for ((i=1; i<=30; i++)); do
    if compose exec kafka bash -lc 'kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1'; then
      return 0
    fi
    sleep 2
  done
  fail "Kafka no respondió tras el arranque"
}

wait_for_http() {
  local url=$1
  local attempts=${2:-30}
  for ((i=1; i<=attempts; i++)); do
    if curl -sf "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  fail "El endpoint $url no respondió satisfactoriamente"
}

log "Deteniendo cualquier stack previo"
compose down --remove-orphans --timeout 10 >/dev/null 2>&1 || true

log "Arrancando Zookeeper y Kafka"
compose up -d zookeeper kafka
wait_for_service zookeeper
wait_for_service kafka
wait_for_kafka

for topic in "$REQUEST_TOPIC" "$RESPONSE_TOPIC"; do
  log "Asegurando tópico $topic"
  compose exec kafka bash -lc "kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic '$topic' --replication-factor 1 --partitions 1" >/dev/null 2>&1 || true
done
sleep 2

declare -A MTM_PORTS=(
  [mtm1]=8081
  [mtm2]=8082
  [mtm3]=8083
  [mtm4]=8084
)

for mtm in mtm1 mtm2 mtm3 mtm4; do
  log "Arrancando $mtm"
  compose up -d "$mtm"
  wait_for_service "$mtm"
done

declare -A NODE_PORTS=(
  [nodea]=8091
  [nodeb]=8092
  [nodec]=8093
  [noded]=8094
)

for node in nodea nodeb nodec noded; do
  log "Arrancando $node"
  compose up -d "$node"
  wait_for_service "$node"
  wait_for_http "http://localhost:${NODE_PORTS[$node]}/healthz" 40
done

log "Arrancando kafka-ibft"
compose up -d kafka_ibft
wait_for_service kafka_ibft
sleep 2

log "Iniciando envío de transacción Kafka"
go run ./cmd/kafka_client --brokers "$KAFKA_BROKERS_HOST" --request-topic "$REQUEST_TOPIC" \
  --response-topic "$RESPONSE_TOPIC" --data "$DATA_PAYLOAD" --key "$REQUEST_KEY" --type "contract-call" --timeout 60s

log "Prueba completada"

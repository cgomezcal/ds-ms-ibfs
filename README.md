# ds-ms-ibfs

A minimal distributed microservice demonstrating a simplified IBFT-style consensus across HTTP nodes, written in Go. This is a demo/learning implementation (not production IBFT), with a compact engine, an HTTP node, unit tests, and an integration test that spins up a 4-node in-process cluster.

## Features

- Simplified IBFT engine (PrePrepare -> Prepare -> Commit) with configurable validators
- Ed25519 signatures with a deterministic demo keystore (not secure for production)
- HTTP-based peer broadcasting and JSON APIs
- In-process integration test for 4-node consensus
- CLI to run nodes and configure peers/validators
- Health and state endpoints for observability
- Kafka bridge for transaction execution requests and responses

## Project layout

- `cmd/node`: CLI to start a node
- `internal/ibft`: IBFT engine (consensus logic, Ed25519 signing/verification, demo keystore)
- `internal/server/node.go`: HTTP node implementation (message handling, propose, state)
- `internal/server/node_integration_test.go`: 4-node cluster test
- `Dockerfile`, `docker-compose.yml`: Containerized 4-node demo
- `.github/workflows/ci.yml`: Build and test workflow

## Endpoints

- `GET /healthz` → "ok"
- `POST /v1/ibft/propose` → `{ "value": "<any-string>" }` returns PrePrepare
- `POST /v1/ibft/message` → for peer-to-peer IBFT messages (internal)
- `GET /v1/ibft/state` → `{ id, round, state, value }`

## Run a local 4-node demo

Below is the exact configuration used in our test run. You can reuse it locally.

- Node A: `--id A --addr :18081 --peers http://localhost:18082,http://localhost:18083,http://localhost:18084 --validators A,B,C,D`
- Node B: `--id B --addr :18082 --peers http://localhost:18081,http://localhost:18083,http://localhost:18084 --validators A,B,C,D`
- Node C: `--id C --addr :18083 --peers http://localhost:18081,http://localhost:18082,http://localhost:18084 --validators A,B,C,D`
- Node D: `--id D --addr :18084 --peers http://localhost:18081,http://localhost:18082,http://localhost:18083 --validators A,B,C,D`

Once nodes are running, propose a value on A:

- `POST http://localhost:18081/v1/ibft/propose` with body `{ "value": "block-1" }`

Then check state on all nodes:

- `GET http://localhost:18081/v1/ibft/state`
- `GET http://localhost:18082/v1/ibft/state`
- `GET http://localhost:18083/v1/ibft/state`
- `GET http://localhost:18084/v1/ibft/state`

You should see `state: committed` and the same `value` hash on all nodes.

### Using Docker Compose

You can also spin up a 4-node cluster with Docker:

1) Build and start the cluster:

- `docker compose up --build -d`

2) Propose a value to node A:

- `curl -sS -X POST localhost:18081/v1/ibft/propose -H 'Content-Type: application/json' -d '{"value":"block-1"}'`

3) Inspect state on each node:

- `curl -sS localhost:18081/v1/ibft/state | jq .`
- `curl -sS localhost:18082/v1/ibft/state | jq .`
- `curl -sS localhost:18083/v1/ibft/state | jq .`
- `curl -sS localhost:18084/v1/ibft/state | jq .`

## Kafka integration

The node can emit BESU transaction results to Kafka and a companion CLI can drive execution from Kafka topics.

### Node configuration

Set the following environment variables before starting `cmd/node`:

- `KAFKA_BROKERS`: comma-separated broker list (e.g. `localhost:9092`)
- `KAFKA_RESPONSE_TOPIC` (optional): topic for publishing IBFT transaction results. Defaults to `execute_transaction_response`.

When consensus finalizes a transaction and the leader submits it to Besu, the node publica una traza detallada en `execution_flow` junto con el resultado. Cada servicio (cliente Kafka, bridge, nodo IBFT, publicación Kafka) añade un paso, de modo que puedas reconstruir el recorrido completo del mensaje.

> **Schema overview**
>
> - Execute request topic (`execute_transaction`):
>   ```json
>   {
>     "data": "<hex-or-plain-payload>",
>     "key": "<uuid-or-custom-id>",
>     "timestamp": "2025-10-07T10:21:30.123456Z",
>     "type": "<logical-transaction-type>",
>     "execution_flow": [
>       {
>         "component": "kafka_client",
>         "instance_id": "cli-1",
>         "role": "request_published",
>         "timestamp": "2025-10-07T10:21:30.123456Z"
>       }
>     ]
>   }
>   ```
>   `data` y `key` son obligatorios. El campo `timestamp` se rellena automáticamente en milisegundos UTC y `type` es opcional. `execution_flow` es opcional: si está presente, cada componente adicional hará `append` de un nuevo paso preservando los previos.
> - Execute response topic (`execute_transaction_response`):
>   ```json
>   {
>     "node_id": "A",
>     "data": "<original-data>",
>     "proposal_hash": "<ibft-hash>",
>     "key": "<matching-key>",
>     "status": "success|error",
>     "tx_hash": "<besu-tx-hash>",
>     "error": "<optional-error>",
>     "timestamp": "2025-10-07T10:21:30.123456Z",
>     "type": "<logical-transaction-type>",
>     "execution_flow": [
>       {
>         "component": "kafka_client",
>         "role": "request_published",
>         "timestamp": "2025-10-07T10:21:30.123456Z"
>       },
>       {
>         "component": "kafka_ibft",
>         "role": "request_forward",
>         "timestamp": "2025-10-07T10:21:31.000000Z"
>       },
>       {
>         "component": "ibft_node",
>         "role": "ibft_propose",
>         "metadata": {"proposal_hash": "..."},
>         "timestamp": "2025-10-07T10:21:32.250000Z"
>       }
>     ]
>   }
>   ```
>   Messages are emitted with Kafka key = `key` when available.

### Kafka bridge CLI

`cmd/kafka_ibft` escucha el tópico de peticiones, añade su propio paso a `execution_flow`, reenvía la carga al endpoint IBFT y muestra las respuestas (incluyendo el número de etapas registradas).

Published messages must include a `key`:

Environment variables:

- `KAFKA_BROKERS` (required)
- `KAFKA_REQUEST_TOPIC` (default `execute_transaction`)
- `KAFKA_RESPONSE_TOPIC` (default `execute_transaction_response`)
- `KAFKA_REQUEST_GROUP_ID` / `KAFKA_RESPONSE_GROUP_ID` (optional consumer group names)
- `IBFT_EXECUTE_URL` (default `http://localhost:8080/v1/tx/execute-transaction`)

Run the bridge:

```bash
go run ./cmd/kafka_ibft
```

Publish a test message for execution (from another terminal):

```bash
kafka-console-producer --broker-list localhost:9092 --topic execute_transaction <<'EOF'
{"data":"hello from kafka","key":"demo-001"}
EOF
```

Verás cómo el bridge reenvía la petición, enriquece `execution_flow` y muestra la respuesta final con el número total de pasos registrados.

### Kafka transaction client (`cmd/kafka_client`)

Este binario crea la petición inicial, añade el primer paso de `execution_flow` (`request_published`) y espera al mensaje correlacionado en la cola de respuestas:

```bash
go run ./cmd/kafka_client \
	--brokers localhost:9092 \
	--data 'hola-ibft' \
	--request-topic execute_transaction \
	--response-topic execute_transaction_response
```

Flags of interest:

- `--data`: payload to execute (required)
- `--key`: optional custom identifier (defaults to a UUID)
- `--request-id`: deprecated alias for `--key`
- `--timeout`: wait duration for the response (default 30s)

The command prints the JSON response (including `key`, `status`, and `tx_hash`) and exits with a non-zero status if the response times out or the operation is cancelled.

### Docker Compose with Confluent Kafka

The provided `docker-compose.yml` spins up Confluent ZooKeeper/Kafka alongside the four IBFT nodes and the Kafka bridge.

1. Build and launch the stack:

	```bash
	docker compose up -d --build zookeeper kafka nodea nodeb nodec noded kafka_ibft
	```

 The Kafka broker listens on `localhost:9092`, while internal services point to `kafka:29092`.

2. Publish a transaction payload:

	```bash
	docker exec -i ds-ms-ibfs-kafka-1 kafka-console-producer --bootstrap-server localhost:9092 --topic execute_transaction <<'EOF'
	{"data":"hola desde compose","key":"compose-demo-1"}
	EOF
	```

3. Follow the bridge output:

	```bash
	docker logs -f ds-ms-ibfs-kafka_ibft-1
	```

4. Tear everything down when finished:

	```bash
	docker compose down
	```

### Escenario 4×4 con la misma clave privada

Para reproducir el entorno solicitado (4 nodos MTM y 4 nodos IBFT compartiendo la misma clave Ethereum) se incluye el overlay `docker-compose.samekey.yml`. Lánzalo junto al descriptor principal:

```bash
docker compose -f docker-compose.yml -f docker-compose.samekey.yml up -d \
  zookeeper kafka mtm1 mtm2 mtm3 mtm4 nodea nodeb nodec noded kafka_ibft
```

Todos los servicios (MTM e IBFT) utilizan la clave `0xb71c7…f2f0`. El bridge `kafka_ibft` y los nodos IBFT registran pasos detallados (`execute request received`, `collect entry accepted`, `collect quorum progress`, etc.) dentro del `execution_flow`, de modo que el recorrido queda trazado extremo a extremo.

Para observar la ejecución en vivo:

1. Publica una solicitud mediante `cmd/kafka_client` o `kafka-console-producer` (ver secciones anteriores).
2. Sigue los logs correlacionados:

```bash
docker compose logs -f nodea nodeb nodec noded kafka_ibft | grep -E "collect|execution"
```

### Validación automatizada

La suite automatizada cubre el caso de clave compartida:

```bash
go test ./...
```

Los tests `TestTxAggregationSameWalletParticipants` y `TestExecuteTransaction_FourNodesSameKey` validan que el líder cuenta cada participante aunque firme con la misma wallet, alcanza el umbral y registra la transacción completa en el estado del nodo.

### Prueba in situ rápida

Para lanzar una verificación rápida del flujo directo sin montar toda la infraestructura de Docker puedes usar el script `scripts/run_insitu_test.sh`, que levanta el MTM stub, inicia un nodo líder local y realiza una llamada real a `/v1/tx/execute-transaction`. **Por defecto apunta al Besu público `http://besu-dev.sirt-xfsc.click:8545`**, pero puedes sobreescribir la URL exportando `BESU_RPC_URL`:

```bash
./scripts/run_insitu_test.sh
```

El proceso muestra por consola el código HTTP devuelto y los logs relevantes del nodo y del stub. Puedes conservar los logs exportando `KEEP_LOGS=1` o sobreescribir puertos y payload mediante las variables `MTM_PORT`, `NODE_PORT`, `DATA_PAYLOAD`, `REQUEST_KEY` y `BESU_RPC_URL`.

Si prefieres una invocación más corta, el `Makefile` incluye los objetivos:

```bash
make insitu-test
make insitu-test-keep-logs
```

> Consejo: el script comprueba `web3_clientVersion` antes de arrancar el nodo IBFT, de forma que fallará inmediatamente si la URL (por defecto o personalizada) no responde.

## Notes and limitations

- This is a simplified IBFT-like flow for demonstration only. It is not a production IBFT implementation.
- Signatures use Ed25519 with a deterministic keystore derived from validator IDs. This is for demo/testing only and is not secure.
- Networking and retries are basic; there is no round-change, proposer selection, or persistence.

## Next steps (hardening)

- Real key management: secure key storage and non-deterministic keys.
- Proposer rotation and round-change with timeouts.
- Persistent WAL/state across restarts and multiple rounds/blocks.
- Security: auth between peers, allowlist, rate limiting.
- Structured logging and tracing.

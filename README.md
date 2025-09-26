# ds-ms-ibfs

A minimal distributed microservice demonstrating a simplified IBFT-style consensus across HTTP nodes, written in Go. This is a demo/learning implementation (not production IBFT), with a compact engine, an HTTP node, unit tests, and an integration test that spins up a 4-node in-process cluster.

## Features

- Simplified IBFT engine (PrePrepare -> Prepare -> Commit) with configurable validators
- Ed25519 signatures with a deterministic demo keystore (not secure for production)
- HTTP-based peer broadcasting and JSON APIs
- In-process integration test for 4-node consensus
- CLI to run nodes and configure peers/validators
- Health and state endpoints for observability

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

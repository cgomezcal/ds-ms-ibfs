# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS build
WORKDIR /app
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -o /out/node ./cmd/node && \
    go build -o /out/kafka_ibft ./cmd/kafka_ibft

FROM alpine:3.19
WORKDIR /srv
COPY --from=build /out/node /usr/local/bin/node
COPY --from=build /out/kafka_ibft /usr/local/bin/kafka_ibft
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/node"]

package main

import (
    "bytes"
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "log/slog"
    "net/http"
    "os"
    "os/signal"
    "strconv"
    "strings"
    "sync"
    "syscall"
    "time"

    kafkautil "github.com/cgomezcal/ds-ms-ibfs/internal/kafka"
    "github.com/cgomezcal/ds-ms-ibfs/pkg/protocol"
    kafka "github.com/segmentio/kafka-go"
)

type executePayload struct {
    Data          string                   `json:"data"`
    Key           string                   `json:"key"`
    ExecutionFlow []protocol.ExecutionStep `json:"execution_flow"`
}

type responsePayload struct {
    NodeID        string                    `json:"node_id"`
    Data          string                    `json:"data"`
    ProposalHash  string                    `json:"proposal_hash"`
    Key           string                    `json:"key"`
    Status        string                    `json:"status"`
    TxHash        string                    `json:"tx_hash"`
    Error         string                    `json:"error"`
    Timestamp     string                    `json:"timestamp"`
    ExecutionFlow *protocol.ExecutionFlow   `json:"execution_flow"`
}

func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer cancel()

    logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

    brokersEnv := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
    if brokersEnv == "" {
        logger.Error("KAFKA_BROKERS env var is required")
        os.Exit(1)
    }
    brokerParts := strings.Split(brokersEnv, ",")
    brokers := make([]string, 0, len(brokerParts))
    for _, b := range brokerParts {
        if v := strings.TrimSpace(b); v != "" {
            brokers = append(brokers, v)
        }
    }
    if len(brokers) == 0 {
        logger.Error("no valid Kafka brokers provided")
        os.Exit(1)
    }

    requestTopic := strings.TrimSpace(os.Getenv("KAFKA_REQUEST_TOPIC"))
    if requestTopic == "" {
        requestTopic = "execute_transaction"
    }
    responseTopic := strings.TrimSpace(os.Getenv("KAFKA_RESPONSE_TOPIC"))
    if responseTopic == "" {
        responseTopic = "execute_transaction_response"
    }
    requestGroup := strings.TrimSpace(os.Getenv("KAFKA_REQUEST_GROUP_ID"))
    if requestGroup == "" {
        requestGroup = "kafka-ibft-executor"
    }
    responseGroup := strings.TrimSpace(os.Getenv("KAFKA_RESPONSE_GROUP_ID"))
    if responseGroup == "" {
        responseGroup = "kafka-ibft-listener"
    }

    executeURL := strings.TrimSpace(os.Getenv("IBFT_EXECUTE_URL"))
    if executeURL == "" {
        executeURL = "http://localhost:8080/v1/tx/execute-transaction"
    }

    httpClient := &http.Client{Timeout: 5 * time.Second}

    execHandler := func(ctx context.Context, msg kafka.Message) error {
        var payload executePayload
        if err := json.Unmarshal(msg.Value, &payload); err != nil {
            logger.Warn("invalid execute payload", "err", err, "topic", msg.Topic, "offset", msg.Offset)
            return nil
        }
        if strings.TrimSpace(payload.Data) == "" {
            logger.Warn("missing data in execute payload", "topic", msg.Topic, "offset", msg.Offset)
            return nil
        }
        if strings.TrimSpace(payload.Key) == "" {
            logger.Warn("missing key in execute payload", "topic", msg.Topic, "offset", msg.Offset)
            return nil
        }

        flow := protocol.CloneExecutionFlow(payload.ExecutionFlow)
        meta := map[string]string{
            "topic":     msg.Topic,
            "partition": strconv.Itoa(msg.Partition),
        }
        flow = append(flow, protocol.NewExecutionStep("kafka_ibft", "", "request_forward", meta))
        payload.ExecutionFlow = flow

        body, err := json.Marshal(payload)
        if err != nil {
            logger.Error("failed to marshal enriched execute payload", "err", err)
            return nil
        }

        req, err := http.NewRequestWithContext(ctx, http.MethodPost, executeURL, bytes.NewReader(body))
        if err != nil {
            logger.Error("failed to build execute request", "err", err)
            return nil
        }
        req.Header.Set("Content-Type", "application/json")
        resp, err := httpClient.Do(req)
        if err != nil {
            logger.Error("execute request failed", "err", err)
            return nil
        }
        defer resp.Body.Close()
        io.Copy(io.Discard, resp.Body)
        if resp.StatusCode >= 300 {
            logger.Warn("execute request returned non-success", "status", resp.StatusCode)
            return nil
        }
        logger.Info("execute request forwarded", "topic", msg.Topic, "offset", msg.Offset, "status", resp.StatusCode, "key", payload.Key)
        return nil
    }

    respHandler := func(ctx context.Context, msg kafka.Message) error {
        var payload responsePayload
        if err := json.Unmarshal(msg.Value, &payload); err != nil {
            fmt.Printf("[execute_transaction_response] raw=%s\n", string(msg.Value))
            return nil
        }
        stepCount := 0
        subCount := 0
        if payload.ExecutionFlow != nil {
            stepCount = len(payload.ExecutionFlow.Steps)
            subCount = len(payload.ExecutionFlow.Subflows)
        }
        if payload.Status == "success" {
            fmt.Printf("[execute_transaction_response] status=success node=%s tx=%s data=%s key=%s steps=%d subflows=%d at=%s\n", payload.NodeID, payload.TxHash, payload.Data, payload.Key, stepCount, subCount, payload.Timestamp)
        } else {
            fmt.Printf("[execute_transaction_response] status=%s node=%s error=%s data=%s key=%s steps=%d subflows=%d at=%s\n", payload.Status, payload.NodeID, payload.Error, payload.Data, payload.Key, stepCount, subCount, payload.Timestamp)
        }
        return nil
    }

    requestConsumer := kafkautil.NewConsumer(brokers, requestGroup, requestTopic, logger, execHandler)
    responseConsumer := kafkautil.NewConsumer(brokers, responseGroup, responseTopic, logger, respHandler)

    var wg sync.WaitGroup
    wg.Add(2)

    go func() {
        defer wg.Done()
        if err := requestConsumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
            logger.Error("request consumer stopped", "err", err)
            cancel()
        }
    }()

    go func() {
        defer wg.Done()
        if err := responseConsumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
            logger.Error("response consumer stopped", "err", err)
            cancel()
        }
    }()

    <-ctx.Done()
    requestConsumer.Close()
    responseConsumer.Close()
    wg.Wait()

    logger.Info("kafka_ibft terminated")
}

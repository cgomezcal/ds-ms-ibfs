package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cgomezcal/ds-ms-ibfs/pkg/protocol"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type executeRequest struct {
	Data          string                   `json:"data"`
	Key           string                   `json:"key"`
	Timestamp     string                   `json:"timestamp,omitempty"`
	Type          string                   `json:"type,omitempty"`
	ExecutionFlow []protocol.ExecutionStep `json:"execution_flow"`
}

type executeResponse struct {
	NodeID        string                  `json:"node_id"`
	Data          string                  `json:"data"`
	ProposalHash  string                  `json:"proposal_hash"`
	Key           string                  `json:"key"`
	Status        string                  `json:"status"`
	TxHash        string                  `json:"tx_hash"`
	Error         string                  `json:"error"`
	Timestamp     string                  `json:"timestamp"`
	Type          string                  `json:"type"`
	ExecutionFlow *protocol.ExecutionFlow `json:"execution_flow"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var (
		brokersFlag   string
		requestTopic  string
		responseTopic string
		requestData   string
		key           string
		requestType   string
		timeoutFlag   time.Duration
	)

	flag.StringVar(&brokersFlag, "brokers", "localhost:29092", "Comma separated list of Kafka brokers")
	flag.StringVar(&requestTopic, "request-topic", "execute_transaction", "Kafka topic to publish execute requests")
	flag.StringVar(&responseTopic, "response-topic", "execute_transaction_response", "Kafka topic to read execute responses")
	flag.StringVar(&requestData, "data", "", "Raw data payload to execute")
	flag.StringVar(&key, "key", "", "Optional key identifier (defaults to random UUID)")
	flag.StringVar(&requestType, "type", "", "Optional logical transaction type")
	flag.DurationVar(&timeoutFlag, "timeout", 30*time.Second, "Time to wait for the blockchain response")
	flag.Parse()

	brokers := strings.Split(brokersFlag, ",")
	filtered := brokers[:0]
	for _, b := range brokers {
		if v := strings.TrimSpace(b); v != "" {
			filtered = append(filtered, v)
		}
	}
	brokers = filtered
	if len(brokers) == 0 {
		fmt.Fprintln(os.Stderr, "at least one broker is required")
		os.Exit(1)
	}

	requestData = strings.TrimSpace(requestData)
	if requestData == "" {
		fmt.Fprintln(os.Stderr, "--data flag is required")
		os.Exit(1)
	}

	key = strings.TrimSpace(key)
	if key == "" {
		key = uuid.NewString()
	}
	requestType = strings.TrimSpace(requestType)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger.Info("publishing execute transaction", "key", key, "type", requestType, "brokers", strings.Join(brokers, ","))

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    responseTopic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	if err := reader.SetOffset(kafka.LastOffset); err != nil {
		logger.Warn("failed to set reader offset", "err", err)
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  requestTopic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	payload := executeRequest{
		Data:          requestData,
		Key:           key,
		Timestamp:     time.Now().UTC().Format(time.RFC3339Nano),
		Type:          requestType,
		ExecutionFlow: []protocol.ExecutionStep{},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to marshal payload: %v\n", err)
		os.Exit(1)
	}

	writeCtx, cancelWrite := context.WithTimeout(ctx, 5*time.Second)
	defer cancelWrite()
	if err := writer.WriteMessages(writeCtx, kafka.Message{Key: []byte(key), Value: body, Time: time.Now()}); err != nil {
		fmt.Fprintf(os.Stderr, "failed to publish request: %v\n", err)
		os.Exit(1)
	}
	logger.Info("request published", "topic", requestTopic)

	responseCtx, cancel := context.WithTimeout(ctx, timeoutFlag)
	defer cancel()

	for {
		msg, err := reader.ReadMessage(responseCtx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				fmt.Fprintf(os.Stderr, "timed out waiting for response (key=%s)\n", key)
				os.Exit(2)
			}
			if errors.Is(err, context.Canceled) {
				fmt.Fprintf(os.Stderr, "operation canceled\n")
				os.Exit(3)
			}
			fmt.Fprintf(os.Stderr, "response read error: %v\n", err)
			os.Exit(1)
		}

		matches := string(msg.Key) == key
		var resp executeResponse
		if !matches {
			if err := json.Unmarshal(msg.Value, &resp); err != nil {
				logger.Debug("ignoring malformed response", "err", err)
				continue
			}
			matches = resp.Key == key
		} else {
			if err := json.Unmarshal(msg.Value, &resp); err != nil {
				logger.Debug("ignoring malformed response", "err", err)
				continue
			}
		}

		if !matches {
			continue
		}

		pretty, _ := json.MarshalIndent(resp, "", "  ")
		fmt.Println(string(pretty))
		logger.Info("response received", "status", resp.Status, "tx_hash", resp.TxHash, "type", resp.Type)
		return
	}
}

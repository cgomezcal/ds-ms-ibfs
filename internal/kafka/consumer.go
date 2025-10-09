package kafka

import (
    "context"
    "errors"
    "log/slog"
    "net"
    "time"

    "github.com/segmentio/kafka-go"
)

type HandlerFunc func(context.Context, kafka.Message) error

type Consumer struct {
    reader  *kafka.Reader
    log     *slog.Logger
    handler HandlerFunc
}

func NewConsumer(brokers []string, groupID, topic string, logger *slog.Logger, handler HandlerFunc) *Consumer {
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers: brokers,
        GroupID: groupID,
        Topic:   topic,
        Logger:  kafka.LoggerFunc(func(msg string, args ...interface{}) {}),
        ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
            if logger != nil {
                logger.Error("kafka consumer error", "msg", msg, "args", args)
            }
        }),
        HeartbeatInterval: 3 * time.Second,
        CommitInterval:    time.Second,
        MinBytes:          1,
        MaxBytes:          10e6,
    })
    return &Consumer{reader: reader, log: logger, handler: handler}
}

func (c *Consumer) Close() error {
    if c.reader != nil {
        return c.reader.Close()
    }
    return nil
}

func (c *Consumer) Run(ctx context.Context) error {
    if c.reader == nil || c.handler == nil {
        return nil
    }

    backoff := time.Second
    for {
        msg, err := c.reader.ReadMessage(ctx)
        if err != nil {
            if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
                return context.Canceled
            }

            if ne, ok := err.(net.Error); ok && (ne.Timeout() || ne.Temporary()) {
                c.logTemporaryError(ne)
                time.Sleep(backoff)
                continue
            }

            var opErr *net.OpError
            if errors.As(err, &opErr) {
                c.logTemporaryError(opErr)
                time.Sleep(backoff)
                continue
            }

            var ke kafka.Error
            if errors.As(err, &ke) {
                if ke.Timeout() || ke.Temporary() {
                    c.logTemporaryError(ke)
                    time.Sleep(backoff)
                    continue
                }
            }

            if c.log != nil {
                c.log.Error("kafka consumer fatal error", "err", err)
            }
            return err
        }

        if c.log != nil {
            c.log.Debug("kafka message received", "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
        }
        if err := c.handler(ctx, msg); err != nil {
            if c.log != nil {
                c.log.Error("kafka handler error", "err", err)
            }
        }
        backoff = time.Second
    }
}

func (c *Consumer) logTemporaryError(err error) {
    if c.log != nil {
        c.log.Warn("kafka consumer retrying", "err", err)
    }
}

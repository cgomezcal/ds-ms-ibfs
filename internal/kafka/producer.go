package kafka

import (
    "context"
    "log/slog"
    "time"

    "github.com/segmentio/kafka-go"
)

type Producer struct {
    writer *kafka.Writer
    log    *slog.Logger
}

func NewProducer(brokers []string, topic string, logger *slog.Logger) *Producer {
    writer := &kafka.Writer{
        Addr:                   kafka.TCP(brokers...),
        Topic:                  topic,
        Balancer:               &kafka.LeastBytes{},
        AllowAutoTopicCreation: true,
    }
    return &Producer{writer: writer, log: logger}
}

func (p *Producer) Close() error {
    if p.writer != nil {
        return p.writer.Close()
    }
    return nil
}

func (p *Producer) Publish(ctx context.Context, key, value []byte) error {
    if p.writer == nil {
        return nil
    }
    msg := kafka.Message{
        Key:   key,
        Value: value,
        Time:  time.Now(),
    }
    if err := p.writer.WriteMessages(ctx, msg); err != nil {
        if p.log != nil {
            p.log.Error("kafka publish failed", "err", err)
        }
        return err
    }
    if p.log != nil {
        p.log.Debug("kafka message published", "topic", p.writer.Topic, "key", string(key))
    }
    return nil
}

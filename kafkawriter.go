package samsa

import (
	"context"

	"github.com/IBM/sarama"
)

// Config represents a configuration for an AsyncKafkaWriter
type Config struct {
	BufferSize int

	Endpoints []string
	Topic     string

	AsyncProducer sarama.AsyncProducer
	SyncProducer  sarama.SyncProducer
}

// KafkaWriter represents a type that provides functionality to write data to a Kafka topic.
//
// The Write method is used to write the provided buffer of bytes to the Kafka topic.
// It returns the number of bytes written and any error that occurred during the write operation.
//
// The WriteWithContext method is similar to Write, but it also accepts a context.Context
// as the first argument. The context can be used to control the write operation, such as setting a timeout or cancellation.
//
// The Close method is used to close the KafkaWriter and release any resources associated with it.
// It returns an error if there was an issue closing the KafkaWriter.
type KafkaWriter interface {
	Write(buf []byte) (n int, err error)
	WriteWithContext(ctx context.Context, buf []byte) (n int, err error)

	Close() error
}

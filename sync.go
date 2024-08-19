package samsa

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

// SyncKafkaWriter represents a writer that sends messages to Kafka using an sync producer
//
// Fields:
// - produce: a channel used to send messages to the Kafka producer
// - conf: the configuration for the Kafka writer
// - debug: a flag indicating whether debug mode is enabled
// - wg: a WaitGroup used for synchronization
// - once: a sync.Once used for one-time initialization
type SyncKafkaWriter struct {
	producer sarama.SyncProducer
	conf     Config
}

// NewSyncKafkaWriterWithContext creates a new NewSyncKafkaWriterWithContext with a context and configuration.
// It launches a goroutine to synchronously create a SyncKafkaWriter using the provided configuration.
// The function waits for the creation to complete or for the context to be cancelled.
// If the context is cancelled before the creation is completed, it returns an error indicating a connect timeout.
// If the creation is completed successfully, it returns the created SyncKafkaWriter.
func NewSyncKafkaWriterWithContext(ctx context.Context, conf Config) (writer KafkaWriter, err error) {
	done := make(chan struct{})
	go func() {
		writer, err = NewSyncKafkaWriter(conf)
		close(done)
	}()

	select {
	case <-ctx.Done():
		return writer, fmt.Errorf(
			"connect timeout while connecting to kafka peers %s",
			strings.Join(conf.Endpoints, ","),
		)
	case <-done:
		return writer, err
	}
}

// NewSyncKafkaWriter creates a new SyncKafkaWriter with the provided configuration.
// It initializes a new Kafka configuration and sets the producer properties.
// If no producer is defined in the configuration, it creates a new synchronous producer using the given endpoints and Kafka configuration.
// If an error occurs during producer creation, it returns an error.
// The SyncKafkaWriter struct is then initialized with the configuration and a buffered channel for producing messages.
// It launches a goroutine that listens for producer errors and incoming messages to be produced.
// If an error occurs during message production, it prints the error message and the failed message to stderr.
// If the channel for producing messages is closed, the goroutine will close the producer and finish execution.
// Finally, it returns a pointer to the SyncKafkaWriter and a nil error.
func NewSyncKafkaWriter(conf Config) (KafkaWriter, error) {
	var err error

	// If the user failed to provide a producer, create one
	if conf.SyncProducer == nil {
		kafkaConfig := sarama.NewConfig()
		kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
		kafkaConfig.Producer.Compression = sarama.CompressionSnappy
		kafkaConfig.Producer.Flush.Frequency = 200 * time.Millisecond
		kafkaConfig.Producer.Retry.Backoff = 10 * time.Second
		kafkaConfig.Producer.Retry.Max = 6
		kafkaConfig.Producer.Return.Errors = true
		kafkaConfig.Producer.Return.Successes = true

		conf.SyncProducer, err = sarama.NewSyncProducer(conf.Endpoints, kafkaConfig)
		if err != nil {
			return nil, fmt.Errorf("kafka producer error %w", err)
		}
	}

	h := SyncKafkaWriter{
		producer: conf.SyncProducer,
		conf:     conf,
	}

	return &h, nil
}

// Write writes a byte buffer to the SyncKafkaWriter's producer input channel. It appends a
// copy of the buffer to the producer input channel. If the input channel buffer
// is full, it returns an error indicating a buffer overflow. The error message
// will include the contents of the buffer that was dropped. This function
// returns the number of bytes written and a nil error on success.
func (h *SyncKafkaWriter) Write(buf []byte) (n int, err error) {
	_, _, err = h.producer.SendMessage(
		&sarama.ProducerMessage{
			Value: sarama.ByteEncoder(buf),
			Topic: h.conf.Topic,
			Key:   nil,
		},
	)

	if err != nil {
		return 0, err
	}

	return len(buf), nil
}

// WriteWithContext writes a byte buffer to the SyncKafkaWriter's producer input channel,
// but unlike the Write method it respects the context timeout
// It appends a copy of the buffer to the producer input channel.
// If the context expires before the method can write to the channel, it returns an error
// indicating a deadline exceeded. If the input channel buffer is full, it returns an error
// indicating a buffer overflow. The error message will include the contents of the buffer
// that was dropped. This function returns the number of bytes written and a nil error on success.
func (h *SyncKafkaWriter) WriteWithContext(ctx context.Context, buf []byte) (n int, err error) {

	done := make(chan struct{})
	var writeErr error

	go func() {
		_, _, writeErr = h.producer.SendMessage(&sarama.ProducerMessage{Value: sarama.ByteEncoder(buf), Topic: h.conf.Topic, Key: nil})
		close(done)
	}()

	select {
	case <-ctx.Done():
		return 0, ctx.Err()

	case <-done:
		if writeErr != nil {
			return 0, writeErr
		}

		return len(buf), nil
	}
}

// Close closes the SyncKafkaWriter's producer connection to the Kafka cluster.
// It returns an error if the producer is unable to successfully close the connection.
// Once closed, the SyncKafkaWriter should not be used for further writing operations.
func (h *SyncKafkaWriter) Close() error {
	return h.producer.Close()
}

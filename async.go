package samsa

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// defaultBufferSize represents the default size of the buffer for storing produced messages before sending them to Kafka. It is used as the capacity of the produce channel in the AsyncKafkaWriter struct
const defaultBufferSize = 200

// AsyncKafkaWriter represents a writer that sends messages to Kafka using an async producer
//
// Fields:
// - produce: a channel used to send messages to the Kafka producer
// - conf: the configuration for the Kafka writer
// - debug: a flag indicating whether debug mode is enabled
// - wg: a WaitGroup used for synchronization
// - once: a sync.Once used for one-time initialization
type AsyncKafkaWriter struct {
	producer sarama.AsyncProducer
	produce  chan []byte
	conf     Config

	// Sync stuff
	wg   sync.WaitGroup
	once sync.Once
}

// NewAsyncKafkaWriterWithContext creates a new NewAsyncKafkaWriterWithContext with a context and configuration.
// It launches a goroutine to asynchronously create an AsyncKafkaWriter using the provided configuration.
// The function waits for the creation to complete or for the context to be cancelled.
// If the context is cancelled before the creation is completed, it returns an error indicating a connect timeout.
// If the creation is completed successfully, it returns the created AsyncKafkaWriter.
func NewAsyncKafkaWriterWithContext(ctx context.Context, conf Config) (writer *AsyncKafkaWriter, err error) {
	done := make(chan struct{})
	go func() {
		writer, err = NewAsyncKafkaWriter(conf)
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

// NewAsyncKafkaWriter creates a new AsyncKafkaWriter with the provided configuration.
// It initializes a new Kafka configuration and sets the producer properties.
// If no producer is defined in the configuration, it creates a new asynchronous producer using the given endpoints and Kafka configuration.
// If an error occurs during producer creation, it returns an error.
// The AsyncKafkaWriter struct is then initialized with the configuration and a buffered channel for producing messages.
// It launches a goroutine that listens for producer errors and incoming messages to be produced.
// If an error occurs during message production, it prints the error message and the failed message to stderr.
// If the channel for producing messages is closed, the goroutine will close the producer and finish execution.
// Finally, it returns a pointer to the AsyncKafkaWriter and a nil error.
func NewAsyncKafkaWriter(conf Config) (*AsyncKafkaWriter, error) {
	var err error

	// If the user failed to provide a producer, create one
	if conf.AsyncProducer == nil {
		kafkaConfig := sarama.NewConfig()
		kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
		kafkaConfig.Producer.Compression = sarama.CompressionSnappy
		kafkaConfig.Producer.Flush.Frequency = 200 * time.Millisecond
		kafkaConfig.Producer.Retry.Backoff = 10 * time.Second
		kafkaConfig.Producer.Retry.Max = 6
		kafkaConfig.Producer.Return.Errors = true

		conf.AsyncProducer, err = sarama.NewAsyncProducer(conf.Endpoints, kafkaConfig)
		if err != nil {
			return nil, fmt.Errorf("kafka producer error %w", err)
		}
	}

	if conf.BufferSize <= 0 {
		conf.BufferSize = defaultBufferSize
	}

	h := AsyncKafkaWriter{
		produce:  make(chan []byte, conf.BufferSize),
		producer: conf.AsyncProducer,
		conf:     conf,
	}

	h.wg.Add(1)
	go func() {
		for {
			select {
			case err := <-h.producer.Errors():
				msg, _ := err.Msg.Value.Encode()
				_, _ = fmt.Fprintf(os.Stderr, "[kafkawriter] produce error '%s' for: %s\n", err.Err, string(msg))

			case buf, ok := <-h.produce:
				if !ok {

					if err := h.producer.Close(); err != nil {
						_, _ = fmt.Fprintf(os.Stderr, "[kafkawriter] producer close error: %s\n", err)
					}
					h.wg.Done()
					return
				}

				h.producer.Input() <- &sarama.ProducerMessage{
					Value: sarama.ByteEncoder(buf),
					Topic: conf.Topic,
					Key:   nil,
				}
			}
		}
	}()

	return &h, nil
}

// Write writes a byte buffer to the AsyncKafkaWriter's producer input channel. It appends a
// copy of the buffer to the producer input channel. If the input channel buffer
// is full, it returns an error indicating a buffer overflow. The error message
// will include the contents of the buffer that was dropped. This function
// returns the number of bytes written and a nil error on success.
func (h *AsyncKafkaWriter) Write(buf []byte) (n int, err error) {
	select {

	case h.produce <- append([]byte{}, buf...):

	default:
		// If the producer input channel buffer is full, then we better drop
		// a log record than block program execution.
		err = fmt.Errorf("[kafkawriter] buffer overflow: %s", string(buf))
		_, _ = fmt.Fprintln(os.Stderr, err)
		return 0, err
	}

	return len(buf), nil
}

// WriteWithContext writes a byte buffer to the AsyncKafkaWriter's producer input channel,
// but unlike the Write method it respects the context timeout
// It appends a copy of the buffer to the producer input channel.
// If the context expires before the method can write to the channel, it returns an error
// indicating a deadline exceeded. If the input channel buffer is full, it returns an error
// indicating a buffer overflow. The error message will include the contents of the buffer
// that was dropped. This function returns the number of bytes written and a nil error on success.
func (h *AsyncKafkaWriter) WriteWithContext(ctx context.Context, buf []byte) (n int, err error) {
	select {
	case h.produce <- append([]byte{}, buf...):
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		// If the producer input channel buffer is full, then we better drop
		// a log record than block program execution.
		err = fmt.Errorf("[kafkawriter] buffer overflow: %s", string(buf))
		_, _ = fmt.Fprintln(os.Stderr, err)
		return 0, err
	}

	return len(buf), nil
}

// Close closes the AsyncKafkaWriter's producer by calling the Close method on the underlying producer.
// It returns an error if there was an error closing the producer.
func (h *AsyncKafkaWriter) Close() error {
	return h.producer.Close()
}

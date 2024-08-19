package samsa

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/IBM/sarama"
)

type MockSyncProducer struct {
	sarama.SyncProducer
	sendMessageError error
}

func (msp *MockSyncProducer) SendMessage(_ *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	return 0, 0, msp.sendMessageError
}

func (msp *MockSyncProducer) Close() error {
	return nil
}

func TestNewSyncKafkaWriter(t *testing.T) {
	mockSyncProducer := new(MockSyncProducer)

	tests := map[string]struct {
		config        Config
		expectedError error
	}{
		"Normal/NoError": {
			config: Config{
				Endpoints:    []string{"localhost:9092"},
				Topic:        "test-topic",
				SyncProducer: mockSyncProducer,
			},
			expectedError: nil,
		},
		"Error/ConnectionRefused": {
			config: Config{
				Endpoints: []string{"localhost:9092"},
				Topic:     "test-topic",
			},
			expectedError: sarama.ErrOutOfBrokers,
		},
	}

	for name, tt := range tests {
		t.Run(
			name, func(t *testing.T) {
				writer, err := NewSyncKafkaWriter(tt.config)
				if tt.expectedError != nil {
					if err == nil {
						t.Errorf("expected error %s, got nil", tt.expectedError)
						return
					}
					if errors.Is(tt.expectedError, err) {
						t.Errorf("expected error %s, got %s", tt.expectedError, err.Error())
						return
					}
				} else {
					if err != nil {
						t.Errorf("expected no error, got %v", err)
						return
					}

					if writer == nil {
						t.Errorf("expected writer to be created, got nil")
						return
					}

					defer func(writer KafkaWriter) {
						_ = writer.Close()
					}(writer)
				}
			},
		)
	}
}

func TestWriteSync(t *testing.T) {
	mockSyncProducer := new(MockSyncProducer)

	tests := map[string]struct {
		config          Config
		buf             []byte
		expectedError   error
		expectedWritten int
	}{
		"Normal/NoError": {
			config: Config{
				Endpoints:    []string{"localhost:9092"},
				Topic:        "test-topic",
				SyncProducer: mockSyncProducer,
			},
			buf:             []byte("Test Message"),
			expectedError:   nil,
			expectedWritten: 12,
		},
		"Error/SendMessage": {
			config: Config{
				BufferSize:    1,
				Endpoints:     []string{"localhost:9999"},
				Topic:         "test_topic",
				AsyncProducer: nil,
				SyncProducer:  &MockSyncProducer{sendMessageError: fmt.Errorf("send message error")},
			},
			buf:             []byte("Test Message"),
			expectedError:   fmt.Errorf("send message error"),
			expectedWritten: 0,
		},
	}

	for name, tt := range tests {
		t.Run(
			name, func(t *testing.T) {
				writer, _ := NewSyncKafkaWriter(tt.config)
				n, err := writer.Write(tt.buf)

				if err != nil && err.Error() != tt.expectedError.Error() {
					t.Errorf("expected error %v, got %v", tt.expectedError, err)
					return
				}
				if n != tt.expectedWritten {
					t.Errorf("expected %d bytes written, got %d", tt.expectedWritten, n)
				}
			},
		)
	}
}

func TestWriteWithContext(t *testing.T) {
	mockSyncProducer := new(MockSyncProducer)

	tests := map[string]struct {
		config          Config
		context         context.Context
		buf             []byte
		expectedError   error
		expectedWritten int
	}{
		"Normal/NoError": {
			config: Config{
				Endpoints:    []string{"localhost:9092"},
				Topic:        "test-topic",
				SyncProducer: mockSyncProducer,
			},
			context:         context.Background(),
			buf:             []byte("Test Message"),
			expectedError:   nil,
			expectedWritten: 12,
		},
		"Error/SendMessage": {
			config: Config{
				BufferSize:    1,
				Endpoints:     []string{"localhost:9999"},
				Topic:         "test_topic",
				AsyncProducer: nil,
				SyncProducer:  &MockSyncProducer{sendMessageError: fmt.Errorf("send message error")},
			},
			context:         context.Background(),
			buf:             []byte("Test Message"),
			expectedError:   fmt.Errorf("send message error"),
			expectedWritten: 0,
		},
	}
	for name, tt := range tests {
		t.Run(
			name, func(t *testing.T) {
				writer, _ := NewSyncKafkaWriter(tt.config)
				n, err := writer.WriteWithContext(tt.context, tt.buf)
				if err != nil && tt.expectedError.Error() != err.Error() {
					t.Errorf("expected error %v, got %v", tt.expectedError, err)
				}
				if n != tt.expectedWritten {
					t.Errorf("expected %d bytes written, got %d", tt.expectedWritten, n)
				}
			},
		)
	}
}

func TestNewSyncKafkaWriterWithContext(t *testing.T) {
	mockSyncProducer := new(MockSyncProducer)
	ctxWtimeout, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()

	tests := map[string]struct {
		config        Config
		context       context.Context
		expectedError string
	}{
		"Normal/NoError": {
			config: Config{
				Endpoints:    []string{"localhost:9092"},
				Topic:        "test-topic",
				SyncProducer: mockSyncProducer,
			},
			context:       context.Background(),
			expectedError: "",
		},
		"Error/ContextTimeout": {
			config: Config{
				Endpoints:    []string{"localhost:9999"},
				Topic:        "test-topic",
				SyncProducer: mockSyncProducer,
			},
			context:       ctxWtimeout,
			expectedError: "connect timeout while connecting to kafka peers localhost:9999",
		},
	}

	for name, tt := range tests {
		t.Run(
			name, func(t *testing.T) {
				writer, err := NewSyncKafkaWriterWithContext(tt.context, tt.config)

				if tt.expectedError != "" {
					if err == nil {
						t.Errorf("expected error %s, got nil", tt.expectedError)
						return
					}
					if err.Error() != tt.expectedError {
						t.Errorf("expected error %s, got %s", tt.expectedError, err.Error())
						return
					}
				} else {
					if err != nil {
						t.Errorf("expected no error, got %v", err)
						return
					}

					if writer == nil {
						t.Errorf("expected writer to be created, got nil")
						return
					}
				}
			},
		)
	}
}

func TestClose(t *testing.T) {
	mockSyncProducer := new(MockSyncProducer)

	tests := map[string]struct {
		config        Config
		expectedError error
	}{
		"Normal/NoError": {
			config: Config{
				Endpoints:    []string{"localhost:9092"},
				Topic:        "test-topic",
				SyncProducer: mockSyncProducer,
			},
			expectedError: nil,
		},
	}

	for name, tt := range tests {
		t.Run(
			name, func(t *testing.T) {
				writer, _ := NewSyncKafkaWriter(tt.config)
				err := writer.Close()
				if !errors.Is(err, tt.expectedError) {
					t.Errorf("expected error %v, got %v", tt.expectedError, err)
					return
				}
			},
		)
	}
}

package samsa

import (
	"testing"

	"github.com/IBM/sarama"
)

func TestNew(t *testing.T) {
	mockAsyncProducer := new(MockAsyncProducer)

	cases := []struct {
		name      string
		cfg       Config
		shouldErr bool
	}{
		{
			name: "Success",
			cfg: Config{
				Endpoints:     []string{"localhost:9092"},
				Topic:         "test-topic",
				AsyncProducer: mockAsyncProducer,
			},
			shouldErr: false,
		},
		{
			name: "Error",
			cfg: Config{
				Endpoints: []string{},
				Topic:     "",
			},
			shouldErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name, func(t *testing.T) {
				_, err := NewAsyncKafkaWriter(tc.cfg)
				if (err != nil) != tc.shouldErr {
					t.Errorf("New() error = %v, shouldErr %v", err, tc.shouldErr)
				}
			},
		)
	}
}

type MockAsyncProducer struct {
	sarama.AsyncProducer
	inputChan chan *sarama.ProducerMessage
	errChan   chan *sarama.ProducerError
}

func (m *MockAsyncProducer) Input() chan<- *sarama.ProducerMessage {
	return m.inputChan
}

func (m *MockAsyncProducer) Errors() <-chan *sarama.ProducerError {
	return m.errChan
}

func TestWriteAsync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bufferSize int
		msg        []byte
		wantN      int
		wantErr    bool
	}{
		{
			name:       "RegularCase",
			bufferSize: 1,
			msg:        []byte("Message"),
			wantN:      7,
			wantErr:    false,
		},
		{
			name:       "EmptyMessage",
			bufferSize: 1,
			msg:        []byte(""),
			wantN:      0,
			wantErr:    false,
		},
	}

	for _, test := range tests {
		t.Run(
			test.name, func(t *testing.T) {
				conf := Config{
					Topic:      "test-topic",
					BufferSize: test.bufferSize,
					AsyncProducer: &MockAsyncProducer{
						inputChan: make(chan *sarama.ProducerMessage),
						errChan:   make(chan *sarama.ProducerError),
					},
				}
				writer, err := NewAsyncKafkaWriter(conf)
				if err != nil {
					t.Fatalf("unable to create KafkaWriter: %v", err)
				}

				gotN, gotErr := writer.Write(test.msg)
				if gotN != test.wantN || (gotErr != nil) != test.wantErr {
					t.Fatalf("KafkaWriter.Write() = %v, %v, want %v, %v", gotN, gotErr, test.wantN, test.wantErr)
				}
			},
		)
	}
}

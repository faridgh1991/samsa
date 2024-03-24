package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/faridgh1991/samsa"
)

// main is the entry point of the program.
func main() {
	kafkaWriter, err := kafka_writer.NewWithContext(
		context.Background(),
		kafka_writer.Config{
			Endpoints:  []string{"localhost:9093"},
			Topic:      "testing_topic",
			BufferSize: 100,
		},
	)
	if err != nil {
		panic(err)
	}

	logger := slog.New(
		slog.NewJSONHandler(
			kafkaWriter,
			&slog.HandlerOptions{
				Level: slog.LevelInfo,
			},
		),
	)

	logger.LogAttrs(
		context.Background(),
		slog.LevelInfo,
		"hello, world!",
		slog.String("user", os.Getenv("USER")),
	)

	logger.Log(
		context.Background(),
		slog.LevelWarn,
		"finished",
		"now", time.Now(),
	)

	time.Sleep(time.Second)
}

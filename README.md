# Samsa
This is a Golang based library that enables you to write to Kafka service from your application effortlessly. It offers a simplified interface to create logs and write them into a Kafka topic.


## Installation
```Bash
go get github.com/faridgh1991/samsa
```

## Usage

### Using Samsa with a logger
```go
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	kw "github.com/faridgh1991/samsa"
)

// main is the entry point of the program.
func main() {
	kafkaWriter, err := kw.NewWithContext(
		context.Background(),
		kw.Config{
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
```

### Call Write directly
```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/faridgh1991/samsa"
)

// main is the entry point of the program.
func main() {
	kafkaWriter, err := samsa.NewWithContext(
		context.Background(),
		samsa.Config{
			Endpoints:  []string{"localhost:9093"},
			Topic:      "testing_topic",
			BufferSize: 100,
		},
	)

	if err != nil {
		panic(err)
	}

	n, err := kafkaWriter.Write([]byte("Hello world!"))
	if err != nil {
		fmt.Println(n, err)
		return
	}
	fmt.Println(n)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*300)
	defer cancel()
	n, err = kafkaWriter.WriteWithContext(ctx, []byte("finish"))
	if err != nil {
		fmt.Println(n, err)
		return
	}
	fmt.Println(n)

	time.Sleep(time.Second)
}
```
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/faridgh1991/samsa"
)

// main is the entry point of the program.
func main() {
	kafkaWriter, err := samsa.NewAsyncKafkaWriterWithContext(
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

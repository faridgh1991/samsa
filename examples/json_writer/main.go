package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/faridgh1991/samsa"
)

type jsonKafkaWriter struct {
	jsonEncoder *json.Encoder
}

func newJSONKafkaWriter() *jsonKafkaWriter {

	kafkaWriter, err := samsa.NewWithContext(
		context.Background(),
		samsa.Config{
			Endpoints: []string{"localhost:9093"},
			Topic:     "testing_topic_json",
		},
	)

	if err != nil {
		panic(err)
	}

	je := json.NewEncoder(kafkaWriter)

	return &jsonKafkaWriter{
		jsonEncoder: je,
	}
}

func (jkw *jsonKafkaWriter) WriteToKafka(v any) error {
	return jkw.jsonEncoder.Encode(v)

}

// main is the entry point of the program.
func main() {

	driver := &driver{
		Name: "john smith",
		Age:  23,
		Car: &car{
			Brand:       "Toyota",
			Model:       "RAV4",
			CarBody:     "crossover",
			Color:       "gray",
			ProductDate: date(time.Now()),
		},
	}

	jkw := newJSONKafkaWriter()

	err := jkw.WriteToKafka(driver)
	if err != nil {
		panic(err)
	}

	time.Sleep(time.Second)
}

type driver struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
	Car  *car   `json:"car,omitempty"`
}

type car struct {
	Brand       string `json:"brand"`
	Model       string `json:"model"`
	CarBody     string `json:"car_body"`
	Color       string `json:"color"`
	ProductDate date   `json:"product_date"`
}

type date time.Time

// MarshalJSON is Implementation of Marshaler interface for CustomTime\
func (ct *date) MarshalJSON() ([]byte, error) {
	if ct == nil {
		return []byte("null"), nil
	}
	return []byte(fmt.Sprintf("\"%s\"", ct.String())), nil
}

func (ct *date) String() string {
	if ct == nil {
		return "null"
	}
	t := time.Time(*ct)
	return t.Format(time.DateOnly)
}

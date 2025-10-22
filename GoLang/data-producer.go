package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	Kafka "github.com/segmentio/kafka-go"
)

const (
	Reset = "\033[0m"
	Red   = "\033[31m"
)

func printError(err error) {
	fmt.Printf("%s-------ERROR-------\n    %v\n--------END--------\n%s", Red, err, Reset)
}

func worker(rows <-chan string, wg *sync.WaitGroup, writer *Kafka.Writer, batchSize int) {
	defer wg.Done()
	batch := make([]Kafka.Message, 0, batchSize)

	for row := range rows {
		batch = append(batch, Kafka.Message{Value: []byte(row)})

		if len(batch) >= batchSize {
			err := writer.WriteMessages(context.Background(), batch...)
			if err != nil {
				printError(err)
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		err := writer.WriteMessages(context.Background(), batch...)
		if err != nil {
			printError(err)
		}
	}
}

func main() {
	topic := "KafkaMusicStream"
	numWorkers := 16
	batchSize := 500
	channelSize := 10000

	writer := &Kafka.Writer{
		Addr: Kafka.TCP("localhost:9092"),
		Topic: topic,
		RequiredAcks: Kafka.RequireNone,
		BatchSize: batchSize,
	}
	defer writer.Close()

	file, err := os.Open("../data/SpotifyFeatures.csv")
	if err != nil {
		printError(err)
		return
	}
	defer file.Close()

	rowChannel := make(chan string, channelSize)

	var wg sync.WaitGroup

	for range numWorkers {
		wg.Add(1)
		go worker(rowChannel, &wg, writer, batchSize)
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		rowChannel <- strings.TrimSpace(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		printError(err)
	}

	close(rowChannel)
	wg.Wait()

	fmt.Println("All rows sent to Kafka successfully!")
}

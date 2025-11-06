package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
	"math/rand"

	Kafka "github.com/segmentio/kafka-go"
)

const (
	Reset = "\033[0m"
	Red   = "\033[31m"
	Green = "\033[32m"
	BoldYellow = "\033[1;33m"
)

func printError(err error) {
	fmt.Printf("%s-------ERROR-------\n    %v\n--------END--------\n%s", Red, err, Reset)
}

func printSuccess(rows int) {
	fmt.Printf("%s-------SUCCESS-------\n    %d rows sended\n---------END---------\n%s", Green, rows, Reset)
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
			} else {
				printSuccess(len(batch))
			}
			batch = batch[:0]

			sleep := rand.Int31n(10)
			time.Sleep(time.Duration(sleep) * time.Second)
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
	numWorkers := 8
	batchSize := 100
	channelSize := 10000

	writer := &Kafka.Writer{
		Addr:         Kafka.TCP("localhost:9092"),
		Topic:        topic,
		RequiredAcks: Kafka.RequireNone,
		BatchSize:    batchSize,
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

	fmt.Printf("%sAll rows sent to Kafka successfully!%s", BoldYellow, Reset)
}

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/pashapdev/pgpubsub/pkg/queue"
)

func listen(ch *queue.Notify, index int) {
	for {
		select {
		case n := <-ch.Messages:
			fmt.Printf("channel %d get message: %s\n", index, n)
		case err := <-ch.Err:
			fmt.Printf("channel %d get error: %v\n", index, err)
		}
	}
}

func main() {
	connString := "postgres://postgres:postgres@localhost:5432/postgres"
	q, err := queue.New(connString, "events")
	if err != nil {
		panic(err)
	}
	defer q.Close()
	ctx := context.Background()

	sub1 := q.Subscriber()
	defer sub1.Close()
	ch1, err := sub1.Subscribe(ctx)
	if err != nil {
		panic(err)
	}
	defer ch1.Close()

	sub2 := q.Subscriber()
	defer sub2.Close()
	ch2, err := sub2.Subscribe(ctx)
	if err != nil {
		panic(err)
	}
	defer ch2.Close()

	go listen(ch1, 1)
	go listen(ch2, 2)

	forever := make(chan bool)
	go func() {
		for i := 0; i < 3; i++ {
			message := fmt.Sprintf("message_%d", i)
			if err := q.Publish(context.Background(), message); err != nil {
				panic(err)
			}
			fmt.Printf("message %s was send\n", message)
			time.Sleep(time.Second)
		}
	}()

	forever <- true
}

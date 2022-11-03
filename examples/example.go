package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/pashapdev/pgpubsub"
)

const (
	user     = "postgres"
	password = "postgres"
	address  = "localhost"
	port     = 5432
	db       = "db_test"
)

func connString(user, password, address, db string, port int) string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		address,
		port,
		db,
		user,
		password)
}

func listen(ch *pgpubsub.Notify, index int) {
	for {
		select {
		case msg := <-ch.Messages:
			log.Println("channel", index, "get message", msg)
		case err := <-ch.Err:
			log.Println("channel", index, "get err", err)
		}
	}
}

func main() {
	const eventsChannel = "events"
	q, err := pgpubsub.New(connString(user, password, address, db, port), eventsChannel)
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
			message := "message_" + strconv.Itoa(i)
			log.Println("sending message", message)
			if err := q.Publish(context.Background(), message); err != nil {
				panic(err)
			}
			time.Sleep(time.Second)
		}
	}()

	forever <- true
}

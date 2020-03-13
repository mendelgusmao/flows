package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/mendelgusmao/flows"
)

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go producer()

	for id := 0; id < 5; id++ {
		go consumer(id)
	}

	<-c
}

func producer() {
	redis, err := connect()

	if err != nil {
		fmt.Println("[producer]", err)
		return
	}

	publisher := flows.NewPublisher(redis)

	for {
		publisher.Publish("time", []byte(time.Now().Format("15:04:05")))
		publisher.PublishJSON("json_time", map[string]string{
			"time": time.Now().Format("15:04:05"),
		})

		time.Sleep(1 * time.Second)
	}
}

func consumer(id int) {
	redis, err := connect()

	if err != nil {
		fmt.Printf("[consumer %d]\n", err)
		return
	}

	hub := flows.NewHub(redis)

	topics := []string{"time", "json_time"}
	subscription, err := hub.Subscribe(fmt.Sprintf("%d", id), topics)

	if err != nil {
		fmt.Printf("[consumer %d]\n", err)
		return
	}

	go hub.Deliver()
	ch := subscription.Channel()

	for {
		response := <-ch
		timeInMap := make(map[string]string)

		if response.TryJSON(&timeInMap) {
			fmt.Printf("[consumer %d] topic=%s json=%v\n", id, response.Topic, timeInMap)
		} else {
			fmt.Printf("[consumer %d] topic=%s content=%s\n", id, response.Topic, response.Content)
		}
	}
}

func connect() (redis.Conn, error) {
	return redis.Dial("tcp", ":6379")
}

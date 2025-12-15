package main

import (
	"competing-consumers-queue/queue"
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

func main() {
	ctx := context.Background()
	q := queue.NewBounded(10)

	const consumers = 4
	const messages = 20

	var wg sync.WaitGroup

	// start consumers
	for i := 0; i < consumers; i++ {
		wg.Add(1)
		id := i
		go func() {
			defer wg.Done()
			for {
				msg, err := q.Dequeue(ctx)
				if err != nil {
					return
				}
				// process message
				fmt.Printf("consumer %d processed message %d : %s\n", id, msg.ID, msg.Body)
				time.Sleep(50 * time.Millisecond)
			}
		}()
	}

	// publisher
	for i := 0; i < messages; i++ {
		if err := q.Enqueue(ctx, queue.Message{ID: i, Body: "Hydden technical challenge message"}); err != nil {
			log.Printf("enqueue error: %v", err)
		}
	}

	// shutdown and wait for consumers to finish
	q.Shutdown(ctx)
	wg.Wait()
	fmt.Println("All done")
}

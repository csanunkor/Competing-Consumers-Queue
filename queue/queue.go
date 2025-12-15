package queue

import (
	"context"
	"errors"
	"io"
	"sync"
)

type Message struct {
	ID   int
	Body string
}

type Queue interface {
	Enqueue(ctx context.Context, msg Message) error
	Dequeue(ctx context.Context) (Message, error)
	Shutdown(ctx context.Context) error
}

// NewBounded returns a bounded, FIFO queue with the given capacity.
// Capacity must be >= 1.
func NewBounded(capacity int) Queue {
	if capacity < 1 {
		capacity = 1
	}
	return &boundedQueue{
		ch:       make(chan Message, capacity),
		closedCh: make(chan struct{}),
	}
}

type boundedQueue struct {
	ch       chan Message
	closedCh chan struct{} // closed when shutdown initiated, lets us know to stop Enqueue

	// track active Enqueue calls so we can safely close ch
	enqueueWG sync.WaitGroup
	once      sync.Once
}

// Enqueue blocks when the queue is full, respects context cancellation,
// and returns an error if Shutdown has been initiated.
func (q *boundedQueue) Enqueue(ctx context.Context, msg Message) error {
	// Fast-path: if already shutdown, return error
	select {
	case <-q.closedCh:
		return errors.New("queue shutdown")
	default:
	}

	q.enqueueWG.Add(1)
	defer q.enqueueWG.Done()

	select {
	case <-q.closedCh:
		return errors.New("queue shutdown")
	case <-ctx.Done():
		return ctx.Err()
	case q.ch <- msg:
		return nil
	}
}

// Dequeue blocks when the queue is empty, respects context cancellation,
// and returns io.EOF when the queue has been shut down and drained.
func (q *boundedQueue) Dequeue(ctx context.Context) (Message, error) {
	select {
	case <-ctx.Done():
		return Message{}, ctx.Err()
	case msg, ok := <-q.ch:
		if !ok {
			return Message{}, io.EOF
		}
		return msg, nil
	}
}

// Shutdown stops accepting new messages and allows in-flight messages to
// be processed. It waits for currently running Enqueue calls to return
// and then closes the internal channel to signal consumers.
func (q *boundedQueue) Shutdown(ctx context.Context) error {
	var err error
	q.once.Do(func() {
		close(q.closedCh) // signal Enqueue callers to stop

		// Wait for any in-progress Enqueue to return (which will happen
		// because closedCh is closed or they finish sending).
		done := make(chan struct{})
		go func() {
			q.enqueueWG.Wait()
			close(done)
		}()

		select {
		case <-done:
			// safe to close q.ch now, no senders remain
			close(q.ch)
		case <-ctx.Done():
			err = ctx.Err()
		}
	})
	return err
}

package queue

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func TestFIFO(t *testing.T) {
	q := NewBounded(5)
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		if err := q.Enqueue(ctx, Message{ID: i}); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	for i := 1; i <= 5; i++ {
		msg, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("dequeue failed: %v", err)
		}
		if msg.ID != i {
			t.Fatalf("expected id %d got %d", i, msg.ID)
		}
	}

	// Shutdown and ensure EOF on empty queue
	_ = q.Shutdown(ctx)
	_, err := q.Dequeue(ctx)
	if err != io.EOF {
		t.Fatalf("expected EOF after shutdown, got %v", err)
	}
}

func TestMessageProcessedExactlyOnce(t *testing.T) {
	q := NewBounded(10)
	ctx := context.Background()

	const N = 1000
	const consumers = 5

	var wg sync.WaitGroup
	processed := make(chan int, N)

	// consumers
	for i := 0; i < consumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				msg, err := q.Dequeue(ctx)
				if err == io.EOF {
					return
				}
				if err != nil {
					return
				}
				processed <- msg.ID
			}
		}()
	}

	// publisher
	for i := 0; i < N; i++ {
		if err := q.Enqueue(ctx, Message{ID: i}); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	// shutdown once all messages enqueued
	if err := q.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	wg.Wait()
	close(processed)

	seen := make([]bool, N)
	count := 0
	for id := range processed {
		if id < 0 || id >= N {
			t.Fatalf("invalid id: %d", id)
		}
		if seen[id] {
			t.Fatalf("duplicate processing of id %d", id)
		}
		seen[id] = true
		count++
	}

	if count != N {
		t.Fatalf("expected %d processed items, got %d", N, count)
	}
}

func TestEnqueueBlocksWhenFull(t *testing.T) {
	q := NewBounded(1)
	ctx := context.Background()

	// fill queue
	if err := q.Enqueue(ctx, Message{ID: 1}); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		// this should block until we dequeue
		err := q.Enqueue(context.Background(), Message{ID: 2})
		done <- err
	}()

	// ensure enqueue goroutine is blocked for a short time
	select {
	case <-done:
		t.Fatalf("enqueue returned early, expected it to block")
	case <-time.After(50 * time.Millisecond):
	}

	// now dequeue one so enqueuer can finish
	msg, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if msg.ID != 1 {
		t.Fatalf("unexpected id %d", msg.ID)
	}

	// enqueuer should finish shortly
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("enqueue returned error after space freed: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("enqueue did not finish after space freed")
	}
}

func TestShutdownStopsEnqueueAndDrains(t *testing.T) {
	q := NewBounded(2)
	ctx := context.Background()

	// fill queue
	if err := q.Enqueue(ctx, Message{ID: 1}); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if err := q.Enqueue(ctx, Message{ID: 2}); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// this enqueue will block
	enqDone := make(chan error, 1)
	go func() {
		enqDone <- q.Enqueue(context.Background(), Message{ID: 3})
	}()

	// give goroutine a moment to start and block
	time.Sleep(20 * time.Millisecond)

	// shutdown: should cause blocked enqueues to return an error
	if err := q.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	// blocked enqueue should return with error
	select {
	case err := <-enqDone:
		if err == nil {
			t.Fatalf("expected blocked enqueue to return error after shutdown")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("blocked enqueue did not return after shutdown")
	}

	// drain remaining messages
	count := 0
	for {
		_, err := q.Dequeue(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("dequeue error: %v", err)
		}
		count++
	}

	if count != 2 {
		t.Fatalf("expected to drain 2 messages after shutdown, drained %d", count)
	}
}

func TestEnqueueContextCancellation(t *testing.T) {
	q := NewBounded(1)
	ctx := context.Background()
	if err := q.Enqueue(ctx, Message{ID: 1}); err != nil {
		t.Fatalf("setup enqueue failed: %v", err)
	}

	ctx2, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- q.Enqueue(ctx2, Message{ID: 2}) }()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("enqueue did not return after cancel")
	}
}

func TestDequeueContextCancellation(t *testing.T) {
	q := NewBounded(1)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := q.Dequeue(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

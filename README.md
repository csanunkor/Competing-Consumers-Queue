Competing Consumers Queue
================================

This small project demonstrates a competing-consumers pattern implemented in Go.

Features
- Bounded FIFO queue
- Blocking `Enqueue` when full and blocking `Dequeue` when empty
- Graceful `Shutdown` which stops accepting new messages and drains the queue

Usage

Run the demo:

```bash
go run ./cmd/demo
```

Run tests:

```bash
go test ./...
```

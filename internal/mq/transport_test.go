package mq

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rajakushwah/gpu-telemetry/internal/platform"
)

// TestTransportErrors checks that broker errors reach the client with the right meaning.
func TestTransportErrors(t *testing.T) {
	b, err := Open(Options{Dir: t.TempDir(), Partitions: 1, Capacity: 2, SegmentBytes: 1 << 20, SessionTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewHandler(b))
	defer srv.Close()
	defer b.Close()
	c := NewClient(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msg := func(key string) Message { return Message{Key: key, Value: json.RawMessage(`1`)} }

	if err := c.Publish(ctx, []Message{msg("")}); !platform.IsPermanent(err) {
		t.Fatalf("invalid publish: want a permanent error, not endless retries; got %v", err)
	}
	st, _ := c.GroupHeartbeat(ctx, "g", "m")
	if err := c.Publish(ctx, []Message{msg("a"), msg("b")}); err != nil {
		t.Fatal(err)
	}
	if err := c.do(ctx, http.MethodPost, "/v1/publish", PublishRequest{Messages: []Message{msg("c")}}, nil); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("full partition: want ErrBackpressure, got %v", err)
	}
	commit := CommitRequest{Member: "m", Generation: st.Generation + 1, Partition: 0, Offset: 1}
	if err := c.Commit(ctx, "g", commit); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale generation: want ErrFenced, got %v", err)
	}
	commit.Generation, commit.Offset = st.Generation, 99
	if err := c.Commit(ctx, "g", commit); !platform.IsPermanent(err) {
		t.Fatalf("offset past end: want a permanent 400, got %v", err)
	}
}

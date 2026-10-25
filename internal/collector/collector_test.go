package collector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/rajakushwah/gpu-telemetry/internal/mq"
	"github.com/rajakushwah/gpu-telemetry/internal/telemetry"
)

type fakeWriter struct {
	n   int
	err error
}

func (f *fakeWriter) Write(_ context.Context, pts []telemetry.Point) error {
	f.n += len(pts)
	return f.err
}

// TestHandle checks that poison records are skipped and write errors are returned for retry.
func TestHandle(t *testing.T) {
	valid, _ := json.Marshal(telemetry.Point{Timestamp: time.Now(), GPUUUID: "GPU-1", Metric: "m", Value: 1})
	recs := []mq.Record{
		{Offset: 0, Message: mq.Message{Key: "GPU-1", Value: valid}},
		{Offset: 1, Message: mq.Message{Key: "GPU-1", Value: json.RawMessage(`"not a point"`)}},
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	w := &fakeWriter{}
	if err := New(w, log).Handle(context.Background(), recs); err != nil || w.n != 1 {
		t.Fatalf("want 1 point written and no error, got %d, %v", w.n, err)
	}
	w = &fakeWriter{err: errors.New("db down")}
	if err := New(w, log).Handle(context.Background(), recs); err == nil {
		t.Fatal("a failed write must be returned so the consumer retries the batch")
	}
}

package mq

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func msgs(n int) []Message {
	out := make([]Message, n)
	for i := range out {
		out[i] = Message{Key: "k", Value: json.RawMessage(`{"n":1}`)}
	}
	return out
}

func TestPartitionAppendFetch(t *testing.T) {
	p, err := OpenPartition(t.TempDir(), 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Append(msgs(3), 0); err != nil {
		t.Fatal(err)
	}
	recs, hw := p.Fetch(context.Background(), 1, 10, 0)
	if len(recs) != 2 || recs[0].Offset != 1 || hw != 3 {
		t.Fatalf("got %d records from offset %d, hw %d", len(recs), recs[0].Offset, hw)
	}
}

func TestPartitionBackpressureProtectsUncommitted(t *testing.T) {
	p, _ := OpenPartition(t.TempDir(), 4, 1<<20)
	defer p.Close()
	if err := p.Append(msgs(4), 0); err != nil {
		t.Fatal(err)
	}
	if err := p.Append(msgs(1), 0); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("want backpressure, got %v", err)
	}
	if err := p.Append(msgs(2), 2); err != nil { // consumers committed up to 2
		t.Fatalf("append after commit: %v", err)
	}
}

func TestPartitionLongPollWakesOnAppend(t *testing.T) {
	p, _ := OpenPartition(t.TempDir(), 10, 1<<20)
	defer p.Close()
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = p.Append(msgs(1), 0)
	}()
	recs, _ := p.Fetch(context.Background(), 0, 10, 2*time.Second)
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
}

func TestPartitionRecoversAndTruncatesTornTail(t *testing.T) {
	dir := t.TempDir()
	p, _ := OpenPartition(dir, 100, 200) // tiny segments force rolling
	for range 5 {
		if err := p.Append(msgs(2), 0); err != nil {
			t.Fatal(err)
		}
	}
	_ = p.Close()

	segs, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	if len(segs) < 2 {
		t.Fatalf("expected rolled segments, got %d", len(segs))
	}
	f, _ := os.OpenFile(segs[len(segs)-1], os.O_WRONLY|os.O_APPEND, 0)
	_, _ = f.Write([]byte{0, 0, 0, 50, 1, 2}) // simulate a crash mid-frame
	_ = f.Close()

	p, err := OpenPartition(dir, 100, 200)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if hw := p.HighWatermark(); hw != 10 {
		t.Fatalf("recovered high watermark %d, want 10", hw)
	}
	if err := p.Append(msgs(1), 0); err != nil {
		t.Fatal(err)
	}
	recs, _ := p.Fetch(context.Background(), 0, 100, 0)
	if len(recs) != 11 || recs[10].Offset != 10 {
		t.Fatalf("got %d records after recovery", len(recs))
	}
}

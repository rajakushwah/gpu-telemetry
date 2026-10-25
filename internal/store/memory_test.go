package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rajakushwah/gpu-telemetry/internal/telemetry"
)

func pt(sec int, metric string) telemetry.Point {
	return telemetry.Point{Timestamp: time.Unix(int64(sec), 0).UTC(), GPUUUID: "GPU-1", GPUID: "0", Hostname: "h", Metric: metric, Value: float64(sec)}
}

func TestMemoryDedupFilterAndPaging(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	_ = m.Write(ctx, []telemetry.Point{pt(3, "a"), pt(1, "a"), pt(2, "a"), pt(2, "b")})
	_ = m.Write(ctx, []telemetry.Point{pt(2, "a")}) // redelivery

	page, _ := m.Telemetry(ctx, "GPU-1", Query{})
	if len(page.Items) != 4 || page.Next != nil {
		t.Fatalf("dedup: got %d items", len(page.Items))
	}
	if page.Items[0].Timestamp.Unix() != 1 || page.Items[2].Metric != "b" {
		t.Fatal("items must be ordered by (timestamp, metric)")
	}

	window, _ := m.Telemetry(ctx, "GPU-1", Query{Start: time.Unix(2, 0), End: time.Unix(2, 0)})
	if len(window.Items) != 2 {
		t.Fatalf("inclusive window: got %d items", len(window.Items))
	}

	first, _ := m.Telemetry(ctx, "GPU-1", Query{Limit: 3})
	rest, _ := m.Telemetry(ctx, "GPU-1", Query{Limit: 3, After: first.Next})
	if len(first.Items) != 3 || first.Next == nil || len(rest.Items) != 1 || rest.Items[0].Timestamp.Unix() != 3 {
		t.Fatalf("paging: %d then %d", len(first.Items), len(rest.Items))
	}

	if _, err := m.Telemetry(ctx, "missing", Query{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if gpus, _ := m.ListGPUs(ctx); len(gpus) != 1 || gpus[0].LastSeen.Unix() != 3 {
		t.Fatalf("gpus %+v", gpus)
	}
}

func TestConnectRejectsUnknownStore(t *testing.T) {
	if _, err := Connect(context.Background(), "nope", ""); err == nil {
		t.Fatal("expected error")
	}
	if _, err := Connect(context.Background(), "postgres", ""); err == nil {
		t.Fatal("expected DSN error")
	}
}

func TestColumnBuilders(t *testing.T) {
	p := pt(1, "a")
	p.Labels = map[string]string{"k": "v"}
	args := gpuColumnsOf([]telemetry.Point{pt(1, "a"), pt(5, "b")})
	if seen := args[5].([]time.Time); len(seen) != 1 || seen[0].Unix() != 5 {
		t.Fatalf("gpu upsert must use newest point: %v", seen)
	}
	cols, err := telemetryColumnsOf([]telemetry.Point{p})
	if err != nil || cols[4].([]string)[0] != `{"k":"v"}` {
		t.Fatalf("labels column %v, err %v", cols[4], err)
	}
}

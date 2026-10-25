// Package e2e runs the whole pipeline in-process: broker, 2 streamers,
// 2 collectors and the API over an in-memory store, using the real CSV.
package e2e

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/rajakushwah/gpu-telemetry/internal/api"
	"github.com/rajakushwah/gpu-telemetry/internal/collector"
	"github.com/rajakushwah/gpu-telemetry/internal/mq"
	"github.com/rajakushwah/gpu-telemetry/internal/store"
	"github.com/rajakushwah/gpu-telemetry/internal/streamer"
)

const csvPath = "../../data/dcgm_metrics_20250718_134233.csv"

func TestPipelineShardsAndDeliversEveryGPU(t *testing.T) {
	if testing.Short() {
		t.Skip("system test")
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b, err := mq.Open(mq.Options{Dir: t.TempDir(), Partitions: 8, Capacity: 20000, SegmentBytes: 4 << 20, SessionTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	broker := httptest.NewServer(mq.NewHandler(b))
	defer broker.Close()
	defer b.Close()

	st := store.NewMemory()
	var wg sync.WaitGroup
	run := func(fn func(context.Context) error) {
		wg.Add(1)
		go func() { defer wg.Done(); _ = fn(ctx) }()
	}
	for _, id := range []string{"streamer-a", "streamer-b"} {
		cfg := streamer.Config{ID: id, CSVPath: csvPath, Interval: 200 * time.Millisecond, Heartbeat: 100 * time.Millisecond, BatchSize: 250}
		run(streamer.New(cfg, mq.NewClient(broker.URL), log).Run)
	}
	for _, id := range []string{"collector-a", "collector-b"} {
		cfg := mq.ConsumerConfig{Group: "collectors", Member: id, Heartbeat: 100 * time.Millisecond, BatchSize: 500, Wait: 200 * time.Millisecond}
		run(mq.NewConsumer(mq.NewClient(broker.URL), cfg, collector.New(st, log).Handle, log).Run)
	}

	apiSrv := httptest.NewServer(api.New(st, log))
	defer apiSrv.Close()
	const wantGPUs = 247 // distinct UUIDs in the sample CSV
	var got api.GPUList
	for ctx.Err() == nil {
		if resp, err := http.Get(apiSrv.URL + "/api/v1/gpus"); err == nil {
			_ = json.NewDecoder(resp.Body).Decode(&got)
			resp.Body.Close()
			if len(got.Items) == wantGPUs {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(got.Items) != wantGPUs {
		t.Fatalf("API lists %d GPUs, want %d", len(got.Items), wantGPUs)
	}

	page, err := st.Telemetry(ctx, got.Items[0].UUID, store.Query{})
	if err != nil || len(page.Items) == 0 {
		t.Fatalf("no telemetry for %s: %v", got.Items[0].UUID, err)
	}
	if age := time.Since(page.Items[0].Timestamp); age < 0 || age > time.Minute {
		t.Fatalf("timestamp %v is not the processing time", page.Items[0].Timestamp)
	}
	cancel()
	wg.Wait()
}

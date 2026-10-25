package streamer

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/rajakushwah/gpu-telemetry/internal/mq"
)

// fakeClient records published keys and hands out a fixed shard.
type fakeClient struct {
	shard mq.StreamerState
	keys  []string
}

func (f *fakeClient) Publish(_ context.Context, msgs []mq.Message) error {
	for _, m := range msgs {
		f.keys = append(f.keys, m.Key)
	}
	return nil
}
func (f *fakeClient) StreamerHeartbeat(context.Context, string) (mq.StreamerState, error) {
	return f.shard, nil
}
func (f *fakeClient) StreamerLeave(context.Context, string) error { return nil }

// TestShardsSplitGPUs checks that two streamers publish disjoint GPUs that together cover the CSV.
func TestShardsSplitGPUs(t *testing.T) {
	csv := "timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw\n"
	for _, gpu := range []string{"GPU-a", "GPU-b", "GPU-c", "GPU-d", "GPU-e", "GPU-f"} {
		csv += `t,m,0,nvidia0,` + gpu + `,H100,host,,,,1,` + "\n"
	}
	path := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(path, []byte(csv), 0o600); err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for index := range 2 {
		fc := &fakeClient{shard: mq.StreamerState{Generation: 1, Index: index, Count: 2}}
		s := New(Config{ID: "s", CSVPath: path, BatchSize: 2}, fc, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err := s.renew(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := s.pass(context.Background()); err != nil {
			t.Fatal(err)
		}
		for _, k := range fc.keys {
			seen[k]++
		}
	}
	if len(seen) != 6 {
		t.Fatalf("shards cover %d of 6 GPUs", len(seen))
	}
	for gpu, n := range seen {
		if n != 1 {
			t.Fatalf("%s published %d times, want exactly once", gpu, n)
		}
	}
}

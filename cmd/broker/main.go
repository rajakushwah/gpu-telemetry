// Command broker runs the custom message queue server.
package main

import (
	"sync"
	"time"

	"github.com/rajakushwah/gpu-telemetry/internal/mq"
	"github.com/rajakushwah/gpu-telemetry/internal/platform"
)

func main() {
	log := platform.Logger("broker")
	ctx, stop := platform.SignalContext()
	defer stop()

	b, err := mq.Open(mq.Options{
		Dir:          platform.Env("DATA_DIR", "/var/lib/broker"),
		Partitions:   platform.EnvInt("PARTITIONS", 16),
		Capacity:     platform.EnvInt("PARTITION_CAPACITY", 10000),
		SegmentBytes: int64(platform.EnvInt("SEGMENT_BYTES", 64<<20)),
		SessionTTL:   platform.EnvDuration("SESSION_TTL", 6*time.Second),
	})
	if err != nil {
		platform.Fatal(log, "open broker", err)
	}
	var flusher sync.WaitGroup
	flusher.Go(func() { b.FlushEvery(ctx, platform.EnvDuration("FSYNC_INTERVAL", 50*time.Millisecond)) })

	srv := platform.NewServer(platform.Env("LISTEN_ADDR", ":9000"), mq.NewHandler(b))
	if err := platform.Serve(ctx, srv, log); err != nil {
		log.Error("server stopped", "err", err)
	}
	flusher.Wait() // Close flushes too, and Flush must not run concurrently
	if err := b.Close(); err != nil {
		log.Error("close broker", "err", err)
	}
}

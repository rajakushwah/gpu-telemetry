// Command streamer replays the telemetry CSV onto the message queue.
package main

import (
	"time"

	"github.com/rajakushwah/gpu-telemetry/internal/mq"
	"github.com/rajakushwah/gpu-telemetry/internal/platform"
	"github.com/rajakushwah/gpu-telemetry/internal/streamer"
)

func main() {
	log := platform.Logger("streamer")
	ctx, stop := platform.SignalContext()
	defer stop()

	s := streamer.New(streamer.Config{
		ID:        platform.InstanceID(),
		CSVPath:   platform.Env("CSV_PATH", "/data/telemetry.csv"),
		Interval:  platform.EnvDuration("STREAM_INTERVAL", time.Second),
		Heartbeat: platform.EnvDuration("HEARTBEAT", 2*time.Second),
		BatchSize: platform.EnvInt("BATCH_SIZE", 500),
	}, mq.NewClient(platform.Env("BROKER_URL", "http://localhost:9000")), log)

	if err := s.Run(ctx); err != nil {
		platform.Fatal(log, "streamer stopped", err)
	}
}

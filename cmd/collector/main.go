// Command collector consumes telemetry from the message queue and persists it.
package main

import (
	"os"
	"time"

	"github.com/rajakushwah/gpu-telemetry/internal/collector"
	"github.com/rajakushwah/gpu-telemetry/internal/mq"
	"github.com/rajakushwah/gpu-telemetry/internal/platform"
	"github.com/rajakushwah/gpu-telemetry/internal/store"
)

func main() {
	log := platform.Logger("collector")
	ctx, stop := platform.SignalContext()
	defer stop()

	st, err := store.Connect(ctx, platform.Env("STORE", "postgres"), os.Getenv("DATABASE_URL"))
	if err != nil {
		platform.Fatal(log, "connect store", err)
	}
	defer st.Close()

	consumer := mq.NewConsumer(
		mq.NewClient(platform.Env("BROKER_URL", "http://localhost:9000")),
		mq.ConsumerConfig{
			Group:     platform.Env("CONSUMER_GROUP", "collectors"),
			Member:    platform.InstanceID(),
			Heartbeat: platform.EnvDuration("HEARTBEAT", 2*time.Second),
			BatchSize: platform.EnvInt("BATCH_SIZE", 1000),
			Wait:      platform.EnvDuration("FETCH_WAIT", 5*time.Second),
		},
		collector.New(st, log).Handle,
		log,
	)
	if err := consumer.Run(ctx); err != nil {
		log.Error("consumer stopped", "err", err)
	}
}

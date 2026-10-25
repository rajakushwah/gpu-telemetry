// Package streamer replays a telemetry CSV onto the message queue, publishing
// only the GPUs in this instance's shard so streamers scale horizontally.
package streamer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rajakushwah/gpu-telemetry/internal/mq"
	"github.com/rajakushwah/gpu-telemetry/internal/platform"
	"github.com/rajakushwah/gpu-telemetry/internal/telemetry"
)

// Config configures a Streamer.
type Config struct {
	ID        string
	CSVPath   string
	Interval  time.Duration // pause between passes over the CSV
	Heartbeat time.Duration // shard lease renewal period
	BatchSize int
}

// Client is the broker API a Streamer needs; *mq.Client implements it over HTTP.
type Client interface {
	Publish(ctx context.Context, msgs []mq.Message) error
	StreamerHeartbeat(ctx context.Context, member string) (mq.StreamerState, error)
	StreamerLeave(ctx context.Context, member string) error
}

// Streamer publishes telemetry rows for its shard of GPUs.
type Streamer struct {
	cfg    Config
	client Client
	shard  atomic.Pointer[mq.StreamerState]
	log    *slog.Logger
}

// New builds a Streamer; call Run to start it.
func New(cfg Config, client Client, log *slog.Logger) *Streamer {
	return &Streamer{cfg: cfg, client: client, log: log.With("member", cfg.ID)}
}

// Run registers with the broker, then streams the CSV in a loop until ctx ends.
func (s *Streamer) Run(ctx context.Context) error {
	if err := platform.Retry(ctx, func() error { return s.renew(ctx) }); err != nil {
		return fmt.Errorf("register streamer: %w", err)
	}
	var wg sync.WaitGroup
	defer func() {
		wg.Wait() // an in-flight heartbeat after Leave would re-register a ghost member
		leaveCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := s.client.StreamerLeave(leaveCtx, s.cfg.ID); err != nil {
			s.log.Warn("leave failed; shard rebalances after the lease expires", "err", err)
		}
	}()
	wg.Go(func() { s.heartbeat(ctx) })
	for {
		if err := s.pass(ctx); err != nil && ctx.Err() == nil {
			s.log.Error("stream pass failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(s.cfg.Interval):
		}
	}
}

// heartbeat keeps the lease alive and picks up shard changes when streamers join or leave.
func (s *Streamer) heartbeat(ctx context.Context) {
	t := time.NewTicker(s.cfg.Heartbeat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.renew(ctx); err != nil && ctx.Err() == nil {
				s.log.Warn("heartbeat failed", "err", err)
			}
		}
	}
}

func (s *Streamer) renew(ctx context.Context) error {
	st, err := s.client.StreamerHeartbeat(ctx, s.cfg.ID)
	if err != nil {
		return err
	}
	if old := s.shard.Swap(&st); old == nil || old.Generation != st.Generation {
		s.log.Info("shard assigned", "index", st.Index, "count", st.Count, "generation", st.Generation)
	}
	return nil
}

// owns reports whether this instance is responsible for gpu under the current shard.
func (s *Streamer) owns(gpu string) bool {
	st := s.shard.Load()
	return st != nil && st.Count > 0 && mq.PartitionFor(gpu, st.Count) == st.Index
}

// pass streams the CSV once, publishing owned rows in batches.
func (s *Streamer) pass(ctx context.Context) error {
	f, err := os.Open(s.cfg.CSVPath)
	if err != nil {
		return err
	}
	defer f.Close()
	rd, err := telemetry.NewReader(f)
	if err != nil {
		return err
	}
	batch := make([]mq.Message, 0, s.cfg.BatchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := s.client.Publish(ctx, batch)
		batch = batch[:0]
		return err
	}
	for ctx.Err() == nil {
		p, err := rd.Next(time.Now())
		if errors.Is(err, io.EOF) {
			break
		} else if errors.Is(err, telemetry.ErrBadRow) {
			s.log.Warn("skipping row", "err", err)
			continue
		} else if err != nil {
			return err
		}
		if !s.owns(p.GPUUUID) {
			continue
		}
		val, err := json.Marshal(p)
		if err != nil {
			return err
		}
		if batch = append(batch, mq.Message{Key: p.GPUUUID, Value: val}); len(batch) >= s.cfg.BatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

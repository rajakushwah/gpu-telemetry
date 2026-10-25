package mq

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/rajakushwah/gpu-telemetry/internal/platform"
)

// Handler processes one batch from a partition. Returning an error retries the
// same batch, so handlers must be idempotent (delivery is at-least-once). An
// error wrapped with platform.Permanent skips the batch instead.
type Handler func(ctx context.Context, recs []Record) error

// ConsumerConfig configures a group consumer.
type ConsumerConfig struct {
	Group     string
	Member    string
	Heartbeat time.Duration // must be well below the broker session TTL
	BatchSize int
	Wait      time.Duration // long-poll duration per fetch
}

// Consumer joins a group and runs one fetch loop per assigned partition,
// restarting the loops whenever the group generation changes (rebalance).
type Consumer struct {
	client *Client
	cfg    ConsumerConfig
	handle Handler
	log    *slog.Logger
}

// NewConsumer builds a consumer; call Run to start it.
func NewConsumer(c *Client, cfg ConsumerConfig, h Handler, log *slog.Logger) *Consumer {
	return &Consumer{client: c, cfg: cfg, handle: h, log: log.With("group", cfg.Group, "member", cfg.Member)}
}

// Run heartbeats until ctx ends, then stops workers and leaves the group.
func (c *Consumer) Run(ctx context.Context) error {
	var gen uint64
	workers := &workerSet{}
	defer func() {
		workers.stop()
		leaveCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := c.client.GroupLeave(leaveCtx, c.cfg.Group, c.cfg.Member); err != nil {
			c.log.Warn("leave failed; partitions rebalance after the lease expires", "err", err)
		}
	}()
	tick := time.NewTicker(c.cfg.Heartbeat)
	defer tick.Stop()
	for {
		st, err := c.client.GroupHeartbeat(ctx, c.cfg.Group, c.cfg.Member)
		switch {
		case err != nil && ctx.Err() == nil:
			c.log.Warn("heartbeat failed", "err", err)
		case err == nil && st.Generation != gen:
			workers.stop()
			gen = st.Generation
			workers.start(ctx, st.Partitions, func(wctx context.Context, p int) {
				c.consume(wctx, st.Generation, p, st.Offsets[p])
			})
			c.log.Info("partitions assigned", "generation", gen, "partitions", st.Partitions)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

// consume fetches, handles and commits one partition until fenced or cancelled.
func (c *Consumer) consume(ctx context.Context, gen uint64, p int, offset uint64) {
	log := c.log.With("partition", p, "generation", gen)
	for ctx.Err() == nil {
		resp, err := c.client.Fetch(ctx, p, offset, c.cfg.BatchSize, c.cfg.Wait)
		if err != nil {
			if ctx.Err() == nil {
				log.Warn("fetch failed", "err", err)
				sleep(ctx, time.Second)
			}
			continue
		}
		if len(resp.Records) == 0 {
			continue
		}
		err = platform.Retry(ctx, func() error { return c.handle(ctx, resp.Records) })
		if ctx.Err() != nil {
			return
		}
		if err != nil { // permanent: skip the batch rather than stall the partition
			log.Error("dropping batch", "count", len(resp.Records), "err", err)
		}
		next := resp.Records[len(resp.Records)-1].Offset + 1
		err = c.client.Commit(ctx, c.cfg.Group, CommitRequest{Member: c.cfg.Member, Generation: gen, Partition: p, Offset: next})
		if errors.Is(err, ErrFenced) {
			log.Info("partition revoked")
			return
		} else if err != nil && ctx.Err() == nil {
			log.Warn("commit failed; batch may be redelivered", "err", err)
		}
		offset = next
	}
}

// workerSet runs one goroutine per partition under a shared cancellable context.
type workerSet struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (w *workerSet) start(ctx context.Context, partitions []int, fn func(context.Context, int)) {
	ctx, w.cancel = context.WithCancel(ctx)
	for _, p := range partitions {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			fn(ctx, p)
		}()
	}
}

// stop cancels all workers and waits for them, so a revoked partition is never consumed twice by this member.
func (w *workerSet) stop() {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

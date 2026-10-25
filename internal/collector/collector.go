// Package collector turns queue records into persisted telemetry.
package collector

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/rajakushwah/gpu-telemetry/internal/mq"
	"github.com/rajakushwah/gpu-telemetry/internal/store"
	"github.com/rajakushwah/gpu-telemetry/internal/telemetry"
)

// Collector decodes records and writes them through a store.Writer.
type Collector struct {
	w   store.Writer
	log *slog.Logger
}

// New returns a Collector writing to w.
func New(w store.Writer, log *slog.Logger) *Collector {
	return &Collector{w: w, log: log}
}

// Handle is an mq.Handler. Undecodable records are logged and skipped so a
// single poison message cannot stall its partition; write errors are returned
// so the consumer retries the batch.
func (c *Collector) Handle(ctx context.Context, recs []mq.Record) error {
	pts := make([]telemetry.Point, 0, len(recs))
	for _, r := range recs {
		var p telemetry.Point
		if err := json.Unmarshal(r.Value, &p); err != nil {
			c.log.Warn("dropping undecodable record", "offset", r.Offset, "err", err)
			continue
		}
		if err := p.Validate(); err != nil {
			c.log.Warn("dropping invalid record", "offset", r.Offset, "err", err)
			continue
		}
		pts = append(pts, p)
	}
	if len(pts) == 0 {
		return nil
	}
	return c.w.Write(ctx, pts)
}

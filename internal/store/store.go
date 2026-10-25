// Package store persists telemetry and serves the read queries behind the API.
// Implementations must make Write idempotent on (gpu_uuid, timestamp, metric)
// because the queue delivers at-least-once.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rajakushwah/gpu-telemetry/internal/platform"
	"github.com/rajakushwah/gpu-telemetry/internal/telemetry"
)

// ErrNotFound is returned when the requested GPU has no telemetry.
var ErrNotFound = errors.New("store: not found")

const (
	DefaultLimit = 1000
	MaxLimit     = 10000
)

// GPU describes a device that has reported telemetry.
type GPU struct {
	UUID      string    `json:"uuid"`
	GPUID     string    `json:"gpu_id"`
	Device    string    `json:"device,omitempty"`
	ModelName string    `json:"model_name,omitempty"`
	Hostname  string    `json:"hostname"`
	LastSeen  time.Time `json:"last_seen"`
}

// point returns a Point carrying this GPU's attributes.
func (g GPU) point() telemetry.Point {
	return telemetry.Point{GPUUUID: g.UUID, GPUID: g.GPUID, Device: g.Device, ModelName: g.ModelName, Hostname: g.Hostname}
}

// Cursor is a keyset position: results continue strictly after (TS, Metric).
type Cursor struct {
	TS     time.Time `json:"t"`
	Metric string    `json:"m"`
}

// Query filters a telemetry read. Zero Start/End mean unbounded; bounds are inclusive.
type Query struct {
	Start, End time.Time
	Metric     string
	Limit      int
	After      *Cursor
}

func (q Query) limit() int {
	if q.Limit <= 0 || q.Limit > MaxLimit {
		return DefaultLimit
	}
	return q.Limit
}

// Page is one page of telemetry ordered by (timestamp, metric).
type Page struct {
	Items []telemetry.Point
	Next  *Cursor // nil on the last page
}

// Writer persists telemetry.
type Writer interface {
	Write(ctx context.Context, pts []telemetry.Point) error
}

// Reader answers API queries.
type Reader interface {
	ListGPUs(ctx context.Context) ([]GPU, error)
	Telemetry(ctx context.Context, gpuUUID string, q Query) (Page, error)
}

// Store is the full storage contract.
type Store interface {
	Writer
	Reader
	Ping(ctx context.Context) error
	Close()
}

// Connect opens the store named by kind ("postgres" or "memory"), retrying
// until the database is reachable or ctx ends.
func Connect(ctx context.Context, kind, dsn string) (Store, error) {
	switch kind {
	case "memory":
		return NewMemory(), nil
	case "postgres":
		if dsn == "" {
			return nil, errors.New("DATABASE_URL is required for the postgres store")
		}
		var p *Postgres
		if err := platform.Retry(ctx, func() (err error) { p, err = NewPostgres(ctx, dsn); return err }); err != nil {
			return nil, err
		}
		return p, nil
	default:
		return nil, fmt.Errorf("unknown store %q", kind)
	}
}

// paginate trims a limit+1 result set and derives the next cursor.
func paginate(pts []telemetry.Point, limit int) Page {
	if len(pts) <= limit {
		return Page{Items: pts}
	}
	last := pts[limit-1]
	return Page{Items: pts[:limit], Next: &Cursor{TS: last.Timestamp, Metric: last.Metric}}
}

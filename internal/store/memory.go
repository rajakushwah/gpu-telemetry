package store

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"time"

	"github.com/rajakushwah/gpu-telemetry/internal/telemetry"
)

// Memory is an in-process Store for tests and demos, with the same dedup and
// ordering semantics as Postgres. Data is lost on restart.
type Memory struct {
	mu   sync.Mutex
	gpus map[string]*memGPU
}

type memGPU struct {
	info   GPU
	seen   map[pointKey]struct{}
	points []telemetry.Point
	sorted bool
}

type pointKey struct {
	ts     int64
	metric string
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory { return &Memory{gpus: map[string]*memGPU{}} }

// Write stores points, ignoring duplicates.
func (m *Memory) Write(_ context.Context, pts []telemetry.Point) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range pts {
		g, ok := m.gpus[p.GPUUUID]
		if !ok {
			g = &memGPU{seen: map[pointKey]struct{}{}}
			m.gpus[p.GPUUUID] = g
		}
		g.info = GPU{UUID: p.GPUUUID, GPUID: p.GPUID, Device: p.Device, ModelName: p.ModelName, Hostname: p.Hostname,
			LastSeen: maxTime(g.info.LastSeen, p.Timestamp)}
		k := pointKey{p.Timestamp.UnixMicro(), p.Metric}
		if _, dup := g.seen[k]; dup {
			continue
		}
		g.seen[k] = struct{}{}
		g.points = append(g.points, p)
		g.sorted = false
	}
	return nil
}

// ListGPUs returns GPUs ordered by hostname then GPU index.
func (m *Memory) ListGPUs(context.Context) ([]GPU, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]GPU, 0, len(m.gpus))
	for _, g := range m.gpus {
		out = append(out, g.info)
	}
	slices.SortFunc(out, func(a, b GPU) int {
		return cmp.Or(cmp.Compare(a.Hostname, b.Hostname), cmp.Compare(a.GPUID, b.GPUID))
	})
	return out, nil
}

// Telemetry filters and pages one GPU's points ordered by (timestamp, metric).
func (m *Memory) Telemetry(_ context.Context, uuid string, q Query) (Page, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.gpus[uuid]
	if !ok {
		return Page{}, ErrNotFound
	}
	if !g.sorted {
		slices.SortFunc(g.points, comparePoints)
		g.sorted = true
	}
	limit := q.limit()
	out := []telemetry.Point{}
	for _, p := range g.points {
		switch {
		case !q.Start.IsZero() && p.Timestamp.Before(q.Start),
			!q.End.IsZero() && p.Timestamp.After(q.End),
			q.Metric != "" && p.Metric != q.Metric,
			q.After != nil && comparePoints(p, telemetry.Point{Timestamp: q.After.TS, Metric: q.After.Metric}) <= 0:
			continue
		}
		if out = append(out, p); len(out) > limit {
			break
		}
	}
	return paginate(out, limit), nil
}

// Ping always succeeds.
func (m *Memory) Ping(context.Context) error { return nil }

// Close is a no-op.
func (m *Memory) Close() {}

func comparePoints(a, b telemetry.Point) int {
	return cmp.Or(a.Timestamp.Compare(b.Timestamp), cmp.Compare(a.Metric, b.Metric))
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

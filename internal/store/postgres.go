package store

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rajakushwah/gpu-telemetry/internal/platform"
	"github.com/rajakushwah/gpu-telemetry/internal/telemetry"
)

//go:embed schema.sql
var schema string

const migrationLockID = 7313 // arbitrary advisory lock key for schema setup

// Batched writes pass column arrays and unnest them server-side: one round trip per batch.
const (
	upsertGPUs = `
INSERT INTO gpus (uuid, gpu_id, device, model_name, hostname, last_seen)
SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::timestamptz[])
ON CONFLICT (uuid) DO UPDATE SET gpu_id = EXCLUDED.gpu_id, device = EXCLUDED.device,
  model_name = EXCLUDED.model_name, hostname = EXCLUDED.hostname,
  last_seen = GREATEST(gpus.last_seen, EXCLUDED.last_seen)`

	insertTelemetry = `
INSERT INTO telemetry (ts, gpu_uuid, metric, value, labels)
SELECT ts, u, m, v, NULLIF(l, '')::jsonb
FROM unnest($1::timestamptz[], $2::text[], $3::text[], $4::float8[], $5::text[]) AS t(ts, u, m, v, l)
ON CONFLICT DO NOTHING`

	selectTelemetry = `
SELECT ts, metric, value, labels FROM telemetry
WHERE gpu_uuid = $1
  AND ($2::timestamptz IS NULL OR ts >= $2)
  AND ($3::timestamptz IS NULL OR ts <= $3)
  AND ($4::text = '' OR metric = $4)
  AND ($5::timestamptz IS NULL OR (ts, metric) > ($5, $6::text))
ORDER BY ts, metric
LIMIT $7`

	gpuColumns = `uuid, gpu_id, device, model_name, hostname, last_seen`
)

// Postgres stores telemetry in TimescaleDB.
type Postgres struct{ pool *pgxpool.Pool }

// NewPostgres connects and applies the schema.
func NewPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, platform.Permanent(fmt.Errorf("parse DATABASE_URL: %w", err))
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	p := &Postgres{pool: pool}
	if err := p.migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return p, nil
}

func (p *Postgres) migrate(ctx context.Context) error {
	return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", migrationLockID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, schema)
		return err
	})
}

// Write upserts GPU metadata and inserts points in one transaction. GPUs are
// upserted in sorted order so concurrent collectors cannot deadlock.
func (p *Postgres) Write(ctx context.Context, pts []telemetry.Point) error {
	gpuArgs := gpuColumnsOf(pts)
	telArgs, err := telemetryColumnsOf(pts)
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, upsertGPUs, gpuArgs...); err != nil {
			return fmt.Errorf("upsert gpus: %w", err)
		}
		if _, err := tx.Exec(ctx, insertTelemetry, telArgs...); err != nil {
			return fmt.Errorf("insert telemetry: %w", err)
		}
		return nil
	})
}

// gpuColumnsOf returns upsertGPUs arguments: the newest point per GPU, sorted by UUID.
func gpuColumnsOf(pts []telemetry.Point) []any {
	latest := map[string]telemetry.Point{}
	for _, pt := range pts {
		if cur, ok := latest[pt.GPUUUID]; !ok || pt.Timestamp.After(cur.Timestamp) {
			latest[pt.GPUUUID] = pt
		}
	}
	var uuid, id, dev, model, host []string
	var seen []time.Time
	for _, k := range slices.Sorted(maps.Keys(latest)) {
		pt := latest[k]
		uuid, id, dev = append(uuid, k), append(id, pt.GPUID), append(dev, pt.Device)
		model, host, seen = append(model, pt.ModelName), append(host, pt.Hostname), append(seen, pt.Timestamp)
	}
	return []any{uuid, id, dev, model, host, seen}
}

// telemetryColumnsOf returns insertTelemetry arguments as parallel column arrays.
func telemetryColumnsOf(pts []telemetry.Point) ([]any, error) {
	ts := make([]time.Time, len(pts))
	uuid, metric, labels := make([]string, len(pts)), make([]string, len(pts)), make([]string, len(pts))
	value := make([]float64, len(pts))
	for i, pt := range pts {
		ts[i], uuid[i], metric[i], value[i] = pt.Timestamp, pt.GPUUUID, pt.Metric, pt.Value
		if len(pt.Labels) > 0 {
			b, err := json.Marshal(pt.Labels)
			if err != nil {
				return nil, err
			}
			labels[i] = string(b)
		}
	}
	return []any{ts, uuid, metric, value, labels}, nil
}

// ListGPUs returns GPUs ordered by hostname then GPU index.
func (p *Postgres) ListGPUs(ctx context.Context) ([]GPU, error) {
	rows, err := p.pool.Query(ctx, "SELECT "+gpuColumns+" FROM gpus ORDER BY hostname, gpu_id")
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanGPU)
}

// Telemetry returns one page of a GPU's telemetry ordered by (ts, metric).
func (p *Postgres) Telemetry(ctx context.Context, uuid string, q Query) (Page, error) {
	rows, err := p.pool.Query(ctx, "SELECT "+gpuColumns+" FROM gpus WHERE uuid = $1", uuid)
	if err != nil {
		return Page{}, err
	}
	g, err := pgx.CollectExactlyOneRow(rows, scanGPU)
	if errors.Is(err, pgx.ErrNoRows) {
		return Page{}, ErrNotFound
	} else if err != nil {
		return Page{}, err
	}
	var afterTS *time.Time
	afterMetric := ""
	if q.After != nil {
		afterTS, afterMetric = &q.After.TS, q.After.Metric
	}
	limit := q.limit()
	rows, err = p.pool.Query(ctx, selectTelemetry, uuid, nullTime(q.Start), nullTime(q.End), q.Metric, afterTS, afterMetric, limit+1)
	if err != nil {
		return Page{}, err
	}
	pts, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (telemetry.Point, error) {
		pt := g.point()
		var lb []byte
		if err := row.Scan(&pt.Timestamp, &pt.Metric, &pt.Value, &lb); err != nil {
			return pt, err
		}
		if lb != nil {
			if err := json.Unmarshal(lb, &pt.Labels); err != nil {
				return pt, err
			}
		}
		pt.Timestamp = pt.Timestamp.UTC()
		return pt, nil
	})
	if err != nil {
		return Page{}, err
	}
	return paginate(pts, limit), nil
}

// Ping checks database connectivity (used by readiness probes).
func (p *Postgres) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

// Close releases the connection pool.
func (p *Postgres) Close() { p.pool.Close() }

func scanGPU(row pgx.CollectableRow) (GPU, error) {
	var g GPU
	err := row.Scan(&g.UUID, &g.GPUID, &g.Device, &g.ModelName, &g.Hostname, &g.LastSeen)
	g.LastSeen = g.LastSeen.UTC()
	return g, err
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

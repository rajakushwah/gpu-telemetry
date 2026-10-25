package telemetry

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
)

// ErrBadRow marks a row that cannot be parsed; callers skip it and continue.
var ErrBadRow = errors.New("telemetry: invalid row")

var requiredColumns = []string{"metric_name", "gpu_id", "uuid", "Hostname", "value"}

// Reader streams Points from a DCGM CSV export, locating columns by header name.
type Reader struct {
	csv *csv.Reader
	col map[string]int
}

// NewReader reads and validates the header row.
func NewReader(r io.Reader) (*Reader, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	cr.ReuseRecord = true
	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	col := make(map[string]int, len(header))
	for i, h := range header {
		col[strings.TrimSpace(h)] = i
	}
	for _, k := range requiredColumns {
		if _, ok := col[k]; !ok {
			return nil, fmt.Errorf("csv missing column %q", k)
		}
	}
	return &Reader{csv: cr, col: col}, nil
}

// Next parses the next row and stamps it with ts, the processing time (the CSV
// timestamp is ignored). Truncating to microseconds matches PostgreSQL precision
// so redelivered points deduplicate exactly. It returns io.EOF at end of input.
func (r *Reader) Next(ts time.Time) (Point, error) {
	rec, err := r.csv.Read()
	var perr *csv.ParseError
	if errors.As(err, &perr) {
		return Point{}, fmt.Errorf("%w: %v", ErrBadRow, err)
	} else if err != nil {
		return Point{}, err
	}
	get := func(k string) string {
		if i, ok := r.col[k]; ok && i < len(rec) {
			return strings.TrimSpace(rec[i])
		}
		return ""
	}
	v, err := strconv.ParseFloat(get("value"), 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return Point{}, fmt.Errorf("%w: value %q", ErrBadRow, get("value"))
	}
	p := Point{
		Timestamp: ts.UTC().Truncate(time.Microsecond),
		GPUUUID:   get("uuid"),
		GPUID:     get("gpu_id"),
		Device:    get("device"),
		ModelName: get("modelName"),
		Hostname:  get("Hostname"),
		Metric:    get("metric_name"),
		Value:     v,
		Labels:    parseLabels(get("labels_raw")),
	}
	// Kubernetes attribution columns are optional; keep them as labels when present.
	for _, k := range []string{"container", "pod", "namespace"} {
		if val := get(k); val != "" {
			if p.Labels == nil {
				p.Labels = map[string]string{}
			}
			p.Labels[k] = val
		}
	}
	if err := p.Validate(); err != nil {
		return Point{}, fmt.Errorf("%w: %v", ErrBadRow, err)
	}
	return p, nil
}

// parseLabels parses a Prometheus label set such as `a="x",b="y"`; malformed trailing input is ignored.
func parseLabels(s string) map[string]string {
	out := map[string]string{}
	for {
		eq := strings.Index(s, `="`)
		if eq < 0 {
			break
		}
		key := strings.TrimSpace(strings.TrimLeft(s[:eq], ", "))
		s = s[eq+2:]
		var val strings.Builder
		i := 0
		for ; i < len(s) && s[i] != '"'; i++ {
			if s[i] == '\\' && i+1 < len(s) {
				i++
			}
			val.WriteByte(s[i])
		}
		if i >= len(s) {
			break // unterminated value
		}
		out[key] = val.String()
		s = s[i+1:]
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

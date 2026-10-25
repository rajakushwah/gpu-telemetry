package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rajakushwah/gpu-telemetry/internal/store"
	"github.com/rajakushwah/gpu-telemetry/internal/telemetry"
)

func newTestServer(t *testing.T) *httptest.Server {
	st := store.NewMemory()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var pts []telemetry.Point
	for i := range 5 {
		pts = append(pts, telemetry.Point{Timestamp: base.Add(time.Duration(i) * time.Second), GPUUUID: "GPU-1", GPUID: "0", Hostname: "h", Metric: "m", Value: float64(i)})
	}
	_ = st.Write(context.Background(), pts)
	srv := httptest.NewServer(New(st, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(srv.Close)
	return srv
}

func get[T any](t *testing.T, url string, wantStatus int) T {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		t.Fatalf("GET %s: status %d, want %d", url, resp.StatusCode, wantStatus)
	}
	var v T
	_ = json.NewDecoder(resp.Body).Decode(&v)
	return v
}

func TestListGPUs(t *testing.T) {
	srv := newTestServer(t)
	if got := get[GPUList](t, srv.URL+"/api/v1/gpus", 200); len(got.Items) != 1 || got.Items[0].UUID != "GPU-1" {
		t.Fatalf("got %+v", got)
	}
}

func TestTelemetryWindowAndPaging(t *testing.T) {
	srv := newTestServer(t)
	u := srv.URL + "/api/v1/gpus/GPU-1/telemetry"
	window := get[TelemetryPage](t, u+"?start_time=2026-01-01T00:00:01Z&end_time=2026-01-01T00:00:03Z", 200)
	if len(window.Items) != 3 {
		t.Fatalf("inclusive window returned %d items", len(window.Items))
	}
	first := get[TelemetryPage](t, u+"?limit=2", 200)
	next := get[TelemetryPage](t, u+"?limit=2&cursor="+first.NextCursor, 200)
	if len(first.Items) != 2 || first.NextCursor == "" || next.Items[0].Value != 2 {
		t.Fatalf("paging broken: %+v / %+v", first, next)
	}
}

func TestTelemetryErrors(t *testing.T) {
	srv := newTestServer(t)
	u := srv.URL + "/api/v1/gpus/"
	get[ErrorResponse](t, u+"GPU-404/telemetry", 404)
	get[ErrorResponse](t, u+"GPU-1/telemetry?start_time=yesterday", 400)
	get[ErrorResponse](t, u+"GPU-1/telemetry?start_time=2026-01-02T00:00:00Z&end_time=2026-01-01T00:00:00Z", 400)
	get[ErrorResponse](t, u+"GPU-1/telemetry?limit=0", 400)
	get[ErrorResponse](t, u+"GPU-1/telemetry?cursor=not-base64!", 400)
}

func TestHealth(t *testing.T) {
	srv := newTestServer(t)
	for _, p := range []string{"/healthz", "/readyz"} {
		resp, err := http.Get(srv.URL + p)
		if err != nil || resp.StatusCode != http.StatusNoContent {
			t.Fatalf("%s: %v %v", p, resp.StatusCode, err)
		}
		resp.Body.Close()
	}
}

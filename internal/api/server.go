// Package api serves the public REST API over a store.Store.
package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	httpSwagger "github.com/swaggo/http-swagger/v2"

	"github.com/rajakushwah/gpu-telemetry/internal/store"
	"github.com/rajakushwah/gpu-telemetry/internal/telemetry"
)

// GPUList is the response of GET /gpus.
type GPUList struct {
	Items []store.GPU `json:"items"`
}

// TelemetryPage is one page of telemetry ordered by (timestamp, metric).
type TelemetryPage struct {
	Items      []telemetry.Point `json:"items"`
	NextCursor string            `json:"next_cursor,omitempty"`
}

// ErrorResponse is the body of every non-2xx response.
type ErrorResponse struct {
	Error string `json:"error"`
}

// Server holds the API's dependencies.
type Server struct {
	store store.Store
	log   *slog.Logger
}

// New returns the HTTP handler: API routes, health probes and Swagger UI.
func New(st store.Store, log *slog.Logger) http.Handler {
	s := &Server{store: st, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/gpus", s.listGPUs)
	mux.HandleFunc("GET /api/v1/gpus/{id}/telemetry", s.telemetry)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /readyz", s.ready)
	mux.Handle("GET /swagger/", httpSwagger.Handler(httpSwagger.URL("/swagger/doc.json")))
	return s.middleware(mux)
}

// listGPUs godoc
//
//	@Summary		List GPUs
//	@Description	Returns every GPU for which telemetry is available.
//	@Tags			gpus
//	@Produce		json
//	@Success		200	{object}	GPUList
//	@Failure		500	{object}	ErrorResponse
//	@Router			/gpus [get]
func (s *Server) listGPUs(w http.ResponseWriter, r *http.Request) {
	gpus, err := s.store.ListGPUs(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, GPUList{Items: gpus})
}

// telemetry godoc
//
//	@Summary		Query telemetry for a GPU
//	@Description	Returns telemetry for one GPU ordered by time. Time bounds are inclusive; use next_cursor to page.
//	@Tags			gpus
//	@Produce		json
//	@Param			id			path		string	true	"GPU UUID"
//	@Param			start_time	query		string	false	"Inclusive lower bound (RFC 3339)"
//	@Param			end_time	query		string	false	"Inclusive upper bound (RFC 3339)"
//	@Param			metric		query		string	false	"Metric name, e.g. DCGM_FI_DEV_GPU_TEMP"
//	@Param			limit		query		int		false	"Page size (1-10000)"	default(1000)
//	@Param			cursor		query		string	false	"next_cursor from the previous page"
//	@Success		200			{object}	TelemetryPage
//	@Failure		400			{object}	ErrorResponse
//	@Failure		404			{object}	ErrorResponse
//	@Failure		500			{object}	ErrorResponse
//	@Router			/gpus/{id}/telemetry [get]
func (s *Server) telemetry(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" || len(id) > 128 {
		writeError(w, http.StatusBadRequest, "invalid gpu id")
		return
	}
	q, err := parseQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	page, err := s.store.Telemetry(r.Context(), id, q)
	if err != nil {
		s.fail(w, err)
		return
	}
	resp := TelemetryPage{Items: page.Items}
	if page.Next != nil {
		resp.NextCursor = encodeCursor(*page.Next)
	}
	writeJSON(w, http.StatusOK, resp)
}

// ready reports 503 until the store is reachable.
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "store unavailable")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// parseQuery validates time window, metric, limit and cursor parameters.
func parseQuery(r *http.Request) (store.Query, error) {
	v := r.URL.Query()
	var q store.Query
	var err error
	if q.Start, err = parseTime(v.Get("start_time")); err != nil {
		return q, errors.New("start_time must be RFC 3339")
	}
	if q.End, err = parseTime(v.Get("end_time")); err != nil {
		return q, errors.New("end_time must be RFC 3339")
	}
	if !q.Start.IsZero() && !q.End.IsZero() && q.Start.After(q.End) {
		return q, errors.New("start_time must not be after end_time")
	}
	q.Metric = v.Get("metric")
	if len(q.Metric) > 128 {
		return q, errors.New("metric too long")
	}
	q.Limit = store.DefaultLimit
	if l := v.Get("limit"); l != "" {
		if q.Limit, err = strconv.Atoi(l); err != nil || q.Limit < 1 || q.Limit > store.MaxLimit {
			return q, errors.New("limit must be between 1 and 10000")
		}
	}
	if c := v.Get("cursor"); c != "" {
		cur, err := decodeCursor(c)
		if err != nil {
			return q, errors.New("invalid cursor")
		}
		q.After = &cur
	}
	return q, nil
}

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}

// Cursors are opaque to clients: base64url-encoded JSON of the last (ts, metric).
func encodeCursor(c store.Cursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (store.Cursor, error) {
	var c store.Cursor
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err == nil {
		err = json.Unmarshal(b, &c)
	}
	return c, err
}

// fail maps store errors to responses without leaking internal details.
func (s *Server) fail(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "gpu not found")
		return
	}
	s.log.Error("request failed", "err", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, ErrorResponse{Error: msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// middleware adds panic recovery, security headers and access logging.
func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			if p := recover(); p != nil {
				s.log.Error("panic", "panic", p, "path", r.URL.Path)
				writeError(rec, http.StatusInternalServerError, "internal error")
			}
			s.log.Info("request", "method", r.Method, "path", r.URL.Path, "status", rec.status, "duration_ms", time.Since(start).Milliseconds())
		}()
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(rec, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

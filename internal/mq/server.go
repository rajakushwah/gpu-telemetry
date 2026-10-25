package mq

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

const (
	maxBodyBytes = 8 << 20
	maxFetch     = 5000
	maxWait      = 30 * time.Second
)

// validName bounds member and group identifiers (pod names fit).
var validName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

type handler struct{ b *Broker }

// NewHandler exposes the broker over an HTTP/JSON API.
func NewHandler(b *Broker) http.Handler {
	h := &handler{b}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/publish", h.publish)
	mux.HandleFunc("GET /v1/partitions/{p}/records", h.fetch)
	mux.HandleFunc("POST /v1/groups/{group}/heartbeat", h.groupHeartbeat)
	mux.HandleFunc("POST /v1/groups/{group}/leave", h.groupLeave)
	mux.HandleFunc("POST /v1/groups/{group}/commit", h.commit)
	mux.HandleFunc("POST /v1/streamers/heartbeat", h.streamerHeartbeat)
	mux.HandleFunc("POST /v1/streamers/leave", h.streamerLeave)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return mux
}

func (h *handler) publish(w http.ResponseWriter, r *http.Request) {
	var req PublishRequest
	if !decode(w, r, &req) {
		return
	}
	for _, m := range req.Messages {
		if m.Key == "" {
			fail(w, http.StatusBadRequest, "message key is required")
			return
		}
	}
	if err := h.b.Publish(req.Messages); err != nil {
		failErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) fetch(w http.ResponseWriter, r *http.Request) {
	p, err1 := strconv.Atoi(r.PathValue("p"))
	q := r.URL.Query()
	offset, err2 := strconv.ParseUint(q.Get("offset"), 10, 64)
	if err := errors.Join(err1, err2); err != nil {
		fail(w, http.StatusBadRequest, "invalid partition or offset")
		return
	}
	limit := clamp(atoiOr(q.Get("max"), 1000), 1, maxFetch)
	wait := time.Duration(clamp(atoiOr(q.Get("wait_ms"), 0), 0, int(maxWait/time.Millisecond))) * time.Millisecond
	resp, err := h.b.Fetch(r.Context(), p, offset, limit, wait)
	if err != nil {
		failErr(w, err)
		return
	}
	writeJSON(w, resp)
}

func (h *handler) groupHeartbeat(w http.ResponseWriter, r *http.Request) {
	if group, m, ok := h.member(w, r); ok {
		writeJSON(w, h.b.GroupHeartbeat(group, m.Member))
	}
}

func (h *handler) groupLeave(w http.ResponseWriter, r *http.Request) {
	if group, m, ok := h.member(w, r); ok {
		h.b.GroupLeave(group, m.Member)
		w.WriteHeader(http.StatusNoContent)
	}
}

func (h *handler) commit(w http.ResponseWriter, r *http.Request) {
	group := r.PathValue("group")
	var req CommitRequest
	if !decode(w, r, &req) {
		return
	}
	if !validName.MatchString(group) || !validName.MatchString(req.Member) {
		fail(w, http.StatusBadRequest, "invalid group or member")
		return
	}
	if err := h.b.Commit(group, req); err != nil {
		failErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) streamerHeartbeat(w http.ResponseWriter, r *http.Request) {
	var m MemberRequest
	if decode(w, r, &m) && validMember(w, m.Member) {
		writeJSON(w, h.b.StreamerHeartbeat(m.Member))
	}
}

func (h *handler) streamerLeave(w http.ResponseWriter, r *http.Request) {
	var m MemberRequest
	if decode(w, r, &m) && validMember(w, m.Member) {
		h.b.StreamerLeave(m.Member)
		w.WriteHeader(http.StatusNoContent)
	}
}

// member decodes and validates the group path value and member body shared by group endpoints.
func (h *handler) member(w http.ResponseWriter, r *http.Request) (string, MemberRequest, bool) {
	var m MemberRequest
	group := r.PathValue("group")
	if !decode(w, r, &m) || !validMember(w, m.Member) {
		return "", m, false
	}
	if !validName.MatchString(group) {
		fail(w, http.StatusBadRequest, "invalid group")
		return "", m, false
	}
	return group, m, true
}

func validMember(w http.ResponseWriter, id string) bool {
	if !validName.MatchString(id) {
		fail(w, http.StatusBadRequest, "invalid member")
		return false
	}
	return true
}

// decode reads a size-limited JSON body, writing a 400 on failure.
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(v); err != nil {
		fail(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}

// failErr maps domain errors to HTTP status codes the client understands.
func failErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrBackpressure):
		fail(w, http.StatusTooManyRequests, err.Error())
	case errors.Is(err, ErrFenced):
		fail(w, http.StatusConflict, err.Error())
	case errors.Is(err, ErrNoPartition):
		fail(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrBadOffset):
		fail(w, http.StatusBadRequest, err.Error())
	default:
		slog.Error("broker request failed", "err", err)
		fail(w, http.StatusInternalServerError, "internal error")
	}
}

func fail(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func atoiOr(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}

func clamp(v, lo, hi int) int { return min(max(v, lo), hi) }

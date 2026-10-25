// Package platform holds process-level helpers shared by every binary:
// configuration, logging, HTTP serving and retries.
package platform

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

// Env returns the environment variable key, or def when it is unset or empty.
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// EnvInt parses an integer variable, falling back to def (with a warning) when invalid.
func EnvInt(key string, def int) int {
	return envParse(key, def, strconv.Atoi)
}

// EnvDuration parses a duration variable such as "500ms" or "2s".
func EnvDuration(key string, def time.Duration) time.Duration {
	return envParse(key, def, time.ParseDuration)
}

func envParse[T any](key string, def T, parse func(string) (T, error)) T {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	v, err := parse(raw)
	if err != nil {
		slog.Warn("invalid env value, using default", "key", key, "value", raw)
		return def
	}
	return v
}

// Logger installs a JSON logger tagged with the component name as the default.
func Logger(component string) *slog.Logger {
	l := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("component", component)
	slog.SetDefault(l)
	return l
}

// Fatal logs err and exits; used only during startup in main packages.
func Fatal(log *slog.Logger, msg string, err error) {
	log.Error(msg, "err", err)
	os.Exit(1)
}

// SignalContext is cancelled on SIGINT/SIGTERM so every component shuts down gracefully.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// InstanceID returns a member ID unique per process (pod name in Kubernetes).
func InstanceID() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

// NewServer returns an http.Server with timeouts that bound slow or idle clients.
func NewServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second, // above the broker's 30s long-poll cap
		IdleTimeout:       120 * time.Second,
	}
}

// Serve runs srv until ctx is cancelled, then drains in-flight requests.
func Serve(ctx context.Context, srv *http.Server, log *slog.Logger) error {
	errc := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", srv.Addr)
		errc <- srv.ListenAndServe()
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// permanentError marks a failure that retrying cannot fix, such as a rejected request.
type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent wraps err so Retry returns it immediately instead of retrying.
func Permanent(err error) error { return permanentError{err} }

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

// Retry calls fn with capped exponential backoff and jitter until it succeeds,
// fails permanently, or ctx ends.
func Retry(ctx context.Context, fn func() error) error {
	delay := 100 * time.Millisecond
	for {
		err := fn()
		if err == nil || ctx.Err() != nil || IsPermanent(err) {
			return err
		}
		slog.Warn("operation failed, retrying", "err", err, "in", delay)
		// Non-cryptographic jitter is fine: it only spreads retries over time.
		sleep := delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
		delay = min(delay*2, 5*time.Second)
	}
}

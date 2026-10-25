package store

import (
	"context"
	"testing"
	"time"
)

func TestConnectFailsFastOnMalformedDSN(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := Connect(ctx, "postgres", "postgres://%zz"); err == nil || ctx.Err() != nil {
		t.Fatalf("want an immediate parse error, got err=%v ctxErr=%v", err, ctx.Err())
	}
}

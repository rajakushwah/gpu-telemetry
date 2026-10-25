package platform

import (
	"context"
	"errors"
	"testing"
)

func TestRetry(t *testing.T) {
	calls := 0
	err := Retry(context.Background(), func() error {
		if calls++; calls < 3 {
			return errors.New("transient")
		}
		return Permanent(errors.New("bad request"))
	})
	if !IsPermanent(err) || calls != 3 {
		t.Fatalf("want 2 retries then a permanent stop, got %d calls, err %v", calls, err)
	}
}

package mq

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBrokerOffsetsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	open := func() *Broker {
		b, err := Open(Options{Dir: dir, Partitions: 1, Capacity: 16, SegmentBytes: 1 << 20, SessionTTL: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	b := open()
	if err := b.Publish([]Message{{Key: "k", Value: json.RawMessage(`1`)}}); err != nil {
		t.Fatal(err)
	}
	st := b.GroupHeartbeat("g", "m")
	if err := b.Commit("g", CommitRequest{Member: "m", Generation: st.Generation, Partition: 0, Offset: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b = open()
	defer b.Close()
	if got := b.GroupHeartbeat("g", "m").Offsets[0]; got != 1 {
		t.Fatalf("committed offset after restart = %d, want 1", got)
	}
}
